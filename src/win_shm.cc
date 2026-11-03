#include "duct/duct.h"

#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <new>
#include <random>
#include <string>
#include <string_view>
#include <vector>

#if defined(_WIN32)
#include <windows.h>
#else
// This file is Windows-specific, should not be compiled on non-Windows
#error "This file should only be compiled on Windows"
#endif

namespace duct {
namespace {

static std::string sanitize_name(std::string_view s) {
  std::string out;
  out.reserve(s.size());
  for (char c : s) {
    if ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_') {
      out.push_back(c);
    } else {
      out.push_back('_');
    }
  }
  if (out.empty()) out = "duct";
  return out;
}

static std::string random_conn_id_hex16() {
  std::random_device rd;
  std::uint64_t v = (static_cast<std::uint64_t>(rd()) << 32) ^ static_cast<std::uint64_t>(rd());
  char buf[17];
  std::snprintf(buf, sizeof(buf), "%016llx", static_cast<unsigned long long>(v));
  return std::string(buf);
}

static std::uint32_t fnv1a_32(std::string_view s) {
  std::uint32_t h = 2166136261u;
  for (unsigned char c : s) {
    h ^= c;
    h *= 16777619u;
  }
  return h;
}

static std::string hex8(std::uint32_t v) {
  char buf[9];
  std::snprintf(buf, sizeof(buf), "%08x", v);
  return std::string(buf);
}

// Convert to Windows compatible names (use session namespace to avoid admin rights)
static std::string make_win_name(const std::string& prefix, const std::string& suffix) {
  return "Duct_" + prefix + "_" + suffix;
}

struct alignas(64) RingMeta {
  std::atomic_uint32_t head{0};  // producer increments
  std::atomic_uint32_t tail{0};  // consumer increments
};

struct ShmLayout {
  std::uint32_t slot_bytes = 0;
  std::uint32_t slot_count = 0;
  RingMeta c2s_meta;
  RingMeta s2c_meta;
  // Slot data follows, same layout as the POSIX implementation.
};

static bool geometry_valid(std::uint32_t slot_bytes, std::uint32_t slot_count) {
  return slot_bytes != 0 && slot_bytes <= (1u << 30) && slot_count != 0;
}

static std::size_t layout_size(std::uint32_t slot_bytes, std::uint32_t slot_count) {
  const std::size_t slot_stride = (static_cast<std::size_t>(slot_bytes) + 8u + 7u) & ~std::size_t(7u);
  return sizeof(ShmLayout) + 2u * static_cast<std::size_t>(slot_count) * slot_stride;
}

static std::uint8_t* slot_data(ShmLayout* l, bool c2s, std::uint32_t idx, std::uint32_t slot_bytes) {
  const std::size_t slot_stride = (static_cast<std::size_t>(slot_bytes) + 8u + 7u) & ~std::size_t(7u);
  std::size_t base = sizeof(ShmLayout);
  if (!c2s) base += static_cast<std::size_t>(l->slot_count) * slot_stride;
  return reinterpret_cast<std::uint8_t*>(l) + base + static_cast<std::size_t>(idx) * slot_stride;
}

static std::uint32_t* slot_len(ShmLayout* l, bool c2s, std::uint32_t idx, std::uint32_t slot_bytes) {
  return reinterpret_cast<std::uint32_t*>(slot_data(l, c2s, idx, slot_bytes));
}

static std::uint8_t* slot_payload(ShmLayout* l, bool c2s, std::uint32_t idx, std::uint32_t slot_bytes) {
  return slot_data(l, c2s, idx, slot_bytes) + 8u;
}

static RingMeta* ring_meta(ShmLayout* l, bool c2s) {
  return c2s ? &l->c2s_meta : &l->s2c_meta;
}

struct ShmNames {
  std::string base;  // already sanitized
  std::string connid;
  std::string shm;            // CreateFileMapping name
  std::string c2s_items;      // Event name for items available
  std::string c2s_spaces;     // Event name for space available
  std::string s2c_items;      // Event name for items available
  std::string s2c_spaces;     // Event name for space available
  std::string bootstrap_pipe; // Named pipe path
};

static ShmNames make_names(std::string_view bus_name, std::string connid) {
  ShmNames n;
  n.base = sanitize_name(bus_name);
  n.connid = std::move(connid);

  std::string hash8 = hex8(fnv1a_32(n.base));
  std::string conn8 = n.connid.substr(0, 8);
  std::string prefix = "d" + hash8 + conn8;

  n.shm = make_win_name(prefix, "shm");
  n.c2s_items = make_win_name(prefix, "c2i");
  n.c2s_spaces = make_win_name(prefix, "c2s");
  n.s2c_items = make_win_name(prefix, "s2i");
  n.s2c_spaces = make_win_name(prefix, "s2s");

  // Bootstrap rendezvous via named pipe
  n.bootstrap_pipe = R"(\\.\pipe\duct_shm_)" + hash8;

  return n;
}

// Security attributes for global namespace objects
static SECURITY_ATTRIBUTES* get_global_security_attributes() {
  static SECURITY_DESCRIPTOR sd;
  static SECURITY_ATTRIBUTES sa;

  // Initialize security descriptor
  if (!InitializeSecurityDescriptor(&sd, SECURITY_DESCRIPTOR_REVISION)) {
    return nullptr;
  }

  // Set DACL to allow everyone
  if (!SetSecurityDescriptorDacl(&sd, TRUE, NULL, FALSE)) {
    return nullptr;
  }

  sa.nLength = sizeof(SECURITY_ATTRIBUTES);
  sa.lpSecurityDescriptor = &sd;
  sa.bInheritHandle = FALSE;

  return &sa;
}

struct ShmHandles {
  HANDLE shm_handle = INVALID_HANDLE_VALUE;
  ShmLayout* mem = nullptr;
  std::uint32_t slot_bytes = 0;
  std::uint32_t slot_count = 0;
  HANDLE c2s_items = INVALID_HANDLE_VALUE;
  HANDLE c2s_spaces = INVALID_HANDLE_VALUE;
  HANDLE s2c_items = INVALID_HANDLE_VALUE;
  HANDLE s2c_spaces = INVALID_HANDLE_VALUE;
};

static void close_handles(ShmHandles* h) {
  if (!h) return;

  if (h->mem && h->shm_handle != INVALID_HANDLE_VALUE) {
    UnmapViewOfFile(h->mem);
    h->mem = nullptr;
  }
  if (h->shm_handle != INVALID_HANDLE_VALUE) {
    CloseHandle(h->shm_handle);
    h->shm_handle = INVALID_HANDLE_VALUE;
  }
  if (h->c2s_items != INVALID_HANDLE_VALUE) {
    CloseHandle(h->c2s_items);
    h->c2s_items = INVALID_HANDLE_VALUE;
  }
  if (h->c2s_spaces != INVALID_HANDLE_VALUE) {
    CloseHandle(h->c2s_spaces);
    h->c2s_spaces = INVALID_HANDLE_VALUE;
  }
  if (h->s2c_items != INVALID_HANDLE_VALUE) {
    CloseHandle(h->s2c_items);
    h->s2c_items = INVALID_HANDLE_VALUE;
  }
  if (h->s2c_spaces != INVALID_HANDLE_VALUE) {
    CloseHandle(h->s2c_spaces);
    h->s2c_spaces = INVALID_HANDLE_VALUE;
  }
}

static Result<void> wait_handle_opt(HANDLE handle, std::chrono::milliseconds timeout) {
  // Wait on a Win32 sync primitive (semaphore/event) with optional timeout.
  // 等待 Win32 同步对象（信号量/Event），支持可选超时。
  DWORD result;
  if (timeout.count() == 0) {
    result = WaitForSingleObject(handle, INFINITE);
  } else {
    result = WaitForSingleObject(handle, static_cast<DWORD>(timeout.count()));
  }

  switch (result) {
    case WAIT_OBJECT_0:
      return {};
    case WAIT_TIMEOUT:
      return Status::timeout("wait timeout");
    default:
      return Status::io_error("WaitForSingleObject failed");
  }
}

static Result<ShmHandles> create_resources(const ShmNames& n, const ShmOptions& opt) {
  ShmHandles h;
  if (!geometry_valid(static_cast<std::uint32_t>(opt.slot_bytes), opt.slot_count)) {
    return Status::invalid_argument("invalid shm geometry (slot_bytes/slot_count)");
  }
  const std::size_t size = layout_size(static_cast<std::uint32_t>(opt.slot_bytes), opt.slot_count);
  h.slot_bytes = static_cast<std::uint32_t>(opt.slot_bytes);
  h.slot_count = opt.slot_count;

  // Create shared memory with default security (current user session)
  h.shm_handle = CreateFileMappingA(
    INVALID_HANDLE_VALUE,
    NULL,  // Use default security attributes
    PAGE_READWRITE,
    0,
    static_cast<DWORD>(size),
    n.shm.c_str()
  );
  if (h.shm_handle == NULL) {
    DWORD error = GetLastError();
    return Status::io_error("CreateFileMapping failed with error: " + std::to_string(error));
  }

  h.mem = static_cast<ShmLayout*>(MapViewOfFile(
    h.shm_handle,
    FILE_MAP_ALL_ACCESS,
    0, 0,
    size
  ));
  if (h.mem == nullptr) {
    close_handles(&h);
    return Status::io_error("MapViewOfFile failed");
  }

  // Construct the layout in place instead of memset: RingMeta holds atomics,
  // and byte-wise clearing of an atomic object is undefined behavior.
  ::new (static_cast<void*>(h.mem)) ShmLayout{};
  h.mem->slot_bytes = h.slot_bytes;
  h.mem->slot_count = h.slot_count;

  // IMPORTANT: use counting semaphores (not Events). Auto-reset Events behave like capacity=1 and
  // break ring-buffer backpressure. Semaphores correctly model item/space counts.
  // 重要：这里必须用计数信号量（而不是 Event）。自动重置 Event 本质是 0/1，等价于容量=1，会破坏环形队列的背压语义。
  // Named semaphores mirror POSIX semaphores:
  // - items starts at 0 (ring empty)
  // - spaces starts at kSlotCount (ring has capacity)
  // 命名信号量语义与 POSIX sem_t 对齐：items 初始为 0（空），spaces 初始为 kSlotCount（满容量）。
  const LONG max_count = static_cast<LONG>(h.slot_count);
  h.c2s_items = CreateSemaphoreA(NULL, /*lInitialCount=*/0, max_count, n.c2s_items.c_str());
  h.c2s_spaces = CreateSemaphoreA(NULL, /*lInitialCount=*/max_count, max_count, n.c2s_spaces.c_str());
  h.s2c_items = CreateSemaphoreA(NULL, /*lInitialCount=*/0, max_count, n.s2c_items.c_str());
  h.s2c_spaces = CreateSemaphoreA(NULL, /*lInitialCount=*/max_count, max_count, n.s2c_spaces.c_str());

  if (h.c2s_items == NULL || h.c2s_spaces == NULL ||
      h.s2c_items == NULL || h.s2c_spaces == NULL) {
    DWORD error = GetLastError();
    close_handles(&h);
    return Status::io_error("CreateSemaphore failed with error: " + std::to_string(error));
  }

  return h;
}

static Result<ShmHandles> open_resources(const ShmNames& n, const ShmOptions& expected) {
  ShmHandles h;

  // Open shared memory
  h.shm_handle = OpenFileMappingA(FILE_MAP_ALL_ACCESS, FALSE, n.shm.c_str());
  if (h.shm_handle == NULL) {
    DWORD error = GetLastError();
    return Status::io_error("OpenFileMapping failed with error: " + std::to_string(error));
  }

  h.mem = static_cast<ShmLayout*>(MapViewOfFile(
    h.shm_handle,
    FILE_MAP_ALL_ACCESS,
    0, 0,
    layout_size(static_cast<std::uint32_t>(expected.slot_bytes), expected.slot_count)
  ));
  if (h.mem == nullptr) {
    close_handles(&h);
    return Status::io_error("MapViewOfFile failed");
  }

  h.slot_bytes = static_cast<std::uint32_t>(expected.slot_bytes);
  h.slot_count = expected.slot_count;
  if (h.mem->slot_bytes != h.slot_bytes || h.mem->slot_count != h.slot_count) {
    close_handles(&h);
    return Status::protocol_error("shm geometry mismatch");
  }

  // Open semaphores. Need SYNCHRONIZE for waits and SEMAPHORE_MODIFY_STATE for release.
  // 打开信号量：等待需要 SYNCHRONIZE；ReleaseSemaphore 需要 SEMAPHORE_MODIFY_STATE。
  DWORD access = SYNCHRONIZE | SEMAPHORE_MODIFY_STATE;
  h.c2s_items = OpenSemaphoreA(access, FALSE, n.c2s_items.c_str());
  h.c2s_spaces = OpenSemaphoreA(access, FALSE, n.c2s_spaces.c_str());
  h.s2c_items = OpenSemaphoreA(access, FALSE, n.s2c_items.c_str());
  h.s2c_spaces = OpenSemaphoreA(access, FALSE, n.s2c_spaces.c_str());

  if (h.c2s_items == NULL || h.c2s_spaces == NULL ||
      h.s2c_items == NULL || h.s2c_spaces == NULL) {
    DWORD error = GetLastError();
    close_handles(&h);
    return Status::io_error("OpenSemaphore failed with error: " + std::to_string(error));
  }

  return h;
}

// Bootstrap rendezvous uses a named pipe:
// client sends 16-byte connid, server opens the corresponding shared resources.
// 启动握手使用命名管道：客户端发送 16 字节 connid，服务端据此打开对应共享资源。
static Result<HANDLE> create_bootstrap_pipe(const std::string& path) {
  HANDLE pipe = CreateNamedPipeA(
      path.c_str(),
      PIPE_ACCESS_DUPLEX,
      PIPE_TYPE_MESSAGE | PIPE_READMODE_MESSAGE | PIPE_WAIT,
      1,    // Max instances
      512,  // Out buffer size
      512,  // In buffer size
      0,    // Default timeout
      get_global_security_attributes());

  if (pipe == INVALID_HANDLE_VALUE) {
    DWORD error = GetLastError();
    return Status::io_error("CreateNamedPipe failed with error: " + std::to_string(error));
  }
  return pipe;
}

class ShmPipe final : public Pipe {
 public:
  ShmPipe(ShmHandles h, ShmNames n, bool owner, bool is_client)
      : h_(h), names_(std::move(n)), owner_(owner), is_client_(is_client) {}

  ~ShmPipe() override { close(); }

  Result<void> send(const Message& msg, const SendOptions& opt) override {
    if (!h_.mem) return Status::closed("pipe closed");

    const bool tx_ring = is_client_;
    HANDLE spaces = is_client_ ? h_.c2s_spaces : h_.s2c_spaces;
    HANDLE items = is_client_ ? h_.c2s_items : h_.s2c_items;
    RingMeta* tx = ring_meta(h_.mem, tx_ring);
    const std::uint32_t slot_bytes = h_.slot_bytes;
    const std::uint32_t slot_count = h_.slot_count;

    auto st = wait_handle_opt(spaces, opt.timeout);
    if (!st.ok()) return st;

    std::uint32_t head = tx->head.load(std::memory_order_relaxed);
    std::uint32_t idx = head % slot_count;
    *slot_len(h_.mem, tx_ring, idx, slot_bytes) = static_cast<std::uint32_t>(msg.size());
    if (msg.size() != 0) {
      std::memcpy(slot_payload(h_.mem, tx_ring, idx, slot_bytes), msg.data(), msg.size());
    }
    tx->head.store(head + 1, std::memory_order_release);

    if (!ReleaseSemaphore(items, /*lReleaseCount=*/1, NULL)) {
      DWORD error = GetLastError();
      return Status::io_error("ReleaseSemaphore(items) failed with error: " + std::to_string(error));
    }
    return {};
  }

  Result<Message> recv(const RecvOptions& opt) override {
    if (!h_.mem) return Status::closed("pipe closed");

    const bool rx_ring = !is_client_;
    HANDLE items = is_client_ ? h_.s2c_items : h_.c2s_items;
    HANDLE spaces = is_client_ ? h_.s2c_spaces : h_.c2s_spaces;
    RingMeta* rx = ring_meta(h_.mem, rx_ring);
    const std::uint32_t slot_bytes = h_.slot_bytes;
    const std::uint32_t slot_count = h_.slot_count;

    auto st = wait_handle_opt(items, opt.timeout);
    if (!st.ok()) return st.status();

    std::uint32_t tail = rx->tail.load(std::memory_order_relaxed);
    std::uint32_t idx = tail % slot_count;
    std::uint32_t len = *slot_len(h_.mem, rx_ring, idx, slot_bytes);
    if (len > slot_bytes) {
      return Status::protocol_error("shm slot len too large");
    }

    Message m = Message::from_bytes(slot_payload(h_.mem, rx_ring, idx, slot_bytes), len);
    rx->tail.store(tail + 1, std::memory_order_release);

    if (!ReleaseSemaphore(spaces, /*lReleaseCount=*/1, NULL)) {
      DWORD error = GetLastError();
      return Status::io_error("ReleaseSemaphore(spaces) failed with error: " + std::to_string(error));
    }
    return m;
  }

  void close() override {
    if (!h_.mem && h_.shm_handle == INVALID_HANDLE_VALUE) return;
    close_handles(&h_);

    if (owner_) {
      // Clean up named objects
      // Note: Windows automatically cleans up when all handles are closed
    }
  }

 private:
  ShmHandles h_{};
  ShmNames names_{};
  bool owner_ = false;
  bool is_client_ = false;
};

class ShmListener final : public Listener {
 public:
  ShmListener(ShmNames names, HANDLE bootstrap_pipe, ShmOptions expected)
      : names_(std::move(names)), bootstrap_pipe_(bootstrap_pipe), expected_(expected) {}
  ~ShmListener() override { close(); }

  Result<std::unique_ptr<Pipe>> accept() override {
    if (bootstrap_pipe_ == INVALID_HANDLE_VALUE) {
      return Status::closed("listener closed");
    }

    // Wait for client connection.
    // 等待客户端连接。
    HANDLE pipe = bootstrap_pipe_;
    BOOL connected = ConnectNamedPipe(pipe, NULL);
    if (!connected && GetLastError() != ERROR_PIPE_CONNECTED) {
      CloseHandle(pipe);
      bootstrap_pipe_ = INVALID_HANDLE_VALUE;
      auto next = create_bootstrap_pipe(names_.bootstrap_pipe);
      if (next.ok()) {
        bootstrap_pipe_ = next.value();
      }
      return Status::io_error("ConnectNamedPipe failed");
    }

    // Read connection ID from client.
    // 读取客户端发送的 connid。
    char connid[16];
    DWORD bytes_read;
    BOOL read_ok = ReadFile(pipe, connid, sizeof(connid), &bytes_read, NULL);
    CloseHandle(pipe);
    bootstrap_pipe_ = INVALID_HANDLE_VALUE;

    // Recreate bootstrap pipe instance for subsequent accepts.
    // 为后续 accept() 重新创建管道实例（本实现一次只接受一个并发连接）。
    auto next = create_bootstrap_pipe(names_.bootstrap_pipe);
    if (next.ok()) {
      bootstrap_pipe_ = next.value();
    }

    if (!read_ok || bytes_read != sizeof(connid)) {
      return Status::io_error("Failed to read connection ID");
    }

    ShmNames n = make_names(names_.base, std::string(connid, sizeof(connid)));
    auto h = open_resources(n, expected_);
    if (!h.ok()) return h.status();

    return std::unique_ptr<Pipe>(new ShmPipe(h.value(), std::move(n),
                                           /*owner=*/false, /*is_client=*/false));
  }

  Result<std::string> local_address() const override {
    return std::string("shm://") + names_.base;
  }

  void close() override {
    if (bootstrap_pipe_ != INVALID_HANDLE_VALUE) {
      CloseHandle(bootstrap_pipe_);
      bootstrap_pipe_ = INVALID_HANDLE_VALUE;
    }
  }

 private:
  ShmNames names_;
  HANDLE bootstrap_pipe_ = INVALID_HANDLE_VALUE;
  ShmOptions expected_{};
};

}  // namespace

Result<std::unique_ptr<Listener>> shm_listen(const std::string& name, const ListenOptions& opt) {
  if (!geometry_valid(static_cast<std::uint32_t>(opt.shm.slot_bytes), opt.shm.slot_count)) {
    return Status::invalid_argument("invalid shm geometry (slot_bytes/slot_count)");
  }
  ShmNames n = make_names(name, "0000000000000000");
  auto pipe = create_bootstrap_pipe(n.bootstrap_pipe);
  if (!pipe.ok()) return pipe.status();
  return std::unique_ptr<Listener>(new ShmListener(std::move(n), pipe.value(), opt.shm));
}

Result<std::unique_ptr<Pipe>> shm_dial(const std::string& name, const DialOptions& opt) {
  if (opt.timeout.count() < 0) {
    return Status::invalid_argument("dial timeout must not be negative");
  }
  std::string connid = random_conn_id_hex16();
  ShmNames n = make_names(name, connid);

  auto created = create_resources(n, opt.shm);
  if (!created.ok()) return created.status();

  // Connect to bootstrap pipe with retry + timeout:
  // - ERROR_FILE_NOT_FOUND: server not listening yet
  // - ERROR_PIPE_BUSY: server is busy; wait for an instance
  // 带超时重试连接启动管道：
  // - ERROR_FILE_NOT_FOUND：服务端尚未开始监听
  // - ERROR_PIPE_BUSY：服务端繁忙，等待可用实例
  HANDLE pipe = INVALID_HANDLE_VALUE;
  constexpr DWORD kDefaultDialTimeoutMs = 5000;
  DWORD timeout_ms = opt.timeout.count() == 0 ? kDefaultDialTimeoutMs : static_cast<DWORD>(opt.timeout.count());
  ULONGLONG start = GetTickCount64();
  for (;;) {
    pipe = CreateFileA(
      n.bootstrap_pipe.c_str(),
      GENERIC_READ | GENERIC_WRITE,
      0,
      NULL,
      OPEN_EXISTING,
      FILE_ATTRIBUTE_NORMAL,
      NULL
    );
    if (pipe != INVALID_HANDLE_VALUE) break;

    DWORD error = GetLastError();
    ULONGLONG elapsed = GetTickCount64() - start;
    if (elapsed >= timeout_ms) {
      close_handles(&created.value());
      return Status::timeout("timeout connecting to shm bootstrap pipe");
    }

    if (error == ERROR_PIPE_BUSY) {
      DWORD remaining = timeout_ms - static_cast<DWORD>(elapsed);
      if (!WaitNamedPipeA(n.bootstrap_pipe.c_str(), remaining)) {
        close_handles(&created.value());
        return Status::timeout("timeout connecting to shm bootstrap pipe");
      }
      continue;
    }
    if (error == ERROR_FILE_NOT_FOUND) {
      Sleep(1);
      continue;
    }

    close_handles(&created.value());
    return Status::io_error("Failed to connect to named pipe with error: " + std::to_string(error));
  }

  // Send connection ID to server
  DWORD bytes_written;
  BOOL write_ok = WriteFile(pipe, connid.data(), static_cast<DWORD>(connid.size()),
                           &bytes_written, NULL);
  CloseHandle(pipe);

  if (!write_ok || bytes_written != connid.size()) {
    close_handles(&created.value());
    return Status::io_error("Failed to send connection ID");
  }

  return std::unique_ptr<Pipe>(new ShmPipe(created.value(), std::move(n),
                                          /*owner=*/true, /*is_client=*/true));
}

}  // namespace duct
