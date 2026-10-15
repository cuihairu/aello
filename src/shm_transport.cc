#include "duct/duct.h"

#if defined(_WIN32)
// Windows implementation is in win_shm.cc
// Windows shared memory functions are declared here but implemented in win_shm.cc
namespace duct {

// Forward declarations for Windows shared memory functions (implemented in win_shm.cc)
Result<std::unique_ptr<Listener>> shm_listen(const std::string& name, const ListenOptions& opt);
Result<std::unique_ptr<Pipe>> shm_dial(const std::string& name, const DialOptions& opt);

}  // namespace duct

#else
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <random>
#include <string>
#include <string_view>
#include <vector>

#include <fcntl.h>
#include <semaphore.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/un.h>
#include <time.h>
#include <unistd.h>

namespace duct {
namespace {

#if !defined(_WIN32)
constexpr std::size_t kSlotPayloadMax = 64 * 1024;
constexpr std::uint32_t kSlotCount = 64;  // 64 * 64KB = 4MB per ring

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

static std::string errno_suffix() {
  int e = errno;
  return std::string(" (errno=") + std::to_string(e) + " " + std::strerror(e) + ")";
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

#if !defined(__APPLE__)
static timespec to_abs_timespec(std::chrono::milliseconds dur) {
  using namespace std::chrono;
  timespec ts{};
#if defined(CLOCK_REALTIME)
  ::clock_gettime(CLOCK_REALTIME, &ts);
#else
  timeval tv{};
  ::gettimeofday(&tv, nullptr);
  ts.tv_sec = tv.tv_sec;
  ts.tv_nsec = tv.tv_usec * 1000;
#endif
  auto ns = duration_cast<nanoseconds>(dur).count();
  ts.tv_sec += static_cast<time_t>(ns / 1000000000LL);
  ts.tv_nsec += static_cast<long>(ns % 1000000000LL);
  if (ts.tv_nsec >= 1000000000L) {
    ts.tv_sec += 1;
    ts.tv_nsec -= 1000000000L;
  }
  return ts;
}
#endif

static Result<void> sem_wait_opt(sem_t* sem, std::chrono::milliseconds timeout) {
  if (timeout.count() == 0) {
    while (::sem_wait(sem) != 0) {
      if (errno == EINTR) continue;
      return Status::io_error("sem_wait failed");
    }
    return {};
  }

  // macOS historically does not provide sem_timedwait; fall back to trywait loop.
#if defined(__APPLE__)
  auto deadline = std::chrono::steady_clock::now() + timeout;
  for (;;) {
    if (::sem_trywait(sem) == 0) return {};
    if (errno == EINTR) continue;
    if (errno != EAGAIN) return Status::io_error("sem_trywait failed");

    if (std::chrono::steady_clock::now() >= deadline) {
      return Status::timeout("semaphore wait timeout");
    }
    timespec nap{};
    nap.tv_sec = 0;
    nap.tv_nsec = 1000 * 1000;  // 1ms
    (void)::nanosleep(&nap, nullptr);
  }
#else
  timespec ts = to_abs_timespec(timeout);
  for (;;) {
    if (::sem_timedwait(sem, &ts) == 0) return {};
    if (errno == EINTR) continue;
    if (errno == ETIMEDOUT) return Status::timeout("sem_timedwait timeout");
    return Status::io_error("sem_timedwait failed");
  }
#endif
}

struct alignas(64) RingMeta {
  std::atomic_uint32_t head{0};  // producer increments
  std::atomic_uint32_t tail{0};  // consumer increments
};

struct Slot {
  std::uint32_t len = 0;
  std::uint32_t _pad = 0;
  std::uint8_t data[kSlotPayloadMax];
};

struct Ring {
  RingMeta meta;
  Slot slots[kSlotCount];
};

struct ShmLayout {
  Ring c2s;
  Ring s2c;
};

static constexpr std::size_t kShmSize = sizeof(ShmLayout);

struct ShmNames {
  std::string base;  // already sanitized
  std::string connid;
  std::string shm;            // shm_open name (must start with '/')
  std::string c2s_items_sem;  // sem_open names (must start with '/')
  std::string c2s_spaces_sem;
  std::string s2c_items_sem;
  std::string s2c_spaces_sem;
  std::string bootstrap_path;  // filesystem path for AF_UNIX
};

static ShmNames make_names(std::string_view bus_name, std::string connid) {
  ShmNames n;
  n.base = sanitize_name(bus_name);
  n.connid = std::move(connid);

  // macOS/POSIX have tight limits on shm/sem name length. Keep identifiers short:
  // - hash8: stable per bus name
  // - conn8: random per connection (32-bit entropy; good enough for local IPC)
  std::string hash8 = hex8(fnv1a_32(n.base));
  std::string conn8 = n.connid.substr(0, 8);
  std::string prefix = "d" + hash8 + conn8;

  n.shm = "/" + prefix + "m";
  n.c2s_items_sem = "/" + prefix + "a";
  n.c2s_spaces_sem = "/" + prefix + "b";
  n.s2c_items_sem = "/" + prefix + "c";
  n.s2c_spaces_sem = "/" + prefix + "d";

  // Bootstrap rendezvous is via a filesystem unix socket path.
  n.bootstrap_path = "/tmp/duct_shm_" + hash8 + ".sock";
  return n;
}

static Result<int> uds_listen(std::string_view path, int backlog) {
  int fd = ::socket(AF_UNIX, SOCK_STREAM, 0);
  if (fd < 0) return Status::io_error("socket(AF_UNIX) failed" + errno_suffix());

  sockaddr_un addr{};
  addr.sun_family = AF_UNIX;
  if (path.size() >= sizeof(addr.sun_path)) {
    ::close(fd);
    return Status::invalid_argument("uds path too long");
  }
  std::snprintf(addr.sun_path, sizeof(addr.sun_path), "%s", std::string(path).c_str());

  ::unlink(addr.sun_path);
  if (::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
    ::close(fd);
    return Status::io_error("bind(AF_UNIX) failed" + errno_suffix());
  }
  if (::listen(fd, backlog) != 0) {
    ::close(fd);
    return Status::io_error("listen(AF_UNIX) failed" + errno_suffix());
  }
  return fd;
}

static Result<int> uds_connect(std::string_view path) {
  int fd = ::socket(AF_UNIX, SOCK_STREAM, 0);
  if (fd < 0) return Status::io_error("socket(AF_UNIX) failed" + errno_suffix());

  sockaddr_un addr{};
  addr.sun_family = AF_UNIX;
  if (path.size() >= sizeof(addr.sun_path)) {
    ::close(fd);
    return Status::invalid_argument("uds path too long");
  }
  std::snprintf(addr.sun_path, sizeof(addr.sun_path), "%s", std::string(path).c_str());

  if (::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
    ::close(fd);
    return Status::io_error("connect(AF_UNIX) failed" + errno_suffix());
  }
  return fd;
}

static Result<void> write_all(int fd, const void* p, std::size_t n) {
  const std::uint8_t* cur = static_cast<const std::uint8_t*>(p);
  while (n != 0) {
    ssize_t w = ::send(fd, cur, n, 0);
    if (w < 0) {
      if (errno == EINTR) continue;
      return Status::io_error("send() failed" + errno_suffix());
    }
    if (w == 0) return Status::closed("peer closed");
    cur += static_cast<std::size_t>(w);
    n -= static_cast<std::size_t>(w);
  }
  return {};
}

static Result<void> read_exact(int fd, void* p, std::size_t n) {
  std::uint8_t* cur = static_cast<std::uint8_t*>(p);
  while (n != 0) {
    ssize_t r = ::recv(fd, cur, n, 0);
    if (r < 0) {
      if (errno == EINTR) continue;
      return Status::io_error("recv() failed" + errno_suffix());
    }
    if (r == 0) return Status::closed("peer closed");
    cur += static_cast<std::size_t>(r);
    n -= static_cast<std::size_t>(r);
  }
  return {};
}

struct ShmHandles {
  int shm_fd = -1;
  ShmLayout* mem = nullptr;
  sem_t* c2s_items = SEM_FAILED;
  sem_t* c2s_spaces = SEM_FAILED;
  sem_t* s2c_items = SEM_FAILED;
  sem_t* s2c_spaces = SEM_FAILED;
};

static void close_handles(ShmHandles* h) {
  if (!h) return;
  if (h->mem) {
    ::munmap(h->mem, kShmSize);
    h->mem = nullptr;
  }
  if (h->shm_fd >= 0) {
    ::close(h->shm_fd);
    h->shm_fd = -1;
  }
  if (h->c2s_items != SEM_FAILED) ::sem_close(h->c2s_items);
  if (h->c2s_spaces != SEM_FAILED) ::sem_close(h->c2s_spaces);
  if (h->s2c_items != SEM_FAILED) ::sem_close(h->s2c_items);
  if (h->s2c_spaces != SEM_FAILED) ::sem_close(h->s2c_spaces);
  h->c2s_items = SEM_FAILED;
  h->c2s_spaces = SEM_FAILED;
  h->s2c_items = SEM_FAILED;
  h->s2c_spaces = SEM_FAILED;
}

static Result<ShmHandles> create_resources(const ShmNames& n) {
  ShmHandles h;

  h.shm_fd = ::shm_open(n.shm.c_str(), O_CREAT | O_EXCL | O_RDWR, 0600);
  if (h.shm_fd < 0) {
    return Status::io_error("shm_open(create) failed: " + n.shm + errno_suffix());
  }
  if (::ftruncate(h.shm_fd, static_cast<off_t>(kShmSize)) != 0) {
    close_handles(&h);
    ::shm_unlink(n.shm.c_str());
    return Status::io_error("ftruncate(shm) failed" + errno_suffix());
  }
  void* p = ::mmap(nullptr, kShmSize, PROT_READ | PROT_WRITE, MAP_SHARED, h.shm_fd, 0);
  if (p == MAP_FAILED) {
    close_handles(&h);
    ::shm_unlink(n.shm.c_str());
    return Status::io_error("mmap(shm) failed" + errno_suffix());
  }
  h.mem = static_cast<ShmLayout*>(p);
  std::memset(h.mem, 0, kShmSize);

  // Semaphores:
  // - items start at 0
  // - spaces start at slot count
  h.c2s_items = ::sem_open(n.c2s_items_sem.c_str(), O_CREAT | O_EXCL, 0600, 0);
  h.c2s_spaces = ::sem_open(n.c2s_spaces_sem.c_str(), O_CREAT | O_EXCL, 0600, kSlotCount);
  h.s2c_items = ::sem_open(n.s2c_items_sem.c_str(), O_CREAT | O_EXCL, 0600, 0);
  h.s2c_spaces = ::sem_open(n.s2c_spaces_sem.c_str(), O_CREAT | O_EXCL, 0600, kSlotCount);

  if (h.c2s_items == SEM_FAILED || h.c2s_spaces == SEM_FAILED || h.s2c_items == SEM_FAILED ||
      h.s2c_spaces == SEM_FAILED) {
    close_handles(&h);
    ::sem_unlink(n.c2s_items_sem.c_str());
    ::sem_unlink(n.c2s_spaces_sem.c_str());
    ::sem_unlink(n.s2c_items_sem.c_str());
    ::sem_unlink(n.s2c_spaces_sem.c_str());
    ::shm_unlink(n.shm.c_str());
    return Status::io_error("sem_open(create) failed" + errno_suffix());
  }

  return h;
}

static Result<ShmHandles> open_resources(const ShmNames& n) {
  ShmHandles h;
  h.shm_fd = ::shm_open(n.shm.c_str(), O_RDWR, 0600);
  if (h.shm_fd < 0) {
    return Status::io_error("shm_open(open) failed: " + n.shm + errno_suffix());
  }
  void* p = ::mmap(nullptr, kShmSize, PROT_READ | PROT_WRITE, MAP_SHARED, h.shm_fd, 0);
  if (p == MAP_FAILED) {
    close_handles(&h);
    return Status::io_error("mmap(shm) failed" + errno_suffix());
  }
  h.mem = static_cast<ShmLayout*>(p);

  h.c2s_items = ::sem_open(n.c2s_items_sem.c_str(), 0);
  h.c2s_spaces = ::sem_open(n.c2s_spaces_sem.c_str(), 0);
  h.s2c_items = ::sem_open(n.s2c_items_sem.c_str(), 0);
  h.s2c_spaces = ::sem_open(n.s2c_spaces_sem.c_str(), 0);

  if (h.c2s_items == SEM_FAILED || h.c2s_spaces == SEM_FAILED || h.s2c_items == SEM_FAILED ||
      h.s2c_spaces == SEM_FAILED) {
    close_handles(&h);
    return Status::io_error("sem_open(open) failed" + errno_suffix());
  }

  return h;
}

class ShmPipe final : public Pipe {
 public:
  // is_client determines which ring is TX vs RX.
  ShmPipe(ShmHandles h, ShmNames n, bool owner, bool is_client)
      : h_(h), names_(std::move(n)), owner_(owner), is_client_(is_client) {}

  ~ShmPipe() override { close(); }

  Result<void> send(const Message& msg, const SendOptions& opt) override {
    if (!h_.mem) return Status::closed("pipe closed");
    if (msg.size() > kSlotPayloadMax) {
      return Status::invalid_argument("message too large; fragmentation TODO");
    }

    Ring* tx = is_client_ ? &h_.mem->c2s : &h_.mem->s2c;
    sem_t* spaces = is_client_ ? h_.c2s_spaces : h_.s2c_spaces;
    sem_t* items = is_client_ ? h_.c2s_items : h_.s2c_items;

    auto st = sem_wait_opt(spaces, opt.timeout);
    if (!st.ok()) return st;

    std::uint32_t head = tx->meta.head.load(std::memory_order_relaxed);
    std::uint32_t idx = head % kSlotCount;
    tx->slots[idx].len = static_cast<std::uint32_t>(msg.size());
    if (msg.size() != 0) {
      std::memcpy(tx->slots[idx].data, msg.data(), msg.size());
    }
    tx->meta.head.store(head + 1, std::memory_order_release);
    ::sem_post(items);
    return {};
  }

  Result<Message> recv(const RecvOptions& opt) override {
    if (!h_.mem) return Status::closed("pipe closed");

    Ring* rx = is_client_ ? &h_.mem->s2c : &h_.mem->c2s;
    sem_t* items = is_client_ ? h_.s2c_items : h_.c2s_items;
    sem_t* spaces = is_client_ ? h_.s2c_spaces : h_.c2s_spaces;

    auto st = sem_wait_opt(items, opt.timeout);
    if (!st.ok()) return st.status();

    std::uint32_t tail = rx->meta.tail.load(std::memory_order_relaxed);
    std::uint32_t idx = tail % kSlotCount;
    std::uint32_t len = rx->slots[idx].len;
    if (len > kSlotPayloadMax) {
      return Status::protocol_error("shm slot len too large");
    }

    Message m = Message::from_bytes(rx->slots[idx].data, len);
    rx->meta.tail.store(tail + 1, std::memory_order_release);
    ::sem_post(spaces);
    return m;
  }

  void close() override {
    if (!h_.mem && h_.shm_fd < 0) return;
    close_handles(&h_);
    if (owner_) {
      ::sem_unlink(names_.c2s_items_sem.c_str());
      ::sem_unlink(names_.c2s_spaces_sem.c_str());
      ::sem_unlink(names_.s2c_items_sem.c_str());
      ::sem_unlink(names_.s2c_spaces_sem.c_str());
      ::shm_unlink(names_.shm.c_str());
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
  explicit ShmListener(ShmNames names, int fd) : names_(std::move(names)), fd_(fd) {}
  ~ShmListener() override { close(); }

  Result<std::unique_ptr<Pipe>> accept() override {
    if (fd_ < 0) return Status::closed("listener closed");
    int cfd = ::accept(fd_, nullptr, nullptr);
    if (cfd < 0) return Status::io_error("accept(uds bootstrap) failed" + errno_suffix());

    char connid[16];
    auto st = read_exact(cfd, connid, sizeof(connid));
    ::close(cfd);
    if (!st.ok()) return st.status();

    ShmNames n = make_names(names_.base, std::string(connid, sizeof(connid)));
    auto h = open_resources(n);
    if (!h.ok()) return h.status();
    return std::unique_ptr<Pipe>(new ShmPipe(h.value(), std::move(n), /*owner=*/false, /*is_client=*/false));
  }

  Result<std::string> local_address() const override { return std::string("shm://") + names_.base; }

  void close() override {
    if (fd_ >= 0) {
      ::close(fd_);
      fd_ = -1;
      // Keep path cleanup best-effort.
      ::unlink(names_.bootstrap_path.c_str());
    }
  }

 private:
  ShmNames names_;
  int fd_ = -1;
};
}  // namespace duct
#endif  // !_WIN32

// Linux/Unix implementation functions
Result<std::unique_ptr<Listener>> shm_listen(const std::string& name, const ListenOptions& opt) {
  ShmNames n = make_names(name, "0000000000000000");
  auto fd = uds_listen(n.bootstrap_path, opt.backlog);
  if (!fd.ok()) return fd.status();
  return std::unique_ptr<Listener>(new ShmListener(std::move(n), fd.value()));
}

Result<std::unique_ptr<Pipe>> shm_dial(const std::string& name, const DialOptions& opt) {
  (void)opt;
  std::string connid = random_conn_id_hex16();
  ShmNames n = make_names(name, connid);

  auto created = create_resources(n);
  if (!created.ok()) return created.status();

  auto cfd = uds_connect(n.bootstrap_path);
  if (!cfd.ok()) {
    close_handles(&created.value());
    ::sem_unlink(n.c2s_items_sem.c_str());
    ::sem_unlink(n.c2s_spaces_sem.c_str());
    ::sem_unlink(n.s2c_items_sem.c_str());
    ::sem_unlink(n.s2c_spaces_sem.c_str());
    ::shm_unlink(n.shm.c_str());
    return cfd.status();
  }

  auto st = write_all(cfd.value(), connid.data(), connid.size());
  ::close(cfd.value());
  if (!st.ok()) {
    close_handles(&created.value());
    ::sem_unlink(n.c2s_items_sem.c_str());
    ::sem_unlink(n.c2s_spaces_sem.c_str());
    ::sem_unlink(n.s2c_items_sem.c_str());
    ::sem_unlink(n.s2c_spaces_sem.c_str());
    ::shm_unlink(n.shm.c_str());
    return st.status();
  }

  return std::unique_ptr<Pipe>(new ShmPipe(created.value(), std::move(n), /*owner=*/true, /*is_client=*/true));
}

}  // namespace duct
#endif  // !_WIN32
