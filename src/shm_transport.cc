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
#include "duct/socket_utils.h"

#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <dirent.h>
#include <limits>
#include <memory>
#include <random>
#include <signal.h>
#include <string>
#include <string_view>
#include <sys/mman.h>
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

constexpr std::uint32_t kFirstChunkFlag = 1u << 31;
constexpr std::uint32_t kLastChunkFlag = 1u << 30;
static constexpr std::size_t kMaxReassembledBytes = 64 * 1024 * 1024;  // 64 MiB

// Bound for waits between fragments of a multi-slot message (send and recv).
// The first fragment honors the caller timeout; continuation fragments use this
// bounded wait so a peer that stopped mid-message cannot wedge the transport
// worker (and thus QosPipe::close()) forever. It is intentionally generous: a
// large message streams many fragments and the peer thread may be descheduled
// between them, so a tight wait would false-timeout a live peer under load.
// Must stay below the crash-recovery close() deadline (tests expect a worker
// blocked on a dead sender to unblock within ~3s) while comfortably exceeding
// any realistic scheduling gap between two 64 KiB fragments.
static constexpr std::chrono::milliseconds kMidFragmentWait(2000);

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

struct ShmLayout {
  std::uint32_t slot_bytes = 0;   // payload capacity per slot
  std::uint32_t slot_count = 0;   // slots per ring (each direction)
  std::atomic_uint32_t owner_pid{0};  // PID of the creating (owner) process
  std::atomic_uint32_t magic{0};      // kShmMagic on a live, initialized segment
  // Set (with release ordering) by whichever endpoint closes first, so a peer
  // blocked in recv()/send() on a named semaphore can observe the close and
  // return kClosed instead of waiting forever on a peer that never signals.
  // The flag lives in the shared mapping because closing one side's handles
  // does not signal the other side's semaphore objects.
  std::atomic_uint32_t closed{0};
  RingMeta c2s_meta;
  RingMeta s2c_meta;
  // Slot data follows the header, laid out as:
  //   c2s slot[0..slot_count), s2c slot[0..slot_count)
  // Each slot is 8-byte-aligned: [u32 len][u32 pad][slot_bytes bytes].
};

static constexpr std::uint32_t kShmMagic = 0x4D484353;  // 'SHCM' little-endian marker

// Total mapped size for a validated geometry. Uses checked arithmetic so an
// oversized slot_count cannot wrap the mapped size below the data offsets.
static std::size_t layout_size(std::uint32_t slot_bytes, std::uint32_t slot_count) {
  // Each slot is 8 bytes of header (u32 len + u32 pad) followed by the payload,
  // rounded up to 8-byte alignment so the stride is stable across peers.
  const std::size_t slot_stride = (static_cast<std::size_t>(slot_bytes) + 8u + 7u) & ~std::size_t(7u);
  constexpr std::size_t kRings = 2u;
  if (slot_stride != 0 &&
      slot_count > (std::numeric_limits<std::size_t>::max() - sizeof(ShmLayout)) /
                       (kRings * slot_stride)) {
    return 0;
  }
  return sizeof(ShmLayout) + kRings * static_cast<std::size_t>(slot_count) * slot_stride;
}

struct Slot {
  std::uint32_t len = 0;
  std::uint32_t _pad = 0;
  // Flexible payload: layout_size() sizes the mapping. We cannot use a
  // flexible array member in a struct that participates in layout math with
  // offsetof, so treat slots as raw bytes and index them manually.
};

// A slot within a ring. data points into the shared mapping.
static std::uint8_t* slot_data(ShmLayout* l, bool c2s, std::uint32_t idx, std::uint32_t slot_bytes) {
  const std::size_t slot_stride = (static_cast<std::size_t>(slot_bytes) + 8u + 7u) & ~std::size_t(7u);
  std::size_t ring_base = sizeof(ShmLayout);
  // c2s comes first, s2c second.
  if (!c2s) ring_base += static_cast<std::size_t>(l->slot_count) * slot_stride;
  return reinterpret_cast<std::uint8_t*>(l) + ring_base + static_cast<std::size_t>(idx) * slot_stride;
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

static bool geometry_valid(std::uint32_t slot_bytes, std::uint32_t slot_count) {
  // slot_bytes must leave the top two bits of the 32-bit length word free so the
  // first/last chunk flags do not collide with the length.
  return slot_bytes != 0 && slot_bytes < kLastChunkFlag && slot_count != 0;
}

// Validate ShmOptions.slot_bytes (a size_t) before narrowing to uint32_t so an
// oversized value (e.g. 4GiB+16) is rejected instead of silently truncated.
static bool options_geometry_valid(std::size_t slot_bytes, std::uint32_t slot_count) {
  if (slot_bytes > std::numeric_limits<std::uint32_t>::max()) return false;
  const std::uint32_t bytes = static_cast<std::uint32_t>(slot_bytes);
  if (!geometry_valid(bytes, slot_count)) return false;
  // layout_size() returns 0 when the mapped size would overflow size_t; a
  // caller must reject that geometry instead of creating a tiny segment whose
  // slot offsets fall outside the mapping.
  return layout_size(bytes, slot_count) != 0;
}

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

  // The bootstrap socket lives in /tmp; never unlink an arbitrary file or
  // steal a live shm bus. Same policy as uds_listen (see socket_utils).
  auto prep = socket_utils::ensure_stale_uds_removed(addr);
  if (!prep.ok()) {
    ::close(fd);
    return prep.status();
  }
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

static Result<int> uds_connect(std::string_view path, std::chrono::milliseconds timeout) {
  int fd = ::socket(AF_UNIX, SOCK_STREAM, 0);
  if (fd < 0) return Status::io_error("socket(AF_UNIX) failed" + errno_suffix());

  sockaddr_un addr{};
  addr.sun_family = AF_UNIX;
  if (path.size() >= sizeof(addr.sun_path)) {
    ::close(fd);
    return Status::invalid_argument("uds path too long");
  }
  std::snprintf(addr.sun_path, sizeof(addr.sun_path), "%s", std::string(path).c_str());

  if (timeout.count() > 0) {
    auto st = socket_utils::set_nonblocking(fd, true);
    if (!st.ok()) {
      ::close(fd);
      return st.status();
    }
    int rc = ::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
    if (rc == 0) {
      (void)socket_utils::set_nonblocking(fd, false);
      return fd;
    }
    // EINPROGRESS/EALREADY: connect is pending. EAGAIN: Linux reports a full
    // listen backlog this way; the connection is still in progress and we must
    // wait for POLLOUT then check SO_ERROR. Any other error is immediate.
    if (errno != EINPROGRESS && errno != EALREADY && errno != EINTR && errno != EAGAIN) {
      ::close(fd);
      return Status::io_error("connect(AF_UNIX) failed" + errno_suffix());
    }
    st = socket_utils::wait_writable(fd, timeout);
    if (!st.ok()) {
      ::close(fd);
      if (st.status().code() == StatusCode::kTimeout) {
        return Status::timeout("connect(AF_UNIX) timed out");
      }
      return st.status();
    }
    int so_error = 0;
    socklen_t errlen = sizeof(so_error);
    if (::getsockopt(fd, SOL_SOCKET, SO_ERROR, &so_error, &errlen) < 0 || so_error != 0) {
      ::close(fd);
      if (so_error != 0) {
        errno = so_error;
      }
      return Status::io_error("connect(AF_UNIX) failed: SO_ERROR=" +
                              std::to_string(so_error) + " " + std::strerror(so_error));
    }
    (void)socket_utils::set_nonblocking(fd, false);
    return fd;
  }

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
  std::uint32_t slot_bytes = 0;
  std::uint32_t slot_count = 0;
  sem_t* c2s_items = SEM_FAILED;
  sem_t* c2s_spaces = SEM_FAILED;
  sem_t* s2c_items = SEM_FAILED;
  sem_t* s2c_spaces = SEM_FAILED;
};

static void close_handles(ShmHandles* h) {
  if (!h) return;
  if (h->mem) {
    std::size_t size = sizeof(ShmLayout);
    if (h->slot_bytes != 0 && h->slot_count != 0) {
      size = layout_size(h->slot_bytes, h->slot_count);
    }
    ::munmap(h->mem, size);
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

static Result<ShmHandles> create_resources(const ShmNames& n, const ShmOptions& opt) {
  ShmHandles h;
  if (!options_geometry_valid(opt.slot_bytes, opt.slot_count)) {
    return Status::invalid_argument("invalid shm geometry (slot_bytes/slot_count)");
  }
  const std::size_t size = layout_size(static_cast<std::uint32_t>(opt.slot_bytes), opt.slot_count);
  h.slot_bytes = static_cast<std::uint32_t>(opt.slot_bytes);
  h.slot_count = opt.slot_count;

  h.shm_fd = ::shm_open(n.shm.c_str(), O_CREAT | O_EXCL | O_RDWR, 0600);
  if (h.shm_fd < 0) {
    return Status::io_error("shm_open(create) failed: " + n.shm + errno_suffix());
  }
  if (::ftruncate(h.shm_fd, static_cast<off_t>(size)) != 0) {
    close_handles(&h);
    ::shm_unlink(n.shm.c_str());
    return Status::io_error("ftruncate(shm) failed" + errno_suffix());
  }
  void* p = ::mmap(nullptr, size, PROT_READ | PROT_WRITE, MAP_SHARED, h.shm_fd, 0);
  if (p == MAP_FAILED) {
    close_handles(&h);
    ::shm_unlink(n.shm.c_str());
    return Status::io_error("mmap(shm) failed" + errno_suffix());
  }
  h.mem = static_cast<ShmLayout*>(p);
  // Value-initialize instead of memset: ShmLayout contains std::atomic members,
  // which are not trivially copy-assignable, so memset on them is undefined behavior
  // (-Wclass-memaccess). The fresh shm segment is already zeroed by the OS, and
  // value-initialization makes the atomics/slots explicitly zero.
  ::new (static_cast<void*>(h.mem)) ShmLayout{};
  h.mem->slot_bytes = h.slot_bytes;
  h.mem->slot_count = h.slot_count;
  h.mem->owner_pid.store(static_cast<std::uint32_t>(::getpid()), std::memory_order_release);
  h.mem->magic.store(kShmMagic, std::memory_order_release);

  // Semaphores:
  // - items start at 0
  // - spaces start at slot count
  h.c2s_items = ::sem_open(n.c2s_items_sem.c_str(), O_CREAT | O_EXCL, 0600, 0);
  h.c2s_spaces = ::sem_open(n.c2s_spaces_sem.c_str(), O_CREAT | O_EXCL, 0600, h.slot_count);
  h.s2c_items = ::sem_open(n.s2c_items_sem.c_str(), O_CREAT | O_EXCL, 0600, 0);
  h.s2c_spaces = ::sem_open(n.s2c_spaces_sem.c_str(), O_CREAT | O_EXCL, 0600, h.slot_count);

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

static Result<ShmHandles> open_resources(const ShmNames& n, const ShmOptions& expected) {
  ShmHandles h;
  h.shm_fd = ::shm_open(n.shm.c_str(), O_RDWR, 0600);
  if (h.shm_fd < 0) {
    return Status::io_error("shm_open(open) failed: " + n.shm + errno_suffix());
  }
  // Map the header first to read the geometry. fstat gives the real size.
  struct stat st{};
  if (::fstat(h.shm_fd, &st) != 0) {
    ::close(h.shm_fd);
    return Status::io_error("fstat(shm) failed" + errno_suffix());
  }
  if (!options_geometry_valid(expected.slot_bytes, expected.slot_count)) {
    ::close(h.shm_fd);
    return Status::invalid_argument("invalid expected shm geometry");
  }
  const std::uint32_t slot_bytes = static_cast<std::uint32_t>(expected.slot_bytes);
  const std::uint32_t slot_count = expected.slot_count;
  const std::size_t size = layout_size(slot_bytes, slot_count);
  if (static_cast<std::size_t>(st.st_size) < size) {
    ::close(h.shm_fd);
    return Status::protocol_error("shm segment smaller than expected layout");
  }
  void* p = ::mmap(nullptr, size, PROT_READ | PROT_WRITE, MAP_SHARED, h.shm_fd, 0);
  if (p == MAP_FAILED) {
    close_handles(&h);
    return Status::io_error("mmap(shm) failed" + errno_suffix());
  }
  h.mem = static_cast<ShmLayout*>(p);
  h.slot_bytes = slot_bytes;
  h.slot_count = slot_count;

  // The dialer records the geometry it actually created. A mismatch means the
  // peers disagree on layout and would corrupt the ring.
  if (h.mem->magic.load(std::memory_order_acquire) != kShmMagic) {
    close_handles(&h);
    return Status::protocol_error("shm segment not initialized");
  }
  if (h.mem->slot_bytes != slot_bytes || h.mem->slot_count != slot_count) {
    close_handles(&h);
    return Status::protocol_error("shm geometry mismatch");
  }

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

    const bool tx_ring = is_client_;  // client sends on c2s
    sem_t* spaces = is_client_ ? h_.c2s_spaces : h_.s2c_spaces;
    sem_t* items = is_client_ ? h_.c2s_items : h_.s2c_items;
    RingMeta* tx = ring_meta(h_.mem, tx_ring);
    const std::uint32_t slot_bytes = h_.slot_bytes;
    const std::uint32_t slot_count = h_.slot_count;

    auto peer_closed = [&] {
      return h_.mem->closed.load(std::memory_order_acquire) != 0;
    };
    if (peer_closed()) return Status::closed("shm peer closed");

    // Fragment payloads larger than one slot into multiple slots. Each slot
    // header stores the remaining length; the final slot carries a zero-length
    // marker via a high bit so recv() can reassemble. We use a leading bit in
    // len to flag the first chunk and a separate marker for the last chunk.
    const std::size_t total = msg.size();
    std::size_t offset = 0;
    bool first = true;
    do {
      const std::size_t chunk = std::min(static_cast<std::size_t>(slot_bytes), total - offset);
      const bool last = (offset + chunk == total);

      // Once the first chunk is committed the message cannot be rolled back.
      // Waits for subsequent chunks use the blocking form so a mid-message
      // timeout does not leave a partially written stream that corrupts the
      // peer's reassembly. The first chunk still honors the caller timeout.
      // Keep waits after the first chunk bounded so a stopped reader cannot hold
      // QosPipe::close() open or race close() with an in-flight semaphore wait.
      // The bound is generous: a large message streams many fragments and the
      // reader thread may be descheduled between them, so a tight wait would
      // false-timeout a live peer under load (kTimeout mid-reassembly).
      auto st = sem_wait_opt(spaces, first ? opt.timeout : kMidFragmentWait);
      if (!st.ok()) return st;
      if (peer_closed()) return Status::closed("shm peer closed");

      std::uint32_t head = tx->head.load(std::memory_order_relaxed);
      std::uint32_t idx = head % slot_count;
      std::uint32_t len = static_cast<std::uint32_t>(chunk);
      if (first) len |= kFirstChunkFlag;
      if (last) len |= kLastChunkFlag;
      *slot_len(h_.mem, tx_ring, idx, slot_bytes) = len;
      if (chunk != 0) {
        std::memcpy(slot_payload(h_.mem, tx_ring, idx, slot_bytes), msg.data() + offset, chunk);
      }
      tx->head.store(head + 1, std::memory_order_release);
      ::sem_post(items);
      offset += chunk;
      first = false;
    } while (offset < total);
    return {};
  }

  Result<Message> recv(const RecvOptions& opt) override {
    if (!h_.mem) return Status::closed("pipe closed");

    const bool rx_ring = !is_client_;  // client receives on s2c
    sem_t* items = is_client_ ? h_.s2c_items : h_.c2s_items;
    sem_t* spaces = is_client_ ? h_.s2c_spaces : h_.c2s_spaces;
    RingMeta* rx = ring_meta(h_.mem, rx_ring);
    const std::uint32_t slot_bytes = h_.slot_bytes;
    const std::uint32_t slot_count = h_.slot_count;

    // Deliver anything already queued before reporting the close so a peer
    // that sent and closed in quick succession loses no data.
    auto peer_closed_and_drained = [&] {
      return h_.mem->closed.load(std::memory_order_acquire) != 0 &&
             rx->head.load(std::memory_order_acquire) ==
                 rx->tail.load(std::memory_order_relaxed);
    };
    if (peer_closed_and_drained()) return Status::closed("shm peer closed");

    std::vector<std::uint8_t> assembled;
    bool first = true;
    for (;;) {
      // Once reassembly has started we must consume the whole message to keep the
      // stream synchronized. The first chunk honors the caller timeout; later
      // chunks use a bounded wait so a stopped sender cannot wedge the worker
      // (and thus QosPipe::close()) forever. A timeout mid-fragment leaves the
      // stream out of sync, which the next read reports as a protocol error.
      // The bound is generous: a 64 MiB message arrives in many fragments and
      // the sender thread may be descheduled between them, so a tight wait
      // would false-timeout a live peer under load (kTimeout mid-reassembly).
      auto st = sem_wait_opt(items, first ? opt.timeout : kMidFragmentWait);
      if (!st.ok()) return st.status();
      // A wake with an empty ring means the token came from close(), not data.
      if (peer_closed_and_drained()) return Status::closed("shm peer closed");

      std::uint32_t tail = rx->tail.load(std::memory_order_relaxed);
      std::uint32_t idx = tail % slot_count;
      std::uint32_t len = *slot_len(h_.mem, rx_ring, idx, slot_bytes);
      const bool is_first = (len & kFirstChunkFlag) != 0;
      const bool is_last = (len & kLastChunkFlag) != 0;
      len &= ~(kFirstChunkFlag | kLastChunkFlag);
      // A protocol error in the slot payload must not wedge the ring: consume
      // the slot (advance tail, post a space token) before returning so later
      // recv() calls make progress instead of reading the same poisoned slot
      // forever and leaking spaces.
      auto consume_and_fail = [&](const char* what) {
        rx->tail.store(tail + 1, std::memory_order_release);
        ::sem_post(spaces);
        return Status::protocol_error(what);
      };
      if (len > slot_bytes) {
        return consume_and_fail("shm slot len too large");
      }

      if (first) {
        if (!is_first) return consume_and_fail("shm fragment stream out of sync");
        if (len > kMaxReassembledBytes) {
          return consume_and_fail("shm fragment stream too large");
        }
        assembled.resize(len);
        if (len != 0) std::memcpy(assembled.data(), slot_payload(h_.mem, rx_ring, idx, slot_bytes), len);
        first = false;
        rx->tail.store(tail + 1, std::memory_order_release);
        ::sem_post(spaces);
        if (is_last) return Message::from_bytes(assembled.data(), assembled.size());
        continue;
      }

      const std::size_t old = assembled.size();
      if (old > kMaxReassembledBytes || len > kMaxReassembledBytes - old) {
        return consume_and_fail("shm fragment stream too large");
      }
      assembled.resize(old + len);
      if (len != 0) std::memcpy(assembled.data() + old, slot_payload(h_.mem, rx_ring, idx, slot_bytes), len);
      rx->tail.store(tail + 1, std::memory_order_release);
      ::sem_post(spaces);
      if (is_last) return Message::from_bytes(assembled.data(), assembled.size());
    }
  }

  void close() override {
    if (!h_.mem && h_.shm_fd < 0) return;
    if (h_.mem) {
      h_.mem->closed.store(1, std::memory_order_release);
      // Wake the peer if it is blocked in send()/recv() on a named semaphore:
      // closing this side's handles does not signal the peer's semaphores, so
      // a waiter with no timeout would block forever. Post the peer's recv
      // items sem and the peer's send spaces sem; the peer re-checks the
      // closed flag after waking and returns kClosed once its ring is drained.
      sem_t* peer_rx_items = is_client_ ? h_.c2s_items : h_.s2c_items;
      sem_t* peer_tx_spaces = is_client_ ? h_.s2c_spaces : h_.c2s_spaces;
      ::sem_post(peer_rx_items);
      ::sem_post(peer_tx_spaces);
    }
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
  ShmListener(ShmNames names, int fd, ShmOptions expected)
      : names_(std::move(names)), fd_(fd), expected_(expected) {}
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
    auto h = open_resources(n, expected_);
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
  ShmOptions expected_{};
};
}  // namespace duct

#if !defined(_WIN32)
// Pre-63a492e shm segments used a fixed 64-slot / 64KiB layout. Their
// owner_pid/magic live at the end of an ~8 MiB struct, so the new header read
// (offset 0-15) cannot identify them. Keep the legacy magic offset so upgraded
// builds can still reclaim orphans left by an older binary.
constexpr std::size_t kLegacyMagicOffset = 8389764;  // owner_pid|magic tail

// Reclaim orphaned shm resources for a bus. A connection's owner records its
// PID in the segment header. If the owner died without running close() (e.g.
// SIGKILL), the segment and its four semaphores are left behind. Scan /dev/shm
// for segments of this bus and unlink any whose recorded owner PID is dead.
static void reclaim_orphaned_bus(const std::string& bus_name) {
  const std::string hash8 = hex8(fnv1a_32(sanitize_name(bus_name)));
  const std::string prefix = "d" + hash8;

  DIR* dir = ::opendir("/dev/shm");
  if (!dir) return;
  struct dirent* ent;
  while ((ent = ::readdir(dir)) != nullptr) {
    const std::string name = ent->d_name;
    if (name.size() < prefix.size() + 8 || name.compare(0, prefix.size(), prefix) != 0) {
      continue;
    }
    // Only consider shm segments (end with 'm'). The segment is
    // "<prefix><connid>m"; semaphores are "<prefix><connid><a-d>" (glibc
    // renders POSIX semaphore names in /dev/shm with a "sem." prefix).
    if (name.compare(0, 4, "sem.") == 0) continue;
    if (name.back() != 'm') continue;

    const std::string seg_name = "/" + name;
    int fd = ::shm_open(seg_name.c_str(), O_RDWR, 0600);
    if (fd < 0) continue;
    // Reject undersized files before mapping: mapping a file shorter than the
    // header and reading past its last page raises SIGBUS (local DoS via a
    // hostile /dev/shm file). fstat gives the real size.
    struct stat st{};
    if (::fstat(fd, &st) != 0 || st.st_size < static_cast<off_t>(sizeof(ShmLayout))) {
      ::close(fd);
      continue;
    }
    // Read the header to get geometry. We cannot know the size without it, so
    // map just the fixed header then the full size.
    void* p = ::mmap(nullptr, sizeof(ShmLayout), PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if (p == MAP_FAILED) {
      ::close(fd);
      continue;
    }

    const auto* layout = static_cast<const ShmLayout*>(p);
    std::uint32_t owner = layout->owner_pid.load(std::memory_order_acquire);
    const std::uint32_t magic = layout->magic.load(std::memory_order_acquire);
    std::uint32_t slot_bytes = layout->slot_bytes;
    std::uint32_t slot_count = layout->slot_count;
    ::munmap(p, sizeof(ShmLayout));

    bool legacy = false;
    if (magic != kShmMagic) {
      // Maybe a pre-geometry segment. Read its magic from the legacy tail
      // offset without mapping (pread cannot SIGBUS). If it matches, it is a
      // valid legacy aello segment whose owner_pid/magic are at that offset.
      if (st.st_size >= static_cast<off_t>(kLegacyMagicOffset + 4u)) {
        std::uint32_t tail_owner = 0;
        std::uint32_t tail_magic = 0;
        if (::pread(fd, &tail_owner, sizeof(tail_owner), kLegacyMagicOffset - 4u) ==
                static_cast<ssize_t>(sizeof(tail_owner)) &&
            ::pread(fd, &tail_magic, sizeof(tail_magic), kLegacyMagicOffset) ==
                static_cast<ssize_t>(sizeof(tail_magic))) {
          legacy = (tail_magic == kShmMagic);
          if (legacy) {
            owner = tail_owner;
            // Legacy segments used a fixed 64-slot / 64 KiB layout whose
            // slot_bytes/slot_count live at offset 0 (the old RingMeta head/tail
            // fields), so the new header read above yields zero. Substitute the
            // fixed geometry so geometry_valid() below accepts them.
            slot_bytes = 64 * 1024;
            slot_count = 64;
          }
        }
      }
      if (!legacy) {
        ::close(fd);
        continue;  // not a live aello segment
      }
    }
    if (!geometry_valid(slot_bytes, slot_count)) {
      ::close(fd);
      continue;
    }
    if (owner == 0) {
      ::close(fd);
      continue;
    }
    if (::kill(static_cast<pid_t>(owner), 0) == 0 || errno == EPERM) {
      ::close(fd);
      continue;  // owner still alive
    }
    ::close(fd);

    // Owner is gone: unlink the segment and its semaphores.
    ::shm_unlink(seg_name.c_str());
    const std::string conn = name.substr(prefix.size(), name.size() - prefix.size() - 1);
    // The semaphore names created by make_names() are "/" + prefix + conn + [a-d];
    // match them exactly instead of the old "/sem.d..." form which never hit.
    const std::string sem_prefix = "/" + prefix + conn;
    ::sem_unlink((sem_prefix + "a").c_str());
    ::sem_unlink((sem_prefix + "b").c_str());
    ::sem_unlink((sem_prefix + "c").c_str());
    ::sem_unlink((sem_prefix + "d").c_str());
  }
  ::closedir(dir);
}
#endif

// Linux/Unix implementation functions
Result<std::unique_ptr<Listener>> shm_listen(const std::string& name, const ListenOptions& opt) {
#if !defined(_WIN32)
  reclaim_orphaned_bus(name);
#endif
  if (!options_geometry_valid(opt.shm.slot_bytes, opt.shm.slot_count)) {
    return Status::invalid_argument("invalid shm geometry (slot_bytes/slot_count)");
  }
  ShmNames n = make_names(name, "0000000000000000");
  auto fd = uds_listen(n.bootstrap_path, opt.backlog);
  if (!fd.ok()) return fd.status();
  return std::unique_ptr<Listener>(new ShmListener(std::move(n), fd.value(), opt.shm));
}

Result<std::unique_ptr<Pipe>> shm_dial(const std::string& name, const DialOptions& opt) {
  if (opt.timeout.count() < 0) {
    return Status::invalid_argument("dial timeout must not be negative");
  }
  std::string connid = random_conn_id_hex16();
  ShmNames n = make_names(name, connid);

  auto created = create_resources(n, opt.shm);
  if (!created.ok()) return created.status();

  auto cfd = uds_connect(n.bootstrap_path, opt.timeout);
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
