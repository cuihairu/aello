#include "test_support.h"
#include "shm_tests.h"

#include <algorithm>
#include <string>
#include <vector>

#if !defined(_WIN32)
#include <dirent.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>
#include <cstring>
#endif

void test_shm_echo_one() {
  auto lis_r = duct::listen("shm://duct_testbus");
  EXPECT_TRUE(lis_r.ok());
  if (!lis_r.ok()) return;

  auto server = std::async(std::launch::async, [&]() -> duct::Result<void> {
    auto p = lis_r.value()->accept();
    if (!p.ok()) return p.status();
    auto msg = p.value()->recv({});
    if (!msg.ok()) return msg.status();
    return p.value()->send(msg.value(), {});
  });

  // Give the listener a moment to bind the bootstrap socket path.
  std::this_thread::sleep_for(std::chrono::milliseconds(10));

  // Disable QoS wrapper for transport-level tests.
  // 关闭 QoS 包装层，专注验证底层 shm 传输语义。
  duct::DialOptions dial_opt;
  dial_opt.qos.snd_hwm_bytes = 0;
  dial_opt.qos.rcv_hwm_bytes = 0;
  auto c = duct::dial("shm://duct_testbus", dial_opt);
  EXPECT_TRUE(c.ok());
  if (!c.ok()) return;

  auto st = c.value()->send(duct::Message::from_string("hello"), {});
  EXPECT_TRUE(st.ok());
  if (!st.ok()) return;

  auto echoed = c.value()->recv({});
  EXPECT_TRUE(echoed.ok());
  if (!echoed.ok()) return;

  std::string s(reinterpret_cast<const char*>(echoed.value().data()), echoed.value().size());
  EXPECT_EQ(s, "hello");

  auto sst = server.wait_for(std::chrono::seconds(1));
  EXPECT_TRUE(sst == std::future_status::ready);
  if (sst == std::future_status::ready) {
    auto sr = server.get();
    EXPECT_TRUE(sr.ok());
  }

  lis_r.value()->close();
}


void test_shm_crash_recovery_reclaims_orphan() {
#if !defined(_WIN32)
  // POSIX semaphore objects appear in /dev/shm with a "sem." prefix; the
  // shared-memory segment itself appears as "ddfb0e26d<conn>m".
  const std::string prefix = "ddfb0e26d";
  auto leftovers = [&prefix]() {
    std::set<std::string> found;
    DIR* dir = ::opendir("/dev/shm");
    if (!dir) return found;
    while (dirent* ent = ::readdir(dir)) {
      std::string name = ent->d_name;
      if (name.rfind(prefix, 0) == 0 || name.rfind("sem." + prefix, 0) == 0) {
        found.insert("/dev/shm/" + name);
      }
    }
    ::closedir(dir);
    return found;
  };

  for (const auto& stale : leftovers()) (void)::unlink(stale.c_str());

  int pfd[2];
  EXPECT_TRUE(::pipe(pfd) == 0);
  pid_t listener = ::fork();
  EXPECT_TRUE(listener >= 0);
  if (listener < 0) return;
  if (listener == 0) {
    ::close(pfd[0]);
    auto lis = duct::listen("shm://duct_crashbus");
    if (!lis.ok()) _exit(1);
    char ready = 'R';
    (void)::write(pfd[1], &ready, 1);
    auto p = lis.value()->accept();
    if (!p.ok()) _exit(1);
    for (;;) ::pause();
  }
  ::close(pfd[1]);
  char ready = 0;
  (void)::read(pfd[0], &ready, 1);
  ::close(pfd[0]);

  pid_t dialer = ::fork();
  EXPECT_TRUE(dialer >= 0);
  if (dialer < 0) {
    ::kill(listener, SIGKILL);
    ::waitpid(listener, nullptr, 0);
    return;
  }
  if (dialer == 0) {
    auto p = duct::dial("shm://duct_crashbus");
    if (!p.ok()) _exit(1);
    for (;;) ::pause();
  }

  std::this_thread::sleep_for(std::chrono::milliseconds(300));
  EXPECT_TRUE(::kill(dialer, SIGKILL) == 0);
  EXPECT_TRUE(::waitpid(dialer, nullptr, 0) == dialer);

  EXPECT_TRUE(::kill(listener, SIGKILL) == 0);
  EXPECT_TRUE(::waitpid(listener, nullptr, 0) == listener);

  const std::set<std::string> orphaned = leftovers();
  EXPECT_EQ(orphaned.size(), 5u);

  auto lis2 = duct::listen("shm://duct_crashbus");
  EXPECT_TRUE(lis2.ok());
  if (lis2.ok()) {
    lis2.value()->close();
  }
  EXPECT_EQ(leftovers().size(), 0u);
#endif
}


// Reclaim must also unlink segments left by a pre-geometry (pre-63a492e) binary.
// Those segments carry owner_pid/magic at a fixed tail offset (~8 MiB), so the
// new header read at offset 0-15 sees garbage and would previously skip them.
// It must also tolerate an undersized hostile file without raising SIGBUS.
void test_shm_reclaims_legacy_orphan() {
#if defined(_WIN32)
  return;
#else
  // Reproduce src/shm_transport.cc's sanitize_name/fnv1a_32/hex8.
  auto sanitize = [](std::string_view s) {
    std::string out;
    for (char c : s) {
      if ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
          (c >= '0' && c <= '9') || c == '_') {
        out.push_back(c);
      } else {
        out.push_back('_');
      }
    }
    if (out.empty()) out = "duct";
    return out;
  };
  auto fnv1a = [](std::string_view s) {
    std::uint32_t h = 2166136261u;
    for (unsigned char c : s) {
      h ^= c;
      h *= 16777619u;
    }
    return h;
  };
  auto hex8 = [](std::uint32_t v) {
    char buf[9];
    std::snprintf(buf, sizeof(buf), "%08x", v);
    return std::string(buf);
  };
  const std::string bus = "duct_legacybus";
  const std::string prefix = "d" + hex8(fnv1a(sanitize(bus)));
  auto leftovers = [&prefix]() {
    std::set<std::string> found;
    DIR* dir = ::opendir("/dev/shm");
    if (!dir) return found;
    while (dirent* ent = ::readdir(dir)) {
      std::string name = ent->d_name;
      if (name.rfind(prefix, 0) == 0 || name.rfind("sem." + prefix, 0) == 0) {
        found.insert("/dev/shm/" + name);
      }
    }
    ::closedir(dir);
    return found;
  };
  for (const auto& stale : leftovers()) (void)::unlink(stale.c_str());

  // A dead PID that is safe to reuse as the recorded owner.
  pid_t dead = ::fork();
  EXPECT_TRUE(dead >= 0);
  if (dead < 0) return;
  if (dead == 0) _exit(0);
  EXPECT_TRUE(::waitpid(dead, nullptr, 0) == dead);

  const std::string conn = "c0ffee01";
  const std::string seg = "/dev/shm/" + prefix + conn + "m";
  constexpr std::size_t kLegacySize = 8389768;       // old ShmLayout size
  constexpr std::size_t kOwnerOff = kLegacySize - 8; // owner_pid
  constexpr std::size_t kMagicOff = kLegacySize - 4; // magic
  constexpr std::uint32_t kMagic = 0x4D484353;       // 'SHCM'
  int fd = ::open(seg.c_str(), O_CREAT | O_RDWR | O_TRUNC, 0600);
  EXPECT_TRUE(fd >= 0);
  if (fd < 0) return;
  EXPECT_TRUE(::ftruncate(fd, static_cast<off_t>(kLegacySize)) == 0);
  EXPECT_TRUE(::pwrite(fd, &dead, sizeof(dead), kOwnerOff) ==
              static_cast<ssize_t>(sizeof(dead)));
  EXPECT_TRUE(::pwrite(fd, &kMagic, sizeof(kMagic), kMagicOff) ==
              static_cast<ssize_t>(sizeof(kMagic)));
  ::close(fd);

  // An undersized hostile file matching the prefix must not raise SIGBUS.
  const std::string tiny = "/dev/shm/" + prefix + conn + "t";
  int tfd = ::open(tiny.c_str(), O_CREAT | O_RDWR | O_TRUNC, 0600);
  if (tfd >= 0) {
    EXPECT_TRUE(::write(tfd, "x", 1) == 1);
    ::close(tfd);
  }

  EXPECT_TRUE(leftovers().size() >= 2u);
  auto lis = duct::listen("shm://" + bus);
  EXPECT_TRUE(lis.ok());
  if (lis.ok()) lis.value()->close();
  // Both the legacy segment and the tiny file are gone; the tiny file is
  // skipped (not SIGBUS) because it ends in 't', while the legacy segment (ends
  // in 'm') is reclaimed.
  auto left = leftovers();
  EXPECT_EQ(left.count(seg), 0u);
  EXPECT_EQ(left.count(tiny), 1u);  // untouched by reclaim (not an 'm' file)
  (void)::unlink(tiny.c_str());
#endif
}


void test_shm_backpressure_timeout() {
  auto lis_r = duct::listen("shm://duct_testbp");
  EXPECT_TRUE(lis_r.ok());
  if (!lis_r.ok()) return;

  auto accepted = std::promise<duct::Result<std::unique_ptr<duct::Pipe>>>();
  auto fut = accepted.get_future();
  std::thread t([&] { accepted.set_value(lis_r.value()->accept()); });

  std::this_thread::sleep_for(std::chrono::milliseconds(10));

  duct::DialOptions dial_opt;
  dial_opt.qos.snd_hwm_bytes = 0;
  dial_opt.qos.rcv_hwm_bytes = 0;
  auto c = duct::dial("shm://duct_testbp", dial_opt);
  EXPECT_TRUE(c.ok());
  if (!c.ok()) {
    t.join();
    return;
  }

  auto sr = fut.get();
  EXPECT_TRUE(sr.ok());
  if (!sr.ok()) {
    t.join();
    return;
  }

  // Do not recv on server side: fill client's TX ring until it blocks.
  (void)sr.value();

  duct::SendOptions opt;
  opt.timeout = std::chrono::milliseconds(50);

  bool saw_timeout = false;
  for (int i = 0; i < 256; ++i) {
    auto st = c.value()->send(duct::Message::from_string("x"), opt);
    if (!st.ok()) {
      EXPECT_EQ(st.status().code(), duct::StatusCode::kTimeout);
      saw_timeout = true;
      break;
    }
  }
  EXPECT_TRUE(saw_timeout);

  lis_r.value()->close();
  t.join();
}


void test_shm_burst_without_receiver() {
  auto lis_r = duct::listen("shm://duct_testburst");
  EXPECT_TRUE(lis_r.ok());
  if (!lis_r.ok()) return;

  auto accepted = std::promise<duct::Result<std::unique_ptr<duct::Pipe>>>();
  auto fut = accepted.get_future();
  std::thread t([&] { accepted.set_value(lis_r.value()->accept()); });

  std::this_thread::sleep_for(std::chrono::milliseconds(10));

  duct::DialOptions dial_opt;
  dial_opt.qos.snd_hwm_bytes = 0;
  dial_opt.qos.rcv_hwm_bytes = 0;
  auto c = duct::dial("shm://duct_testburst", dial_opt);
  EXPECT_TRUE(c.ok());
  if (!c.ok()) {
    t.join();
    return;
  }

  auto sr = fut.get();
  EXPECT_TRUE(sr.ok());
  if (!sr.ok()) {
    t.join();
    return;
  }

  // Do not recv on server side: client should be able to burst-send several messages
  // without blocking on a fresh ring.
  (void)sr.value();

  duct::SendOptions opt;
  opt.timeout = std::chrono::milliseconds(100);

  for (int i = 0; i < 8; ++i) {
    auto st = c.value()->send(duct::Message::from_string("x"), opt);
    EXPECT_TRUE(st.ok());
    if (!st.ok()) break;
  }

  lis_r.value()->close();
  t.join();
}


void test_shm_dial_timeout_honored() {
#if defined(_WIN32)
  // Windows shm dial honors opt.timeout via WaitNamedPipe. A missing bootstrap
  // pipe must fail promptly (timeout), never hang.
  duct::DialOptions dial_opt;
  dial_opt.timeout = std::chrono::milliseconds(150);
  dial_opt.qos.snd_hwm_bytes = 0;
  dial_opt.qos.rcv_hwm_bytes = 0;
  const auto start = std::chrono::steady_clock::now();
  auto c = duct::dial("shm://duct_timeout_no_listener", dial_opt);
  const auto elapsed = std::chrono::steady_clock::now() - start;
  EXPECT_TRUE(!c.ok());
  if (!c.ok()) {
    EXPECT_EQ(c.status().code(), duct::StatusCode::kTimeout);
  }
  EXPECT_TRUE(elapsed < std::chrono::seconds(3));
#else
  // No listener on the bootstrap socket. AF_UNIX connect returns ENOENT immediately
  // on Linux, so this verifies the primary negative-path guarantee: dial with a
  // bounded timeout must return promptly rather than hang. The positive timeout
  // path (listener backlog full) is covered by the UDS listen/accept tests once
  // `uds://` is wired.
  duct::DialOptions dial_opt;
  dial_opt.timeout = std::chrono::milliseconds(150);
  dial_opt.qos.snd_hwm_bytes = 0;
  dial_opt.qos.rcv_hwm_bytes = 0;
  const auto start = std::chrono::steady_clock::now();
  auto c = duct::dial("shm://duct_timeout_no_listener", dial_opt);
  const auto elapsed = std::chrono::steady_clock::now() - start;
  EXPECT_TRUE(!c.ok());
  EXPECT_TRUE(elapsed < std::chrono::seconds(3));
#endif
}


void test_shm_dial_positive_timeout_path() {
#if defined(_WIN32)
  return;
#else
  duct::ListenOptions listen_opt;
  listen_opt.backlog = 1;
  auto lis_r = duct::listen("shm://duct_shm_timeout_pos", listen_opt);
  EXPECT_TRUE(lis_r.ok());
  if (!lis_r.ok()) return;

  // Occupy the listener's accept queue. Each successful shm dial completes a
  // UDS connection into the backlog (the server never accepts), so a later
  // dial must hit the full-backlog EAGAIN and then the bounded wait path.
  duct::DialOptions occupy_opt;
  occupy_opt.timeout = std::chrono::milliseconds(200);
  occupy_opt.qos.snd_hwm_bytes = 0;
  occupy_opt.qos.rcv_hwm_bytes = 0;
  std::vector<std::shared_ptr<duct::Pipe>> occupied;
  for (int i = 0; i < 8; ++i) {
    auto occ = duct::dial("shm://duct_shm_timeout_pos", occupy_opt);
    if (occ.ok()) {
      occupied.emplace_back(occ.value().release());
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  EXPECT_TRUE(!occupied.empty());

  duct::DialOptions dial_opt;
  dial_opt.timeout = std::chrono::milliseconds(150);
  dial_opt.qos.snd_hwm_bytes = 0;
  dial_opt.qos.rcv_hwm_bytes = 0;
  const auto start = std::chrono::steady_clock::now();
  auto c = duct::dial("shm://duct_shm_timeout_pos", dial_opt);
  const auto elapsed = std::chrono::steady_clock::now() - start;
  EXPECT_TRUE(!c.ok());
  // The full-backlog connect must enter the bounded non-blocking wait and fail
  // promptly (kTimeout or kIoError), never hang.
  EXPECT_TRUE(elapsed < std::chrono::seconds(2));

  for (auto& p : occupied) p->close();
  lis_r.value()->close();
#endif
}


void test_shm_fragmentation_and_reassembly() {
#if defined(_WIN32)
  return;
#else
  auto lis_r = duct::listen("shm://duct_testfrag");
  EXPECT_TRUE(lis_r.ok());
  if (!lis_r.ok()) return;

  auto server = std::async(std::launch::async, [&]() -> duct::Result<void> {
    auto p = lis_r.value()->accept();
    if (!p.ok()) return p.status();
    auto msg = p.value()->recv({});
    if (!msg.ok()) return msg.status();
    return p.value()->send(msg.value(), {});
  });

  duct::DialOptions dial_opt;
  dial_opt.qos.snd_hwm_bytes = 0;
  dial_opt.qos.rcv_hwm_bytes = 0;
  auto c = duct::dial("shm://duct_testfrag", dial_opt);
  EXPECT_TRUE(c.ok());
  if (!c.ok()) return;

  const std::string payload(64 * 1024 + 123, 'q');
  auto st = c.value()->send(duct::Message::from_string(payload), {});
  EXPECT_TRUE(st.ok());
  if (!st.ok()) {
    c.value()->close();
    return;
  }

  auto echoed = c.value()->recv({});
  EXPECT_TRUE(echoed.ok());
  if (echoed.ok()) {
    EXPECT_EQ(echoed.value().size(), payload.size());
    EXPECT_EQ(echoed.value().as_string_view(), payload);
  }

  c.value()->close();
  auto sst = server.wait_for(std::chrono::seconds(2));
  if (sst == std::future_status::ready) {
    auto sr = server.get();
    EXPECT_TRUE(sr.ok());
  } else {
    EXPECT_TRUE(false);
  }
  lis_r.value()->close();
#endif
}


// The shm reassembly path must cap the accumulated fragment size like the wire
// path, so a peer streaming chunks without a last-chunk marker cannot grow the
// buffer without bound. Sending just over 64 MiB must be rejected.
void test_shm_reassembly_size_capped() {
#if defined(_WIN32)
  return;
#else
  auto lis_r = duct::listen("shm://duct_testreasm_cap");
  EXPECT_TRUE(lis_r.ok());
  if (!lis_r.ok()) return;

  // Send > 64 MiB in the background; the sender may block once the receiver
  // stops consuming after rejecting the oversize reassembly.
  auto server = std::async(std::launch::async, [&]() -> duct::Result<void> {
    auto p = lis_r.value()->accept();
    if (!p.ok()) return p.status();
    const std::string payload(64 * 1024 * 1024 + 64 * 1024, 'r');
    auto st = p.value()->send(duct::Message::from_string(payload), {});
    if (!st.ok()) return st.status();
    return {};
  });

  duct::DialOptions dial_opt;
  dial_opt.qos.snd_hwm_bytes = 0;
  dial_opt.qos.rcv_hwm_bytes = 0;
  auto c = duct::dial("shm://duct_testreasm_cap", dial_opt);
  EXPECT_TRUE(c.ok());
  if (!c.ok()) {
    lis_r.value()->close();
    return;
  }

  auto msg = c.value()->recv({});
  EXPECT_TRUE(!msg.ok());
  if (!msg.ok()) {
    EXPECT_EQ(msg.status().code(), duct::StatusCode::kProtocolError);
  }

  c.value()->close();
  lis_r.value()->close();
  auto sst = server.wait_for(std::chrono::seconds(2));
  if (sst == std::future_status::ready) (void)server.get();
#endif
}


// The first chunk of a reassembled shm message must also respect the 64 MiB
// cap. Previously only continuation chunks were checked, so a slot_bytes near
// 2^30 let a single first chunk allocate ~1 GiB before the cap was enforced.
void test_shm_first_chunk_size_capped() {
#if defined(_WIN32)
  return;
#else
  // slot_bytes just under 2^30 is a valid geometry but far above the 64 MiB
  // reassembly cap. A message that fits in one slot yet exceeds the cap must be
  // rejected at the first-chunk check, not allocated (~1 GiB) and reassembled.
  duct::ListenOptions listen_opt;
  listen_opt.shm.slot_bytes = (1u << 30) - (1u << 20);  // ~1 GiB payload slot
  listen_opt.shm.slot_count = 2;
  auto lis_r = duct::listen("shm://duct_testfirstcap", listen_opt);
  EXPECT_TRUE(lis_r.ok());
  if (!lis_r.ok()) return;

  auto server = std::async(std::launch::async, [&]() -> duct::Result<void> {
    auto p = lis_r.value()->accept();
    if (!p.ok()) return p.status();
    // Fits in one ~1 GiB slot but is > 64 MiB: the receiver's first-chunk cap
    // must fire before assembling the oversized message.
    const std::string payload(64 * 1024 * 1024 + 64 * 1024, 'f');
    auto st = p.value()->send(duct::Message::from_string(payload), {});
    if (!st.ok()) return st.status();
    return {};
  });

  duct::DialOptions dial_opt;
  dial_opt.shm.slot_bytes = (1u << 30) - (1u << 20);
  dial_opt.shm.slot_count = 2;
  dial_opt.qos.snd_hwm_bytes = 0;
  dial_opt.qos.rcv_hwm_bytes = 0;
  auto c = duct::dial("shm://duct_testfirstcap", dial_opt);
  EXPECT_TRUE(c.ok());
  if (!c.ok()) {
    lis_r.value()->close();
    return;
  }

  auto msg = c.value()->recv({});
  EXPECT_TRUE(!msg.ok());
  if (!msg.ok()) {
    EXPECT_EQ(msg.status().code(), duct::StatusCode::kProtocolError);
  }

  c.value()->close();
  lis_r.value()->close();
  auto sst = server.wait_for(std::chrono::seconds(2));
  if (sst == std::future_status::ready) (void)server.get();
#endif
}


void test_shm_fragmentation_midway_timeout_keeps_stream_synced() {
#if defined(_WIN32)
  return;
#else
  auto lis_r = duct::listen("shm://duct_testfrag_timeout");
  EXPECT_TRUE(lis_r.ok());
  if (!lis_r.ok()) return;

  auto server = std::async(std::launch::async, [&]() -> duct::Result<void> {
    auto p = lis_r.value()->accept();
    if (!p.ok()) return p.status();
    // Delay consumption so the client's short per-chunk timeout expires while
    // reassembly is still pending. The stream must remain synchronized.
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    auto msg = p.value()->recv({});
    if (!msg.ok()) return msg.status();
    return p.value()->send(msg.value(), {});
  });

  duct::DialOptions dial_opt;
  dial_opt.qos.snd_hwm_bytes = 0;
  dial_opt.qos.rcv_hwm_bytes = 0;
  auto c = duct::dial("shm://duct_testfrag_timeout", dial_opt);
  EXPECT_TRUE(c.ok());
  if (!c.ok()) return;

  const std::string payload(64 * 1024 + 123, 'x');
  duct::SendOptions so;
  so.timeout = std::chrono::milliseconds(50);
  auto st = c.value()->send(duct::Message::from_string(payload), so);
  EXPECT_TRUE(st.ok());
  if (!st.ok()) {
    c.value()->close();
    return;
  }

  auto echoed = c.value()->recv({});
  EXPECT_TRUE(echoed.ok());
  if (echoed.ok()) {
    EXPECT_EQ(echoed.value().size(), payload.size());
  }

  c.value()->close();
  auto sst = server.wait_for(std::chrono::seconds(2));
  if (sst == std::future_status::ready) {
    EXPECT_TRUE(server.get().ok());
  } else {
    EXPECT_TRUE(false);
  }
  lis_r.value()->close();
#endif
}


void test_shm_configurable_slot_geometry() {
  duct::ListenOptions listen_opt;
  listen_opt.shm.slot_bytes = 16;
  listen_opt.shm.slot_count = 2;
  auto lis_r = duct::listen("shm://duct_testgeom", listen_opt);
  EXPECT_TRUE(lis_r.ok());
  if (!lis_r.ok()) return;

  auto server = std::async(std::launch::async, [&]() -> duct::Result<void> {
    auto p = lis_r.value()->accept();
    if (!p.ok()) return p.status();
    auto msg = p.value()->recv({});
    if (!msg.ok()) return msg.status();
    return p.value()->send(msg.value(), {});
  });

  std::this_thread::sleep_for(std::chrono::milliseconds(10));
  duct::DialOptions dial_opt;
  dial_opt.shm.slot_bytes = 16;
  dial_opt.shm.slot_count = 2;
  auto c = duct::dial("shm://duct_testgeom", dial_opt);
  EXPECT_TRUE(c.ok());
  if (!c.ok()) {
    lis_r.value()->close();
    return;
  }

  auto st = c.value()->send(duct::Message::from_string("ok"), {});
  EXPECT_TRUE(st.ok());
  auto echoed = c.value()->recv({});
  EXPECT_TRUE(echoed.ok());
  if (echoed.ok()) {
    EXPECT_EQ(echoed.value().as_string_view(), "ok");
  }

  auto sst = server.wait_for(std::chrono::seconds(2));
  EXPECT_TRUE(sst == std::future_status::ready);
  if (sst == std::future_status::ready) {
    EXPECT_TRUE(server.get().ok());
  }
  c.value()->close();
  lis_r.value()->close();
}


void test_shm_geometry_mismatch_rejected() {
  duct::ListenOptions listen_opt;
  listen_opt.shm.slot_bytes = 32;
  listen_opt.shm.slot_count = 2;
  auto lis_r = duct::listen("shm://duct_testgeom_mismatch", listen_opt);
  EXPECT_TRUE(lis_r.ok());
  if (!lis_r.ok()) return;

  auto server = std::async(std::launch::async, [&]() -> duct::Result<std::unique_ptr<duct::Pipe>> {
    return lis_r.value()->accept();
  });

  std::this_thread::sleep_for(std::chrono::milliseconds(10));
  duct::DialOptions dial_opt;
  dial_opt.shm.slot_bytes = 16;  // dialer creates a smaller segment
  dial_opt.shm.slot_count = 2;
  auto c = duct::dial("shm://duct_testgeom_mismatch", dial_opt);
  EXPECT_TRUE(c.ok());
  if (c.ok()) c.value()->close();

  auto accepted = server.wait_for(std::chrono::seconds(2));
  EXPECT_TRUE(accepted == std::future_status::ready);
  if (accepted == std::future_status::ready) {
    auto p = server.get();
    EXPECT_TRUE(!p.ok());
  }
  lis_r.value()->close();
}


// Boundary geometry must be rejected up front instead of corrupting the ring:
// slot_bytes at the kLastChunkFlag boundary (2^30) collides with the last-chunk
// flag and an oversized slot_bytes (>= 2^32) must be rejected before narrowing.
// (A slot_count whose layout_size would overflow size_t cannot occur once
// slot_bytes is capped below 2^30, so that path is not reachable here.)
void test_shm_rejects_invalid_geometry() {
  duct::ListenOptions listen_opt;
  listen_opt.shm.slot_bytes = (1u << 30);  // == kLastChunkFlag boundary
  listen_opt.shm.slot_count = 2;
  auto lis_r = duct::listen("shm://duct_testgeom_invalid", listen_opt);
  EXPECT_TRUE(!lis_r.ok());
  if (lis_r.ok()) {
    lis_r.value()->close();
    return;
  }

  // slot_bytes > 4 GiB would be truncated by a narrowing cast before validation.
  listen_opt.shm.slot_bytes = static_cast<std::size_t>(1) << 32;
  listen_opt.shm.slot_count = 1;
  lis_r = duct::listen("shm://duct_testgeom_invalid", listen_opt);
  EXPECT_TRUE(!lis_r.ok());
  if (lis_r.ok()) {
    lis_r.value()->close();
    return;
  }

}


// Oversize messages must be rejected synchronously instead of being written
// past a slot (POSIX fragments, Windows has no fragmentation and returns
// kInvalidArgument from send).
void test_shm_rejects_oversize_message() {
  auto lis_r = duct::listen("shm://duct_oversize");
  EXPECT_TRUE(lis_r.ok());
  if (!lis_r.ok()) return;

  auto server = std::async(std::launch::async, [&]() -> duct::Result<void> {
    auto p = lis_r.value()->accept();
    if (!p.ok()) return p.status();
    auto msg = p.value()->recv({});
    if (!msg.ok()) return msg.status();
    return p.value()->send(msg.value(), {});
  });

  duct::DialOptions dial_opt;
  dial_opt.qos.snd_hwm_bytes = 0;
  dial_opt.qos.rcv_hwm_bytes = 0;
  auto c = duct::dial("shm://duct_oversize", dial_opt);
  EXPECT_TRUE(c.ok());
  if (!c.ok()) {
    lis_r.value()->close();
    return;
  }

  // 1 MiB is larger than the default 64 KiB slot. POSIX shm fragments across
  // slots and reassembles, so it accepts and echoes the payload intact; Windows
  // shm has no fragmentation and rejects it synchronously.
  const std::string payload(1024 * 1024, 'o');
#if defined(_WIN32)
  auto st = c.value()->send(duct::Message::from_string(payload), {});
  EXPECT_TRUE(!st.ok());
  if (!st.ok()) {
    EXPECT_EQ(st.status().code(), duct::StatusCode::kInvalidArgument);
  }
  c.value()->close();
  auto sst = server.wait_for(std::chrono::seconds(2));
  if (sst == std::future_status::ready) {
    auto sr = server.get();
    EXPECT_TRUE(!sr.ok());
    if (!sr.ok()) {
      const auto code = sr.status().code();
      EXPECT_TRUE(code == duct::StatusCode::kClosed || code == duct::StatusCode::kIoError);
    }
  }
#else
  EXPECT_TRUE(c.value()->send(duct::Message::from_string(payload), {}).ok());
  auto echoed = c.value()->recv({});
  EXPECT_TRUE(echoed.ok());
  if (echoed.ok()) EXPECT_EQ(echoed.value().size(), payload.size());

  c.value()->close();
  auto sst = server.wait_for(std::chrono::seconds(2));
  if (sst == std::future_status::ready) {
    EXPECT_TRUE(server.get().ok());
  } else {
    EXPECT_TRUE(false);
  }
#endif
  lis_r.value()->close();
}


void test_multi_process_kill9_restart() {
#if !defined(_WIN32)
  // Server child: listens on an ephemeral tcp port and echoes until killed.
  auto lis_r = duct::listen("tcp://127.0.0.1:0");
  EXPECT_TRUE(lis_r.ok());
  if (!lis_r.ok()) return;
  auto addr_r = lis_r.value()->local_address();
  EXPECT_TRUE(addr_r.ok());
  if (!addr_r.ok()) return;
  const std::string addr = addr_r.value();

  pid_t child = ::fork();
  if (child < 0) {
    EXPECT_TRUE(false);
    return;
  }
  if (child == 0) {
    // Child: accept and echo forever. Never returns; killed by SIGKILL.
    auto p = lis_r.value()->accept();
    if (!p.ok()) _exit(1);
    for (;;) {
      auto msg = p.value()->recv({});
      if (!msg.ok()) _exit(1);
      if (!p.value()->send(msg.value(), {}).ok()) _exit(1);
    }
  }

  // Parent: dial and echo cross-process.
  duct::DialOptions dial_opt;
  dial_opt.qos.snd_hwm_bytes = 0;
  dial_opt.qos.rcv_hwm_bytes = 0;
  auto c = duct::dial(addr, dial_opt);
  EXPECT_TRUE(c.ok());
  if (c.ok()) {
    duct::SendOptions sopt;
    sopt.timeout = std::chrono::milliseconds(500);
    auto st = c.value()->send(duct::Message::from_string("ping-1"), sopt);
    EXPECT_TRUE(st.ok());
    if (st.ok()) {
      auto echoed = c.value()->recv({});
      EXPECT_TRUE(echoed.ok());
      if (echoed.ok()) EXPECT_EQ(echoed.value().as_string_view(), "ping-1");
    }
  }

  // Kill the server with SIGKILL while the connection is still alive so the
  // child is blocked in recv and dies by signal deterministically. Then close
  // the parent pipe (the child is already gone; no EOF race).
  EXPECT_TRUE(::kill(child, SIGKILL) == 0);
  int status = 0;
  EXPECT_TRUE(::waitpid(child, &status, 0) == child);
  EXPECT_TRUE(WIFSIGNALED(status) && WTERMSIG(status) == SIGKILL);
  if (c.ok()) c.value()->close();

  // A fresh dial after restart works: start a second child on a new port.
  auto lis2_r = duct::listen("tcp://127.0.0.1:0");
  EXPECT_TRUE(lis2_r.ok());
  if (!lis2_r.ok()) return;
  auto addr2_r = lis2_r.value()->local_address();
  EXPECT_TRUE(addr2_r.ok());
  if (!addr2_r.ok()) return;
  const std::string addr2 = addr2_r.value();

  pid_t child2 = ::fork();
  if (child2 < 0) {
    EXPECT_TRUE(false);
    return;
  }
  if (child2 == 0) {
    auto p = lis2_r.value()->accept();
    if (!p.ok()) _exit(1);
    auto msg = p.value()->recv({});
    if (!msg.ok()) _exit(1);
    _exit(p.value()->send(msg.value(), {}).ok() ? 0 : 1);
  }

  auto c2 = duct::dial(addr2, dial_opt);
  EXPECT_TRUE(c2.ok());
  if (c2.ok()) {
    duct::SendOptions sopt;
    sopt.timeout = std::chrono::milliseconds(500);
    auto st = c2.value()->send(duct::Message::from_string("ping-2"), sopt);
    EXPECT_TRUE(st.ok());
    if (st.ok()) {
      auto echoed = c2.value()->recv({});
      EXPECT_TRUE(echoed.ok());
      if (echoed.ok()) EXPECT_EQ(echoed.value().as_string_view(), "ping-2");
    }
  }
  EXPECT_TRUE(::kill(child2, SIGKILL) == 0);
  EXPECT_TRUE(::waitpid(child2, &status, 0) == child2);
  if (c2.ok()) c2.value()->close();
  lis2_r.value()->close();
#endif
}

// A peer blocked in recv() with no timeout must observe close() and return
// kClosed instead of waiting forever. Closing one side's handles does not
// signal the peer's named semaphores, so close() sets a shared closed flag and
// posts wake tokens; the waiter re-checks after waking and reports kClosed
// once the ring is drained. Mirrors the Windows shm fix (same defect class).
void test_shm_recv_unblocks_on_peer_close() {
  struct WakeState {
    std::atomic<bool> entered{false};  // blocking recv about to be called
    std::atomic<bool> done{false};
    duct::StatusCode code = duct::StatusCode::kOk;
  };
  auto lis_r = duct::listen("shm://duct_shm_wake_close");
  EXPECT_TRUE(lis_r.ok());
  if (!lis_r.ok()) return;
  auto lis = std::make_shared<std::unique_ptr<duct::Listener>>(
      std::move(lis_r.value()));
  auto state = std::make_shared<WakeState>();

  std::thread waiter([lis, state]() {
    auto p = (*lis)->accept();
    if (!p.ok()) {
      state->code = p.status().code();
      state->done.store(true, std::memory_order_release);
      return;
    }
    // Consume one message first so the peer is provably connected before the
    // no-timeout recv starts; this removes the accept/handshake race.
    auto sync = p.value()->recv({.timeout = std::chrono::seconds(5)});
    if (!sync.ok()) {
      state->code = sync.status().code();
      state->done.store(true, std::memory_order_release);
      return;
    }
    state->entered.store(true, std::memory_order_release);
    auto m = p.value()->recv({});  // no timeout: must still unblock on close
    state->code = m.ok() ? duct::StatusCode::kOk : m.status().code();
    state->done.store(true, std::memory_order_release);
  });

  duct::DialOptions dial_opt;
  dial_opt.qos.snd_hwm_bytes = 0;
  dial_opt.qos.rcv_hwm_bytes = 0;
  auto c = duct::dial("shm://duct_shm_wake_close", dial_opt);
  EXPECT_TRUE(c.ok());
  if (!c.ok()) {
    waiter.detach();
    (*lis)->close();
    return;
  }

  auto st = c.value()->send(duct::Message::from_string("sync"), {});
  EXPECT_TRUE(st.ok());

  // Wait until the peer is about to enter the blocking recv, then close.
  for (int i = 0; i < 100 && !state->entered.load(std::memory_order_acquire);
       ++i) {
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
  }
  EXPECT_TRUE(state->entered.load(std::memory_order_acquire));
  std::this_thread::sleep_for(std::chrono::milliseconds(50));
  c.value()->close();  // must wake the accepted side's recv

  for (int i = 0; i < 80 && !state->done.load(std::memory_order_acquire); ++i) {
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
  }
  if (state->done.load(std::memory_order_acquire)) {
    waiter.join();
  } else {
    // Fail fast instead of hanging the suite on a stuck accept/recv thread.
    waiter.detach();
  }
  EXPECT_TRUE(state->done.load(std::memory_order_acquire));
  EXPECT_EQ(state->code, duct::StatusCode::kClosed);

  (*lis)->close();
}

// accept() must be interruptible by listener close(): the bootstrap wait used
// to block uninterruptibly (overlapped-free ConnectNamedPipe on Windows,
// ::accept(2) on POSIX), so close() could neither wake it nor run cleanly
// beside it.
void test_shm_accept_unblocks_on_listener_close() {
  auto lis_r = duct::listen("shm://duct_shm_accept_close");
  EXPECT_TRUE(lis_r.ok());
  if (!lis_r.ok()) return;

  // Deliberately heap-allocated: if accept() stays blocked (regression), the
  // leaked future avoids the std::async destructor joining the stuck thread
  // and hanging the suite; the process tears it down at exit.
  auto* acc = new std::future<duct::Result<std::unique_ptr<duct::Pipe>>>(
      std::async(std::launch::async, [&]() { return lis_r.value()->accept(); }));
  std::this_thread::sleep_for(std::chrono::milliseconds(200));

  lis_r.value()->close();
  auto st = acc->wait_for(std::chrono::seconds(3));
  EXPECT_TRUE(st == std::future_status::ready);
  if (st == std::future_status::ready) {
    auto r = acc->get();
    delete acc;
    EXPECT_TRUE(!r.ok());
    if (!r.ok()) {
      EXPECT_EQ(r.status().code(), duct::StatusCode::kClosed);
    }
  }
}


#if !defined(_WIN32)
// Snapshot of the shm bootstrap unix socket paths currently in /tmp. The
// bootstrap path is derived from an internal hash of the bus name, so tests
// that need it compare snapshots taken before and after listen().
static std::vector<std::string> list_duct_shm_bootstrap_socks() {
  std::vector<std::string> out;
  DIR* d = ::opendir("/tmp");
  if (!d) return out;
  while (struct dirent* e = ::readdir(d)) {
    const std::string name = e->d_name;
    if (name.rfind("duct_shm_", 0) == 0 && name.size() > 5 &&
        name.compare(name.size() - 5, 5, ".sock") == 0) {
      out.push_back("/tmp/" + name);
    }
  }
  ::closedir(d);
  return out;
}
#endif

// The connid handshake read after ::accept() must also be interruptible by
// listener close(): a silent client (connected to the bootstrap socket but
// never writing the connid) must not pin accept() forever. The bootstrap
// accept itself is poll-sliced; this covers the read on the accepted fd.
void test_shm_accept_connid_read_unblocks_on_listener_close() {
#if defined(_WIN32)
  return;
#else
  const auto before = list_duct_shm_bootstrap_socks();
  auto lis_r = duct::listen("shm://duct_shm_connid_close");
  EXPECT_TRUE(lis_r.ok());
  if (!lis_r.ok()) return;
  const auto after = list_duct_shm_bootstrap_socks();
  std::string boot;
  for (const auto& p : after) {
    if (std::find(before.begin(), before.end(), p) == before.end()) boot = p;
  }
  EXPECT_TRUE(!boot.empty());
  if (boot.empty()) {
    lis_r.value()->close();
    return;
  }

  // Silent client: connect but never write the 16-byte connid.
  int cfd = ::socket(AF_UNIX, SOCK_STREAM, 0);
  EXPECT_TRUE(cfd >= 0);
  if (cfd < 0) {
    lis_r.value()->close();
    return;
  }
  struct sockaddr_un addr {};
  addr.sun_family = AF_UNIX;
  std::strncpy(addr.sun_path, boot.c_str(), sizeof(addr.sun_path) - 1);
  EXPECT_TRUE(::connect(cfd, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) == 0);

  // Deliberately heap-allocated: if accept() stays blocked (regression), the
  // leaked future avoids the std::async destructor joining the stuck thread
  // and hanging the suite; the process tears it down at exit.
  auto* acc = new std::future<duct::Result<std::unique_ptr<duct::Pipe>>>(
      std::async(std::launch::async, [&]() { return lis_r.value()->accept(); }));
  std::this_thread::sleep_for(std::chrono::milliseconds(200));

  lis_r.value()->close();
  auto st = acc->wait_for(std::chrono::seconds(3));
  EXPECT_TRUE(st == std::future_status::ready);
  if (st == std::future_status::ready) {
    auto r = acc->get();
    delete acc;
    EXPECT_TRUE(!r.ok());
    if (!r.ok()) {
      EXPECT_EQ(r.status().code(), duct::StatusCode::kClosed);
    }
  }
  ::close(cfd);
#endif
}
