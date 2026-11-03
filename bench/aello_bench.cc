#include "duct/duct.h"

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <csignal>
#include <string>
#include <vector>

#if !defined(_WIN32)
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>
#endif

namespace {

struct Result {
  double latency_us = 0.0;      // mean round-trip latency (microseconds)
  double throughput_mps = 0.0;  // round-trips per second
  bool ok = false;
};

// Run a ping-pong benchmark against a child echo server. The child is forked;
// the parent drives N round-trips and reports latency + throughput.
Result bench_transport(const std::string& address, int messages, int payload_size) {
#if defined(_WIN32)
  return {};
#else
  int sync_pipe[2];
  int addr_pipe[2];
  if (::pipe(sync_pipe) != 0) return {};
  if (::pipe(addr_pipe) != 0) { ::close(sync_pipe[0]); ::close(sync_pipe[1]); return {}; }

  pid_t child = ::fork();
  if (child < 0) {
    ::close(sync_pipe[0]); ::close(sync_pipe[1]);
    ::close(addr_pipe[0]); ::close(addr_pipe[1]);
    return {};
  }
  if (child == 0) {
    // Child: listen, notify parent, accept, echo messages until closed.
    ::close(sync_pipe[0]);
    ::close(addr_pipe[0]);
    auto lis_r = duct::listen(address);
    if (!lis_r.ok()) _exit(1);
    // Publish the effective bound address (e.g. ephemeral port) to the parent.
    auto la = lis_r.value()->local_address();
    if (!la.ok()) _exit(1);
    const std::string bound = la.value();
    const std::size_t len = bound.size();
    if (::write(addr_pipe[1], &len, sizeof(len)) != static_cast<ssize_t>(sizeof(len))) _exit(1);
    if (::write(addr_pipe[1], bound.data(), len) != static_cast<ssize_t>(len)) _exit(1);
    char ready = 'R';
    if (::write(sync_pipe[1], &ready, 1) != 1) _exit(1);
    auto p = lis_r.value()->accept();
    if (!p.ok()) _exit(1);
    for (;;) {
      auto msg = p.value()->recv({});
      if (!msg.ok()) _exit(1);
      if (!p.value()->send(msg.value(), {}).ok()) _exit(1);
    }
  }

  ::close(sync_pipe[1]);
  ::close(addr_pipe[1]);
  char ready = 0;
  if (::read(sync_pipe[0], &ready, 1) != 1) {
    ::close(sync_pipe[0]);
    ::close(addr_pipe[0]);
    ::kill(child, SIGKILL);
    ::waitpid(child, nullptr, 0);
    return {};
  }
  ::close(sync_pipe[0]);

  // Read the effective bound address from the child.
  std::size_t len = 0;
  if (::read(addr_pipe[0], &len, sizeof(len)) != static_cast<ssize_t>(sizeof(len))) {
    ::close(addr_pipe[0]);
    ::kill(child, SIGKILL);
    ::waitpid(child, nullptr, 0);
    return {};
  }
  std::string bound(len, '\0');
  if (::read(addr_pipe[0], bound.data(), len) != static_cast<ssize_t>(len)) {
    ::close(addr_pipe[0]);
    ::kill(child, SIGKILL);
    ::waitpid(child, nullptr, 0);
    return {};
  }
  ::close(addr_pipe[0]);

  duct::DialOptions dial_opt;
  dial_opt.qos.snd_hwm_bytes = 0;
  dial_opt.qos.rcv_hwm_bytes = 0;
  auto c = duct::dial(bound, dial_opt);
  if (!c.ok()) {
    ::kill(child, SIGKILL);
    ::waitpid(child, nullptr, 0);
    return {};
  }

  const std::string payload(static_cast<std::size_t>(payload_size), 'x');
  const duct::Message msg = duct::Message::from_string(payload);

  // Warm-up: 10 round-trips to stabilize any lazy initialization.
  for (int i = 0; i < 10; ++i) {
    if (!c.value()->send(msg, {}).ok()) break;
    if (!c.value()->recv({}).ok()) break;
  }

  // Timing loop: measure total wall time for `messages` round-trips.
  const auto start = std::chrono::steady_clock::now();
  for (int i = 0; i < messages; ++i) {
    if (!c.value()->send(msg, {}).ok()) {
      c.value()->close();
      ::kill(child, SIGKILL);
      ::waitpid(child, nullptr, 0);
      return {};
    }
    auto r = c.value()->recv({});
    if (!r.ok()) {
      c.value()->close();
      ::kill(child, SIGKILL);
      ::waitpid(child, nullptr, 0);
      return {};
    }
  }
  const auto end = std::chrono::steady_clock::now();
  const double total_us = std::chrono::duration<double, std::micro>(end - start).count();
  const double total_s = total_us / 1e6;

  c.value()->close();
  ::kill(child, SIGKILL);
  ::waitpid(child, nullptr, 0);

  Result r;
  r.latency_us = total_us / messages;
  r.throughput_mps = messages / total_s;
  r.ok = true;
  return r;
#endif
}

}  // namespace

int main(int argc, char** argv) {
  int messages = 10000;
  int payload = 64;
  if (argc >= 2) messages = std::atoi(argv[1]);
  if (argc >= 3) payload = std::atoi(argv[2]);
  if (messages <= 0 || payload <= 0) {
    std::fprintf(stderr, "usage: %s [messages] [payload_bytes]\n", argv[0]);
    return 2;
  }

  const char* shm_name = "/aello_bench_shm";
  const char* uds_path = "/tmp/aello_bench_uds.sock";
  const std::string tcp_addr = "tcp://127.0.0.1:0";

  std::vector<std::pair<std::string, std::string>> targets = {
      {"shm", std::string("shm://") + shm_name},
      {"uds", std::string("uds://") + uds_path},
      {"tcp", tcp_addr},
  };

  for (auto& [name, address] : targets) {
    auto r = bench_transport(address, messages, payload);
    if (!r.ok) {
      std::printf("%-4s: FAILED\n", name.c_str());
      continue;
    }
    std::printf("%-4s: mean RTT %.2f us, throughput %.0f round-trips/s\n",
                name.c_str(), r.latency_us, r.throughput_mps);
  }
  return 0;
}
