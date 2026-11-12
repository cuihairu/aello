#include "test_support.h"
#include "pipe_tests.h"


void test_pipe_echo_one() {
#if !defined(_WIN32)
  auto lis_r = duct::listen("pipe://duct_testpipe_echo");
  EXPECT_TRUE(!lis_r.ok());
  if (!lis_r.ok()) {
    EXPECT_EQ(lis_r.status().code(), duct::StatusCode::kNotSupported);
  }
  return;
#else
  auto lis_r = duct::listen("pipe://duct_testpipe_echo");
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
  dial_opt.qos.snd_hwm_bytes = 0;
  dial_opt.qos.rcv_hwm_bytes = 0;
  auto c = duct::dial("pipe://duct_testpipe_echo", dial_opt);
  EXPECT_TRUE(c.ok());
  if (!c.ok()) return;

  auto st = c.value()->send(duct::Message::from_string("hello"), {});
  EXPECT_TRUE(st.ok());
  if (!st.ok()) return;

  duct::RecvOptions recv_opt;
  recv_opt.timeout = std::chrono::seconds(5);
  auto echoed = c.value()->recv(recv_opt);
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
#endif
}


void test_pipe_rejects_oversize_message() {
#if !defined(_WIN32)
  auto lis_r = duct::listen("pipe://duct_testpipe_oversize");
  EXPECT_TRUE(!lis_r.ok());
  if (!lis_r.ok()) {
    EXPECT_EQ(lis_r.status().code(), duct::StatusCode::kNotSupported);
  }
  return;
#else
  auto lis_r = duct::listen("pipe://duct_testpipe_oversize");
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

  // QoS enabled (default): an oversize pipe message must be rejected
  // synchronously by QosPipe::send() rather than enqueued and silently dropped
  // by the drain worker after the transport rejects it.
  duct::DialOptions dial_opt;
  dial_opt.qos.enabled = true;
  dial_opt.qos.snd_hwm_bytes = 4 * 1024 * 1024;
  auto c = duct::dial("pipe://duct_testpipe_oversize", dial_opt);
  EXPECT_TRUE(c.ok());
  if (!c.ok()) {
    // The dial failed; the accept thread may still be blocked in
    // ConnectNamedPipe. Close the listener to cancel it so the async future's
    // destructor does not join a permanently blocked accept thread (which would
    // hang the whole test process instead of reporting the failure).
    lis_r.value()->close();
    if (server.wait_for(std::chrono::seconds(2)) == std::future_status::ready) {
      server.get();
    }
    return;
  }

  const std::string payload(duct::wire::kMaxFramePayload + 1, 'x');
  auto st = c.value()->send(duct::Message::from_string(payload), {});
  EXPECT_TRUE(!st.ok());
  if (!st.ok()) {
    EXPECT_EQ(st.status().code(), duct::StatusCode::kInvalidArgument);
  }

  c.value()->close();
  // Generous window: under wine/emulated named pipes the server thread can be
  // slow to unwind after the client close, and a missed join here fails the
  // test spuriously.
  auto sst = server.wait_for(std::chrono::seconds(5));
  if (sst == std::future_status::ready) {
    auto sr = server.get();
    EXPECT_TRUE(!sr.ok());
    if (!sr.ok()) {
      EXPECT_EQ(sr.status().code(), duct::StatusCode::kClosed);
    }
  }

  lis_r.value()->close();
#endif
}


// A dial that fails (no server listening) must not leave a thread blocked in
// ConnectNamedPipe whose std::async future destructor joins forever: closing
// the listener cancels the pending accept.
void test_pipe_dial_failure_unblocks_accept() {
#if !defined(_WIN32)
  auto lis_r = duct::listen("pipe://duct_testpipe_noserver");
  EXPECT_TRUE(!lis_r.ok());
  if (!lis_r.ok()) {
    EXPECT_EQ(lis_r.status().code(), duct::StatusCode::kNotSupported);
  }
  return;
#else
  auto lis_r = duct::listen("pipe://duct_testpipe_noserver");
  EXPECT_TRUE(lis_r.ok());
  if (!lis_r.ok()) return;

  auto server = std::async(std::launch::async, [&]() -> duct::Result<void> {
    auto p = lis_r.value()->accept();
    if (!p.ok()) return p.status();
    return {};
  });

  // Wait until the accept thread is blocked in ConnectNamedPipe, then close the
  // listener. The accept must return kClosed promptly instead of hanging.
  std::this_thread::sleep_for(std::chrono::milliseconds(50));
  lis_r.value()->close();

  auto sst = server.wait_for(std::chrono::seconds(2));
  EXPECT_TRUE(sst == std::future_status::ready);
  if (sst == std::future_status::ready) {
    auto sr = server.get();
    EXPECT_TRUE(!sr.ok());
    if (!sr.ok()) {
      EXPECT_EQ(sr.status().code(), duct::StatusCode::kClosed);
    }
  }
#endif
}


// Several threads may call accept() concurrently. close() must cancel and close
// *every* pending accept, not just the most recently registered one, so no
// accept thread stays blocked in ConnectNamedPipe forever. All accept calls
// must return promptly with kClosed when the listener is closed.
void test_pipe_concurrent_accepts_unblock_on_close() {
#if !defined(_WIN32)
  auto lis_r = duct::listen("pipe://duct_testpipe_concurrent_accept");
  EXPECT_TRUE(!lis_r.ok());
  if (!lis_r.ok()) {
    EXPECT_EQ(lis_r.status().code(), duct::StatusCode::kNotSupported);
  }
  return;
#else
  auto lis_r = duct::listen("pipe://duct_testpipe_concurrent_accept");
  EXPECT_TRUE(lis_r.ok());
  if (!lis_r.ok()) return;

  // Raw threads (not std::async): a std::async future's destructor joins its
  // task, so a stuck accept thread would hang the whole test binary instead of
  // reporting a failure. A thread that misses the deadline is detached and the
  // missed deadline itself is the reported failure. The listener and the
  // per-thread state are shared-owned so a detached thread cannot outlive the
  // stack objects it still references.
  constexpr int kAcceptors = 4;
  struct AcceptState {
    std::atomic<bool> done[kAcceptors];
    duct::StatusCode codes[kAcceptors];
  };
  auto state = std::make_shared<AcceptState>();
  auto listener = std::shared_ptr<duct::Listener>(std::move(lis_r.value()));
  std::thread acceptors[kAcceptors];
  for (int i = 0; i < kAcceptors; ++i) {
    state->done[i].store(false, std::memory_order_relaxed);
    acceptors[i] = std::thread([listener, state, i] {
      auto p = listener->accept();
      state->codes[i] = p.ok() ? duct::StatusCode::kOk : p.status().code();
      state->done[i].store(true, std::memory_order_release);
    });
  }

  // Let every accept thread reach ConnectNamedPipe, then close the listener.
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  listener->close();

  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(3);
  int finished = 0;
  for (int i = 0; i < kAcceptors; ++i) {
    while (!state->done[i].load(std::memory_order_acquire) &&
           std::chrono::steady_clock::now() < deadline) {
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    if (state->done[i].load(std::memory_order_acquire)) {
      ++finished;
    }
  }
  EXPECT_TRUE(finished == kAcceptors);
  for (int i = 0; i < kAcceptors; ++i) {
    if (state->done[i].load(std::memory_order_acquire)) {
      acceptors[i].join();
      EXPECT_TRUE(state->codes[i] == duct::StatusCode::kClosed);
    } else {
      // Stuck past the deadline: detach instead of joining so the process
      // reports the failure rather than hanging on the join.
      acceptors[i].detach();
    }
  }
#endif
}


// A dial against a pipe name that never appears must fail with kTimeout (not
// kIoError) so callers can tell "peer absent within the deadline" apart from a
// transport failure, and the attempt must finish near the caller's deadline
// instead of running far past it.
void test_pipe_dial_absent_reports_timeout() {
#if !defined(_WIN32)
  auto r = duct::dial("pipe://duct_testpipe_absent", duct::DialOptions{});
  EXPECT_TRUE(!r.ok());
  if (!r.ok()) {
    EXPECT_EQ(r.status().code(), duct::StatusCode::kNotSupported);
  }
  return;
#else
  duct::DialOptions dial_opt;
  dial_opt.timeout = std::chrono::milliseconds(300);
  const auto begin = std::chrono::steady_clock::now();
  auto r = duct::dial("pipe://duct_testpipe_absent", dial_opt);
  const auto elapsed = std::chrono::steady_clock::now() - begin;
  EXPECT_TRUE(!r.ok());
  if (!r.ok()) {
    EXPECT_EQ(r.status().code(), duct::StatusCode::kTimeout);
  }
  // The dial must have waited for the peer (not failed instantly) and must
  // stay near the deadline; the upper bound is loose for slow CI machines.
  EXPECT_TRUE(elapsed >= std::chrono::milliseconds(250));
  EXPECT_TRUE(elapsed < std::chrono::seconds(2));
#endif
}


// A timeout that races with successful completion must not discard an
// already-completed read. If recv() reports a timeout, the frame must still be
// available to the next recv(); otherwise the header was consumed and the
// stream is desynchronized.
void test_pipe_timeout_completion_preserves_message() {
#if !defined(_WIN32)
  auto lis_r = duct::listen("pipe://duct_testpipe_timeout_race");
  EXPECT_TRUE(!lis_r.ok());
  if (!lis_r.ok()) {
    EXPECT_EQ(lis_r.status().code(), duct::StatusCode::kNotSupported);
  }
  return;
#else
  auto lis_r = duct::listen("pipe://duct_testpipe_timeout_race");
  EXPECT_TRUE(lis_r.ok());
  if (!lis_r.ok()) return;

  auto accepted = std::promise<duct::Result<std::unique_ptr<duct::Pipe>>>();
  auto accepted_fut = accepted.get_future();
  std::thread server([&] { accepted.set_value(lis_r.value()->accept()); });

  std::this_thread::sleep_for(std::chrono::milliseconds(50));
  duct::DialOptions dial_opt;
  dial_opt.qos.enabled = false;
  dial_opt.timeout = std::chrono::seconds(2);
  auto client = duct::dial("pipe://duct_testpipe_timeout_race", dial_opt);
  EXPECT_TRUE(client.ok());
  if (!client.ok()) {
    lis_r.value()->close();
    server.join();
    return;
  }
  auto server_pipe = accepted_fut.get();
  EXPECT_TRUE(server_pipe.ok());
  if (!server_pipe.ok()) {
    client.value()->close();
    lis_r.value()->close();
    server.join();
    return;
  }

  // Sample the completion-vs-timeout boundary. When recv() times out, the
  // message must remain readable; a timed-out operation that actually completed
  // would consume the header and turn the next read into a protocol error.
  // The sender delay is swept across the timeout so some iterations complete
  // the read and some time it out. The timeout is orders of magnitude larger
  // than the microsecond gap between the server's header and payload writes,
  // so a successful header read never leads to a spurious payload timeout
  // (which would legitimately poison the connection, not preserve the message).
  constexpr int kTimeoutMs = 500;
  for (int i = 0; i < 12; ++i) {
    const int delay_ms = i * 80;  // 0..880 ms, spread across the boundary
    std::thread sender([&] {
      std::this_thread::sleep_for(std::chrono::milliseconds(delay_ms));
      auto sr = server_pipe.value()->send(duct::Message::from_string("x"), {});
      EXPECT_TRUE(sr.ok());
    });

    duct::RecvOptions recv_opt;
    recv_opt.timeout = std::chrono::milliseconds(kTimeoutMs);
    auto first = client.value()->recv(recv_opt);
    sender.join();

    if (!first.ok()) {
      EXPECT_EQ(first.status().code(), duct::StatusCode::kTimeout);
      duct::RecvOptions retry_opt;
      retry_opt.timeout = std::chrono::seconds(2);
      auto retry = client.value()->recv(retry_opt);
      EXPECT_TRUE(retry.ok());
      if (retry.ok()) {
        EXPECT_EQ(std::string(reinterpret_cast<const char*>(retry.value().data()),
                              retry.value().size()), "x");
      }
    } else {
      EXPECT_EQ(std::string(reinterpret_cast<const char*>(first.value().data()),
                            first.value().size()), "x");
    }
  }

  client.value()->close();
  server_pipe.value()->close();
  lis_r.value()->close();
  server.join();
#endif
}


// Once the header of a frame has been written or read, a payload timeout leaves
// the pipe with a partial frame. The transport must poison the connection rather
// than letting the next message be interpreted as payload/header bytes.
void test_pipe_midframe_timeout_poisons_connection() {
#if !defined(_WIN32)
  auto lis_r = duct::listen("pipe://duct_testpipe_midframe_timeout");
  EXPECT_TRUE(!lis_r.ok());
  if (!lis_r.ok()) {
    EXPECT_EQ(lis_r.status().code(), duct::StatusCode::kNotSupported);
  }
  return;
#else
  auto lis_r = duct::listen("pipe://duct_testpipe_midframe_timeout");
  EXPECT_TRUE(lis_r.ok());
  if (!lis_r.ok()) return;

  auto accepted = std::promise<duct::Result<std::unique_ptr<duct::Pipe>>>();
  auto accepted_fut = accepted.get_future();
  std::thread server([&] { accepted.set_value(lis_r.value()->accept()); });

  std::this_thread::sleep_for(std::chrono::milliseconds(50));
  duct::DialOptions dial_opt;
  dial_opt.qos.enabled = false;
  dial_opt.timeout = std::chrono::seconds(2);
  auto client = duct::dial("pipe://duct_testpipe_midframe_timeout", dial_opt);
  EXPECT_TRUE(client.ok());
  if (!client.ok()) {
    lis_r.value()->close();
    server.join();
    return;
  }
  auto server_pipe = accepted_fut.get();
  EXPECT_TRUE(server_pipe.ok());
  if (!server_pipe.ok()) {
    client.value()->close();
    lis_r.value()->close();
    server.join();
    return;
  }

  // The server never reads. A 64 KiB payload plus the 16-byte header exceeds
  // the 64 KiB pipe buffer, so a write eventually times out after the header
  // has already been accepted.
  const std::string payload(duct::wire::kMaxFramePayload, 'x');
  duct::SendOptions send_opt;
  send_opt.timeout = std::chrono::milliseconds(100);
  bool saw_midframe_timeout = false;
  for (int i = 0; i < 8; ++i) {
    auto sent = client.value()->send(duct::Message::from_string(payload), send_opt);
    if (!sent.ok()) {
      EXPECT_EQ(sent.status().code(), duct::StatusCode::kIoError);
      saw_midframe_timeout = true;
      break;
    }
  }
  EXPECT_TRUE(saw_midframe_timeout);

  auto after = client.value()->send(duct::Message::from_string("after"), {});
  EXPECT_TRUE(!after.ok());
  if (!after.ok()) {
    EXPECT_EQ(after.status().code(), duct::StatusCode::kClosed);
  }

  client.value()->close();
  server_pipe.value()->close();
  lis_r.value()->close();
  server.join();
#endif
}

// A negative send/recv timeout must be rejected as kInvalidArgument instead of
// being cast to a ~49.7-day DWORD timeout (same class as the win_shm fix).
void test_pipe_rejects_negative_send_recv_timeout() {
#if !defined(_WIN32)
  auto lis_r = duct::listen("pipe://duct_testpipe_negtimeout");
  EXPECT_TRUE(!lis_r.ok());
  if (!lis_r.ok()) {
    EXPECT_EQ(lis_r.status().code(), duct::StatusCode::kNotSupported);
  }
  return;
#else
  auto lis_r = duct::listen("pipe://duct_testpipe_negtimeout");
  EXPECT_TRUE(lis_r.ok());
  if (!lis_r.ok()) return;

  auto server = std::async(std::launch::async, [&]() -> duct::Result<void> {
    auto p = lis_r.value()->accept();
    if (!p.ok()) return p.status();
    // Park until the client closes; no data is expected to flow.
    auto msg = p.value()->recv({});
    if (!msg.ok()) return msg.status();
    return {};
  });
  std::this_thread::sleep_for(std::chrono::milliseconds(10));

  // Raw transport dial (no QoS wrapper): the timeout reaches pipe_transport
  // directly instead of being replaced by the worker's own bound.
  auto c = duct::dial("pipe://duct_testpipe_negtimeout");
  EXPECT_TRUE(c.ok());
  if (!c.ok()) {
    lis_r.value()->close();
    if (server.wait_for(std::chrono::seconds(2)) == std::future_status::ready) {
      server.get();
    }
    return;
  }

  duct::SendOptions sopt;
  sopt.timeout = std::chrono::milliseconds(-1);
  auto st = c.value()->send(duct::Message::from_string("x"), sopt);
  EXPECT_TRUE(!st.ok());
  if (!st.ok()) {
    EXPECT_EQ(st.status().code(), duct::StatusCode::kInvalidArgument);
  }

  duct::RecvOptions ropt;
  ropt.timeout = std::chrono::milliseconds(-1);
  auto got = c.value()->recv(ropt);
  EXPECT_TRUE(!got.ok());
  if (!got.ok()) {
    EXPECT_EQ(got.status().code(), duct::StatusCode::kInvalidArgument);
  }

  c.value()->close();
  if (server.wait_for(std::chrono::seconds(2)) == std::future_status::ready) {
    server.get();  // expected kClosed
  }
  lis_r.value()->close();
#endif
}
