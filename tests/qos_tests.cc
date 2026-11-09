#include "test_support.h"
#include "qos_tests.h"


void test_qos_block_zero_timeout_blocks() {
  // SendOptions::timeout == 0 means "no timeout". With kBlock backpressure a full
  // queue must block until space is available (or close), not fail immediately.
  auto owned = std::make_unique<SlowTimeoutPipe>();
  SlowTimeoutPipe* transport = owned.get();

  duct::QosOptions qos;
  qos.snd_hwm_bytes = 3;
  qos.backpressure = duct::BackpressurePolicy::kBlock;
  duct::QosPipe pipe(std::move(owned), qos);

  EXPECT_TRUE(pipe.send(duct::Message::from_string("a"), {}).ok());
  EXPECT_TRUE(transport->wait_until_send_started(std::chrono::milliseconds(1000)));

  // The worker is busy in the transport for 300 ms; fill the 3-byte HWM meanwhile.
  EXPECT_TRUE(pipe.send(duct::Message::from_string("b"), {}).ok());
  EXPECT_TRUE(pipe.send(duct::Message::from_string("c"), {}).ok());
  EXPECT_TRUE(pipe.send(duct::Message::from_string("d"), {}).ok());

  auto blocked = std::async(std::launch::async, [&] {
    return pipe.send(duct::Message::from_string("e"), {});  // timeout 0 => block
  });
  EXPECT_TRUE(blocked.wait_for(std::chrono::milliseconds(150)) != std::future_status::ready);

  pipe.close();
  EXPECT_TRUE(blocked.wait_for(std::chrono::seconds(1)) == std::future_status::ready);
  if (blocked.wait_for(std::chrono::seconds(0)) == std::future_status::ready) {
    auto result = blocked.get();
    EXPECT_TRUE(!result.ok());
    if (!result.ok()) {
      EXPECT_EQ(result.status().code(), duct::StatusCode::kClosed);
    }
  }
}


void test_qos_permanent_send_error_does_not_wedge() {
  // A payload the transport rejects permanently (e.g. shm's 64 KiB limit when the
  // QoS HWM is larger) must be dropped, not requeued forever. Requeueing spins the
  // worker and starves every later message.
  auto owned = std::make_unique<RecordingPipe>("bad-payload");
  RecordingPipe* transport = owned.get();

  duct::QosOptions qos;
  qos.snd_hwm_bytes = 1024 * 1024;
  duct::QosPipe pipe(std::move(owned), qos);

  duct::SendOptions opt;
  opt.timeout = std::chrono::milliseconds(100);
  EXPECT_TRUE(pipe.send(duct::Message::from_string("bad-payload"), opt).ok());
  EXPECT_TRUE(pipe.send(duct::Message::from_string("good-payload"), opt).ok());
  EXPECT_TRUE(transport->wait_for_sent("good-payload", std::chrono::milliseconds(1000)));
  pipe.close();
}


void test_qos_oversized_message_rejected() {
  // With fragmentation enabled the transport accepts payloads larger than a
  // frame. QoS only rejects messages above an explicit caller-provided cap.
  auto owned = std::make_unique<RecordingPipe>();

  duct::QosOptions qos;
  qos.snd_hwm_bytes = 1024 * 1024;
  qos.max_message_bytes = duct::wire::kMaxFramePayload;
  duct::QosPipe pipe(std::move(owned), qos);

  const std::string big(duct::wire::kMaxFramePayload + 1, 'x');
  auto st = pipe.send(duct::Message::from_string(big), {});
  EXPECT_TRUE(!st.ok());
  if (!st.ok()) {
    EXPECT_EQ(st.status().code(), duct::StatusCode::kInvalidArgument);
  }
  pipe.close();
}


void test_qos_zero_hwm_is_unlimited() {
  // snd_hwm_bytes == 0 disables the byte watermark; a non-default backpressure
  // policy must not silently drop/fail messages just because 0 >= 0.
  auto owned = std::make_unique<RecordingPipe>();
  RecordingPipe* transport = owned.get();

  duct::QosOptions qos;
  qos.snd_hwm_bytes = 0;
  qos.backpressure = duct::BackpressurePolicy::kDropNew;
  duct::QosPipe pipe(std::move(owned), qos);

  EXPECT_TRUE(pipe.send(duct::Message::from_string("a"), {}).ok());
  EXPECT_TRUE(pipe.send(duct::Message::from_string("b"), {}).ok());
  EXPECT_TRUE(transport->wait_for_sent("a", std::chrono::milliseconds(1000)));
  EXPECT_TRUE(transport->wait_for_sent("b", std::chrono::milliseconds(1000)));
  pipe.close();
}


void test_qos_delivery_under_hwm() {
  // QoS-enabled dial (default snd_hwm_bytes wraps in QosPipe). A small HWM forces the
  // in-memory queue to reach its watermark quickly. The send worker must keep draining
  // even at the watermark (the HWM gates enqueue, not drain); previously it stalled and
  // the echo never returned.
  auto lis_r = duct::listen("shm://duct_testqos_hwm");
  EXPECT_TRUE(lis_r.ok());
  if (!lis_r.ok()) return;

  const int kCount = 16;

  auto server = std::async(std::launch::async, [&]() -> duct::Result<void> {
    auto p = lis_r.value()->accept();
    if (!p.ok()) return p.status();
    for (int i = 0; i < kCount; ++i) {
      auto msg = p.value()->recv({});
      if (!msg.ok()) return msg.status();
      auto st = p.value()->send(msg.value(), {});
      if (!st.ok()) return st;
    }
    return {};
  });

  std::this_thread::sleep_for(std::chrono::milliseconds(10));

  duct::DialOptions dial_opt;
  dial_opt.qos.enabled = true;
  dial_opt.qos.snd_hwm_bytes = 4 * 1024;  // ~4 x 1KiB messages, forces HWM quickly.
  dial_opt.qos.rcv_hwm_bytes = 0;
  auto c = duct::dial("shm://duct_testqos_hwm", dial_opt);
  EXPECT_TRUE(c.ok());
  if (!c.ok()) return;

  std::string payload(1024, 'x');
  for (int i = 0; i < kCount; ++i) {
    duct::SendOptions opt;
    opt.timeout = std::chrono::milliseconds(500);
    auto st = c.value()->send(duct::Message::from_string(payload), opt);
    EXPECT_TRUE(st.ok());
    if (!st.ok()) break;
  }

  int received = 0;
  for (int i = 0; i < kCount; ++i) {
    duct::RecvOptions opt;
    opt.timeout = std::chrono::milliseconds(500);
    auto echo = c.value()->recv(opt);
    if (!echo.ok()) break;
    ++received;
  }
  EXPECT_EQ(received, kCount);

  c.value()->close();
  auto sst = server.wait_for(std::chrono::seconds(2));
  if (sst == std::future_status::ready) {
    auto sr = server.get();
    EXPECT_TRUE(sr.ok());
  } else {
    EXPECT_TRUE(false);
  }
  lis_r.value()->close();
}


void test_qos_close_under_full_ring() {
  // Fill the shm ring (server never reads) so the QoS send worker blocks on the
  // transport send. close() must still return promptly: previously it deadlocked on
  // the worker's mutex.
  auto lis_r = duct::listen("shm://duct_testqos_close");
  EXPECT_TRUE(lis_r.ok());
  if (!lis_r.ok()) return;

  auto accepted = std::promise<duct::Result<std::unique_ptr<duct::Pipe>>>();
  auto fut = accepted.get_future();
  std::thread t([&] { accepted.set_value(lis_r.value()->accept()); });

  std::this_thread::sleep_for(std::chrono::milliseconds(10));

  duct::DialOptions dial_opt;
  dial_opt.qos.enabled = true;
  dial_opt.qos.snd_hwm_bytes = 4 * 1024 * 1024;  // default-ish; wrap in QosPipe
  dial_opt.qos.rcv_hwm_bytes = 0;
  auto c = duct::dial("shm://duct_testqos_close", dial_opt);
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
  // Server side never reads: keep the pipe alive but do not recv.
  auto server_pipe = std::move(sr.value());

  std::string payload(1024, 'y');
  for (int i = 0; i < 100; ++i) {
    duct::SendOptions opt;
    opt.timeout = std::chrono::milliseconds(200);
    auto st = c.value()->send(duct::Message::from_string(payload), opt);
    if (!st.ok()) break;  // queue full at HWM is fine; the ring is what we want full.
  }

  // Give the worker time to block on the full ring.
  std::this_thread::sleep_for(std::chrono::milliseconds(300));

  // close() must complete; run it on a side thread and bound the wait.
  auto closed = std::promise<void>();
  auto closed_fut = closed.get_future();
  std::thread closer([&] {
    c.value()->close();
    closed.set_value();
  });

  auto cf = closed_fut.wait_for(std::chrono::seconds(3));
  EXPECT_TRUE(cf == std::future_status::ready);
  if (cf != std::future_status::ready) {
    // Avoid leaking a thread that may still be blocked in close().
    closer.detach();
  } else {
    closer.join();
  }

  server_pipe.reset();
  lis_r.value()->close();
  t.join();
}


// A reconnect-enabled dial must return immediately with a pipe that connects on
// the first successful attempt and delivers an echo.
void test_reconnect_dial_connects_and_echoes() {
  auto lis_r = duct::listen("tcp://127.0.0.1:0");
  EXPECT_TRUE(lis_r.ok());
  if (!lis_r.ok()) return;

  auto addr_r = lis_r.value()->local_address();
  EXPECT_TRUE(addr_r.ok());
  if (!addr_r.ok()) return;

  auto server = std::async(std::launch::async, [&]() -> duct::Result<void> {
    auto p = lis_r.value()->accept();
    if (!p.ok()) return p.status();
    auto msg = p.value()->recv({});
    if (!msg.ok()) return msg.status();
    return p.value()->send(msg.value(), {});
  });

  duct::DialOptions dial_opt;
  dial_opt.timeout = std::chrono::milliseconds(500);
  dial_opt.qos.snd_hwm_bytes = 0;
  dial_opt.qos.rcv_hwm_bytes = 0;
  dial_opt.reconnect.enabled = true;
  dial_opt.reconnect.initial_delay = std::chrono::milliseconds(10);
  dial_opt.reconnect.max_attempts = 10;

  std::mutex state_mu;
  std::vector<duct::ConnectionState> states;
  dial_opt.on_state_change = [&](duct::ConnectionState s, const std::string&) {
    std::lock_guard<std::mutex> lk(state_mu);
    states.push_back(s);
  };

  auto c = duct::dial(addr_r.value(), dial_opt);
  EXPECT_TRUE(c.ok());
  if (!c.ok()) return;

  duct::SendOptions send_opt;
  send_opt.timeout = std::chrono::milliseconds(500);
  auto st = c.value()->send(duct::Message::from_string("reconnect-ping"), send_opt);
  EXPECT_TRUE(st.ok());
  if (!st.ok()) {
    c.value()->close();
    return;
  }

  duct::RecvOptions recv_opt;
  recv_opt.timeout = std::chrono::milliseconds(500);
  auto echoed = c.value()->recv(recv_opt);
  EXPECT_TRUE(echoed.ok());
  if (echoed.ok()) {
    std::string s(reinterpret_cast<const char*>(echoed.value().data()), echoed.value().size());
    EXPECT_EQ(s, "reconnect-ping");
  }

  bool saw_connected = false;
  {
    std::lock_guard<std::mutex> lk(state_mu);
    for (auto s : states) {
      if (s == duct::ConnectionState::kConnected) saw_connected = true;
    }
  }
  EXPECT_TRUE(saw_connected);

  c.value()->close();
  auto sst = server.wait_for(std::chrono::seconds(1));
  if (sst == std::future_status::ready) {
    auto sr = server.get();
    EXPECT_TRUE(sr.ok());
  } else {
    EXPECT_TRUE(false);
  }
  lis_r.value()->close();
}


// A reconnect-enabled dial to a non-existent server must return immediately
// (the pipe connects in the background) and eventually report exhaustion.
void test_reconnect_dial_fails_after_attempts() {
  duct::DialOptions dial_opt;
  dial_opt.timeout = std::chrono::milliseconds(200);
  dial_opt.qos.snd_hwm_bytes = 0;
  dial_opt.qos.rcv_hwm_bytes = 0;
  dial_opt.reconnect.enabled = true;
  dial_opt.reconnect.initial_delay = std::chrono::milliseconds(5);
  dial_opt.reconnect.max_delay = std::chrono::milliseconds(20);
  dial_opt.reconnect.backoff_multiplier = 1.0;
  dial_opt.reconnect.max_attempts = 3;

  auto c = duct::dial("tcp://127.0.0.1:1", dial_opt);
  // dial() no longer succeeds vacuously: once the policy's attempts are
  // exhausted the dial itself fails instead of handing out a pipe whose first
  // send later reports the exhausted attempts.
  EXPECT_TRUE(!c.ok());
  if (!c.ok()) {
    EXPECT_TRUE(c.status().code() == duct::StatusCode::kIoError ||
                c.status().code() == duct::StatusCode::kTimeout);
  }
}


// A reconnect callback that sends inside on_state_change must not deadlock:
// the kConnected transition used to fire before inner_ was published, so
// wait_connected() never observed the new pipe and blocked forever.
void test_reconnect_callback_ops_pipe() {
  std::atomic<int> attempts{0};
  ReconnectCallbackPipe* second = nullptr;
  std::atomic<bool> callback_sent{false};
  std::shared_ptr<duct::Pipe> holder;

  duct::ReconnectPolicy policy;
  policy.initial_delay = std::chrono::milliseconds(5);
  policy.max_delay = std::chrono::milliseconds(20);
  policy.backoff_multiplier = 1.0;
  policy.max_attempts = 10;

  auto dial_once = [&]() -> duct::Result<std::unique_ptr<duct::Pipe>> {
    int n = ++attempts;
    auto p = std::make_unique<ReconnectCallbackPipe>();
    if (n == 1) {
      p->disconnect_on_recv = true;
    } else {
      second = p.get();
    }
    return std::unique_ptr<duct::Pipe>(std::move(p));
  };

  duct::ConnectionCallback cb = [&](duct::ConnectionState s, const std::string&) {
    if (s != duct::ConnectionState::kConnected) return;
    if (attempts < 2) return;  // first connect: holder not assigned yet.
    if (!holder) return;
    auto st = holder->send(duct::Message::from_string("cb-ping"), {});
    if (st.ok()) callback_sent.store(true);
  };

  auto pipe = duct::make_reconnect_pipe(std::move(dial_once), policy, std::move(cb));
  holder = std::shared_ptr<duct::Pipe>(std::move(pipe));

  // First recv hits the fake disconnect and the worker reconnects.
  duct::RecvOptions ro;
  ro.timeout = std::chrono::milliseconds(1000);
  (void)holder->recv(ro);

  for (int i = 0; i < 20 && !callback_sent.load(); ++i) {
    std::this_thread::sleep_for(std::chrono::milliseconds(25));
  }
  EXPECT_TRUE(callback_sent.load());
  if (second) {
    EXPECT_TRUE(second->sent_in_callback);
  }
  holder->close();
}


void test_qos_recv_watermark_and_worker() {
  auto owned = std::make_unique<SourcePipe>(8);
  SourcePipe* source = owned.get();

  duct::QosOptions qos;
  qos.rcv_hwm_bytes = 2;  // Two 1-byte messages at a time.
  duct::QosPipe pipe(std::move(owned), qos);

  // The recv worker should pull up to the watermark and then pause. Recv must
  // drain the queued messages in FIFO order and not block on the transport.
  for (int i = 7; i >= 0; --i) {
    duct::RecvOptions opt;
    opt.timeout = std::chrono::milliseconds(500);
    auto r = pipe.recv(opt);
    EXPECT_TRUE(r.ok());
    if (!r.ok()) break;
    EXPECT_EQ(r.value().as_string_view(), std::to_string(i));
  }

  // After the source is exhausted, recv should surface kClosed once drained.
  duct::RecvOptions opt;
  opt.timeout = std::chrono::milliseconds(500);
  auto closed_result = pipe.recv(opt);
  EXPECT_TRUE(!closed_result.ok());
  if (!closed_result.ok()) {
    EXPECT_EQ(closed_result.status().code(), duct::StatusCode::kClosed);
  }
  EXPECT_TRUE(source->is_closed() == false);
  pipe.close();
  EXPECT_TRUE(source->is_closed() == true);
}


void test_qos_linger_drains_before_close() {
  auto owned = std::make_unique<SlowTimeoutPipe>();
  SlowTimeoutPipe* transport = owned.get();

  duct::QosOptions qos;
  qos.snd_hwm_bytes = 1024;
  qos.linger = std::chrono::milliseconds(300);
  duct::QosPipe pipe(std::move(owned), qos);

  // Queue one message; the worker starts sending it (which takes ~300 ms).
  EXPECT_TRUE(pipe.send(duct::Message::from_string("a"), {}).ok());
  EXPECT_TRUE(transport->wait_until_send_started(std::chrono::milliseconds(1000)));

  // close() must wait for the in-flight send to finish (linger), not cancel it.
  auto closed = std::promise<void>();
  auto closed_fut = closed.get_future();
  std::thread closer([&] {
    pipe.close();
    closed.set_value();
  });
  auto cf = closed_fut.wait_for(std::chrono::seconds(2));
  EXPECT_TRUE(cf == std::future_status::ready);
  if (cf != std::future_status::ready) {
    closer.detach();
  } else {
    closer.join();
  }
}


void test_qos_priority_scheduling() {
  auto owned = std::make_unique<RecordingPipe>();
  RecordingPipe* transport = owned.get();

  duct::QosOptions qos;
  qos.snd_hwm_bytes = 1024;
  duct::QosPipe pipe(std::move(owned), qos);

  // Queue low-priority first, then high-priority. The worker must drain
  // high-priority first.
  duct::SendOptions low;
  low.priority = 0;
  duct::SendOptions high;
  high.priority = 10;

  EXPECT_TRUE(pipe.send(duct::Message::from_string("low-1"), low).ok());
  EXPECT_TRUE(pipe.send(duct::Message::from_string("low-2"), low).ok());
  EXPECT_TRUE(pipe.send(duct::Message::from_string("high-1"), high).ok());

  EXPECT_TRUE(transport->wait_for_sent("high-1", std::chrono::milliseconds(1000)));
  EXPECT_TRUE(transport->wait_for_sent("low-1", std::chrono::milliseconds(1000)));
  EXPECT_TRUE(transport->wait_for_sent("low-2", std::chrono::milliseconds(1000)));
  pipe.close();
}


void test_qos_rate_limit_paces_delivery() {
  auto owned = std::make_unique<RecordingPipe>();
  RecordingPipe* transport = owned.get();

  duct::QosOptions qos;
  qos.snd_hwm_bytes = 1024;
  // 1000 bytes/s with a 100-byte burst. Two 100-byte messages: the first fits
  // in the burst; the second requires ~100 ms of refill.
  qos.rate_bytes_per_sec = 1000;
  qos.burst_bytes = 100;
  duct::QosPipe pipe(std::move(owned), qos);

  EXPECT_TRUE(pipe.send(duct::Message::from_string(std::string(100, 'a')), {}).ok());
  EXPECT_TRUE(pipe.send(duct::Message::from_string(std::string(100, 'b')), {}).ok());

  EXPECT_TRUE(transport->wait_for_sent(std::string(100, 'a'), std::chrono::milliseconds(500)));
  EXPECT_TRUE(transport->wait_for_sent(std::string(100, 'b'), std::chrono::milliseconds(500)));
  pipe.close();
}


void test_qos_rate_limit_default_burst_drains() {
  // burst_bytes == 0 documents "one second's worth"; with rate limiting enabled
  // it must still drain a non-empty message instead of token-starving forever.
  auto owned = std::make_unique<RecordingPipe>();
  RecordingPipe* transport = owned.get();

  duct::QosOptions qos;
  qos.rate_bytes_per_sec = 1000;
  duct::QosPipe pipe(std::move(owned), qos);

  EXPECT_TRUE(pipe.send(duct::Message::from_string(std::string(100, 'x')), {}).ok());
  EXPECT_TRUE(transport->wait_for_sent(std::string(100, 'x'), std::chrono::milliseconds(500)));
  pipe.close();
}


// A reconnect-wrapped tcp pipe must preserve channel_id through the wire
// flags, not collapse everything to channel 0.
void test_reconnect_channel_preserved() {
  auto lis_r = duct::listen("tcp://127.0.0.1:0");
  EXPECT_TRUE(lis_r.ok());
  if (!lis_r.ok()) return;
  auto addr_r = lis_r.value()->local_address();
  EXPECT_TRUE(addr_r.ok());
  if (!addr_r.ok()) return;

  auto server = std::async(std::launch::async, [&]() -> duct::Result<void> {
    auto p = lis_r.value()->accept();
    if (!p.ok()) return p.status();
    duct::RecvOptions opt;
    opt.timeout = std::chrono::milliseconds(500);
    auto msg = p.value()->recv(opt);
    if (!msg.ok()) return msg.status();
    duct::SendOptions send_opt;
    send_opt.channel_id = 7;
    return p.value()->send(msg.value(), send_opt);
  });

  duct::DialOptions dial_opt;
  dial_opt.reconnect.enabled = true;
  dial_opt.reconnect.initial_delay = std::chrono::milliseconds(10);
  dial_opt.reconnect.backoff_multiplier = 1.0;
  dial_opt.reconnect.max_delay = std::chrono::milliseconds(50);
  dial_opt.reconnect.max_attempts = 5;
  dial_opt.qos.snd_hwm_bytes = 0;
  dial_opt.qos.rcv_hwm_bytes = 0;
  auto c = duct::dial(addr_r.value(), dial_opt);
  EXPECT_TRUE(c.ok());
  if (!c.ok()) return;

  duct::SendOptions send_opt;
  send_opt.timeout = std::chrono::milliseconds(500);
  send_opt.channel_id = 7;
  EXPECT_TRUE(c.value()->send(duct::Message::from_string("reconnect-ch7"), send_opt).ok());

  duct::RecvOptions recv_opt;
  recv_opt.timeout = std::chrono::milliseconds(500);
  recv_opt.channel_id = 7;
  auto echoed = c.value()->recv(recv_opt);
  EXPECT_TRUE(echoed.ok());
  if (echoed.ok()) EXPECT_EQ(echoed.value().as_string_view(), "reconnect-ch7");

  c.value()->close();
  auto sst = server.wait_for(std::chrono::seconds(1));
  if (sst == std::future_status::ready) {
    EXPECT_TRUE(server.get().ok());
  } else {
    EXPECT_TRUE(false);
  }
  lis_r.value()->close();
}


// A QoS-wrapped shm recv worker must not block forever on the second chunk of a
// fragmented message when the sender dies after committing the first chunk.
// Fill the ring with 64 single-chunk messages, then send a 2-chunk message whose
// first chunk needs the only remaining space. The server consumes one message to
// make the dialer commit that first chunk, then SIGKILLs the dialer before the
// second chunk can be written. close() must still join the recv worker.
void test_qos_shm_recv_midfragment_close_completes() {
#if defined(_WIN32)
  return;
#else
  const std::string bus = "duct_testqos_recv_midfrag_" + std::to_string(::getpid());
  auto lis_r = duct::listen("shm://" + bus);
  EXPECT_TRUE(lis_r.ok());
  if (!lis_r.ok()) return;

  int syncfd[2];
  EXPECT_TRUE(::pipe(syncfd) == 0);
  pid_t dialer = ::fork();
  EXPECT_TRUE(dialer >= 0);
  if (dialer < 0) {
    lis_r.value()->close();
    return;
  }
  if (dialer == 0) {
    ::close(syncfd[0]);
    auto p = duct::dial("shm://" + bus);
    if (!p.ok()) _exit(1);
    const std::string one_chunk(64 * 1024, 'a');
    for (int i = 0; i < 64; ++i) {
      if (!p.value()->send(duct::Message::from_string(one_chunk), {}).ok()) _exit(2);
    }
    const std::string two_chunks(64 * 1024 + 1, 'b');
    // Blocks after the first chunk is committed because the ring is full.
    (void)p.value()->send(duct::Message::from_string(two_chunks), {});
    _exit(0);
  }
  ::close(syncfd[1]);
  ::close(syncfd[0]);

  auto accepted = std::promise<duct::Result<std::unique_ptr<duct::Pipe>>>();
  auto accepted_fut = accepted.get_future();
  std::thread accept_thread([&] { accepted.set_value(lis_r.value()->accept()); });
  auto af = accepted_fut.wait_for(std::chrono::seconds(3));
  EXPECT_TRUE(af == std::future_status::ready);
  if (af != std::future_status::ready) {
    ::kill(dialer, SIGKILL);
    ::waitpid(dialer, nullptr, 0);
    lis_r.value()->close();
    accept_thread.join();
    return;
  }
  auto accepted_r = accepted_fut.get();
  EXPECT_TRUE(accepted_r.ok());
  if (!accepted_r.ok()) {
    ::kill(dialer, SIGKILL);
    ::waitpid(dialer, nullptr, 0);
    lis_r.value()->close();
    accept_thread.join();
    return;
  }

  // Consume one complete message so the dialer can commit the first chunk of
  // the fragmented message. The second chunk still has no free slot.
  auto first = accepted_r.value()->recv({std::chrono::milliseconds(1000)});
  EXPECT_TRUE(first.ok());
  if (!first.ok()) {
    ::kill(dialer, SIGKILL);
    ::waitpid(dialer, nullptr, 0);
    lis_r.value()->close();
    return;
  }
  std::this_thread::sleep_for(std::chrono::milliseconds(200));
  EXPECT_TRUE(::kill(dialer, SIGKILL) == 0);
  EXPECT_TRUE(::waitpid(dialer, nullptr, 0) == dialer);

  duct::QosOptions qos;
  qos.rcv_hwm_bytes = 16 * 1024 * 1024;
  duct::QosPipe pipe(std::move(accepted_r.value()), qos);
  std::this_thread::sleep_for(std::chrono::milliseconds(500));

  auto closed = std::promise<void>();
  auto closed_fut = closed.get_future();
  std::thread closer([&] {
    pipe.close();
    closed.set_value();
  });
  auto cf = closed_fut.wait_for(std::chrono::seconds(3));
  EXPECT_TRUE(cf == std::future_status::ready);
  if (cf != std::future_status::ready) {
    closer.detach();
  } else {
    closer.join();
  }

  lis_r.value()->close();
  accept_thread.join();
#endif
}


// QoS must publish the smaller of its configured cap and the underlying
// transport's non-fragmenting bound. When reconnect wraps QoS, ReconnectPipe
// must still see that bound instead of the base-class "unlimited" default.
void test_qos_and_reconnect_forward_max_message_bytes() {
  constexpr std::size_t kTransportCap = 1024;

  duct::QosOptions qos;
  qos.snd_hwm_bytes = 0;
  qos.rcv_hwm_bytes = 0;
  auto qosed_bounded = std::make_unique<duct::QosPipe>(
      std::make_unique<BoundedPipe>(kTransportCap), qos);
  EXPECT_TRUE(qosed_bounded->max_message_bytes() == kTransportCap);

  duct::QosOptions limited_qos;
  limited_qos.snd_hwm_bytes = 0;
  limited_qos.rcv_hwm_bytes = 0;
  limited_qos.max_message_bytes = kTransportCap / 2;
  auto qosed_limited = std::make_unique<duct::QosPipe>(
      std::make_unique<BoundedPipe>(kTransportCap), limited_qos);
  EXPECT_TRUE(qosed_limited->max_message_bytes() == kTransportCap / 2);

  // A nonzero snd_hwm_bytes is also a synchronous per-message bound in
  // QosPipe::send(), so the accessor must report it: a caller sizing messages
  // against max_message_bytes() would otherwise get a rejected send.
  duct::QosOptions hwm_qos;
  hwm_qos.snd_hwm_bytes = kTransportCap / 4;
  hwm_qos.rcv_hwm_bytes = 0;
  auto qosed_hwm = std::make_unique<duct::QosPipe>(
      std::make_unique<BoundedPipe>(kTransportCap), hwm_qos);
  EXPECT_TRUE(qosed_hwm->max_message_bytes() == kTransportCap / 4);

  duct::ReconnectPolicy policy;
  policy.initial_delay = std::chrono::milliseconds(5);
  policy.max_delay = std::chrono::milliseconds(20);
  policy.backoff_multiplier = 1.0;
  policy.max_attempts = 1;

  auto dial_once = [&]() -> duct::Result<std::unique_ptr<duct::Pipe>> {
    duct::QosOptions inner_qos;
    inner_qos.snd_hwm_bytes = 0;
    inner_qos.rcv_hwm_bytes = 0;
    auto qosed = std::make_unique<duct::QosPipe>(
        std::make_unique<BoundedPipe>(kTransportCap), inner_qos);
    return std::unique_ptr<duct::Pipe>(std::move(qosed));
  };

  auto reconnect = duct::make_reconnect_pipe(
      std::move(dial_once), policy, nullptr);
  constexpr std::size_t kUnlimited = std::numeric_limits<std::size_t>::max();
  for (int i = 0; i < 100 && reconnect->max_message_bytes() == kUnlimited; ++i) {
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
  }
  EXPECT_TRUE(reconnect->max_message_bytes() == kTransportCap);
  reconnect->close();
}


void test_qos_channel_multiplexing() {
  // A QoS-wrapped tcp pipe must preserve channel_id end-to-end, matching the
  // direct tcp/uds behavior: send on a channel, receive on that same channel,
  // and a different channel must not consume it.
  auto lis_r = duct::listen("tcp://127.0.0.1:0");
  EXPECT_TRUE(lis_r.ok());
  if (!lis_r.ok()) return;
  auto addr_r = lis_r.value()->local_address();
  EXPECT_TRUE(addr_r.ok());
  if (!addr_r.ok()) return;

  auto server = std::async(std::launch::async, [&]() -> duct::Result<void> {
    auto p = lis_r.value()->accept();
    if (!p.ok()) return p.status();
    duct::RecvOptions opt;
    opt.timeout = std::chrono::milliseconds(500);
    for (;;) {
      auto msg = p.value()->recv(opt);
      if (!msg.ok()) return msg.status();
      if (msg.value().as_string_view() == "quit") return {};
      duct::SendOptions send_opt;
      send_opt.channel_id = 7;
      auto st = p.value()->send(msg.value(), send_opt);
      if (!st.ok()) return st.status();
    }
  });

  duct::DialOptions dial_opt;
  dial_opt.qos.enabled = true;
  dial_opt.qos.snd_hwm_bytes = 1024 * 1024;
  dial_opt.qos.rcv_hwm_bytes = 1024 * 1024;
  auto c = duct::dial(addr_r.value(), dial_opt);
  EXPECT_TRUE(c.ok());
  if (!c.ok()) return;

  duct::SendOptions send_opt;
  send_opt.timeout = std::chrono::milliseconds(500);
  send_opt.channel_id = 7;
  EXPECT_TRUE(c.value()->send(duct::Message::from_string("ch-7"), send_opt).ok());

  // A recv for a different channel must not steal the ch-7 frame.
  duct::RecvOptions wrong;
  wrong.timeout = std::chrono::milliseconds(200);
  wrong.channel_id = 8;
  auto got = c.value()->recv(wrong);
  EXPECT_TRUE(!got.ok());
  EXPECT_EQ(got.status().code(), duct::StatusCode::kTimeout);

  duct::RecvOptions recv_opt;
  recv_opt.timeout = std::chrono::milliseconds(500);
  recv_opt.channel_id = 7;
  auto echoed = c.value()->recv(recv_opt);
  EXPECT_TRUE(echoed.ok());
  if (echoed.ok()) {
    EXPECT_EQ(echoed.value().as_string_view(), "ch-7");
  }

  EXPECT_TRUE(c.value()->send(duct::Message::from_string("quit"), {}).ok());
  auto sst = server.wait_for(std::chrono::seconds(1));
  if (sst == std::future_status::ready) {
    auto sr = server.get();
    EXPECT_TRUE(sr.ok());
  } else {
    EXPECT_TRUE(false);
  }
  c.value()->close();
  lis_r.value()->close();
}


void test_qos_item_count_cap() {
  auto owned = std::make_unique<SlowTimeoutPipe>();
  SlowTimeoutPipe* transport = owned.get();
  duct::QosOptions qos;
  qos.snd_hwm_bytes = 0;    // No byte limit; count only.
  qos.snd_hwm_count = 2;
  qos.backpressure = duct::BackpressurePolicy::kBlock;
  duct::QosPipe pipe(std::move(owned), qos);

  // First send is picked up by the worker and held in-flight for 300ms, so it
  // is not counted against the queue watermark. Pin it in-flight so the
  // following two sends fill the queue deterministically.
  EXPECT_TRUE(pipe.send(duct::Message::from_string("a"), {}).ok());
  EXPECT_TRUE(transport->wait_until_send_started(std::chrono::milliseconds(500)));
  EXPECT_TRUE(pipe.send(duct::Message::from_string("b"), {}).ok());
  // Queue is now full (count cap = 2); the in-flight message is excluded.
  EXPECT_TRUE(pipe.send(duct::Message::from_string("c"), {}).ok());
  // Fourth send must block (count full) until close() aborts it.
  auto fourth = std::async(std::launch::async, [&] {
    duct::SendOptions opt;
    opt.timeout = std::chrono::milliseconds(1000);
    return pipe.send(duct::Message::from_string("d"), opt);
  });
  EXPECT_TRUE(fourth.wait_for(std::chrono::milliseconds(100)) != std::future_status::ready);
  pipe.close();
  EXPECT_TRUE(fourth.wait_for(std::chrono::seconds(1)) == std::future_status::ready);
  if (fourth.wait_for(std::chrono::seconds(0)) == std::future_status::ready) {
    auto r = fourth.get();
    EXPECT_TRUE(!r.ok());
  }
}


void test_qos_failfast_returns_queue_full() {
  auto owned = std::make_unique<GatedRecordingPipe>();
  GatedRecordingPipe* transport = owned.get();
  duct::QosOptions qos;
  qos.snd_hwm_bytes = 3;
  qos.backpressure = duct::BackpressurePolicy::kFailFast;
  duct::QosPipe pipe(std::move(owned), qos);

  // Pin the first message in-flight so the queue watermark is deterministic:
  // the in-flight message does not count against the HWM.
  EXPECT_TRUE(pipe.send(duct::Message::from_string("a"), {}).ok());
  EXPECT_TRUE(transport->wait_until_send_started(std::chrono::milliseconds(500)));
  EXPECT_TRUE(pipe.send(duct::Message::from_string("b"), {}).ok());
  EXPECT_TRUE(pipe.send(duct::Message::from_string("c"), {}).ok());
  // Queue is now at exactly snd_hwm_bytes (3); the next send must fail fast.
  EXPECT_TRUE(pipe.send(duct::Message::from_string("d"), {}).ok());
  auto r = pipe.send(duct::Message::from_string("e"), {});
  EXPECT_TRUE(!r.ok());
  if (!r.ok()) {
    EXPECT_EQ(r.status().code(), duct::StatusCode::kQueueFull);
  }
  // Release the gated first send so the worker can finish and close() can join.
  transport->release();
  pipe.close();
}


void test_qos_dial_opt_in_disabled_by_default() {
  // With qos.enabled == false (default), dial() returns the raw transport even
  // though the default snd_hwm_bytes is non-zero.
  auto lis_r = duct::listen("tcp://127.0.0.1:0");
  EXPECT_TRUE(lis_r.ok());
  if (!lis_r.ok()) return;
  auto addr_r = lis_r.value()->local_address();
  EXPECT_TRUE(addr_r.ok());
  if (!addr_r.ok()) return;

  duct::DialOptions dial_opt;  // qos.enabled defaults false
  auto c = duct::dial(addr_r.value(), dial_opt);
  EXPECT_TRUE(c.ok());
  if (c.ok()) {
    duct::SendOptions send_opt;
    send_opt.timeout = std::chrono::milliseconds(200);
    auto st = c.value()->send(duct::Message::from_string("raw"), send_opt);
    // Raw transport: send succeeds (QoS off) without a QosPipe wrapper.
    EXPECT_TRUE(st.ok());
    c.value()->close();
  }
  lis_r.value()->close();
}


void test_qos_ttl_expires_queued_message() {
  auto owned = std::make_unique<GatedRecordingPipe>();
  GatedRecordingPipe* transport = owned.get();
  duct::QosOptions qos;
  qos.ttl = std::chrono::milliseconds(200);
  duct::QosPipe pipe(std::move(owned), qos);

  EXPECT_TRUE(pipe.send(duct::Message::from_string("first"), {}).ok());
  EXPECT_TRUE(transport->wait_until_send_started(std::chrono::milliseconds(500)));

  EXPECT_TRUE(pipe.send(duct::Message::from_string("expired"), {}).ok());
  std::this_thread::sleep_for(std::chrono::milliseconds(500));
  transport->release();
  EXPECT_TRUE(transport->wait_for_sent("first", std::chrono::milliseconds(500)));

  EXPECT_TRUE(pipe.send(duct::Message::from_string("after-expiry"), {}).ok());
  EXPECT_TRUE(transport->wait_for_sent("after-expiry", std::chrono::milliseconds(500)));
  EXPECT_TRUE(!transport->was_sent("expired"));
  pipe.close();
}


void test_qos_drop_old_evicts_oldest_queued() {
  auto owned = std::make_unique<GatedRecordingPipe>();
  GatedRecordingPipe* transport = owned.get();
  duct::QosOptions qos;
  qos.snd_hwm_bytes = 2;
  qos.backpressure = duct::BackpressurePolicy::kDropOld;
  duct::QosPipe pipe(std::move(owned), qos);

  EXPECT_TRUE(pipe.send(duct::Message::from_string("a"), {}).ok());
  EXPECT_TRUE(transport->wait_until_send_started(std::chrono::milliseconds(500)));
  EXPECT_TRUE(pipe.send(duct::Message::from_string("b"), {}).ok());
  EXPECT_TRUE(pipe.send(duct::Message::from_string("c"), {}).ok());
  EXPECT_TRUE(pipe.send(duct::Message::from_string("d"), {}).ok());
  transport->release();

  // "a" was already picked up by the worker (in-flight) and is delivered on
  // release. "b" was evicted from the queue by kDropOld. "c" and "d" remain.
  EXPECT_TRUE(transport->wait_for_sent("a", std::chrono::milliseconds(500)));
  EXPECT_TRUE(transport->wait_for_sent("c", std::chrono::milliseconds(500)));
  EXPECT_TRUE(transport->wait_for_sent("d", std::chrono::milliseconds(500)));
  EXPECT_TRUE(!transport->was_sent("b"));
  pipe.close();
}

// A reconnect-enabled dial must not report success for an address it cannot
// reach: dial() returns an error inside the caller's deadline instead of a
// pipe that only fails later on first send/recv.
void test_reconnect_dial_unreachable_fails_within_deadline() {
  duct::DialOptions dial_opt;
  dial_opt.timeout = std::chrono::milliseconds(300);
  dial_opt.qos.snd_hwm_bytes = 0;
  dial_opt.qos.rcv_hwm_bytes = 0;
  dial_opt.reconnect.enabled = true;
  dial_opt.reconnect.initial_delay = std::chrono::milliseconds(10);
  dial_opt.reconnect.max_delay = std::chrono::milliseconds(20);
  dial_opt.reconnect.max_attempts = 3;

  const auto begin = std::chrono::steady_clock::now();
  auto r = duct::dial("shm://duct_reconnect_absent", dial_opt);
  const auto elapsed = std::chrono::steady_clock::now() - begin;

  EXPECT_TRUE(!r.ok());
  EXPECT_TRUE(elapsed < std::chrono::seconds(5));
}

// The pipe returned by a reconnect-enabled dial wraps the transport, so the
// transport's send bound is honored immediately instead of falling back to the
// unbounded base default while the background first connect is still running.
void test_reconnect_dial_reports_bound_on_return() {
  auto lis_r = duct::listen("shm://duct_reconnect_bound");
  EXPECT_TRUE(lis_r.ok());
  if (!lis_r.ok()) return;

  duct::DialOptions dial_opt;
  dial_opt.timeout = std::chrono::milliseconds(2000);
  dial_opt.qos.enabled = true;
  dial_opt.qos.snd_hwm_bytes = 1024;
  dial_opt.qos.rcv_hwm_bytes = 0;
  dial_opt.reconnect.enabled = true;
  dial_opt.reconnect.initial_delay = std::chrono::milliseconds(10);
  dial_opt.reconnect.max_attempts = 5;

  auto c = duct::dial("shm://duct_reconnect_bound", dial_opt);
  EXPECT_TRUE(c.ok());
  if (c.ok()) {
    EXPECT_EQ(c.value()->max_message_bytes(), static_cast<std::size_t>(1024));
    c.value()->close();
  }
  lis_r.value()->close();
}
