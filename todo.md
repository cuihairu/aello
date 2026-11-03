# Aello Roadmap

Requirements ledger for humans. One requirement per line; done items carry one evidence line
(commit hash + test count). Source of truth for what is implemented is the code, not this file.

## Core

- [x] Message model and `Result`/`Status` error handling (no exceptions required). Evidence: d56eb85; 8 test cases green.
- [x] Address parsing and scheme dispatch (tcp/shm/pipe/uds; uds parsed but not wired). Evidence: d56eb85.
- [x] TCP transport with framing, TCP_NODELAY, and port-0 support. Evidence: d56eb85.
- [x] POSIX shm transport: fixed-slot double ring + named semaphores + UDS bootstrap. Evidence: 86e46c6.
- [x] Windows shm and named-pipe transports. Evidence: 155190d.
- [x] Wire framing: 16-byte header, 64 KiB max payload, network byte order. Evidence: d56eb85.
- [x] QosPipe send queue with background drain thread (snd_hwm_bytes / backpressure / TTL). Evidence: 155190d.
- [x] Cross-platform CI (GCC / Clang / MSVC). Evidence: c607b29.
- [x] macOS SIGPIPE suppression in wire send. Evidence: 86f45ab.
- [x] Test suite: address parse, shm echo, shm full-ring timeout, wire boundaries. Evidence: 8 cases; ctest 1/1.

## Transport wiring

- [x] Honor `SendOptions`/`RecvOptions.timeout` on tcp; a mid-frame timeout is fatal. Evidence: bd157c7, cec8067.
- [x] Wire `DialOptions.timeout` into tcp/shm dial. Evidence: 4aa9785; 8 new test cases green (13 total, ctest 1/1).
- [x] Wire `uds://` into dial/listen (src/uds_transport.cc implemented but uncompiled and undispatched). Evidence: 5ee7c3a; uds echo test green (ctest 1/1).
- [x] Wire auto-reconnect into dial (src/reconnect_pipe.cc exists but uncompiled and uncalled). Evidence: 8f9152c; reconnect connect/echo and attempt-exhaustion tests green (31 duct cases, ctest 2/2).
- [x] Wire `on_state_change` connection callback. Evidence: 8f9152c; reconnect callback observes kConnected and reconnects, 10/10 stress runs green.
- [x] Implement recv-side watermarks (QosPipe recv currently passes through). Evidence: c9513aa; recv watermark + FIFO + exhaustion test green (33 duct cases, ctest 2/2).
- [x] Implement `rcv_hwm_bytes` and `linger` consumers (declared, unused). Evidence: c9513aa; linger drains in-flight send before close test green (33 duct cases, ctest 2/2).
- [x] Add recv-side QoS queue and background recv worker. Evidence: c9513aa; SourcePipe drains into bounded queue, FIFO order, close test green (33 duct cases, ctest 2/2).
- [x] Compile `socket_utils.cc` when UDS is wired. Evidence: 5ee7c3a; already in CMake source list, now referenced by uds_transport.cc.

## Protocol

- [x] Fragmentation and reassembly for payloads larger than 64 KiB. Evidence: 8c0b3ee; wire + shm multi-chunk round-trip tests green (36 duct cases, ctest 2/2).
- [ ] Reliability (at-least-once): session_id, seq/ack, retransmit, dedup. BLOCKED: needs transport-level protocol redesign and loss-injection test matrix before implementation.
- [x] Priority scheduling and channel_id multiplexing. Evidence: 4dc7aa6; priority drains high-first and channel_id demux round-trip tests green (38 duct cases, ctest 2/2).
- [x] Rate limiting (token bucket) per pipe or per socket. Evidence: f3d2870; paced delivery test green (39 duct cases, ctest 2/2).
- [x] Capability / version negotiation in the handshake. Evidence: bfe0629; tcp handshake version negotiation + post-handshake echo test green (40 duct cases, ctest 2/2).

## QoS and backpressure

- [x] Reject messages above the transport limit synchronously instead of dropping them later. Evidence: 011e118, b0d4f9e.
- [x] Item-count cap or per-slot charging (QosPipe limits bytes only, not entries). Evidence: b96fe1d; snd_hwm_count gates enqueue, count cap + blocking test green (41 duct cases, ctest 2/2).
- [x] Distinct queue-full status code (currently borrows kTimeout / kIoError). Evidence: b96fe1d; kQueueFull + fail-fast/block return test green (41 duct cases, ctest 2/2).
- [x] Make the QoS wrap in dial() an explicit opt-in (currently implicit via non-zero default snd_hwm_bytes). Evidence: b96fe1d; QosOptions.enabled defaults false, dial returns raw transport test green (41 duct cases, ctest 2/2).

## Testing

- [x] Direct QoS tests (send/recv, backpressure policies, TTL, close under load). Evidence: commit; TTL expiry, kDropOld eviction, kDropNew, kFailFast queue-full, kBlock, close-under-load, item cap, recv watermark tests green (45 duct cases, ctest 2/2, 15x stress).
- [ ] Concurrent send/recv tests.
- [ ] Multi-process and `kill -9` / restart test matrix.
- [ ] UDS tests (blocked until UDS is wired).
- [ ] Benchmarks (aello-bench): shm round-trip latency and throughput vs UDS/TCP.

## Deferred features

- [ ] RPC request correlation, timeout, and cancellation.
- [ ] Pub/sub with multiple subscribers.
- [ ] Async/event-loop API (async.h is on the design audit delete list; not part of the library).
- [ ] TLS / mTLS for tcp, peer credential checks for uds.
- [ ] Crash recovery for orphaned shm segments.
- [ ] Install / packaging target (cmake templates unreferenced).

## Cleanup (design audit delete/refactor list)

- [ ] Delete or park async.h, utils.h, logging.h, queue.h, convenience.h, raii.h, socket_utils.h, duct_export.h (uncompiled; see docs/design/aello-audit-2026-10-16.md §3).
- [x] Resolve duplicate `QueuedMessage` definition (include/duct/queue.h:16 and include/duct/qos_pipe.h:18). Evidence: 3022436.
- [x] Fix malformed `DUCT Experimental [[deprecated(...)]]` macro (include/duct/duct_export.h:30). Evidence: 3022436.
- [ ] Make shm slot parameters configurable (currently hard-coded 64 slots x 64 KiB).

## 审核发现

- [x] 审核发现：`EventLoop::stop()` 在锁外 join 已移出的 `thread_` 期间，`runInBackground()` 会因 `thread_.joinable()==false` 重启线程并把 `running_` 置回 true，旧循环线程重新进入 `wait_for`，导致 `stop()` 永久阻塞（复现：并发 stop+restart，10/10 挂起；include/duct/async.h:193-217）。 Evidence: 6b25e95.
- [x] 审核发现：QosPipe worker 的 TTL 清理在持锁删除过期消息并减少 `send_bytes_` 后未 `send_cv_.notify_all()`，`kBlock` 且 `SendOptions.timeout==0` 的发送者在空间仅由 TTL 释放时永不唤醒，可永久挂起（复现：STILL BLOCKED；src/qos_pipe.cc:35-46,143-151）。 Evidence: 56e9b4e.
- [x] 审核发现：`LoggerHolder::retired` 无上限保留所有被替换的 logger，反复 `set_logger()` 会造成无界内存增长（src/logging.cc:37,47）。 Evidence: e951087.
- [x] 审核发现：QosPipe worker 以 100ms 超时“绑定”传输发送，但 wire 的 `SO_SNDTIMEO` 作用于每次 `send()`/`recv()` 系统调用而非整帧，慢对端下 64KiB 帧可耗时多个 100ms，`close()` 的 join 等待并不真正受 100ms 限制（src/qos_pipe.cc:64-69；src/wire.cc:119-142）。 Evidence: 589a847.
- [x] 审核发现：`QosOptions::max_message_bytes` 与底层传输实际上限解耦，调用方若设得更高，超限消息仍被 `send()` 接受并在 worker 中静默丢弃，重新引入本次提交声称修复的数据丢失路径（include/duct/duct.h:71-74；src/qos_pipe.cc:101-104；src/wire.cc:254-256；src/shm_transport.cc:389-390）。 Evidence: 56e9b4e.
- [x] 审核发现：Windows 的 `win_shm.cc::shm_dial` 未校验负超时，`static_cast<DWORD>(opt.timeout.count())` 把 -1 变成约 49.7 天；对不存在的管道走 `ERROR_FILE_NOT_FOUND` + `Sleep(1)` 重试循环，几乎永不返回 kInvalidArgument/kTimeout。新增测试 `test_dial_rejects_negative_timeout` 的 shm 分支在 Windows 上会挂起约 49 天而非断言 `kInvalidArgument`（src/win_shm.cc:495,511-527；tests/duct_tests.cc:568-572）。POSIX 侧的负超时校验（src/shm_transport.cc:533-535）未镜像到 win_shm.cc。 Evidence: 969d156.
- [x] 审核发现：非阻塞 connect 对拒绝的 TCP 端口在 Linux 上 poll 返回 `POLLOUT|POLLERR|POLLHUP`，`wait_writable` 先判 `POLLHUP` 返回 `kClosed("peer closed")`，导致 `connect_with_timeout` 的 `getsockopt(SO_ERROR)` 分支（本意是取回 ECONNREFUSED 等真实错误码）永远不可达，最终只得到通用 "connect() failed"，具体失败原因丢失（src/socket_utils.cc:128-129；src/tcp_transport.cc:146-147,157-159）。实测确认：SO_ERROR=111(ECONNREFUSED)，而 poll 命中 POLLHUP 提前返回。 Evidence: 969d156.
- [x] 审核发现：`uds_connect` 超时分支在 `getsockopt` 成功但 `so_error!=0` 时，`errno` 未被设置（实测仍为 EINPROGRESS=115），`errno_suffix()` 拼出误导性消息 "connect(AF_UNIX) failed: Operation now in progress"；`so_error` 中的真实错误码被丢弃（src/shm_transport.cc:263-265）。同源问题：tcp 侧 `connect_with_timeout` 的失败消息也不含 `so_error`（src/tcp_transport.cc:157-159）。 Evidence: 969d156.
- [x] 审核发现：`test_tcp_dial_timeout_honored` 依赖 `10.255.255.1` 被网关静默丢弃（黑洞）才返回 kTimeout；`10.0.0.0/8` 是私有地址段，若运行环境有路由到真实主机、网关回 ICMP 拒绝（ECONNREFUSED）或容器无路由（立即 EHOSTUNREACH/ENETUNREACH，非 EINPROGRESS），connect 立即失败，`EXPECT_EQ(code, kTimeout)` 断言失败，测试不稳定/跨环境不可移植（tests/duct_tests.cc:513-521）。 Evidence: 5e75e8c.
- [ ] 审核发现：`test_shm_dial_timeout_honored` 的 POSIX 分支无监听者时 AF_UNIX connect 立即 ENOENT（阻塞硬错误，src/shm_transport.cc:252-255），根本不进入新增的非阻塞 connect+`wait_writable`+SO_ERROR 限时路径（src/shm_transport.cc:241-268）；该超时逻辑实为无测试覆盖的死代码，注释“verifies the timeout path is wired”言过其实（tests/duct_tests.cc:542-545）。 注：注释已修正于 5e75e8c，但正超时路径仍无测试覆盖，保持开放。
- [x] 审核发现：`connect_tcp` 的超时是“每地址”而非“整个连接尝试”，且 `getaddrinfo` DNS 解析不受 timeout 约束；多地址主机（如双栈 localhost）实际耗时可达 N×timeout。且首个地址超时即整体失败并放弃后续地址，与阻塞路径“逐个尝试所有地址”的回退语义不一致（src/tcp_transport.cc:184-211,205-208）。无测试覆盖多地址/被拒端口+超时组合。 Evidence: 969d156.
- [x] 审核发现：`include/duct/duct.h:108` 注释仍写 “Reserved: tcp/shm dial do not honor it yet”，与 4aa9785 已接线实现相矛盾，应同步更新（include/duct/duct.h:108-109）。 Evidence: 969d156.
- [x] 审核发现：成功连接路径（非阻塞 connect 后 `set_nonblocking(fd,false)` 恢复阻塞）无任何测试覆盖；若恢复失败或缺失，新建 pipe 将静默运行于非阻塞模式，wire 的 `send`/`recv` 会把 EAGAIN 当作传输错误。新增三个 dial 超时测试全部断言 `!ok()`，未覆盖 happy path（src/tcp_transport.cc:162；src/shm_transport.cc:267；tests/duct_tests.cc:500-571）。 Evidence: 5e75e8c.
- [x] 审核发现：拨号超时复用的 `wait_writable` 在超时后返回 `Status::timeout("write timed out")`，把 connect 阶段的超时报告为“写超时”，诊断语境误导（src/socket_utils.cc:127；src/tcp_transport.cc:146；src/shm_transport.cc:256）。 Evidence: 969d156.
- [x] 审核发现：负超时校验仅加在 tcp/shm（src/tcp_transport.cc:312-314；src/shm_transport.cc:533-535），Windows 的 `win_shm.cc:495` 与 `pipe_transport.cc:260` 仍以 `static_cast<DWORD>(opt.timeout.count())` 把 -1 转成约 49.7 天；新增 `test_dial_rejects_negative_timeout` 未覆盖 pipe 传输，Windows 下对“存在但忙”的命名管道会挂起约 49.7 天而非返回 kInvalidArgument（src/pipe_transport.cc:259-261；tests/duct_tests.cc:558-575）。 Evidence: 969d156.

> 覆盖范围：本审查针对 `f7fd1bfb4f3d..HEAD`（4aa9785 接线 tcp/shm 拨号超时 + 8f75f06/56ca4b2 文档），已逐文件审毕全部 6 个文件。动态验证（本机 Linux，`build-review/libduct.a`）：① 拒绝端口（127.0.0.1:1）非阻塞 connect 的 poll `revents=0x1c`（POLLOUT|POLLERR|POLLHUP），`wait_writable` 先命中 POLLHUP 返回 kClosed，SO_ERROR=111(ECONNREFUSED) 从未被读取，最终表现为通用 io_error "connect() failed" —— 与既有发现一致；② 黑洞地址 10.255.255.1:65000 在 300ms 超时下 301ms 返回 kTimeout，超时接线生效；③ 负超时（shm）返回 kInvalidArgument；④ 500 次失败拨号后 /proc/self/fd 计数 6→6（无 fd 泄漏），2000 次拨号 RSS 4420KB→4420KB（getaddrinfo 已释放，无内存增长）。安全面复核：本 diff 未新增注入/反序列化/越界/竞态路径；错误处理缺口见上。既有未提交 7 条发现（win_shm 负超时、POLLHUP 遮蔽 SO_ERROR、uds errno 消息、10.255.255.1 环境依赖、shm 超时路径无测试、每地址超时语义、duct.h 注释）与本轮结论一致，未重复追加。

> 覆盖范围：本轮审查针对 `56ca4b2716b6..HEAD`（10 提交：e5b2001 文档 / 589a847 wire 帧截止 / 56e9b4e qos TTL 与 max 上限 / 969d156 dial 错误保留与整体超时 / 6b25e95 async stop 重构 / e951087 logging retired 上限 / 5e75e8c tcp 超时测试 / 95a64b2 记录审计 / 5ee7c3a 接线 uds / 49db86c 文档），逐文件审毕：CMakeLists.txt、async.h、duct.h、duct.cc、logging.cc、pipe_transport.cc、qos_pipe.cc、shm_transport.cc、socket_utils.cc、tcp_transport.cc、win_shm.cc、wire.cc、tests/duct_tests.cc，以及本区间新被编译进构建的 `src/uds_transport.cc`（内容未变但 5ee7c3a 起为活代码）。动态验证（本机 Linux，独立 pollprobe.c 编译运行）：拒绝端口 127.0.0.1:1 非阻塞 connect 的 `poll revents=0x1c`（POLLOUT|POLLERR|POLLHUP）、`SO_ERROR=111(ECONNREFUSED)`——据此确认下面第 4 条（wait_writable 仍先判 POLLHUP 返回 kClosed，SO_ERROR 分支对拒绝连接不可达）。注：工作区有未提交改动（src/duct.cc 拆出 dial_impl + reconnect_pipe、CMakeLists 增加 src/reconnect_pipe.cc），不在本范围，未审。

- [x] 审核发现：`EventLoop::stop()` 在 6b25e95 把 `running_=false` 的赋值移到“自调用提前返回”之后——回调内（`thread_.get_id()==std::this_thread::get_id()`）调用 `stop()` 会直接 return，`running_` 永不置 false，循环永不退出（旧代码先置 `running_=false` 再判自调用）；新增注释“the running thread will observe running_ == false and exit”与代码矛盾，且无 EventLoop 测试覆盖 stop-from-callback 路径（include/duct/async.h:211-215）。 Evidence: ebbbc75; async_tests stop-from-callback and normal-close green (3 cases, ctest 2/2).
- [x] 审核发现：589a847 移除 `write_frame` 入口的 `apply_socket_timeout(fd,writing,timeout)`（改为仅当 deadline 非零时在 write_all 循环内按剩余时间重设 SO_SNDTIMEO），导致 timeout==0 的“无超时/阻塞”发送不再清除上一次带超时发送遗留的 SO_SNDTIMEO；同一 pipe 先带超时、后不带超时发送时，后者被上次残留值静默限时，缓冲区满时误报 kTimeout（首字节未发）或 kIoError 且连接被杀（src/wire.cc:271-289,173-185）。无“先超时后无超时”同 pipe 的测试。 Evidence: ebbbc75; blocking-write timeout reset test green (29 duct cases, ctest 2/2).
- [x] 审核发现：589a847 的帧截止时间在剩余 <1ms 时 `duration_cast<milliseconds>` 得 0，`apply_socket_timeout` 把它当“无超时”（SO_SNDTIMEO=0 阻塞），send() 可阻塞超过截止时间；应在剩余>0 但 <1ms 时至少取 1ms（src/wire.cc:182-184）。 Evidence: ebbbc75; 1ms minimum timeout fix green (29 duct cases, ctest 2/2).
- [x] 审核发现：969d156 为“保留真实 connect 错误”而修改的是 `wait_readable`（把 POLLHUP 并入 io_error），但 connect 路径（tcp/uds/shm）实际调用的是 `wait_writable`，其 POLLHUP 分支仍先返回 `closed("peer closed")`（src/socket_utils.cc:128-130）；实测 refused 端口 poll 返回 POLLOUT|POLLERR|POLLHUP、SO_ERROR=111，新增的 `connect() failed: SO_ERROR=...` 分支（src/tcp_transport.cc:162-165；src/shm_transport.cc:263-265）对拒绝连接不可达，ECONNREFUSED 仍被丢弃——修复落在错误函数上，上文对应 [x] 结论与实际代码不符。 Evidence: ebbbc75; wait_writable SO_ERROR preserved, refused-connection test green (29 duct cases, ctest 2/2).
- [x] 审核发现：969d156 将 `wait_readable` 的 POLLHUP 从 `closed("peer closed")` 改为 `io_error`，但唯一调用方 `UdsPipe::recv`（带超时，src/uds_transport.cc:47）会在对端正常关闭（无未读数据）时返回 kIoError 而非 kClosed；`EventLoop::run()` 把 kClosed/kTimeout 视为良性、kIoError 触发 on_error，导致对端正常关闭被误报为错误回调（src/socket_utils.cc:76-81）。注释“let the caller read SO_ERROR”对 recv 调用方不成立。 Evidence: ebbbc75; wait_readable POLLHUP restored to kClosed, normal-close test green (3 async cases, ctest 2/2).
- [x] 审核发现：e951087 将 retired logger 上限设为 4 后，`LoggerHolder::get()` 返回的 `Logger&` 在 ≥5 次 `set_logger()` 后指向已被 erase 丢弃的 logger，重新引入该注释声称要防止的 use-after-free（src/logging.cc:42,50-52）；应改为返回 `shared_ptr<Logger>` 或按代计数延迟销毁（src/logging.cc:33-55）。 Evidence: ebbbc75; owning shared_ptr logging API + replacement test green (29 duct cases, ctest 2/2).
- [x] 审核发现：5ee7c3a 接线并新编译进构建的 `src/uds_transport.cc` 为活代码，存在：① `listen_uds` 在路径长度校验之前无条件 `unlink(path)`（src/uds_transport.cc:184 先于 :189），可删除任意已存在文件/被占用的 socket 路径（无属主或存活校验，可静默抢占在线端点的路径），长路径校验失败也先删除文件；② `uds_dial` 未校验负超时（src/uds_transport.cc:225-234），负超时落入阻塞 connect，backlog 满时无限挂起，与同区间 tcp/shm/pipe/win_shm 的负超时守卫（src/tcp_transport.cc:328；src/shm_transport.cc:540；src/pipe_transport.cc:257；src/win_shm.cc:481）不一致，且 `test_dial_rejects_negative_timeout` 未覆盖 uds；③ `connect_uds` 超时路径返回原始 “write timed out” 而非 connect 语义消息、失败不带 SO_ERROR/strerror（src/uds_transport.cc:145-149,154-157），与同区间 shm/tcp 修复不一致；④ `connect_uds` 忽略 `set_nonblocking(fd,false)` 返回（src/uds_transport.cc:135,160），失败则 pipe 停在非阻塞模式、send/recv 会得 EAGAIN。 Evidence: ebbbc75; safe unlink, negative timeout, SO_ERROR, and nonblocking-restore tests green (29 duct cases, ctest 2/2).
- [x] 审核发现：测试缺口——`test_tcp_dial_timeout_honored` 改用 100.64.0.1 后删除了 `EXPECT_EQ(code,kTimeout)` 断言，仅校验 `!ok()` 且 <3s，超时接线损坏（立即硬错误或阻塞 connect）仍会通过（tests/duct_tests.cc:591-608）；`test_uds_echo_one` 仅 happy path，未覆盖 uds 正超时/backlog 满路径，与 shm 注释“由 UDS listen/accept 测试覆盖”不符（tests/duct_tests.cc:201-260）。 Evidence: ebbbc75; connection semantics and UDS timeout tests green (29 duct cases, ctest 2/2).
