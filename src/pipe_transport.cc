#include "duct/duct.h"

#include "duct/wire.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

#if defined(_WIN32)
#include <windows.h>
#else
// Named pipes are Windows-specific
#error "Named pipes are only supported on Windows"
#endif

namespace duct {
namespace {

using namespace duct::wire;

// Named pipe constants
constexpr DWORD kPipeBufferSize = 64 * 1024;  // 64KB buffer
constexpr DWORD kDefaultTimeoutMs = 5000;     // 5 seconds

static std::string sanitize_name(std::string_view s) {
  std::string out;
  out.reserve(s.size());
  for (char c : s) {
    if ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_' || c == '-') {
      out.push_back(c);
    } else {
      out.push_back('_');
    }
  }
  if (out.empty()) out = "duct";
  return out;
}

  // Convert pipe name to Windows named pipe path.
  // 将 pipe 名称转换为 Windows 命名管道路径（\\.\pipe\...）。
  static std::string make_pipe_path(const std::string& name) {
    std::string sanitized = sanitize_name(name);
    return R"(\\.\pipe\duct_)" + sanitized;
  }

  // Security attributes for named pipes.
  // 命名管道的安全属性：这里设置为允许所有人访问（demo/测试用；生产应更严格）。
  static SECURITY_ATTRIBUTES* get_pipe_security_attributes() {
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

// Create a manual-reset event for overlapped I/O waits.
static HANDLE make_io_event() {
  return CreateEventA(nullptr, TRUE, FALSE, nullptr);
}

enum class IoResult { kDone, kTimeout, kError };

// Run an overlapped I/O operation started on `ov` to completion, bounded by
// `timeout_ms` (0 = wait indefinitely). On timeout the pending operation is
// cancelled (CancelIoEx + GetOverlappedResult with bWait) so the handle can be
// reused or closed without a blocking CloseHandle.
static IoResult run_overlapped_io(HANDLE h, OVERLAPPED& ov, DWORD& bytes, DWORD timeout_ms) {
  DWORD wait_ms = (timeout_ms == 0) ? INFINITE : timeout_ms;
  DWORD w = WaitForSingleObject(ov.hEvent, wait_ms);
  if (w == WAIT_OBJECT_0) {
    if (!GetOverlappedResult(h, &ov, &bytes, FALSE)) {
      return IoResult::kError;
    }
    return IoResult::kDone;
  }
  if (w == WAIT_TIMEOUT) {
    // The operation may have completed between the wait timing out and the
    // cancel. GetOverlappedResult reports that completion; only treat the
    // operation as timed out if it was actually cancelled.
    CancelIoEx(h, &ov);
    if (GetOverlappedResult(h, &ov, &bytes, TRUE)) {
      return IoResult::kDone;
    }
    return GetLastError() == ERROR_OPERATION_ABORTED ? IoResult::kTimeout : IoResult::kError;
  }
  return IoResult::kError;
}

class NamedPipePipe final : public Pipe {
 public:
  explicit NamedPipePipe(HANDLE handle, bool is_server)
    : handle_(handle), is_server_(is_server) {}

  ~NamedPipePipe() override { close(); }

  Result<void> send(const Message& msg, const SendOptions& opt) override {
    if (handle_ == INVALID_HANDLE_VALUE || broken_.load(std::memory_order_acquire)) {
      return Status::closed("pipe closed");
    }

    // A single named-pipe frame cannot carry more than kMaxFramePayload bytes:
    // pipe:// has no fragmentation, so an oversize message can never be
    // reassembled by the peer. Reject it synchronously (mirroring the Windows
    // shm bound) instead of writing a frame the peer will discard, which would
    // leave the payload bytes in the pipe to desynchronize the next frame.
    if (msg.size() > kMaxFramePayload) {
      return Status::invalid_argument("message larger than pipe frame");
    }

    // Frame the message using duct wire protocol
    FrameHeader h;
    h.magic = kProtocolMagic;
    h.version = kProtocolVersion;
    h.header_len = kHeaderLen;
    h.payload_len = static_cast<std::uint32_t>(msg.size());
    h.flags = 0;

    std::uint8_t hdr[kHeaderLen];
    encode_header(h, hdr);

    DWORD timeout_ms = static_cast<DWORD>(opt.timeout.count());

    // Write header.
    {
      OVERLAPPED ov{};
      ov.hEvent = make_io_event();
      if (!ov.hEvent) return Status::io_error("CreateEvent failed");
      DWORD n = 0;
      BOOL ok = WriteFile(handle_, hdr, kHeaderLen, &n, &ov);
      DWORD werr = ok ? ERROR_SUCCESS : GetLastError();
      if (!ok && werr != ERROR_IO_PENDING) {
        CloseHandle(ov.hEvent);
        if (werr == ERROR_BROKEN_PIPE) return Status::closed("pipe closed");
        return Status::io_error("WriteFile header failed with error: " + std::to_string(werr));
      }
      IoResult r = run_overlapped_io(handle_, ov, n, timeout_ms);
      CloseHandle(ov.hEvent);
      if (r == IoResult::kTimeout) {
        if (n == 0) return Status::timeout("write timed out");
        return frame_error("header write timed out after partial frame; pipe unusable");
      }
      if (r == IoResult::kError) {
        DWORD e = GetLastError();
        if (e == ERROR_BROKEN_PIPE) return frame_error("pipe closed", StatusCode::kClosed);
        return frame_error("WriteFile header failed with error: " + std::to_string(e));
      }
      if (n != kHeaderLen) return frame_error("short header write; pipe frame desynced");
    }

    // Write payload.
    if (msg.size() > 0) {
      OVERLAPPED ov{};
      ov.hEvent = make_io_event();
      if (!ov.hEvent) return frame_error("CreateEvent failed after header; pipe unusable");
      DWORD n = 0;
      BOOL ok = WriteFile(handle_, msg.data(), static_cast<DWORD>(msg.size()), &n, &ov);
      DWORD werr = ok ? ERROR_SUCCESS : GetLastError();
      if (!ok && werr != ERROR_IO_PENDING) {
        CloseHandle(ov.hEvent);
        if (werr == ERROR_BROKEN_PIPE) return frame_error("pipe closed", StatusCode::kClosed);
        return frame_error("WriteFile payload failed with error: " + std::to_string(werr));
      }
      IoResult r = run_overlapped_io(handle_, ov, n, timeout_ms);
      CloseHandle(ov.hEvent);
      if (r == IoResult::kTimeout) {
        return frame_error("payload write timed out after header; pipe frame desynced");
      }
      if (r == IoResult::kError) {
        DWORD e = GetLastError();
        if (e == ERROR_BROKEN_PIPE) return frame_error("pipe closed", StatusCode::kClosed);
        return frame_error("WriteFile payload failed with error: " + std::to_string(e));
      }
      if (n != static_cast<DWORD>(msg.size())) {
        return frame_error("short payload write; pipe frame desynced");
      }
    }

    return {};
  }

  Result<Message> recv(const RecvOptions& opt) override {
    if (handle_ == INVALID_HANDLE_VALUE || broken_.load(std::memory_order_acquire)) {
      return Status::closed("pipe closed");
    }

    DWORD timeout_ms = static_cast<DWORD>(opt.timeout.count());

    // Read header.
    std::uint8_t hdr[kHeaderLen];
    {
      OVERLAPPED ov{};
      ov.hEvent = make_io_event();
      if (!ov.hEvent) return Status::io_error("CreateEvent failed");
      DWORD n = 0;
      BOOL ok = ReadFile(handle_, hdr, kHeaderLen, &n, &ov);
      DWORD werr = ok ? ERROR_SUCCESS : GetLastError();
      if (!ok && werr != ERROR_IO_PENDING) {
        CloseHandle(ov.hEvent);
        if (werr == ERROR_BROKEN_PIPE) return Status::closed("pipe closed");
        return Status::io_error("ReadFile header failed with error: " + std::to_string(werr));
      }
      IoResult r = run_overlapped_io(handle_, ov, n, timeout_ms);
      CloseHandle(ov.hEvent);
      if (r == IoResult::kTimeout) {
        if (n == 0) return Status::timeout("read timed out");
        return frame_error("header read timed out after partial frame; pipe unusable");
      }
      if (r == IoResult::kError) {
        DWORD e = GetLastError();
        if (e == ERROR_BROKEN_PIPE) return frame_error("pipe closed", StatusCode::kClosed);
        return frame_error("ReadFile header failed with error: " + std::to_string(e));
      }
      if (n != kHeaderLen) return frame_error("incomplete header read");
    }

    auto decoded = decode_header(hdr);
    if (!decoded.ok()) {
      broken_.store(true, std::memory_order_release);
      return decoded.status();
    }

    FrameHeader header = decoded.value();

    if (header.payload_len > kMaxFramePayload) {
      return frame_error("payload too large: " + std::to_string(header.payload_len) + " bytes",
                         StatusCode::kProtocolError);
    }

    // Read payload.
    std::vector<std::uint8_t> buffer(header.payload_len);
    if (header.payload_len > 0) {
      OVERLAPPED ov{};
      ov.hEvent = make_io_event();
      if (!ov.hEvent) return frame_error("CreateEvent failed after header; pipe unusable");
      DWORD n = 0;
      BOOL ok = ReadFile(handle_, buffer.data(), header.payload_len, &n, &ov);
      DWORD werr = ok ? ERROR_SUCCESS : GetLastError();
      if (!ok && werr != ERROR_IO_PENDING) {
        CloseHandle(ov.hEvent);
        if (werr == ERROR_BROKEN_PIPE) return frame_error("pipe closed", StatusCode::kClosed);
        return frame_error("ReadFile payload failed with error: " + std::to_string(werr));
      }
      IoResult r = run_overlapped_io(handle_, ov, n, timeout_ms);
      CloseHandle(ov.hEvent);
      if (r == IoResult::kTimeout) {
        return frame_error("payload read timed out after header; pipe frame desynced");
      }
      if (r == IoResult::kError) {
        DWORD e = GetLastError();
        if (e == ERROR_BROKEN_PIPE) return frame_error("pipe closed", StatusCode::kClosed);
        return frame_error("ReadFile payload failed with error: " + std::to_string(e));
      }
      if (n != header.payload_len) return frame_error("incomplete payload read");
    }

    return Message::from_bytes(buffer.data(), buffer.size());
  }

  void close() override {
    if (handle_ != INVALID_HANDLE_VALUE) {
      CloseHandle(handle_);
      handle_ = INVALID_HANDLE_VALUE;
    }
  }

  std::size_t max_message_bytes() const override { return kMaxFramePayload; }

 private:
  Status frame_error(std::string message, StatusCode code = StatusCode::kIoError) {
    broken_.store(true, std::memory_order_release);
    return Status(code, std::move(message));
  }

  HANDLE handle_;
  bool is_server_;
  std::atomic<bool> broken_{false};
};

struct NamedPipeListenerState {
  explicit NamedPipeListenerState(std::string path)
    : pipe_path(std::move(path)), stop_event(CreateEventA(nullptr, TRUE, FALSE, nullptr)) {}

  ~NamedPipeListenerState() {
    if (stop_event != NULL) CloseHandle(stop_event);
  }

  std::string pipe_path;
  std::mutex mu;
  std::condition_variable cv;
  HANDLE stop_event = NULL;
  std::atomic<bool> closing{false};
  std::atomic<int> active_accepts{0};
};

class NamedPipeListener final : public Listener {
 public:
  explicit NamedPipeListener(std::string pipe_path, int backlog)
    : state_(std::make_shared<NamedPipeListenerState>(std::move(pipe_path))), backlog_(backlog) {}

  ~NamedPipeListener() override {
    close();
    // Wait for already-registered accepts, but do not destroy state they still
    // need if one is pathologically slow. The shared state (including the stop
    // event) lives until the last accept() returns.
    auto state = state_;
    std::unique_lock<std::mutex> lock(state->mu);
    state->cv.wait_for(lock, std::chrono::seconds(2),
                       [&] { return state->active_accepts.load() == 0; });
  }

  // Decrement the in-flight accept count and wake the destructor's wait. Every
  // accept() return path calls this exactly once after the increment.
  static void finish_accept(const std::shared_ptr<NamedPipeListenerState>& state) {
    state->active_accepts.fetch_sub(1, std::memory_order_release);
    state->cv.notify_all();
  }

  Result<std::unique_ptr<Pipe>> accept() override {
    auto state = state_;
    if (state->closing.load(std::memory_order_acquire)) return Status::closed("listener closed");

    HANDLE pipe = CreateNamedPipeA(
      state->pipe_path.c_str(),
      PIPE_ACCESS_DUPLEX | FILE_FLAG_OVERLAPPED,
      PIPE_TYPE_MESSAGE | PIPE_READMODE_MESSAGE | PIPE_WAIT,
      PIPE_UNLIMITED_INSTANCES,  // Allow unlimited instances
      kPipeBufferSize,           // Output buffer size
      kPipeBufferSize,           // Input buffer size
      kDefaultTimeoutMs,         // Default timeout
      get_pipe_security_attributes()
    );
    if (pipe == INVALID_HANDLE_VALUE) {
      DWORD error = GetLastError();
      return Status::io_error("CreateNamedPipe failed with error: " + std::to_string(error));
    }

    HANDLE ev = make_io_event();
    if (!ev) {
      CloseHandle(pipe);
      return Status::io_error("CreateEvent failed");
    }

    // Register this accept so the destructor waits for it to unwind.
    {
      std::lock_guard<std::mutex> lock(state->mu);
      if (state->closing.load(std::memory_order_relaxed)) {
        CloseHandle(ev);
        CloseHandle(pipe);
        return Status::closed("listener closed");
      }
      state->active_accepts.fetch_add(1, std::memory_order_relaxed);
    }

    OVERLAPPED ov{};
    ov.hEvent = ev;
    BOOL connected = ConnectNamedPipe(pipe, &ov);
    DWORD connect_error = connected ? ERROR_SUCCESS : GetLastError();

    if (connected || connect_error == ERROR_PIPE_CONNECTED) {
      // Client connected before or during our ConnectNamedPipe.
      CloseHandle(ev);
      finish_accept(state);
      return std::unique_ptr<Pipe>(new NamedPipePipe(pipe, true));
    }

    if (connect_error != ERROR_IO_PENDING) {
      CloseHandle(ev);
      CloseHandle(pipe);
      finish_accept(state);
      return Status::io_error("ConnectNamedPipe failed with error: " + std::to_string(connect_error));
    }

    // Wait for the connection to complete or the listener to close.
    HANDLE waits[2] = { ev, state->stop_event };
    DWORD w = WaitForMultipleObjects(2, waits, FALSE, INFINITE);
    if (w == WAIT_OBJECT_0) {
      DWORD n = 0;
      if (!GetOverlappedResult(pipe, &ov, &n, FALSE)) {
        DWORD e = GetLastError();
        CloseHandle(ev);
        CloseHandle(pipe);
        finish_accept(state);
        return Status::io_error("ConnectNamedPipe failed with error: " + std::to_string(e));
      }
      CloseHandle(ev);
      finish_accept(state);
      return std::unique_ptr<Pipe>(new NamedPipePipe(pipe, true));
    }

    // stop_event_ signaled: the listener is closing. Cancel the pending connect
    // and wait for it to unwind before releasing the handle.
    CancelIoEx(pipe, &ov);
    DWORD n = 0;
    GetOverlappedResult(pipe, &ov, &n, TRUE);
    CloseHandle(ev);
    CloseHandle(pipe);
    finish_accept(state);
    return Status::closed("listener closed during accept");
  }

  Result<std::string> local_address() const override {
    auto state = state_;
    // Extract name from pipe path (remove \\.\pipe\duct_ prefix)
    std::string prefix = R"(\\.\pipe\duct_)";
    if (state->pipe_path.find(prefix) == 0) {
      return std::string("pipe://") + state->pipe_path.substr(prefix.length());
    }
    return std::string("pipe://unknown");
  }

  void close() override {
    auto state = state_;
    std::lock_guard<std::mutex> lock(state->mu);
    if (state->closing.load(std::memory_order_relaxed)) return;
    state->closing.store(true, std::memory_order_release);
    // Wake every accept() blocked in WaitForMultipleObjects so it cancels its
    // pending ConnectNamedPipe and returns kClosed promptly instead of hanging.
    if (state->stop_event != NULL) SetEvent(state->stop_event);
  }

 private:
  std::shared_ptr<NamedPipeListenerState> state_;
  int backlog_;
};

}  // namespace

// Named pipe transport functions
Result<std::unique_ptr<Listener>> pipe_listen(const std::string& name, const ListenOptions& opt) {
  std::string pipe_path = make_pipe_path(name);
  auto listener = std::make_unique<NamedPipeListener>(pipe_path, opt.backlog);
  return std::unique_ptr<Listener>(std::move(listener));
}

Result<std::unique_ptr<Pipe>> pipe_dial(const std::string& name, const DialOptions& opt) {
  if (opt.timeout.count() < 0) {
    return Status::invalid_argument("dial timeout must not be negative");
  }
  std::string pipe_path = make_pipe_path(name);

  // Wait for the pipe to become available and connect to it, bounded by the
  // overall dial deadline. The server creates each named-pipe instance lazily
  // inside accept(), so WaitNamedPipe can race the accept thread and return
  // ERROR_FILE_NOT_FOUND; and a successful WaitNamedPipe does not guarantee
  // CreateFile wins the race against another client or the server's pending
  // ConnectNamedPipe (ERROR_PIPE_BUSY). Nor does it guarantee the last instance
  // still exists when CreateFile runs (ERROR_FILE_NOT_FOUND). All three are
  // transient, so retry within the deadline instead of failing the whole dial.
  //
  // Each WaitNamedPipe call gets only the time remaining until the deadline, so
  // retries cannot push the total dial past the caller's bound.
  const auto deadline = std::chrono::steady_clock::now() +
                        std::chrono::milliseconds(
                            opt.timeout.count() == 0 ? kDefaultTimeoutMs
                                                     : opt.timeout.count());
  auto remaining_ms = [&]() -> DWORD {
    auto left = std::chrono::duration_cast<std::chrono::milliseconds>(
                    deadline - std::chrono::steady_clock::now())
                    .count();
    if (left <= 0) return 0;
    return static_cast<DWORD>(left);
  };
  HANDLE pipe = INVALID_HANDLE_VALUE;
  for (;;) {
    DWORD left = remaining_ms();
    if (left == 0) {
      return Status::timeout("timeout waiting for named pipe");
    }
    DWORD wait_result = WaitNamedPipeA(pipe_path.c_str(), left);
    if (wait_result) {
      // Connect to the named pipe. FILE_FLAG_OVERLAPPED is required so the pipe
      // handle matches the overlapped ReadFile/WriteFile used by NamedPipePipe.
      pipe = CreateFileA(
        pipe_path.c_str(),
        GENERIC_READ | GENERIC_WRITE,
        0,
        NULL,
        OPEN_EXISTING,
        FILE_ATTRIBUTE_NORMAL | FILE_FLAG_OVERLAPPED,
        NULL
      );
      if (pipe != INVALID_HANDLE_VALUE) break;
      DWORD error = GetLastError();
      // Busy or transiently absent: retry until the deadline. Anything else is
      // a real failure.
      if (error != ERROR_PIPE_BUSY && error != ERROR_FILE_NOT_FOUND) {
        return Status::io_error("CreateFile failed with error: " + std::to_string(error));
      }
    } else {
      DWORD error = GetLastError();
      if (error == ERROR_TIMEOUT) {
        return Status::timeout("timeout waiting for named pipe");
      }
      if (error != ERROR_FILE_NOT_FOUND && error != ERROR_PIPE_BUSY) {
        return Status::io_error("WaitNamedPipe failed with error: " + std::to_string(error));
      }
    }
    // Briefly yield so the server can create/accept the next instance, then try
    // again. Do not sleep past the deadline.
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }

  // Set pipe to message mode
  DWORD mode = PIPE_READMODE_MESSAGE;
  BOOL result = SetNamedPipeHandleState(pipe, &mode, NULL, NULL);
  if (!result) {
    DWORD error = GetLastError();
    CloseHandle(pipe);
    return Status::io_error("SetNamedPipeHandleState failed with error: " + std::to_string(error));
  }

  auto pipe_ptr = std::make_unique<NamedPipePipe>(pipe, false);
  return std::unique_ptr<Pipe>(std::move(pipe_ptr));
}

}  // namespace duct
