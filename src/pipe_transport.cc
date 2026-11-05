#include "duct/duct.h"

#include "duct/wire.h"

#include <chrono>
#include <cstdint>
#include <memory>
#include <string>
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

class NamedPipePipe final : public Pipe {
 public:
  explicit NamedPipePipe(HANDLE handle, bool is_server)
    : handle_(handle), is_server_(is_server) {}

  ~NamedPipePipe() override { close(); }

  Result<void> send(const Message& msg, const SendOptions& opt) override {
    (void)opt;
    if (handle_ == INVALID_HANDLE_VALUE) {
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

    // Encode header
    std::uint8_t hdr[kHeaderLen];
    encode_header(h, hdr);

    // Write header and payload
    DWORD bytes_written;

    // Write header
    BOOL result = WriteFile(handle_, hdr, kHeaderLen, &bytes_written, NULL);
    if (!result || bytes_written != kHeaderLen) {
      DWORD error = GetLastError();
      if (error == ERROR_BROKEN_PIPE) {
        return Status::closed("pipe closed");
      }
      return Status::io_error("WriteFile header failed with error: " + std::to_string(error));
    }

    // Write payload
    if (msg.size() > 0) {
      result = WriteFile(handle_, msg.data(), static_cast<DWORD>(msg.size()), &bytes_written, NULL);
      if (!result || bytes_written != static_cast<DWORD>(msg.size())) {
        DWORD error = GetLastError();
        if (error == ERROR_BROKEN_PIPE) {
          return Status::closed("pipe closed");
        }
        return Status::io_error("WriteFile payload failed with error: " + std::to_string(error));
      }
    }

    return {};
  }

  Result<Message> recv(const RecvOptions& opt) override {
    (void)opt;
    if (handle_ == INVALID_HANDLE_VALUE) {
      return Status::closed("pipe closed");
    }

    // Read header
    std::uint8_t hdr[kHeaderLen];
    DWORD bytes_read;
    BOOL result = ReadFile(handle_, hdr, kHeaderLen, &bytes_read, NULL);
    if (!result) {
      DWORD error = GetLastError();
      if (error == ERROR_BROKEN_PIPE) {
        return Status::closed("pipe closed");
      }
      return Status::io_error("ReadFile header failed with error: " + std::to_string(error));
    }

    if (bytes_read != kHeaderLen) {
      return Status::io_error("incomplete header read: " + std::to_string(bytes_read) + " bytes");
    }

    // Decode header
    auto decoded = decode_header(hdr);
    if (!decoded.ok()) {
      return decoded.status();
    }

    FrameHeader header = decoded.value();

    // Validate payload size
    if (header.payload_len > kMaxFramePayload) {
      return Status::protocol_error("payload too large: " + std::to_string(header.payload_len) + " bytes");
    }

    // Read payload
    std::vector<std::uint8_t> buffer(header.payload_len);
    if (header.payload_len > 0) {
      result = ReadFile(handle_, buffer.data(), header.payload_len, &bytes_read, NULL);
      if (!result) {
        DWORD error = GetLastError();
        if (error == ERROR_BROKEN_PIPE) {
          return Status::closed("pipe closed");
        }
        return Status::io_error("ReadFile payload failed with error: " + std::to_string(error));
      }

      if (bytes_read != header.payload_len) {
        return Status::io_error("incomplete payload read: " + std::to_string(bytes_read) + " / " + std::to_string(header.payload_len) + " bytes");
      }
    }

    return Message::from_bytes(buffer.data(), buffer.size());
  }

  void close() override {
    if (handle_ != INVALID_HANDLE_VALUE) {
      CloseHandle(handle_);
      handle_ = INVALID_HANDLE_VALUE;
    }
  }

 private:
  HANDLE handle_;
  bool is_server_;
};

class NamedPipeListener final : public Listener {
 public:
  explicit NamedPipeListener(const std::string& pipe_path, int backlog)
    : pipe_path_(pipe_path), backlog_(backlog) {}

  ~NamedPipeListener() override { close(); }

  Result<std::unique_ptr<Pipe>> accept() override {
    HANDLE pipe = CreateNamedPipeA(
      pipe_path_.c_str(),
      PIPE_ACCESS_DUPLEX,
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

    // Wait for client connection
    BOOL connected = ConnectNamedPipe(pipe, NULL);
    if (!connected) {
      DWORD error = GetLastError();
      if (error != ERROR_PIPE_CONNECTED) {
        CloseHandle(pipe);
        return Status::io_error("ConnectNamedPipe failed with error: " + std::to_string(error));
      }
    }

    auto pipe_ptr = std::make_unique<NamedPipePipe>(pipe, true);
  return std::unique_ptr<Pipe>(std::move(pipe_ptr));
  }

  Result<std::string> local_address() const override {
    // Extract name from pipe path (remove \\.\pipe\duct_ prefix)
    std::string prefix = R"(\\.\pipe\duct_)";
    if (pipe_path_.find(prefix) == 0) {
      return std::string("pipe://") + pipe_path_.substr(prefix.length());
    }
    return std::string("pipe://unknown");
  }

  void close() override {
    // Named pipes are automatically cleaned up when all handles are closed
  }

 private:
  std::string pipe_path_;
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

  // Wait for pipe to become available with timeout
  DWORD timeout = static_cast<DWORD>(opt.timeout.count());
  if (timeout == 0) {
    timeout = kDefaultTimeoutMs;
  }

  DWORD wait_result = WaitNamedPipeA(pipe_path.c_str(), timeout);
  if (!wait_result) {
    DWORD error = GetLastError();
    if (error == ERROR_FILE_NOT_FOUND) {
      return Status::io_error("named pipe not found");
    } else if (error == ERROR_TIMEOUT) {
      return Status::timeout("timeout waiting for named pipe");
    }
    return Status::io_error("WaitNamedPipe failed with error: " + std::to_string(error));
  }

  // Connect to the named pipe
  HANDLE pipe = CreateFileA(
    pipe_path.c_str(),
    GENERIC_READ | GENERIC_WRITE,
    0,
    NULL,
    OPEN_EXISTING,
    FILE_ATTRIBUTE_NORMAL,
    NULL
  );

  if (pipe == INVALID_HANDLE_VALUE) {
    DWORD error = GetLastError();
    return Status::io_error("CreateFile failed with error: " + std::to_string(error));
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
