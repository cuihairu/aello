#pragma once

#include <cstdint>

namespace duct {

// Common protocol constants shared across transports.
// Note: shm transport uses shared memory rings for data and a local UDS bootstrap,
// but still benefits from common enums/flags for future capability negotiation.

constexpr std::uint32_t kProtocolMagic = 0x44554354;  // 'D''U''C''T'
constexpr std::uint16_t kProtocolVersion = 1;
// v2 frames carry a reliability trailer (session id + sequence number) after the
// 16-byte base header. The handshake's version byte stays 1: framing extensions
// are selected by the negotiated capability, not by the protocol version.
constexpr std::uint16_t kProtocolVersionV2 = 2;

// Capability bitmask exchanged in the handshake. Bit 0 advertises the reliable
// (at-least-once) framing extension; the connection negotiates it only when both
// sides set it.
constexpr std::uint32_t kCapabilityReliable = 1u << 0;

enum class Scheme : std::uint8_t {
  kUnknown = 0,
  kTcp = 1,
  kUds = 2,
  kShm = 3,
  kPipe = 4,
};

enum class FrameFlags : std::uint32_t {
  kNone = 0,

  // Reliability (planned)
  kReliable = 1u << 0,  // at-least-once enabled for this pipe

  // Fragmentation (planned)
  kFrag = 1u << 4,
  // Final fragment of a multi-frame message (set on the last chunk).
  kFragFinal = 1u << 5,

  // Control frame (e.g. handshake/reliability ACK). Control frames carry
  // protocol metadata rather than application data.
  kCtrl = 1u << 6,

  // Cumulative-ACK control frame. Deliberately distinct from kCtrl: transports
  // swallow kCtrl frames forever (that is what the handshake reply loop relies
  // on), so ACKs need their own flag to reach the reliability layer.
  kAck = 1u << 7,
};

inline constexpr std::uint32_t channel_id_to_flags(std::uint32_t channel_id) {
  return (channel_id & 0xFFFFu) << 16;
}

inline constexpr std::uint32_t flags_to_channel_id(std::uint32_t flags) {
  return (flags >> 16) & 0xFFFFu;
}

inline constexpr FrameFlags operator|(FrameFlags a, FrameFlags b) {
  return static_cast<FrameFlags>(static_cast<std::uint32_t>(a) | static_cast<std::uint32_t>(b));
}

inline constexpr std::uint32_t to_u32(FrameFlags f) {
  return static_cast<std::uint32_t>(f);
}

}  // namespace duct

