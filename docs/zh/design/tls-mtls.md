# TLS / mTLS 提案（2026-11-21）

状态：**设计已签核（2026-11-22），未实现**。用户对决策点 T1–T5 全部按
推荐选项签核。构建中尚无 TLS 依赖（CMake 无 OpenSSL）、无证书配置 API；
`tls://` 地址在地址层以 `kInvalidArgument`（"unknown scheme"）拒绝（由
`test_tls_scheme_rejected_as_unsupported` 钉死）。实现以用户提供真实凭据
为门——测试夹具不是生产证书。

另见[能力与保留契约](../../design/capabilities.md)（英文单源）——在提案
实现之前，「无 TLS 或其他凭据层」一节仍然准确。

## 目标

用 TLS 1.3 加密并认证 `tcp://` 连接，为服务器间游戏后端提供可选的双向
认证（mTLS）。

## 范围

v1 仅 `tcp://`。`uds://` 已通过 `SO_PEERCRED`（`duct.h` 对端凭据）携带
进程身份，本地链路通常视为可信；`shm://`/`pipe://` 天然同主机。
`uds://` 的 TLS 可日后无损跟进。

## 设计

### 分层

TLS 在线格式封帧**之前**包裹裸套接字：TCP 连接 → TLS 握手 → 既有
`wire::handshake` 能力协商（在加密层之上运行）→ 帧。重连包装器在每次
重拨时完整重跑 TLS 握手；会话恢复属 v2。

### 依赖

**T1** —— OpenSSL 3.x（推荐：Linux / macOS / mingw 一套 API；MSVC 消费方
经 vcpkg 取用；CMake `find_package(OpenSSL)` + 导出 target）vs 平台 TLS
后端（SChannel + SecureTransport：零外部依赖，但三套后端需同步维护）。

### 寻址

**T2** —— 保持 `tcp://`，经 `DialOptions.tls` / `ListenOptions.tls` 按
dial/listen 选择启用（推荐：明文与加密部署的地址完全一致）vs 新增
`tls://` scheme（地址显式，但同一部署出现两套 URL、scheme 矩阵翻倍）。

### 配置 API

**T3** —— `DialOptions` 与 `ListenOptions` 各挂一个 `TlsOptions`，与既有
保留字段同型的源码级兼容：

- `enabled`（默认 false）
- `ca_path` —— 信任锚；空 = 系统信任库
- `cert_path` + `key_path` —— 客户端可选，服务端必填
- `verify_peer_name`（客户端，默认 true；hostname/SAN 校验）
- `require_client_cert`（服务端，默认 false —— **T4**：翻 true 即 mTLS）
- 最低版本钉死 TLS 1.3；不设密码套件旋钮（固定现代策略；旋钮引人误配）

### 证书供给（用户侧）

真实密钥/证书材料与信任库布局属部署决策：机群是否跑内部 CA、密钥如何
分发到主机、轮换策略。库只消费路径——绝不生成或保存生产凭据。

### 失败模式

握手失败映射为 dial 的 `kIoError`（连接被拒一类）；证书校验失败映射为
`kInvalidArgument` 并透出校验错误原因。TLS 握手失败永远不会触达线格式
层，既有能力握手与封帧代码零改动。

## 决策点（2026-11-22 用户已签核，全部按推荐）

- **T1——依赖**：OpenSSL 3（弃平台后端）。
- **T2——寻址**：保持 `tcp://`，经 `TlsOptions` 选择启用（弃 `tls://`
  scheme）。
- **T3——配置形状**：如上所列的 `TlsOptions`（按提案接受）。
- **T4——mTLS 默认**：`require_client_cert` 默认 false（按监听器显式开启；
  弃全机群默认 true）。
- **T5——验收**：仓内自签测试夹具（仅测试用 CA + 叶子证书对，显著标记，
  构建期生成——绝不作为生产材料提交）、阴性测试（过期 / 域名不符 /
  不受信 / 密钥错配），加上一次由用户提供真实凭据的手动冒烟。

## 非目标

v1 不做 `uds://`/`shm://`/`pipe://` 的 TLS；不做证书轮换管理；v1 不做
OCSP/CRL 检查；v1 不做会话恢复。

## 验收标准（后续工作；T1–T5 已签核，以真实凭据为门）

实现 TLS 传输包装器，把 `test_tls_scheme_rejected_as_unsupported` 替换为
正反两面握手测试，既有全套测试保持全绿，并在同一变更中把
[capabilities](../../design/capabilities.md) 的「无 TLS」行移入已实现表。
