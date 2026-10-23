---
layout: home

hero:
  name: Aello
  text: 面向游戏服务器的高性能消息运行时
  tagline: 有界内存、低延迟、可观测的进程消息通信，契约明确，传输后端可插拔。
  actions:
    - theme: brand
      text: 阅读设计结论
      link: /zh/design/aello-adoption-2026-10-16
    - theme: alt
      text: 源代码
      link: https://github.com/cuihairu/aello

features:
  - title: 有界内存
    details: 队列不会隐式增长。队列满时按明确策略阻塞、超时或丢弃。
  - title: 先验证单一性能路径
    details: SPSC 共享内存作为首个快路径；更复杂的并发模型等待基准证据。
  - title: 明确契约
    details: 传输成功、接收成功、处理完成与持久化完成是彼此独立的状态。
  - title: 可插拔传输
    details: TCP、共享内存与 Windows 命名管道共用一套 Pipe/Listener 模型，UDS 接线正在进行。
---

## 当前状态

Aello 处于实验阶段、尚未正式发布。当前提供消息模型、`Result`/`Status` 错误处理、覆盖 `tcp://`、`shm://`、`pipe://` 的 `Pipe`/`Listener` API、有界固定槽共享内存环、16 字节帧头以及跨平台 CI。

本站记录驱动下一轮实现的设计裁决与逐文件审计：

- [分析稿吸收结论](/zh/design/aello-adoption-2026-10-16)——采纳什么、延后什么，以及理由。
- [外部分析报告](/zh/design/aello-analysis-2026-10-16)——影响裁决的架构与性能方向。
- [逐文件审计](/zh/design/aello-audit-2026-10-16)——以 file:line 为证据的代码现状。

构建与使用方式见[仓库 README](https://github.com/cuihairu/aello)。
