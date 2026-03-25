# Yellowstone gRPC POSIX SHM（纯 v2）部署清单

> 适用范围：你当前这套代码（已移除 `HEADER_VERSION_LEGACY` 兼容分支），只支持 ring header `version = 2`，并要求账户帧带 CRC32。

## 0) 上线前确认

- [ ] 本次是 **首次部署 SHM**，或你可以接受清理旧的 `/dev/shm` 对象。
- [ ] Writer（插件）与 Reader（bot / `shm-reader`）会 **同步升级**，不混跑旧版本。
- [ ] 已决定是否保留 gRPC account 流用于对比：
  - `disable_grpc_accounts = false`：保留 gRPC + SHM 同时输出（便于 A/B 对比）
  - `disable_grpc_accounts = true`：只走 SHM（通常更省开销）

## 1) 编译产物

- [ ] 编译插件：
  - `cargo build --release -p yellowstone-grpc-geyser`
- [ ] 编译 reader（用于验收/排障）：
  - `cargo build --release -p yellowstone-grpc-client-simple --bin shm-reader`

## 2) 部署文件

- [ ] 部署插件动态库（示例路径）：
  - `target/release/libyellowstone_grpc_geyser.so`
- [ ] 部署插件配置文件（通常是 validator 的 geyser plugin config）。

## 3) 配置 `grpc.shm`

在插件配置里设置 `grpc.shm`（字段名需与当前代码一致）：

```json
{
  "grpc": {
    "shm": {
      "name": "/yellowstone_accounts",
      "ring_bytes": 268435456,
      "mode": "0o600",
      "reset_on_start": true,
      "disable_grpc_accounts": false
    }
  }
}
```

参数建议：

- [ ] `name`：建议固定为 `/yellowstone_accounts`（或你自定义唯一名字）。
- [ ] `ring_bytes`：先给 `256MB`~`1GB`，按 `dropped_global` 再调。
- [ ] `mode`：生产建议 `0o600`（最小权限）。
- [ ] `reset_on_start`：首次部署建议 `true`。

## 4) 首次上线清理（关键）

因为是纯 v2，不保留 legacy 兼容，首次上线建议强制清理旧 SHM 对象：

- [ ] 停止 validator（或确保插件未在写）。
- [ ] 清理旧对象文件（按 `name` 对应路径）：
  - `rm -f /dev/shm/yellowstone_accounts`
- [ ] 确认文件已不存在再重启。

## 5) 启动与日志验收

- [ ] 启动 validator，确认插件加载成功。
- [ ] 日志出现 SHM 启用信息（包含 `name/path/ring_bytes/reset_on_start/disable_grpc_accounts`）。
- [ ] 无 ring header 版本错误（如 `ring version mismatch`）。

## 6) Reader 验收（最小可用）

- [ ] 启动 reader：
  - `cargo run -p yellowstone-grpc-client-simple --bin shm-reader -- --shm-name /yellowstone_accounts`
- [ ] 观察 reader 周期统计：
  - `delivered` 持续增长
  - `dropped_global` 不应快速飙升
  - `skipped_local` 不应长期异常增大

## 7) 并行对比（可选）

若你要对比「普通 gRPC」与「SHM」：

- [ ] 先设 `disable_grpc_accounts = false`
- [ ] 同时跑两个 bot：
  - bot-A：普通 gRPC 账户订阅
  - bot-B：SHM reader
- [ ] 对比延迟、CPU、带宽、丢包后，再决定是否切 `true`

## 8) 常见故障与处理

- `ring version mismatch`
  - 含义：reader/writer 版本不一致，或旧 ring 未清理
  - 处理：统一二进制版本 + 清理 `/dev/shm/<name>` + 重启

- `CRC32 mismatch`
  - 含义：帧完整性失败（数据损坏/读取异常）
  - 处理：先确认 writer/reader 都是当前版本；检查是否误连了错误 SHM 名称；必要时重建对象并重启

- `dropped_global` 持续升高
  - 含义：ring 太小或 reader 处理速度跟不上
  - 处理：增大 `ring_bytes`，优化 bot 消费路径，减少非必要处理

## 9) 回滚预案

- [ ] 若要临时停用 SHM：移除/置空 `grpc.shm` 配置并重启插件。
- [ ] 若要保留服务连续性：保持 `disable_grpc_accounts = false`，让 gRPC 账户流继续可用。

