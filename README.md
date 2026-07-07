# Yellowstone Dragon's Mouth - a Geyser based gRPC interface for Solana

This repo contains a fully functional gRPC interface for Solana, built and maintained by [Triton One](https://triton.one). It is built around Solana's Geyser interface. In this repo, we have the plugin and sample clients for multiple languages.

It provides the ability to get slots, blocks, transactions, deshred pre-execution transactions, and account update notifications over a standardised path.

For additional documentation, please see: https://docs.triton.one/rpc-pool/grpc-subscriptions

#### Known bugs

Block reconstruction inside gRPC plugin is based on information provided by BlockMeta, unfortunately, the number of entries for blocks generated on validators is always equal to zero. These blocks will always have zero entries. See issue on GitHub: https://github.com/solana-labs/solana/issues/33823

### Validator

```bash
solana-validator --geyser-plugin-config yellowstone-grpc-geyser/config.json
```

### Plugin config check

```bash
cargo-fmt && cargo run --bin config-check -- --config yellowstone-grpc-geyser/config.json
```

### Pre-commit hooks

Install repository hooks:

```bash
make install-hooks
```

The pre-commit hook will:

- ensure commit signing is enabled (`commit.gpgsign=true`)
- run `cargo fmt --all -- --check` and print a warning if formatting fails

### Block reconstruction

Geyser interface on block update does not provide detailed information about transactions and account updates. To provide this information with a block message, we must collect all messages and expect a specified order. By default, if we failed to reconstruct full block, we log an error message and increase the `invalid_full_blocks_total` counter in prometheus metrics. If you want to panic on invalid reconstruction, change the option `block_fail_action` in config to `panic` (default value is `log`).

### Filters for streamed data

Please check [yellowstone-grpc-proto/proto/geyser.proto](yellowstone-grpc-proto/proto/geyser.proto) for details.

- `commitment` — commitment level: `processed` / `confirmed` / `finalized`
- `accounts_data_slice` — array of objects `{ offset: uint64, length: uint64 }`, allow to receive only required data from accounts
- `ping` — optional boolean field. Some cloud providers (like Cloudflare, Fly.io) close the stream if the client doesn't send anything during some time. You can send the same filter every N seconds as a workaround, but this would not be optimal since you need to keep this filter. Instead, you can send a subscribe request with `ping` field set to `true` and ignore the rest of the fields in the request. Since we sent a `Ping` message every 15s from the server, you can send a subscribe request with `ping` as a reply and receive a `Pong` message.

#### Slots

- `filter_by_commitment` — by default, slots are sent for all commitment levels, but with this filter, you can receive only the selected commitment level

#### Account

Accounts can be filtered by:

- `account` — account Pubkey, match to any Pubkey from the array
- `owner` — account owner Pubkey, match to any Pubkey from the array
- `filters` — same as `getProgramAccounts` filters, array of `dataSize` or `Memcmp` (bytes, base58, base64 are supported)

If all fields are empty, then all accounts are broadcast. Otherwise, fields work as logical `AND` and values in arrays as logical `OR` (except values in `filters` that works as logical `AND`).

If you only need a fixed set of owners, you can pre-filter account updates in plugin config with `grpc.static_owner_allowlist`. This is applied before messages enter the gRPC pipeline and helps reduce CPU and queue pressure.

For lower-latency local consumption, you can also enable `grpc.shm` and stream account updates into a POSIX shared-memory ring (`shm_open`). A bot process can then read from `/dev/shm/<name_without_leading_slash>`.

#### Transactions

- `vote` — enable/disable broadcast `vote` transactions
- `failed` — enable/disable broadcast `failed` transactions
- `signature` — match only specified transaction
- `account_include` — filter transactions that use any account from the list
- `account_exclude` — opposite to `account_include`
- `account_required` — require all accounts from the list to be used in the transaction

If all fields are empty, then all transactions are broadcast. Otherwise, fields work as logical `AND` and values in arrays as logical `OR`.

#### Deshred transactions

`SubscribeDeshred` is a separate bi-directional stream for pre-execution transactions. Instead of waiting for Replay to execute a transaction and produce `TransactionStatusMeta`, the server reconstructs entries from incoming shreds and streams the decoded transaction as soon as it is available.

This gives you an earlier signal than the regular `transactions` stream, but it comes with less context:

   - available fields — `slot`, `signature`, `is_vote`, raw `transaction`, `loaded_writable_addresses`, `loaded_readonly_addresses`
   - unavailable fields — execution status, error details, logs, inner instructions, balances, compute usage, `TransactionStatusMeta`

`loaded_writable_addresses` and `loaded_readonly_addresses` contain addresses resolved from address lookup tables, so deshred filters can match both static account keys and dynamically loaded addresses.

Availability:

   - the protobuf API and Rust client expose `SubscribeDeshred`
   - this RPC is only available on Triton extension servers
   - the open-source `yellowstone-grpc-geyser` server in this repository currently returns `UNIMPLEMENTED` for `SubscribeDeshred`
   - the implemented version currently lives on the [`master-triton-ext` branch](https://github.com/rpcpool/yellowstone-grpc/tree/master-triton-ext)

The deshred transaction filter supports:

   - `vote` — enable/disable broadcast `vote` transactions
   - `account_include` — match transactions that mention any listed account, including ALT-loaded addresses
   - `account_exclude` — exclude transactions that mention any listed account, including ALT-loaded addresses
   - `account_required` — require all listed accounts to be present, including ALT-loaded addresses

#### Entries

Currently, we do not have filters for the entries, all entries are broadcast.

#### Blocks

- `account_include` — filter transactions and accounts that use any account from the list
- `include_transactions` — include all transactions
- `include_accounts` — include all accounts updates
- `include_entries` — include all entries

#### Blocks meta

Same as `Blocks` but without `transactions`, `accounts`, and entries. Currently, we do not have filters for block meta, all messages are broadcast.

### Limit filters

It's possible to add limits for filters in the config. If the `filters` field is omitted, then filters don't have any limits.

```json
"grpc": {
   "shm": {
      "name": "/yellowstone_accounts",
      "ring_bytes": "67_108_864",
      "mode": "0o600",
      "reset_on_start": true,
      "disable_grpc_accounts": false
   },
   "static_owner_allowlist": [
      "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
   ],
   "filters": {
      "accounts": {
         "max": 1,
         "any": false,
         "account_max": 10,
         "account_reject": ["TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"],
         "owner_max": 10,
         "owner_reject": ["11111111111111111111111111111111"]
      },
      "slots": {
         "max": 1
      },
      "transactions": {
         "max": 1,
         "any": false,
         "account_include_max": 10,
         "account_include_reject": ["TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"],
         "account_exclude_max": 10,
         "account_required_max": 10
      },
      "blocks": {
         "max": 1,
         "account_include_max": 10,
         "account_include_any": false,
         "account_include_reject": ["TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"],
         "include_transactions": true,
         "include_accounts" : false,
         "include_entries" : false
      },
      "blocks_meta": {
         "max": 1
      },
      "entry": {
         "max": 1
      }
   }
}
```

If `disable_grpc_accounts` is `true`, account updates are written only to shared memory and are no longer available in gRPC account subscriptions.

### Unary gRPC methods

#### Ping

#### GetLatestBlockhash

#### GetBlockHeight

#### GetSlot

#### IsBlockhashValid

#### GetVersion

### Examples

- [Go](examples/golang)
- [Rust](examples/rust)
- [TypeScript](examples/typescript)

For a `SubscribeDeshred` CLI example, see [examples/rust](examples/rust).

> [!NOTE]
> Some load balancers will terminate gRPC connections if no messages are sent from the client for a period of time.
> In order to mitigate this, you need to send a message periodically. The `ping` field in the SubscribeRequest is used for this purpose.
> The gRPC server already sends pings to the client, so you can reply with a ping, and your connection will remain open.
> You can see in the rust example how to reply to the ping from the server with the client.

### Projects based on Geyser gRPC

- https://github.com/rpcpool/yellowstone-grpc-kafka — forward gRPC stream to Kafka, dedup, read stream from Kafka with gRPC server




给你一份“服务端集成 POSIX SHM”的完整落地流程（按你现在这套代码）。

  代码已就位（你无需再改逻辑）

  - 配置入口：yellowstone-grpc-geyser/src/config.rs:187、yellowstone-grpc-geyser/src/config.rs:263
  - 写入入口：yellowstone-grpc-geyser/src/plugin.rs:287、yellowstone-grpc-geyser/src/plugin.rs:306
  - POSIX SHM 实现：yellowstone-grpc-geyser/src/shm.rs:62
  - /dev/shm 映射规则：yellowstone-grpc-geyser/src/shm.rs:138

  1) 在 Linux 服务器编译插件

  - 建议直接在 validator 机器编译（macOS 上 affinity 依赖会报错）：
  - cargo build --release -p yellowstone-grpc-geyser
  - 产物路径通常是：target/release/libyellowstone_grpc_geyser.so

  2) 修改 geyser 插件配置
  在你的插件配置（如 yellowstone-grpc-geyser/config.json）里把 grpc.shm 打开（当前默认是 null，见 yellowstone-grpc-
  geyser/config.json:32）：

  "grpc": {
    "shm": {
      "name": "/yellowstone_accounts",
      "ring_bytes": "268_435_456",
      "mode": "0o600",
      "reset_on_start": true,
      "disable_grpc_accounts": false
    },
    "static_owner_allowlist": [
      "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
    ]
  }

  字段含义：

  - name: POSIX shm 名称（建议以 / 开头）
  - ring_bytes: ring 数据区大小（不含 4KB header）
  - mode: 权限（0o600 推荐）
  - reset_on_start: 启动时是否清空 ring
  - disable_grpc_accounts: true 时账户更新只写 SHM，不再走 gRPC 账户流（README.md:136）

  3) 验证配置合法

  - cargo run -p yellowstone-grpc-geyser --bin config-check -- --config /path/to/config.json

  4) 启动 validator

  - solana-validator --geyser-plugin-config /path/to/config.json

  5) 启动后检查

  - 日志里应出现 shm output enabled（yellowstone-grpc-geyser/src/plugin.rs:175）
  - 检查 shm 文件：
      - ls -lh /dev/shm/yellowstone_accounts
      - 文件大小应约等于 4096 + ring_bytes

  6) bot 读取（你现成可用）

  - 示例 reader：
  - cargo run -p yellowstone-grpc-client-simple --bin shm-reader -- --shm-name "/yellowstone_accounts"
  - 去重默认开启（按 slot + write_version），实现见 examples/rust/src/bin/shm-reader.rs:86

  7) 建议切换策略（强烈建议）

  - 第一步先设 disable_grpc_accounts=false，跑一段时间对账稳定性
  - 第二步再切到 true，彻底走 SHM 低延迟路径

  8) 关键风险提示

  - 若日志出现 failed to write account update to shm，写入会被标记失败并停止；若此时 disable_grpc_accounts=true，你会丢
    账户更新。
  - 所以生产建议先双轨运行（gRPC+SHM），确认稳定再关 gRPC 账户流。

  如果你要，我下一条可以直接给你一个“按你的实际 TPS/平均 account 大小”计算 ring_bytes 的推荐值表。
