# 多 Provider 轮询 — 实施计划

> 范围：在故障队列之上新增"多 Provider 轮询"层，支持短期 session 粘性、新 session 轮询选 provider。
> 关系：单 Provider（现状）/ 多 Provider 轮询（新）/ 故障队列（现状）三层叠加，多 Provider 必开故障队列。
> 状态：草案待用户对开放决策点拍板。

---

## 现状摘要

- **Tauri + Rust + TypeScript**，单进程；`AppState` 持有 `db: Arc<Database>` + `proxy_service: ProxyService`。
- **请求路径**：`hyper accept loop → axum router → handlers → forwarder → 上游`。
- **ProviderRouter::select_providers**（`src-tauri/src/proxy/provider_router.rs:37-109`）分两路：
  - `auto_failover_enabled = false` → 仅当前 provider（单 Provider 模式）
  - `auto_failover_enabled = true` → 按 `failover_queue` 顺序（sort_index ASC），过滤熔断器 Open
- **故障队列存储**：`providers.in_failover_queue + sort_index`，无独立表
- **熔断器**：内存 `HashMap<"app:provider_id", Arc<CircuitBreaker>>`，三态机（Closed/Open/HalfOpen），重启清零
- **Session 概念**：`ProxySession` 完整但 `#[allow(dead_code)]`；主要路径用 `RequestContext` 携带 session_id，**不与 Provider 绑定**
- **per-app 锁**：`SwitchLockManager` 串行化同 app 切换（v3.16.1 引入）
- **Failover Guardrails**（v3.15.0）：启用 failover 必须先接管该 app；停止 proxy 时若有 app 仍依赖 takeover 则禁止
- **Official Provider 拦截**（v3.14.0）：Local Routing 模式下阻止切到官方供应商
- **测试现状**：`tests/proxy_commands.rs` 仅 78 行，2 个集成测试，无 mock 上游框架

---

## 目标架构：三层叠加

```
┌──────────────────────────────────────────────────────────┐
│  Layer 0: 单 Provider 模式                              │
│  multi_provider_enabled = false                          │
│  → 等价旧行为（current_provider，可选故障队列）          │
├──────────────────────────────────────────────────────────┤
│  Layer 1: Session 绑定（新）                            │
│  multi_provider_enabled = true                           │
│  → 短期 session 粘性到某 provider（TTL 默认 1h）        │
├──────────────────────────────────────────────────────────┤
│  Layer 2: 轮询池选择（新）                              │
│  → 新 session 按策略从池中选 primary                    │
│  策略: round_robin / weighted_random / least_failures / lowest_latency
├──────────────────────────────────────────────────────────┤
│  Layer 3: 故障队列（强制开启，现状）                    │
│  → session 内 primary 失败仍走队列 fallback             │
│  → 全失败才解绑 session，下次重选                        │
└──────────────────────────────────────────────────────────┘
```

**对外不变量**：
- `multi_provider_enabled = false` ⇒ 100% 旧行为（包括 auto_failover 开关语义）
- `multi_provider_enabled = true` ⇒ 强制 `auto_failover_enabled = true`（一次 DB 事务原子写）
- 旧用户升级后默认 `multi_provider_enabled = 0`，需主动 opt-in

---

## 阶段 0: 数据层与基础类型（地基）

**目标**：
- 灰度开关（环境变量）+ DB 迁移
- 复合 Tauri 命令（一次事务完成 multi+failover 联动）
- 新表 `session_bindings` + 扩展 `proxy_config` / `providers` 列
- Rust struct / DAO / 轮询计数器 / 单元测试

**改动文件**：
- 新增 `src-tauri/src/database/migrations/migration_0008_multi_provider.sql`
- 修改 `src-tauri/src/database/migrations.rs`（注册）
- 新增 `src-tauri/src/database/dao/session_binding.rs`
- 修改 `src-tauri/src/database/dao/proxy.rs`、`providers.rs`
- 新增 `src-tauri/src/proxy/multi_provider.rs`、`pool_counter.rs`、`provider_selection.rs`
- 修改 `src-tauri/src/proxy/provider_router.rs`（只加新方法，不改旧签名）

**SQL 草案**：
```sql
-- 1. 新表
CREATE TABLE IF NOT EXISTS session_bindings (
    session_id     TEXT PRIMARY KEY,
    app_type       TEXT NOT NULL,
    provider_id    TEXT NOT NULL,
    bound_at       INTEGER NOT NULL,
    expires_at     INTEGER NOT NULL,
    last_seen_at   INTEGER NOT NULL,
    request_count  INTEGER NOT NULL DEFAULT 1
);
CREATE INDEX IF NOT EXISTS idx_sb_app        ON session_bindings(app_type);
CREATE INDEX IF NOT EXISTS idx_sb_expires    ON session_bindings(expires_at);

-- 2. 扩展 proxy_config（默认值保旧行为）
ALTER TABLE proxy_config ADD COLUMN multi_provider_enabled INTEGER NOT NULL DEFAULT 0;
ALTER TABLE proxy_config ADD COLUMN pool_strategy           TEXT    NOT NULL DEFAULT 'round_robin';
ALTER TABLE proxy_config ADD COLUMN session_ttl_seconds     INTEGER NOT NULL DEFAULT 3600;
ALTER TABLE proxy_config ADD COLUMN session_ttl_sliding     INTEGER NOT NULL DEFAULT 0;
ALTER TABLE proxy_config ADD COLUMN pool_include_official   INTEGER NOT NULL DEFAULT 0;

-- 3. 扩展 providers
ALTER TABLE providers ADD COLUMN pool_weight  INTEGER NOT NULL DEFAULT 1;
ALTER TABLE providers ADD COLUMN pool_enabled INTEGER NOT NULL DEFAULT 0;
```

**关键守卫**：
- 启动时清理幽灵 binding：`DELETE FROM session_bindings WHERE NOT EXISTS (SELECT 1 FROM providers p WHERE p.id = session_bindings.provider_id AND p.app_type = session_bindings.app_type)`（同时处理 provider 被删/被禁用）
- 环境变量 `CC_SWITCH_MULTI_PROVIDER=off` 强制灰度降级（即使 DB 配了也走旧路径）
- 复合 Tauri 命令 `set_multi_provider_config` 一次事务写：multi_provider_enabled + auto_failover_enabled + pool seed

**测试**：
- 迁移幂等：跑两遍 migration_0008 不报错
- DAO CRUD + 二次校验（命中 binding 后必须二次查 provider 仍可用）
- 灰度开关读环境变量
- struct serde 往返
- 旧行为回归：复用 `provider_router.rs:329-469` 5 个测试，`multi_provider_enabled=false` 路径 bit-by-bit 不变

**完成标准**：
- `cargo test` 全绿
- 老逻辑 0 行为变化（集成测试跑通现有故障队列 E2E 场景）
- 灰度开关、复合命令、session 二次校验全部就位

---

## 可逆性设计：你的"一键回退"路径

> 你的需求：如果多 Provider 不好用，要能删掉我增加的表和列、回到原状。
> 关键发现：项目**已经有一套完整的回退机制**——我之前方案没说清楚，现在补上。

### 项目现成的回退基础设施

| 组件 | 位置 | 状态 |
|---|---|---|
| 自动备份 | `src-tauri/src/database/mod.rs:128-136` `Database::init` 内 | ✅ **升级前自动跑** `db.backup_database_file()` |
| 备份路径 | `~/.cc-switch/backups/db_backup_YYYYMMDD_HHMMSS.db` | ✅ 命名带时间戳 |
| 保留策略 | `crate::settings::effective_backup_retain_count()` | ✅ 默认保留 N 个 |
| 列出备份 | `#[tauri::command] list_db_backups`（`import_export.rs:146`） | ✅ 已暴露给前端 |
| 回退命令 | `#[tauri::command] restore_db_backup(filename)`（`import_export.rs:152`） | ✅ 已暴露给前端 |
| 回退后迁移 | `restore_from_backup` 完成后自动 `create_tables + apply_schema_migrations`（`backup.rs:592-595`） | ⚠️ **新 schema 仍会被应用** |

### 你的 4 条回退路径

#### 路径 1：UI 一键回退（最简单，推荐）
1. 打开 `Settings → 数据管理 → 数据库备份`
2. 选升级前的备份（文件名带 `db_backup_20260606_xxx.db`）
3. 点"恢复" → 自动跑回旧版 schema

**适用场景**：你试用后觉得"功能不好用"，回到 v3.x 行为。

**风险**：如果新版本代码仍在跑，回退后 schema 仍会被迁移到最新（`backup.rs:592-595`）。
- 解决：**先 git revert + 重装旧版二进制**，再恢复备份；或跳过恢复，直接重装旧版（见路径 2）。

#### 路径 2：代码级回退（彻底，推荐用于彻底放弃）
```bash
# 1. 关闭应用
# 2. 切到上一版本 tag
git checkout v3.x.x    # 多 Provider 之前的最后一个 tag

# 3. 重装旧版
#    - dev: cargo build + 启动旧版
#    - 用户: 卸载重装上一版安装包

# 4. 旧版的 create_tables + apply_schema_migrations 启动时自动跑
#    - 遇到未知列（multi_provider_enabled 等）→ 容错跳过（参考 schema.rs:291-313 的 `let _ = conn.execute("ALTER TABLE...")` 模式）
#    - 不需要人工删列
```

**为什么不手动删列？**
- 新版的迁移是**单向向上**的（`while version < SCHEMA_VERSION`，schema.rs:380）
- 旧版代码不知道新列的存在，但 SQLite **不限制未知列**——旧版运行时新列就当看不见
- 删列反而要 `ALTER TABLE DROP COLUMN`（SQLite 3.35+ 支持），每个列一条 ALTER，复杂且容易踩坑
- 列留在表里**零开销**（占字节数 × 行数，几 KB），不影响旧版功能

#### 路径 3：手动删列 + 删表（如果你坚持要"干净"）

```sql
-- 1. 关闭应用

-- 2. 备份当前 db（即使你想删，安全网先有）
sqlite3 ~/.cc-switch/cc-switch.db ".backup /tmp/before_cleanup.db"

-- 3. 删表（这是多 Provider 唯一新增的表）
DROP TABLE IF EXISTS session_bindings;
DROP INDEX IF EXISTS idx_sb_app;
DROP INDEX IF EXISTS idx_sb_expires;

-- 4. 删列（这些是多 Provider 扩展的列）
-- SQLite 3.35+ 支持 DROP COLUMN；项目用 rusqlite，需要客户端版本足够新
ALTER TABLE proxy_config DROP COLUMN multi_provider_enabled;
ALTER TABLE proxy_config DROP COLUMN pool_strategy;
ALTER TABLE proxy_config DROP COLUMN session_ttl_seconds;
ALTER TABLE proxy_config DROP COLUMN session_ttl_sliding;
ALTER TABLE proxy_config DROP COLUMN pool_include_official;

ALTER TABLE providers DROP COLUMN pool_weight;
ALTER TABLE providers DROP COLUMN pool_enabled;

-- 5. 启动旧版本（schema.rs:380 的 while version < SCHEMA_VERSION 不再触发迁移）
```

**风险**：
- 旧版本如果加了新约束/索引，列删除可能因外键依赖失败
- `session_bindings` 表如果被新代码插过数据，需要先清空

#### 路径 4：环境变量灰度降级（最安全，**实施期间推荐**）

```bash
# 临时禁用多 Provider（即使 DB 配了也走旧路径）
export CC_SWITCH_MULTI_PROVIDER=off
cc-switch
```

**实现**：`ProviderRouter::new` 读环境变量，强制调用 `select_providers_legacy`。
**优势**：
- 不改 DB，**100% 可逆**——`unset CC_SWITCH_MULTI_PROVIDER` 就恢复
- 不重装代码
- 适合"试用但担心影响线上"的场景

### 阶段 0 的可逆性前置条件

为了让 4 条路径都可行，阶段 0 必须在做变更时同步落实：

1. **代码层面**：`add_column_if_missing` 是项目惯用法（schema.rs:2179），已天然支持"旧库遇到新列跳过"——无需特殊处理
2. **DB 层面**：所有新列 `DEFAULT 0/1/3600/round_robin`，**老数据自动填充**——零迁移成本
3. **代码层面**：新逻辑只在 `multi_provider_enabled=true` 分支启用，`false` 走 100% 旧代码路径——**灰度开关就足够回退**
4. **回退 UI**：`list_db_backups` / `restore_db_backup` 已经存在，**无需新增**——只需在文档里写明用法
5. **回退测试**：阶段 0 末尾加一个"反向测试"——启动新代码 → 关闭 multi_provider → 跑老 E2E → 验证 0 行为变化

### 阶段 0 末尾交付的可逆性产物

| 产物 | 位置 | 用途 |
|---|---|---|
| `CC_SWITCH_MULTI_PROVIDER=off` 灰度开关 | `provider_router.rs:ProviderRouter::new` | 路径 4（最安全） |
| 升级前自动备份 | 已有，**无需改** | 路径 1 / 路径 3 的安全网 |
| 旧行为回归测试 | `tests/proxy_commands.rs` 扩展 | 验证回退后行为 bit-by-bit 不变 |
| `IMPLEMENTATION_PLAN.md` §"可逆性设计" | 本文档 | 4 条回退路径的 SOP |
| `database/migrations/migration_0011_rollback_multi_provider.sql`（参考脚本） | docs/ | 路径 3 的可复制 SQL |

### 推荐的"上线后试用 → 决策"流程

```
T0: 实施阶段 0-4，灰度开关默认关
     ↓
T1: 启用 CC_SWITCH_MULTI_PROVIDER=on，试用 1-2 周
     ↓
T2a: 觉得好用 → 把灰度开关默认值翻为 on（写进代码或环境变量默认）
T2b: 觉得不好用 → unset CC_SWITCH_MULTI_PROVIDER，立即回退
     ↓
T2b 后续：git revert 多 Provider PR + 重装旧版（路径 2）
```

---

## 阶段 1: ProviderRouter 重构 + Forwarder 改造

**目标**：
- `ProviderRouter::select_providers` 新签名（保留旧方法）
- Session 命中 / 轮询选择 / 降级三路径实现
- `forward_with_retry_inner` 接收 `ProviderSelection`
- FailoverSwitchManager 与 session 粘性协调

**对接点**：
- `src-tauri/src/proxy/provider_router.rs:37-109` 现有 `select_providers`
- `src-tauri/src/proxy/forwarder.rs:341` 现有 `bypass_circuit_breaker = providers.len() == 1`
- `src-tauri/src/proxy/failover_switch.rs:41-72` 切换去重

**新签名**：
```rust
pub async fn select_providers(
    &self,
    app_type: &str,
    session_id: Option<&str>,
) -> Result<ProviderSelection, AppError>

pub enum ProviderSelection {
    Single(Provider),                                          // 单 Provider 模式（bypass=true）
    SessionBound { provider: Provider, session_id: String },   // session 命中
    Pool { primary: Provider, fallbacks: Vec<Provider> },      // 多 Provider + 故障队列
}
```

**核心决策流程**（伪代码）：
```
1. 读 AppProxyConfig（含 multi_provider_enabled + 灰度开关）
2. !multi_provider_enabled 或 灰度关闭 → 调旧 select_providers_legacy
3. session_id 命中且二次校验 provider 仍 enabled/在池 → return SessionBound
4. 过滤熔断器 Open + 过滤 official（除非 pool_include_official）
5. 候选空 → 降级 Single(current_provider) + log warn
6. 候选仅 1 → return Pool { primary, fallbacks: [] }（bypass=true 等价 Single）
7. 按策略选 primary，构造 fallbacks
8. INSERT ... ON CONFLICT(session_id) DO UPDATE SET last_seen_at = now RETURNING ...
   （原子化写 binding；并发同 session 安全）
9. return Pool
```

**bypass_circuit_breaker 新规则**：
- `Single` ⇒ `bypass = true`（旧单 Provider 行为）
- `SessionBound` ⇒ `bypass = false`（session 内允许熔断保护）
- `Pool` ⇒ `primary bypass=true, fallbacks bypass=false`（单元素池自动降级到 Single 行为，避免"开了反而被熔断器挡"）

**Forwarder 改造**：
- 接收 `ProviderSelection`，展开为 `[primary, ...fallbacks]`
- 失败处理：同 session 不解绑（让 fallbacks 接续），**fallback 链全失败才解绑** + 异步 `tokio::spawn` 更新 last_seen_at
- FailoverSwitchManager 协调：session 内 fallback 成功不触发全局热切换（避免 current_providers 反复横跳）；只更新"实际响应客户端的 provider"日志

**单飞锁**（同 session 写 binding 竞态）：
- `Arc<DashMap<(app_type, session_id), Arc<tokio::sync::Mutex<()>>>>` in-flight 锁
- 命中 in-flight → 等同结果（避免重复 SELECT+INSERT 竞态）

**测试**：
- 4 种策略（round_robin / weighted_random / least_failures / lowest_latency）
- 空池、单元素池、池全熔断、池含 official（默认过滤/可显式 opt-in）
- session 命中、过期、失效（provider 被删/禁用/熔断 Open）
- 并发同 session_id 单飞
- 旧 5 个路由测试扩展为 `multi=false/true` 双向

**完成标准**：
- `cargo test proxy::provider_router` + `cargo test proxy::forwarder` 全绿
- 集成测试端到端跑通"多 Provider + 故障队列"与"单 Provider"两路径

---

## 阶段 2: Tauri 命令层

**目标**：
- 复合命令（推荐 6 个新命令 + 3 个老命令改造）
- 启动时启动 session 清理任务（每 5 分钟，分批小事务）
- 灰度开关读环境变量

**新增命令**（`src-tauri/src/commands/proxy.rs`）：

| 命令 | 入参 | 出参 | 说明 |
|---|---|---|---|
| `set_multi_provider_config` | `appType, config: MultiProviderConfig` | `()` | **复合命令**：一次事务写 multi_provider_enabled + 强制 auto_failover=true + seed 队列 |
| `get_multi_provider_config` | `appType: String` | `MultiProviderConfig` | 读配置 |
| `get_provider_pool` | `appType: String` | `Vec<ProviderPoolItem>` | 池成员列表 |
| `set_provider_pool` | `appType, items: Vec<{providerId, weight, poolEnabled}>` | `()` | 批量更新池（**同步** `in_failover_queue` 避免双源真相） |
| `get_session_bindings` | `appType: String` | `Vec<SessionBinding>` | 调试用 |
| `clear_all_session_bindings` | `appType: String` | `()` | 紧急清空 |

**老命令改造**：
- `set_auto_failover_enabled`：当 `multi_provider_enabled=true` 时拒绝独立关闭；强制走 `set_multi_provider_config`
- `update_failover_queue`：同步 `pool_enabled` 字段（双源真相守护：queue 操作必同步 pool）
- `switch_proxy_provider`：session 绑定检查——若切到的 provider 与某 active session 绑定冲突，提示用户

**清理任务**（`src-tauri/src/proxy/cleanup_task.rs`）：
```rust
// 每 5 分钟跑一次
DELETE FROM session_bindings
WHERE session_id IN (
    SELECT session_id FROM session_bindings
    WHERE expires_at < ?1
    LIMIT 500
);
```
- 分批 500 条/事务，避免长事务持锁
- 启动时先跑一次清理幽灵 binding（同阶段 0 启动逻辑）

**测试**：
- 命令入参校验（TTL 60..86400，strategy 枚举）
- 复合命令的原子性（写 multi 时强制 failover=1）
- 清理任务 5min 跑通

**完成标准**：
- `cargo test commands::proxy` 全绿
- 手动验证 6 个新命令 happy path

---

## 阶段 3: 前端类型 + API + Query 层

**目标**：
- 新增 `src/types/multiProvider.ts`
- `src/lib/api/multiProvider.ts`（避免与 `failover.ts` 循环依赖）
- `src/lib/query/multiProvider.ts` + 失效矩阵
- 修复 `failover.ts` L9-21 Provider 类型重复（统一引用 `src/types.ts`）

**类型**：
```ts
export type PoolStrategy =
  | "round_robin" | "weighted_random" | "least_failures" | "lowest_latency";

export interface MultiProviderConfig {
  multiProviderEnabled: boolean;
  poolStrategy: PoolStrategy;
  sessionTtlSeconds: number;       // 60..86400
  sessionTtlSliding: boolean;
  poolIncludeOfficial: boolean;
}

export interface ProviderPoolItem {
  providerId: string;
  providerName: string;
  appType: string;
  weight: number;                  // 1..100
  poolEnabled: boolean;
  sortIndex: number;
  inFailoverQueue: boolean;        // 冗余展示
  health?: ProviderHealth;         // 供 least_failures / lowest_latency 排序提示
}
```

**缓存键**：
- `multiProviderConfig,appType`
- `multiProviderPool,appType`
- `multiProviderBindings,appType`

**失效矩阵**（`useUpdateMultiProviderConfig` onSettled）：
```
multiProviderConfig,appType
failoverQueue,appType
availableProvidersForFailover,appType
autoFailoverEnabled,appType
providers,appType
proxyStatus
```

**与 useAutoFailoverEnabled 协调**：
- 开启 multiProvider 时**只能走复合命令** `set_multi_provider_config`，由后端保证原子性
- 前端不要分别调 multi + failover 两个命令（避免写时序竞态）

**完成标准**：
- `pnpm test` + `pnpm lint` 全绿
- Provider 类型重复消除

---

## 阶段 4: UI 组件 + 集成

**目标**：
- `MultiProviderConfigPanel`：开关 + 策略下拉 + TTL 输入 + 滑窗 checkbox + 官方 opt-in
- `ProviderPoolManager`：拖拽排序（复用 `FailoverQueueManager` 的 dnd-kit）+ 权重输入 + 明确标注"已加入 failover 队列 ≠ 已加入 pool"
- `SessionBindingList`（调试面板，可选）：折叠面板 + 清空按钮
- 整合到 `ProxyPanel.tsx`：新增 Tab "轮询池"
- 联动 `AutoFailoverConfigPanel.tsx`：开启 multi 时展示复合命令提示

**UI 关键细节**：
- weight 默认 1 的虚线边框 + tooltip "未设置时按轮询处理"（避免误以为已加权）
- "一键导入"按钮：把 `in_failover_queue=1` 的 provider 一键加入 pool
- 灰度开关 UI 不可见（环境变量控制，文档提示）

**完成标准**：
- 手动 E2E：开多 Provider → 多次请求 → SessionBindingList 看到绑定 → fallback 一次 → 绑定仍存在 → 关闭多 Provider → 回退单 Provider 行为

---

## 开放决策点（需用户拍板）

| # | 决策点 | 推荐 | 备选 |
|---|---|---|---|
| 1 | session TTL 固定 vs 滑动 | 固定 3600s 默认 + 可切滑动 | 强制滑动 / 24h 固定 |
| 2 | session 绑定失败是否立即解绑 | 否，fallback 全失败才解绑 | 失败 ≥N 次解绑 |
| 3 | 多 Provider 池与故障队列关系 | 共享成员（pool_enabled ⊆ in_failover_queue） | 完全独立 |
| 4 | 默认轮询策略 | round_robin | least_failures（需先积累数据） |
| 5 | pool 是否含 official Provider | 默认过滤，显式 opt-in | 默认允许 |
| 6 | 轮询计数器持久化 | 纯内存（重启微偏置几秒） | 持久化到 DB |
| 7 | 灰度策略 | DB 默认关闭 + 环境变量可强制关 | 全量上线 |
| 8 | SessionBindingList 是否进产品 | 可选调试面板 | 隐藏/不显示 |
| 9 | weight 默认值语义 | 1（不区分未设置） | NULL 区分未设置 vs 1 |

---

## 关键风险与已规避路径

| 风险 | 规避方案 |
|---|---|
| 启动时 binding 指向幽灵 provider | 阶段 0 启动清理 + 阶段 1 命中 binding 后二次校验（双保险） |
| 复合命令与 v3.15.0 守卫冲突 | 复合命令在事务内强制 takeover 顺序检查（multi 必须 takeover 在前） |
| Codex 池混搭 Chat/Responses 协议 | 阶段 1 选 fallback 时**保证同协议簇**（不混跨协议 fallback） |
| FailoverSwitchManager 反复切 | SessionBound 路径下不更新 current_providers；只更新"实际响应客户端"日志 |
| v3.11.0 partial-key 合并回滚教训 | provider 配置零侵入：multi 只读 `in_failover_queue + pool_enabled`，不修改 `settingsConfig` |
| 池全熔断时配置形同虚设 | 阶段 1 新增 `PoolAllCircuitOpen` → 降级 `Single(current)` + warn 日志 |
| 池仅 1 provider 时"开了反而更差" | bypass 规则改为 `Pool { primary } 单独 bypass=true`（单元素池等价 Single） |
| 旧用户升级行为变化 | DB 默认 `multi_provider_enabled=0`；环境变量灰度开关；阶段 0 老逻辑回归测试 |

---

## 相关文件路径速查

**后端**：
- `src-tauri/src/proxy/provider_router.rs:37-109` `select_providers` 主入口
- `src-tauri/src/proxy/forwarder.rs:341` `bypass_circuit_breaker`
- `src-tauri/src/proxy/failover_switch.rs:41-72` 切换去重
- `src-tauri/src/proxy/switch_lock.rs` per-app 锁
- `src-tauri/src/proxy/session.rs` session_id 提取
- `src-tauri/src/proxy/circuit_breaker.rs` 熔断器
- `src-tauri/src/database/dao/proxy.rs` proxy_config / provider_health
- `src-tauri/src/database/dao/failover.rs` 故障队列
- `src-tauri/src/database/migrations.rs` 迁移注册
- `src-tauri/src/commands/proxy.rs` Tauri 命令
- `src-tauri/src/commands/failover.rs:96-98` v3.15.0 守卫

**前端**：
- `src/lib/api/proxy.ts` 14 个方法
- `src/lib/api/failover.ts` 10 个方法（L9-21 Provider 类型重复）
- `src/lib/query/proxy.ts` 8 query + 6 mutation
- `src/lib/query/failover.ts` 11 hook
- `src/hooks/useProxyStatus.ts` 聚合门面
- `src/types/proxy.ts`、`src/types.ts` 类型
- `src/components/proxy/{ProxyPanel,AutoFailoverConfigPanel,FailoverQueueManager,FailoverToggle,CircuitBreakerConfigPanel}.tsx`

**文档**：
- `docs/guides/proxy-guide-zh.md` 代理指南
- `docs/user-manual/zh/4-proxy/4.{1,2,3}-*.md` 服务/路由/故障转移
- `CHANGELOG.md` v3.9.0 / v3.15.0 / v3.16.x 里程碑

---

## 测试策略总览

- **阶段 0 末尾 gate**：复用 `provider_router.rs:329-469` 5 个测试，`multi_provider_enabled=false` 路径 bit-by-bit 不变
- **阶段 1 末尾**：并发同 session_id 单飞测试 + 4 种策略单测
- **阶段 2 末尾**：复合命令原子性 + 清理任务分批
- **阶段 3 末尾**：TypeScript 类型 + 失效矩阵单测
- **阶段 4 末尾**：手动 E2E 跑通 + 跨阶段 1-5 端到端测试 owner
- **整体**：基线 bench（阶段 0 之前）— 当前 `select_providers` 1000 并发 P50/P99 latency，每个阶段后重跑
