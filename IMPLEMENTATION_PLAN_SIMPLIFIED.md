# 多 Provider 轮询（简化版）— 实施计划

> 范围：在故障队列基础上新增"轮询模式"开关，让新 session 轮流使用不同 provider
> 关系：故障队列（现状）+ 轮询模式开关（新），复用故障队列作为轮询池
> 状态：待实施

---

## 核心设计

### 现状

- 故障队列已有多个 provider（`in_failover_queue=true`，按 `sort_index` 排序）
- 当前行为：按顺序 P1 → P2 → P3（主备模式）
- Session ID 提取已存在（`extract_session_id`）

### 目标

新增一个开关 `multi_provider_polling_enabled`：
- **关闭**（默认）：旧逻辑，按 sort_index 顺序 P1 → P2 → P3
- **开启**：新 session 轮流使用不同 provider（round_robin）
  - Session 内复用同一个 provider（短期粘性，TTL 默认 1h）
  - Primary 失败后 fallback 到故障队列其他 provider

### 关键简化

- ❌ 不需要独立的轮询池（`pool_enabled` 字段）
- ❌ 不需要策略选择（硬编码 `round_robin`）
- ❌ 不需要复杂的池管理 UI
- ✅ 直接复用故障队列（`in_failover_queue=true` 的 provider）
- ✅ 只需 1 个开关 + 1 个 TTL 配置

---

## 数据层改动

### 数据库字段

```sql
-- proxy_config 表新增 2 个字段
ALTER TABLE proxy_config ADD COLUMN multi_provider_polling_enabled INTEGER NOT NULL DEFAULT 0;
ALTER TABLE proxy_config ADD COLUMN session_ttl_seconds INTEGER NOT NULL DEFAULT 3600;

-- session_bindings 表（已有，复用）
-- 用于记录 session → provider 映射
```

### 删除的字段（从阶段 0-2 回退）

```sql
-- 删除（不再需要）
ALTER TABLE providers DROP COLUMN pool_enabled;
ALTER TABLE providers DROP COLUMN pool_weight;
ALTER TABLE proxy_config DROP COLUMN pool_strategy;
ALTER TABLE proxy_config DROP COLUMN pool_include_official;
ALTER TABLE proxy_config DROP COLUMN session_ttl_sliding;
```

---

## 后端改动

### 1. ProviderRouter 改动（~20 行）

**改动文件**：`src-tauri/src/proxy/provider_router.rs`

**改动 1**：修改 `load_pool` 方法
```rust
// 当前：从 pool_enabled=true 的 provider 中选择
async fn load_pool(&self, app_type: &str) -> Result<Vec<Provider>, AppError> {
    let all = self.db.get_all_providers(app_type)?;
    let pool: Vec<Provider> = all
        .into_values()
        .filter(|p| p.pool_enabled)  // ❌ 删除这个
        .collect();
    Ok(pool)
}

// 改为：直接用故障队列
async fn load_pool(&self, app_type: &str) -> Result<Vec<Provider>, AppError> {
    let failover_queue = self.db.get_failover_queue(app_type)?;
    // 过滤熔断器 Open 的
    let available: Vec<Provider> = failover_queue.into_iter()
        .filter(|p| self.is_circuit_available(app_type, &p.id).await)
        .collect();
    Ok(available)
}
```

**改动 2**：简化 `select_providers_multi` 方法
```rust
async fn select_providers_multi(
    &self,
    app_type: &str,
    session_id: Option<&str>,
) -> Result<Vec<Provider>, AppError> {
    let config = self.db.get_proxy_config_for_app(app_type).await?;

    // 1. Session 粘性检查
    if let Some(sid) = session_id {
        if let Some(binding) = self.db.get_session_binding(sid)? {
            if binding.app_type == app_type
                && binding.expires_at > unix_now()
            {
                // 二次校验：provider 仍在故障队列 + 熔断器可用
                if let Some(p) = self.db.get_provider_by_id(&binding.provider_id, app_type)? {
                    if p.in_failover_queue && self.is_circuit_available(app_type, &p.id).await {
                        // 命中！复用
                        let fallbacks = self.load_pool_fallbacks(app_type, &p.id).await?;
                        return Ok(vec![p].into_iter().chain(fallbacks).collect());
                    }
                }
                // 失效 → 删绑定
                let _ = self.db.remove_session_binding(sid);
            }
        }
    }

    // 2. 轮询选择（硬编码 round_robin）
    let pool = self.load_pool(app_type).await?;
    if pool.is_empty() {
        return Err(AppError::NoProvidersConfigured);
    }

    let index = self.pool_counter.next(app_type) % pool.len();
    let primary = pool[index].clone();
    let fallbacks: Vec<Provider> = pool.into_iter().filter(|p| p.id != primary.id).collect();

    // 3. 写 session 绑定（短期粘性）
    if let Some(sid) = session_id {
        self.db.upsert_session_binding(
            sid,
            app_type,
            &primary.id,
            config.session_ttl_seconds as i64,
        )?;
    }

    Ok(vec![primary].into_iter().chain(fallbacks).collect())
}
```

**改动 3**：删除不需要的方法
```rust
// 删除
- async fn pick_by_strategy(...)  // 不需要策略选择
- async fn is_pool_include_official(...)  // 不需要 official 过滤
- async fn load_pool_fallbacks(...)  // 合并到 load_pool
```

### 2. Tauri 命令（~30 行）

**新增文件**：`src-tauri/src/commands/multi_provider.rs`

```rust
#[tauri::command]
pub async fn set_multi_provider_polling(
    state: tauri::State<'_, AppState>,
    app_type: String,
    enabled: bool,
) -> Result<(), String> {
    let mut config = state.db.get_proxy_config_for_app(&app_type).await?;

    // 开启时强制 auto_failover=true
    if enabled && !config.auto_failover_enabled {
        config.auto_failover_enabled = true;
    }

    config.multi_provider_polling_enabled = enabled;
    state.db.update_proxy_config_for_app(config).await?;

    // 关闭时清空 session bindings
    if !enabled {
        state.db.clear_session_bindings_for_app(&app_type)?;
    }

    Ok(())
}
```

**注册命令**：`src-tauri/src/lib.rs`
```rust
// 在命令列表中增加
commands::set_multi_provider_polling,
```

### 3. 数据库迁移（~10 行）

**新增文件**：`src-tauri/src/database/migrations/migration_0009_multi_provider_simplified.sql`

```sql
-- 简化版多 Provider 轮询
ALTER TABLE proxy_config ADD COLUMN multi_provider_polling_enabled INTEGER NOT NULL DEFAULT 0;
ALTER TABLE proxy_config ADD COLUMN session_ttl_seconds INTEGER NOT NULL DEFAULT 3600;
```

**注册迁移**：`src-tauri/src/database/schema.rs`
```rust
// 在 SCHEMA_VERSION 中增加版本号
// 在 apply_schema_migrations 中调用新迁移
```

---

## 前端改动

### 1. 类型定义（~10 行）

**修改文件**：`src/types/proxy.ts`

```typescript
export interface AppProxyConfig {
  // ... 现有字段
  multiProviderPollingEnabled: boolean;  // 新增
  sessionTtlSeconds: number;             // 新增
}
```

### 2. API 封装（~15 行）

**新增文件**：`src/lib/api/multiProvider.ts`

```typescript
export async function setMultiProviderPolling(
  appType: AppId,
  enabled: boolean
): Promise<void> {
  return invoke("set_multi_provider_polling", { appType, enabled });
}
```

### 3. React Query Hook（~20 行）

**新增文件**：`src/lib/query/multiProvider.ts`

```typescript
export function useSetMultiProviderPolling() {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: ({ appType, enabled }: { appType: AppId; enabled: boolean }) =>
      setMultiProviderPolling(appType, enabled),
    onSettled: (_, __, { appType }) => {
      queryClient.invalidateQueries({ queryKey: ["appProxyConfig", appType] });
      queryClient.invalidateQueries({ queryKey: ["failoverQueue", appType] });
      queryClient.invalidateQueries({ queryKey: ["proxyStatus"] });
    },
  });
}
```

### 4. UI 组件（~30 行）

**修改文件**：`src/components/proxy/FailoverQueueManager.tsx`

在故障队列管理中增加轮询模式开关：

```tsx
import { useSetMultiProviderPolling } from "@/lib/query/multiProvider";

// 在组件中
const setPolling = useSetMultiProviderPolling();

// 在 UI 中（故障队列列表上方）
<div className="flex items-center justify-between p-3 border rounded-lg mb-4">
  <div>
    <Label className="font-medium">
      {t("multiProvider.pollingLabel", "轮询模式")}
    </Label>
    <p className="text-xs text-muted-foreground">
      {t("multiProvider.pollingHint", "新 session 将轮流使用队列中的不同 provider")}
    </p>
  </div>
  <Switch
    checked={config.multiProviderPollingEnabled}
    disabled={!config.autoFailoverEnabled || setPolling.isPending}
    onCheckedChange={(v) => setPolling.mutate({ appType, enabled: v })}
  />
</div>
```

### 5. i18n（~10 行）

**修改文件**：`src/i18n/locales/zh.json`、`src/i18n/locales/en.json`

```json
{
  "multiProvider": {
    "pollingLabel": "轮询模式",
    "pollingHint": "新 session 将轮流使用队列中的不同 provider",
    "pollingDisabledHint": "请先开启故障转移"
  }
}
```

---

## 测试计划

### 单元测试（~50 行）

**新增文件**：`src-tauri/src/proxy/provider_router.rs` 测试模块

```rust
#[cfg(test)]
mod tests {
    #[tokio::test]
    async fn test_multi_provider_polling_disabled() {
        // multi_provider_polling_enabled=false 时，走旧逻辑
        // 验证按 sort_index 顺序选择
    }

    #[tokio::test]
    async fn test_multi_provider_polling_enabled_round_robin() {
        // multi_provider_polling_enabled=true 时，轮询选择
        // 验证 3 个 provider 轮流：P1 → P2 → P3 → P1
    }

    #[tokio::test]
    async fn test_session_binding_reuse() {
        // 同一个 session_id 复用同一个 provider
    }

    #[tokio::test]
    async fn test_session_binding_expired() {
        // TTL 过期后重新轮询
    }
}
```

### 集成测试（手动）

1. **开启轮询模式**：
   - 在 UI 中开启故障转移
   - 开启轮询模式
   - 发送 3 个请求（不同 session_id）
   - 验证：3 个请求分别路由到 P1、P2、P3

2. **Session 粘性**：
   - 发送请求（session-1）→ 路由到 P1
   - 再发送请求（session-1）→ 应该复用 P1

3. **关闭轮询模式**：
   - 关闭轮询模式
   - 验证：回到旧逻辑（按 sort_index 顺序）

---

## 实施步骤

### 步骤 1：新建分支
```bash
git checkout main
git checkout -b feature/multi-provider-polling-simplified
```

### 步骤 2：数据层（~1 小时）
- [ ] 新增数据库迁移（2 个字段）
- [ ] 更新 AppProxyConfig 类型
- [ ] 删除冗余字段（pool_enabled 等）

### 步骤 3：后端逻辑（~2 小时）
- [ ] 修改 `load_pool` 方法（用故障队列）
- [ ] 简化 `select_providers_multi` 方法
- [ ] 删除不需要的方法（pick_by_strategy 等）
- [ ] 新增 Tauri 命令 `set_multi_provider_polling`

### 步骤 4：前端 UI（~1 小时）
- [ ] 更新类型定义
- [ ] 新增 API 封装
- [ ] 新增 React Query Hook
- [ ] 在故障队列管理中增加开关
- [ ] 更新 i18n

### 步骤 5：测试（~1 小时）
- [ ] 单元测试
- [ ] 手动集成测试
- [ ] 验证旧逻辑不变

### 步骤 6：提交
```bash
git add .
git commit -m "feat(proxy): simplified multi-provider polling (use failover queue as pool)"
git push origin feature/multi-provider-polling-simplified
```

---

## 代码量估算

| 模块 | 改动 | 代码量 |
|------|------|--------|
| 数据库迁移 | 新增 | ~10 行 |
| 后端逻辑 | 修改 | ~50 行 |
| Tauri 命令 | 新增 | ~30 行 |
| 前端类型 | 修改 | ~10 行 |
| 前端 API | 新增 | ~20 行 |
| 前端 UI | 修改 | ~30 行 |
| 测试 | 新增 | ~50 行 |
| **总计** | | **~200 行** |

对比阶段 0-2 的 1800 行，**减少 90%**。

---

## 回退路径

如果新功能有问题：

1. **UI 回退**：关闭轮询模式开关
2. **环境变量回退**：`CC_SWITCH_MULTI_PROVIDER=off`（已实现）
3. **代码回退**：`git revert` 本次 commit

---

## 关键优势

1. **极简**：只加 1 个开关，不需要独立的池管理
2. **复用**：直接用故障队列，不引入新概念
3. **向后兼容**：默认关闭，旧用户无感
4. **易维护**：代码量减少 90%，逻辑清晰
5. **易回退**：UI 一键关闭

---

## 决策点（已确定）

| 决策 | 选择 | 原因 |
|------|------|------|
| 轮询策略 | 硬编码 round_robin | 简化，不需要策略选择 |
| 轮询池来源 | 故障队列 | 复用现有机制 |
| Session TTL | 3600s 默认 | 合理的粘性时长 |
| UI 位置 | 故障队列管理中 | 直观，不增加新组件 |
