-- CC Switch 多 Provider 轮询 — 回退参考脚本
-- 用途：在 git revert + 重装旧版二进制之后，可选地手动清理多 Provider 引入的列和表，
--      让数据库 schema 与升级前完全一致。
--
-- ⚠️ 不建议普通用户运行此脚本：
--   1. 项目已有自动备份 + restore_from_backup + 灰度开关 3 层回退机制（见 IMPLEMENTATION_PLAN.md §"可逆性设计"）
--   2. 列保留在表里**零开销**（几 KB），不影响旧版功能
--   3. SQLite ALTER TABLE DROP COLUMN 在 3.35+ 才支持（rusqlite 客户端版本可能不够）
--
-- 适用场景：开发环境 / 想看"干净回退到 v3.x 行为"的演示
--
-- 运行方式：
--   sqlite3 ~/.cc-switch/cc-switch.db < rollback_multi_provider.sql
--
-- 建议顺序：先备份，再执行，验证 app 可正常启动。

-- ============================================================
-- 第 0 步：安全网（执行任何破坏性操作前先备份）
-- ============================================================
-- 在 shell 中手动执行：
--   sqlite3 ~/.cc-switch/cc-switch.db ".backup /tmp/rollback_safety.db"

-- ============================================================
-- 第 1 步：删除多 Provider 引入的新表（如果有数据先看是否需要导出）
-- ============================================================
DROP TABLE IF EXISTS session_bindings;
DROP INDEX IF EXISTS idx_sb_app;
DROP INDEX IF EXISTS idx_sb_expires;

-- ============================================================
-- 第 2 步：删除多 Provider 扩展的列
-- ============================================================
-- SQLite 3.35+ 支持 DROP COLUMN；老版本会报 "near \"DROP\": syntax error"
-- 客户端版本检查：sqlite3 --version

-- proxy_config 表（5 列）
ALTER TABLE proxy_config DROP COLUMN multi_provider_enabled;
ALTER TABLE proxy_config DROP COLUMN pool_strategy;
ALTER TABLE proxy_config DROP COLUMN session_ttl_seconds;
ALTER TABLE proxy_config DROP COLUMN session_ttl_sliding;
ALTER TABLE proxy_config DROP COLUMN pool_include_official;

-- providers 表（2 列）
ALTER TABLE providers DROP COLUMN pool_weight;
ALTER TABLE providers DROP COLUMN pool_enabled;

-- ============================================================
-- 第 3 步：验证
-- ============================================================
-- 期望返回空：
SELECT 'session_bindings table still exists' AS check
  FROM sqlite_master WHERE type='table' AND name='session_bindings';

-- 期望返回空：
SELECT 'multi_provider_enabled column still exists' AS check
  FROM pragma_table_info('proxy_config') WHERE name='multi_provider_enabled';

-- 期望返回空：
SELECT 'pool_weight column still exists' AS check
  FROM pragma_table_info('providers') WHERE name='pool_weight';

-- ============================================================
-- 第 4 步：清理 user_version（可选）
-- ============================================================
-- 旧版的 SCHEMA_VERSION=10，新版是 11。
-- 如果你想让旧版在启动时显示"已升级到 v10"，把 user_version 改回 10：
-- PRAGMA user_version = 10;
--
-- 不改也行：旧版从 11 启动时，while version < 10 的循环不会触发，无害。
