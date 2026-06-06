//! Session 绑定 DAO
//!
//! 短期 session → provider 粘性绑定（多 Provider 轮询模式使用）
//!
//! 设计要点：
//! - 写：使用 `INSERT ... ON CONFLICT(session_id) DO UPDATE` 原子化（防同 session 并发竞态）
//! - 读：命中后**必须**二次校验 provider 仍 enabled/在池（防幽灵 provider）
//! - 清理：分批 500 条/事务，避开长事务持锁

use crate::database::{lock_conn, Database};
use crate::error::AppError;
use crate::proxy::multi_provider::SessionBinding;
use rusqlite::{params, OptionalExtension};

/// 清理任务：每批最多删除的行数（避免长事务持锁）
pub const CLEANUP_BATCH_SIZE: i64 = 500;

impl Database {
    /// 读取一个 session 的绑定记录
    pub fn get_session_binding(
        &self,
        session_id: &str,
    ) -> Result<Option<SessionBinding>, AppError> {
        let conn = lock_conn!(self.conn);
        let binding = conn
            .query_row(
                "SELECT session_id, app_type, provider_id, bound_at, expires_at,
                        last_seen_at, request_count
                 FROM session_bindings WHERE session_id = ?1",
                [session_id],
                |row| {
                    Ok(SessionBinding {
                        session_id: row.get(0)?,
                        app_type: row.get(1)?,
                        provider_id: row.get(2)?,
                        bound_at: row.get(3)?,
                        expires_at: row.get(4)?,
                        last_seen_at: row.get(5)?,
                        request_count: row.get(6)?,
                    })
                },
            )
            .optional()
            .map_err(|e| AppError::Database(e.to_string()))?;
        Ok(binding)
    }

    /// 读取某 app 下所有有效 session 绑定（用于 UI 展示/调试）
    pub fn list_session_bindings_for_app(
        &self,
        app_type: &str,
    ) -> Result<Vec<SessionBinding>, AppError> {
        let conn = lock_conn!(self.conn);
        let mut stmt = conn
            .prepare(
                "SELECT session_id, app_type, provider_id, bound_at, expires_at,
                        last_seen_at, request_count
                 FROM session_bindings
                 WHERE app_type = ?1 AND expires_at > ?2
                 ORDER BY last_seen_at DESC
                 LIMIT 500",
            )
            .map_err(|e| AppError::Database(e.to_string()))?;
        let now = crate::proxy::multi_provider::unix_now();
        let rows = stmt
            .query_map(params![app_type, now], |row| {
                Ok(SessionBinding {
                    session_id: row.get(0)?,
                    app_type: row.get(1)?,
                    provider_id: row.get(2)?,
                    bound_at: row.get(3)?,
                    expires_at: row.get(4)?,
                    last_seen_at: row.get(5)?,
                    request_count: row.get(6)?,
                })
            })
            .map_err(|e| AppError::Database(e.to_string()))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| AppError::Database(e.to_string()))?;
        Ok(rows)
    }

    /// 原子 upsert：并发同 session_id 安全
    ///
    /// - 若 session_id 不存在 → INSERT
    /// - 若已存在 → `request_count += 1`, 更新 `last_seen_at`
    /// - 若 `update_expires = true`（滑动 TTL）→ 同时更新 `expires_at`
    pub fn upsert_session_binding(
        &self,
        session_id: &str,
        app_type: &str,
        provider_id: &str,
        ttl_seconds: i64,
        update_expires: bool,
    ) -> Result<SessionBinding, AppError> {
        let now = crate::proxy::multi_provider::unix_now();
        let expires_at = now + ttl_seconds;

        let conn = lock_conn!(self.conn);

        if update_expires {
            conn.execute(
                "INSERT INTO session_bindings
                    (session_id, app_type, provider_id, bound_at, expires_at, last_seen_at, request_count)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, 1)
                 ON CONFLICT(session_id) DO UPDATE SET
                    provider_id = excluded.provider_id,
                    expires_at = excluded.expires_at,
                    last_seen_at = excluded.last_seen_at,
                    request_count = request_count + 1",
                params![session_id, app_type, provider_id, now, expires_at, now],
            )
            .map_err(|e| AppError::Database(e.to_string()))?;
        } else {
            conn.execute(
                "INSERT INTO session_bindings
                    (session_id, app_type, provider_id, bound_at, expires_at, last_seen_at, request_count)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, 1)
                 ON CONFLICT(session_id) DO UPDATE SET
                    provider_id = excluded.provider_id,
                    last_seen_at = excluded.last_seen_at,
                    request_count = request_count + 1",
                params![session_id, app_type, provider_id, now, expires_at, now],
            )
            .map_err(|e| AppError::Database(e.to_string()))?;
        }

        Ok(SessionBinding {
            session_id: session_id.to_string(),
            app_type: app_type.to_string(),
            provider_id: provider_id.to_string(),
            bound_at: now,
            expires_at,
            last_seen_at: now,
            request_count: 1,
        })
    }

    /// 命中后只刷 last_seen_at（不增 request_count，phase 2 forwarder 退出时调用）
    pub fn touch_session_binding(&self, session_id: &str) -> Result<(), AppError> {
        let now = crate::proxy::multi_provider::unix_now();
        let conn = lock_conn!(self.conn);
        conn.execute(
            "UPDATE session_bindings SET last_seen_at = ?1 WHERE session_id = ?2",
            params![now, session_id],
        )
        .map_err(|e| AppError::Database(e.to_string()))?;
        Ok(())
    }

    /// 删除一个 session 绑定
    pub fn remove_session_binding(&self, session_id: &str) -> Result<(), AppError> {
        let conn = lock_conn!(self.conn);
        conn.execute(
            "DELETE FROM session_bindings WHERE session_id = ?1",
            [session_id],
        )
        .map_err(|e| AppError::Database(e.to_string()))?;
        Ok(())
    }

    /// 清空某 app 下所有 session 绑定（紧急按钮 / 关闭多 Provider 时）
    pub fn clear_session_bindings_for_app(&self, app_type: &str) -> Result<usize, AppError> {
        let conn = lock_conn!(self.conn);
        let n = conn
            .execute(
                "DELETE FROM session_bindings WHERE app_type = ?1",
                [app_type],
            )
            .map_err(|e| AppError::Database(e.to_string()))?;
        Ok(n)
    }

    /// 清理幽灵 binding：绑定的 provider 已不存在
    ///
    /// 在 `Database::init` 启动时调用一次。
    pub fn cleanup_orphaned_session_bindings(&self) -> Result<usize, AppError> {
        let conn = lock_conn!(self.conn);
        let n = conn
            .execute(
                "DELETE FROM session_bindings
                 WHERE NOT EXISTS (
                    SELECT 1 FROM providers p
                    WHERE p.id = session_bindings.provider_id
                      AND p.app_type = session_bindings.app_type
                 )",
                [],
            )
            .map_err(|e| AppError::Database(e.to_string()))?;
        if n > 0 {
            log::info!("清理了 {n} 条幽灵 session_bindings（指向已删除 provider）");
        }
        Ok(n)
    }

    /// 清理过期 binding（分批小事务）
    ///
    /// 返回被清理的总数（可能多批）
    pub fn cleanup_expired_session_bindings(&self) -> Result<usize, AppError> {
        let now = crate::proxy::multi_provider::unix_now();
        let mut total = 0usize;
        loop {
            let conn = lock_conn!(self.conn);
            let n = conn
                .execute(
                    "DELETE FROM session_bindings
                     WHERE session_id IN (
                        SELECT session_id FROM session_bindings
                        WHERE expires_at < ?1
                        LIMIT ?2
                     )",
                    params![now, CLEANUP_BATCH_SIZE],
                )
                .map_err(|e| AppError::Database(e.to_string()))?;
            if n == 0 {
                break;
            }
            total += n;
            // 防御性：单批跑满 5 次就停（避免极端情况下无限循环）
            if total >= (CLEANUP_BATCH_SIZE as usize) * 5 {
                break;
            }
        }
        if total > 0 {
            log::info!("清理了 {total} 条过期 session_bindings");
        }
        Ok(total)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::provider::Provider;
    use serde_json::json;

    fn seed_provider(db: &Database, id: &str, app: &str) {
        let p = Provider::with_id(id.to_string(), id.to_string(), json!({}), None);
        db.save_provider(app, &p).expect("insert provider");
    }

    #[test]
    fn get_returns_none_when_absent() {
        let db = Database::memory().expect("memory db");
        assert!(db.get_session_binding("missing").unwrap().is_none());
    }

    #[test]
    fn upsert_then_get_roundtrip() {
        let db = Database::memory().expect("memory db");
        seed_provider(&db, "p1", "claude");
        let b = db
            .upsert_session_binding("s1", "claude", "p1", 3600, false)
            .expect("upsert");
        assert_eq!(b.request_count, 1);
        assert_eq!(b.expires_at - b.bound_at, 3600);

        let got = db.get_session_binding("s1").expect("get").expect("present");
        assert_eq!(got.session_id, "s1");
        assert_eq!(got.provider_id, "p1");
    }

    #[test]
    fn upsert_increments_request_count() {
        let db = Database::memory().expect("memory db");
        seed_provider(&db, "p1", "claude");
        db.upsert_session_binding("s1", "claude", "p1", 3600, false)
            .unwrap();
        db.upsert_session_binding("s1", "claude", "p1", 3600, false)
            .unwrap();
        db.upsert_session_binding("s1", "claude", "p1", 3600, false)
            .unwrap();
        let got = db.get_session_binding("s1").unwrap().unwrap();
        assert_eq!(got.request_count, 3);
    }

    #[test]
    fn upsert_with_sliding_extends_expires() {
        let db = Database::memory().expect("memory db");
        seed_provider(&db, "p1", "claude");
        db.upsert_session_binding("s1", "claude", "p1", 60, false)
            .unwrap();
        let first = db.get_session_binding("s1").unwrap().unwrap();

        // 模拟"1 秒后第二次请求"用 sliding
        std::thread::sleep(std::time::Duration::from_millis(1100));
        db.upsert_session_binding("s1", "claude", "p1", 60, true)
            .unwrap();
        let second = db.get_session_binding("s1").unwrap().unwrap();
        assert!(
            second.expires_at > first.expires_at,
            "sliding TTL should extend expires_at: first={}, second={}",
            first.expires_at,
            second.expires_at
        );
    }

    #[test]
    fn upsert_fixed_does_not_extend_expires() {
        let db = Database::memory().expect("memory db");
        seed_provider(&db, "p1", "claude");
        db.upsert_session_binding("s1", "claude", "p1", 60, false)
            .unwrap();
        let first = db.get_session_binding("s1").unwrap().unwrap();
        std::thread::sleep(std::time::Duration::from_millis(1100));
        db.upsert_session_binding("s1", "claude", "p1", 60, false)
            .unwrap();
        let second = db.get_session_binding("s1").unwrap().unwrap();
        assert_eq!(
            second.expires_at, first.expires_at,
            "fixed TTL must keep expires_at stable"
        );
    }

    #[test]
    fn remove_deletes_row() {
        let db = Database::memory().expect("memory db");
        seed_provider(&db, "p1", "claude");
        db.upsert_session_binding("s1", "claude", "p1", 60, false)
            .unwrap();
        db.remove_session_binding("s1").unwrap();
        assert!(db.get_session_binding("s1").unwrap().is_none());
    }

    #[test]
    fn clear_for_app_only_clears_target_app() {
        let db = Database::memory().expect("memory db");
        seed_provider(&db, "p1", "claude");
        seed_provider(&db, "p2", "codex");
        db.upsert_session_binding("s1", "claude", "p1", 60, false)
            .unwrap();
        db.upsert_session_binding("s2", "codex", "p2", 60, false)
            .unwrap();
        let n = db.clear_session_bindings_for_app("claude").unwrap();
        assert_eq!(n, 1);
        assert!(db.get_session_binding("s1").unwrap().is_none());
        assert!(db.get_session_binding("s2").unwrap().is_some());
    }

    #[test]
    fn cleanup_orphans_removes_ghost_bindings() {
        let db = Database::memory().expect("memory db");
        seed_provider(&db, "p1", "claude");
        db.upsert_session_binding("s1", "claude", "p1", 60, false)
            .unwrap();
        db.upsert_session_binding("ghost", "claude", "ghost-provider", 60, false)
            .unwrap();
        let n = db.cleanup_orphaned_session_bindings().unwrap();
        assert_eq!(n, 1);
        assert!(db.get_session_binding("s1").unwrap().is_some());
        assert!(db.get_session_binding("ghost").unwrap().is_none());
    }
}
