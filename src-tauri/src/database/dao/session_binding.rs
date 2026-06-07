//! Session 绑定 DAO
//!
//! 用于多 Provider 轮询模式：记录 session → provider 映射。
//! 绑定一旦建立就不动，仅在 provider 失效（不存在/不在队列/熔断器 Open）时解绑。

use crate::database::{lock_conn, Database};
use crate::error::AppError;
use rusqlite::{params, OptionalExtension};
use serde::{Deserialize, Serialize};

/// Session 绑定记录
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SessionBinding {
    pub session_id: String,
    pub app_type: String,
    pub provider_id: String,
    pub bound_at: i64,
    pub last_seen_at: i64,
    pub request_count: u32,
}

impl Database {
    /// 获取当前 Unix 时间戳（秒）
    pub fn unix_now() -> i64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64
    }

    /// 读取一个 session 的绑定记录
    pub fn get_session_binding(
        &self,
        session_id: &str,
    ) -> Result<Option<SessionBinding>, AppError> {
        let conn = lock_conn!(self.conn);
        let binding = conn
            .query_row(
                "SELECT session_id, app_type, provider_id, bound_at,
                        last_seen_at, request_count
                 FROM session_bindings WHERE session_id = ?1",
                [session_id],
                |row| {
                    Ok(SessionBinding {
                        session_id: row.get(0)?,
                        app_type: row.get(1)?,
                        provider_id: row.get(2)?,
                        bound_at: row.get(3)?,
                        last_seen_at: row.get(4)?,
                        request_count: row.get(5)?,
                    })
                },
            )
            .optional()
            .map_err(|e| AppError::Database(e.to_string()))?;
        Ok(binding)
    }

    /// 原子 upsert session 绑定
    ///
    /// - 若 session_id 不存在 → INSERT
    /// - 若已存在 → `request_count += 1`, 更新 `last_seen_at`
    ///
    /// 绑定一旦建立就不动，仅在 provider 失效时解绑。
    ///
    /// **注意**：调用方持锁期间不能再调用 `get_session_binding`（std::sync::Mutex 不可重入），
    /// 所以本方法不返回最新记录。
    pub fn upsert_session_binding(
        &self,
        session_id: &str,
        app_type: &str,
        provider_id: &str,
    ) -> Result<(), AppError> {
        let now = Self::unix_now();

        let conn = lock_conn!(self.conn);
        conn.execute(
            "INSERT INTO session_bindings (session_id, app_type, provider_id, bound_at, last_seen_at, request_count)
             VALUES (?1, ?2, ?3, ?4, ?5, 1)
             ON CONFLICT(session_id) DO UPDATE SET
               last_seen_at = ?5,
               request_count = request_count + 1",
            params![session_id, app_type, provider_id, now, now],
        )
        .map_err(|e| AppError::Database(e.to_string()))?;
        // conn 在这里自动 drop，释放锁

        Ok(())
    }

    /// 删除 session 绑定
    pub fn remove_session_binding(&self, session_id: &str) -> Result<(), AppError> {
        let conn = lock_conn!(self.conn);
        conn.execute(
            "DELETE FROM session_bindings WHERE session_id = ?1",
            [session_id],
        )
        .map_err(|e| AppError::Database(e.to_string()))?;
        Ok(())
    }

    /// 清空某 app 的所有 session 绑定
    pub fn clear_session_bindings_for_app(&self, app_type: &str) -> Result<usize, AppError> {
        let conn = lock_conn!(self.conn);
        let count = conn
            .execute(
                "DELETE FROM session_bindings WHERE app_type = ?1",
                [app_type],
            )
            .map_err(|e| AppError::Database(e.to_string()))?;
        Ok(count)
    }

    /// 清理指向不存在/已禁用 provider 的幽灵 binding
    pub fn cleanup_orphaned_session_bindings(&self) -> Result<usize, AppError> {
        let conn = lock_conn!(self.conn);
        let count = conn
            .execute(
                "DELETE FROM session_bindings WHERE NOT EXISTS (
                    SELECT 1 FROM providers p
                    WHERE p.id = session_bindings.provider_id
                      AND p.app_type = session_bindings.app_type
                      AND p.in_failover_queue = 1
                )",
                [],
            )
            .map_err(|e| AppError::Database(e.to_string()))?;
        Ok(count)
    }

    /// 列某 app 的所有 session 绑定
    pub fn list_session_bindings_for_app(
        &self,
        app_type: &str,
    ) -> Result<Vec<SessionBinding>, AppError> {
        let conn = lock_conn!(self.conn);
        let mut stmt = conn
            .prepare(
                "SELECT session_id, app_type, provider_id, bound_at,
                        last_seen_at, request_count
                 FROM session_bindings
                 WHERE app_type = ?1
                 ORDER BY last_seen_at DESC
                 LIMIT 500",
            )
            .map_err(|e| AppError::Database(e.to_string()))?;

        let rows = stmt
            .query_map(params![app_type], |row| {
                Ok(SessionBinding {
                    session_id: row.get(0)?,
                    app_type: row.get(1)?,
                    provider_id: row.get(2)?,
                    bound_at: row.get(3)?,
                    last_seen_at: row.get(4)?,
                    request_count: row.get(5)?,
                })
            })
            .map_err(|e| AppError::Database(e.to_string()))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| AppError::Database(e.to_string()))?;

        Ok(rows)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_session_binding_crud() {
        let db = Database::init().unwrap();
        let app_type = "claude";

        // 创建
        db.upsert_session_binding("session-1", app_type, "provider-1").unwrap();

        // 读取
        let fetched = db.get_session_binding("session-1").unwrap().unwrap();
        assert_eq!(fetched.provider_id, "provider-1");
        assert_eq!(fetched.request_count, 1);

        // 更新（同一个 session 再次请求）
        db.upsert_session_binding("session-1", app_type, "provider-1").unwrap();
        let updated = db.get_session_binding("session-1").unwrap().unwrap();
        assert_eq!(updated.request_count, 2);

        // 列表
        let list = db.list_session_bindings_for_app(app_type).unwrap();
        assert_eq!(list.len(), 1);

        // 删除
        db.remove_session_binding("session-1").unwrap();
        assert!(db.get_session_binding("session-1").unwrap().is_none());
    }
}
