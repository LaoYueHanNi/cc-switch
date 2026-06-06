import { invoke } from "@tauri-apps/api/core";
import type {
  MultiProviderConfig,
  ProviderPoolItem,
  SessionBinding,
} from "@/types/proxy";

/**
 * 多 Provider 轮询模式 API（阶段 2）
 *
 * 与 failover.ts 边界：
 * - failover.ts 管"故障队列成员"（in_failover_queue + sort_index）
 * - multiProvider.ts 管"轮询池成员"（pool_enabled + weight + 策略）
 * - setProviderPool 内部同步两套：池成员 ⊆ 故障队列（双源真相守护）
 */
export const multiProviderApi = {
  /** 读多 Provider 配置（per-app） */
  async getConfig(appType: string): Promise<MultiProviderConfig> {
    return invoke("get_multi_provider_config", { appType });
  },

  /**
   * 写多 Provider 配置（复合命令：事务写 multi_provider_enabled + 强制 auto_failover）
   *
   * 副作用：
   * - 开启时强制 auto_failover_enabled=true（多 Provider 强依赖故障队列）
   * - 关闭时清空 session_bindings（避免悬挂绑定）
   */
  async updateConfig(
    appType: string,
    config: MultiProviderConfig,
  ): Promise<void> {
    return invoke("set_multi_provider_config", { appType, config });
  },

  /** 读轮询池成员（pool_enabled=1 的 provider，按 sort_index 排序） */
  async getPool(appType: string): Promise<ProviderPoolItem[]> {
    return invoke("get_provider_pool", { appType });
  },

  /**
   * 批量更新轮询池成员
   *
   * 双源同步：池成员同时加入故障队列
   */
  async setPool(appType: string, items: ProviderPoolItem[]): Promise<void> {
    return invoke("set_provider_pool", { appType, items });
  },

  /** 读某 app 的所有 session 绑定（调试/UI） */
  async getSessionBindings(appType: string): Promise<SessionBinding[]> {
    return invoke("get_session_bindings", { appType });
  },

  /** 清空某 app 的所有 session 绑定 */
  async clearSessionBindings(appType: string): Promise<number> {
    return invoke("clear_session_bindings", { appType });
  },
};
