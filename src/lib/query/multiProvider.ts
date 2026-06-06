import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { multiProviderApi } from "@/lib/api/multiProvider";
import { toast } from "sonner";
import { useTranslation } from "react-i18next";
import { extractErrorMessage } from "@/utils/errorUtils";

/**
 * 读多 Provider 配置
 */
export function useMultiProviderConfig(appType: string) {
  return useQuery({
    queryKey: ["multiProviderConfig", appType],
    queryFn: () => multiProviderApi.getConfig(appType),
    enabled: !!appType,
  });
}

/**
 * 写多 Provider 配置（复合命令）
 *
 * 失效矩阵：
 * - multiProviderConfig / failoverQueue / autoFailoverEnabled / proxyStatus
 * - 因为复合命令会强制联动 auto_failover + 清空 session_bindings
 */
export function useUpdateMultiProviderConfig() {
  const qc = useQueryClient();
  const { t } = useTranslation();
  return useMutation({
    mutationFn: ({
      appType,
      config,
    }: {
      appType: string;
      config: import("@/types/proxy").MultiProviderConfig;
    }) => multiProviderApi.updateConfig(appType, config),
    onSuccess: (_, vars) => {
      qc.invalidateQueries({
        queryKey: ["multiProviderConfig", vars.appType],
      });
      qc.invalidateQueries({ queryKey: ["failoverQueue", vars.appType] });
      qc.invalidateQueries({
        queryKey: ["autoFailoverEnabled", vars.appType],
      });
      qc.invalidateQueries({ queryKey: ["proxyStatus"] });
      qc.invalidateQueries({ queryKey: ["multiProviderPool", vars.appType] });
      qc.invalidateQueries({
        queryKey: ["multiProviderBindings", vars.appType],
      });
    },
    onError: (err: unknown) => {
      toast.error(extractErrorMessage(err) || t("multiProvider.updateFailed"));
    },
  });
}

/**
 * 读轮询池
 */
export function useProviderPool(appType: string) {
  return useQuery({
    queryKey: ["multiProviderPool", appType],
    queryFn: () => multiProviderApi.getPool(appType),
    enabled: !!appType,
  });
}

/**
 * 批量更新轮询池（双源同步 in_failover_queue）
 *
 * 失效：multiProviderPool + failoverQueue + providers
 */
export function useSetProviderPool() {
  const qc = useQueryClient();
  const { t } = useTranslation();
  return useMutation({
    mutationFn: ({
      appType,
      items,
    }: {
      appType: string;
      items: import("@/types/proxy").ProviderPoolItem[];
    }) => multiProviderApi.setPool(appType, items),
    onSuccess: (_, vars) => {
      qc.invalidateQueries({ queryKey: ["multiProviderPool", vars.appType] });
      qc.invalidateQueries({ queryKey: ["failoverQueue", vars.appType] });
      qc.invalidateQueries({ queryKey: ["providers", vars.appType] });
    },
    onError: (err: unknown) => {
      toast.error(extractErrorMessage(err) || t("multiProvider.updatePoolFailed"));
    },
  });
}

/**
 * 读 session 绑定（调试用）
 */
export function useSessionBindings(appType: string) {
  return useQuery({
    queryKey: ["multiProviderBindings", appType],
    queryFn: () => multiProviderApi.getSessionBindings(appType),
    enabled: !!appType,
    refetchInterval: 10000, // 10s 刷新
  });
}

/**
 * 清空 session 绑定
 */
export function useClearSessionBindings() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (appType: string) =>
      multiProviderApi.clearSessionBindings(appType),
    onSuccess: (_, appType) => {
      qc.invalidateQueries({ queryKey: ["multiProviderBindings", appType] });
    },
  });
}
