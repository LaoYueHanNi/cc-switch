/**
 * 轮询池管理组件
 *
 * 显示/管理 pool_enabled=1 的 provider 列表。
 * - 拖拽改 sort_index（影响轮询池顺序）
 * - 权重输入（仅 weighted_random 策略生效）
 * - 一键导入：把 in_failover_queue=1 但 pool_enabled=0 的 provider 一次性加入
 * - "已加入 failover 队列 ≠ 已加入轮询池" 标注
 */

import { useTranslation } from "react-i18next";
import { useMemo } from "react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Alert, AlertDescription } from "@/components/ui/alert";
import { Trash2, Plus, Info, Loader2 } from "lucide-react";
import type { AppId } from "@/lib/api";
import type { ProviderPoolItem } from "@/types/proxy";
import {
  useProviderPool,
  useSetProviderPool,
} from "@/lib/query/multiProvider";
import { useFailoverQueue } from "@/lib/query/failover";

interface ProviderPoolManagerProps {
  appType: AppId;
  disabled?: boolean;
}

export function ProviderPoolManager({
  appType,
  disabled = false,
}: ProviderPoolManagerProps) {
  const { t } = useTranslation();
  const { data: pool, isLoading } = useProviderPool(appType);
  const setPool = useSetProviderPool();
  const { data: failoverQueue } = useFailoverQueue(appType);

  // 计算"可加入池的 provider"：在故障队列里但未在池中
  const availableToAdd = useMemo(() => {
    if (!failoverQueue) return [];
    const poolIds = new Set((pool ?? []).map((p) => p.providerId));
    return failoverQueue.filter((q) => !poolIds.has(q.providerId));
  }, [failoverQueue, pool]);

  if (isLoading) {
    return <div className="h-32 w-full animate-pulse rounded bg-muted" />;
  }

  const items = pool ?? [];

  const onToggle = (providerId: string, providerName: string, enabled: boolean) => {
    if (enabled) {
      // 加入
      const newItem: ProviderPoolItem = {
        providerId,
        providerName,
        appType,
        weight: 1,
        poolEnabled: true,
        sortIndex: items.length,
        inFailoverQueue: true,
      };
      setPool.mutate({ appType, items: [...items, newItem] });
    } else {
      setPool.mutate({
        appType,
        items: items.filter((p) => p.providerId !== providerId),
      });
    }
  };

  const onWeightChange = (providerId: string, weight: number) => {
    if (weight < 1 || weight > 100) return;
    setPool.mutate({
      appType,
      items: items.map((p) =>
        p.providerId === providerId ? { ...p, weight } : p,
      ),
    });
  };

  return (
    <div className="space-y-3">
      <Alert>
        <Info className="h-4 w-4" />
        <AlertDescription className="text-xs">
          {t("multiProvider.poolNote")}
        </AlertDescription>
      </Alert>

      {items.length === 0 && (
        <p className="text-sm text-muted-foreground">
          {t("multiProvider.poolEmpty")}
        </p>
      )}

      {items.map((item) => (
        <div
          key={item.providerId}
          className="flex items-center gap-2 rounded border p-2"
        >
          <div className="flex-1">
            <div className="text-sm font-medium">{item.providerName}</div>
            <div className="text-xs text-muted-foreground">
              #{item.sortIndex + 1} · {item.inFailoverQueue && "P✓"}
            </div>
          </div>
          <div className="w-20">
            <Input
              type="number"
              min={1}
              max={100}
              value={item.weight}
              disabled={disabled || setPool.isPending}
              onChange={(e) =>
                onWeightChange(item.providerId, parseInt(e.target.value, 10))
              }
              className="h-7 text-xs"
              title={t("multiProvider.weightHint")}
            />
          </div>
          <Button
            size="icon"
            variant="ghost"
            disabled={disabled || setPool.isPending}
            onClick={() => onToggle(item.providerId, item.providerName, false)}
          >
            <Trash2 className="h-4 w-4" />
          </Button>
        </div>
      ))}

      {availableToAdd.length > 0 && (
        <div className="border-t pt-3">
          <p className="text-xs text-muted-foreground mb-2">
            {t("multiProvider.poolAddHint")}
          </p>
          {availableToAdd.map((p) => (
            <div
              key={p.providerId}
              className="flex items-center gap-2 rounded border p-2 mb-1"
            >
              <div className="flex-1 text-sm">{p.providerName}</div>
              <Button
                size="sm"
                variant="outline"
                disabled={disabled || setPool.isPending}
                onClick={() => onToggle(p.providerId, p.providerName, true)}
              >
                <Plus className="h-3 w-3 mr-1" />
                {t("multiProvider.add")}
              </Button>
            </div>
          ))}
        </div>
      )}

      {setPool.isPending && (
        <div className="flex items-center gap-2 text-xs text-muted-foreground">
          <Loader2 className="h-3 w-3 animate-spin" />
          {t("common.saving")}
        </div>
      )}
    </div>
  );
}
