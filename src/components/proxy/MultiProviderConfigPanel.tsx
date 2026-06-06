/**
 * 多 Provider 轮询配置面板
 *
 * 控制多 Provider 模式的总开关、策略、TTL、official opt-in。
 * 开启时联动 auto_failover_enabled=true（后端复合命令保证）。
 */

import { useTranslation } from "react-i18next";
import { Switch } from "@/components/ui/switch";
import { Label } from "@/components/ui/label";
import { Input } from "@/components/ui/input";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Alert, AlertDescription } from "@/components/ui/alert";
import { Info, AlertTriangle } from "lucide-react";

import type { AppId } from "@/lib/api";
import type { PoolStrategy } from "@/types/proxy";
import {
  useMultiProviderConfig,
  useUpdateMultiProviderConfig,
} from "@/lib/query/multiProvider";

interface MultiProviderConfigPanelProps {
  appType: AppId;
  disabled?: boolean;
}

export function MultiProviderConfigPanel({
  appType,
  disabled = false,
}: MultiProviderConfigPanelProps) {
  const { t } = useTranslation();
  const { data: config, isLoading } = useMultiProviderConfig(appType);
  const updateConfig = useUpdateMultiProviderConfig();

  if (isLoading || !config) {
    return <div className="h-32 w-full animate-pulse rounded bg-muted" />;
  }

  const update = (patch: Partial<typeof config>) => {
    updateConfig.mutate({
      appType,
      config: { ...config, ...patch },
    });
  };

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <div className="space-y-1">
          <Label className="text-sm font-medium">
            {t("multiProvider.enableLabel")}
          </Label>
          <p className="text-xs text-muted-foreground">
            {t("multiProvider.enableHint")}
          </p>
        </div>
        <Switch
          checked={config.multiProviderEnabled}
          disabled={disabled || updateConfig.isPending}
          onCheckedChange={(v) => update({ multiProviderEnabled: v })}
        />
      </div>

      {config.multiProviderEnabled && (
        <>
          <Alert>
            <Info className="h-4 w-4" />
            <AlertDescription className="text-xs">
              {t("multiProvider.failoverAutoEnabledHint")}
            </AlertDescription>
          </Alert>

          <div className="space-y-2">
            <Label className="text-sm font-medium">
              {t("multiProvider.strategyLabel")}
            </Label>
            <Select
              value={config.poolStrategy}
              disabled={updateConfig.isPending}
              onValueChange={(v) =>
                update({ poolStrategy: v as PoolStrategy })
              }
            >
              <SelectTrigger>
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="round_robin">
                  {t("multiProvider.strategy.roundRobin")}
                </SelectItem>
                <SelectItem value="weighted_random">
                  {t("multiProvider.strategy.weightedRandom")}
                </SelectItem>
                <SelectItem value="least_failures">
                  {t("multiProvider.strategy.leastFailures")}
                </SelectItem>
                <SelectItem value="lowest_latency">
                  {t("multiProvider.strategy.lowestLatency")}
                </SelectItem>
              </SelectContent>
            </Select>
          </div>

          <div className="space-y-2">
            <Label className="text-sm font-medium">
              {t("multiProvider.ttlLabel", {
                value: config.sessionTtlSeconds,
              })}
            </Label>
            <Input
              type="number"
              min={60}
              max={86400}
              value={config.sessionTtlSeconds}
              disabled={updateConfig.isPending}
              onChange={(e) => {
                const v = parseInt(e.target.value, 10);
                if (!isNaN(v) && v >= 60 && v <= 86400) {
                  update({ sessionTtlSeconds: v });
                }
              }}
            />
            <p className="text-xs text-muted-foreground">
              {t("multiProvider.ttlHint")}
            </p>
          </div>

          <div className="flex items-center justify-between">
            <Label className="text-sm font-medium">
              {t("multiProvider.slidingLabel")}
            </Label>
            <Switch
              checked={config.sessionTtlSliding}
              disabled={updateConfig.isPending}
              onCheckedChange={(v) => update({ sessionTtlSliding: v })}
            />
          </div>

          <div className="flex items-center justify-between">
            <div className="space-y-1">
              <Label className="text-sm font-medium">
                {t("multiProvider.includeOfficialLabel")}
              </Label>
              <p className="text-xs text-muted-foreground">
                {t("multiProvider.includeOfficialHint")}
              </p>
            </div>
            <Switch
              checked={config.poolIncludeOfficial}
              disabled={updateConfig.isPending}
              onCheckedChange={(v) => update({ poolIncludeOfficial: v })}
            />
          </div>
        </>
      )}

      {!config.multiProviderEnabled && (
        <Alert variant="destructive">
          <AlertTriangle className="h-4 w-4" />
          <AlertDescription className="text-xs">
            {t("multiProvider.disabledHint")}
          </AlertDescription>
        </Alert>
      )}
    </div>
  );
}
