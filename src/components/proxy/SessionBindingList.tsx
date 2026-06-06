/**
 * Session 绑定调试面板（折叠）
 *
 * 显示某 app 当前所有 session 绑定（短期 session → provider 粘性）。
 * 提供"清空全部"按钮（紧急/调试用）。
 */

import { useTranslation } from "react-i18next";
import {
  Collapsible,
  CollapsibleContent,
  CollapsibleTrigger,
} from "@/components/ui/collapsible";
import { Button } from "@/components/ui/button";

import { ChevronDown, ChevronRight, Trash2, Loader2 } from "lucide-react";
import { useState } from "react";
import type { AppId } from "@/lib/api";
import {
  useSessionBindings,
  useClearSessionBindings,
} from "@/lib/query/multiProvider";

interface SessionBindingListProps {
  appType: AppId;
}

export function SessionBindingList({ appType }: SessionBindingListProps) {
  const { t } = useTranslation();
  const [open, setOpen] = useState(false);
  const { data, isLoading } = useSessionBindings(appType);
  const clearAll = useClearSessionBindings();

  const bindings = data ?? [];

  return (
    <Collapsible open={open} onOpenChange={setOpen}>
      <CollapsibleTrigger asChild>
        <Button variant="ghost" size="sm" className="w-full justify-between">
          <span className="text-xs">
            {t("multiProvider.bindingsTitle", { count: bindings.length })}
          </span>
          {open ? (
            <ChevronDown className="h-3 w-3" />
          ) : (
            <ChevronRight className="h-3 w-3" />
          )}
        </Button>
      </CollapsibleTrigger>
      <CollapsibleContent className="space-y-2 pt-2">
        {isLoading ? (
          <div className="h-16 w-full animate-pulse rounded bg-muted" />
        ) : bindings.length === 0 ? (
          <p className="text-xs text-muted-foreground px-2">
            {t("multiProvider.bindingsEmpty")}
          </p>
        ) : (
          <>
            <div className="space-y-1 max-h-48 overflow-y-auto">
              {bindings.map((b) => (
                <div
                  key={b.sessionId}
                  className="rounded border p-2 text-xs space-y-0.5"
                >
                  <div className="flex justify-between">
                    <span className="font-mono">
                      {b.sessionId.slice(0, 16)}…
                    </span>
                    <span className="text-muted-foreground">
                      {t("multiProvider.bindingRequestCount", {
                        count: b.requestCount,
                      })}
                    </span>
                  </div>
                  <div className="text-muted-foreground">
                    → {b.providerId} · {ttlLabel(b.expiresAt)}
                  </div>
                </div>
              ))}
            </div>
            <Button
              size="sm"
              variant="outline"
              className="w-full"
              disabled={clearAll.isPending}
              onClick={() => {
                if (confirm(t("multiProvider.clearAllConfirm"))) {
                  clearAll.mutate(appType);
                }
              }}
            >
              {clearAll.isPending ? (
                <Loader2 className="h-3 w-3 mr-1 animate-spin" />
              ) : (
                <Trash2 className="h-3 w-3 mr-1" />
              )}
              {t("multiProvider.clearAll")}
            </Button>
          </>
        )}
      </CollapsibleContent>
    </Collapsible>
  );
}

function ttlLabel(expiresAt: number): string {
  const now = Math.floor(Date.now() / 1000);
  const secs = expiresAt - now;
  if (secs <= 0) return "expired";
  if (secs < 60) return `${secs}s`;
  if (secs < 3600) return `${Math.floor(secs / 60)}m`;
  return `${Math.floor(secs / 3600)}h`;
}
