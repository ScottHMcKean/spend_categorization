import { apiFetch } from "@/lib/fetch";
import { Button } from "@/components/ui/button";
import { SchemaBadge } from "@/components/apx/schema-selector";
import { useSchema } from "@/lib/schema-context";
import { useReviewQueue, type QueueItem } from "@/lib/review-queue";
import { createFileRoute, useNavigate } from "@tanstack/react-router";
import { useQuery } from "@tanstack/react-query";
import { useMemo, useState } from "react";

export const Route = createFileRoute("/classifications")({
  component: Classifications,
});

interface PredictionRow {
  order_id: string;
  schema_name: string;
  code?: string;
  label?: string;
  level_path?: string[];
  confidence?: number;
  rationale?: string;
  source?: string;
  classified_at?: string;
}

interface SourceSummary {
  source: string;
  count: number;
  with_confidence: number;
  avg_confidence: number | null;
}

interface PredictionsSummary {
  total: number;
  sources: SourceSummary[];
}

interface CategoryAccuracy {
  category: string;
  precision: number;
  recall: number;
  f1: number;
  spend: number;
  count: number;
}

interface AccuracyResponse {
  overall_accuracy: number;
  overall_precision: number;
  overall_recall: number;
  overall_f1: number;
  total_classified: number;
  total_correct: number;
  source: string;
  categories: CategoryAccuracy[];
}

function ConfidenceBadge({ value, source }: { value: number; source?: string }) {
  const isProb = value >= 0 && value <= 1;
  const display = isProb ? `${(value * 100).toFixed(0)}%` : value.toFixed(2);
  const bucket = isProb ? value : Math.min(1, value / 5);
  const color = bucket >= 0.7 ? "#22C55E" : bucket >= 0.4 ? "#F59E0B" : "#FF3621";
  return (
    <span
      title={`${source ?? "?"} · raw=${value}`}
      className="inline-flex items-center gap-2 font-mono text-xs"
    >
      <span className="w-12 h-1.5 rounded bg-[#E5EBF0] overflow-hidden inline-block">
        <span
          className="block h-full rounded"
          style={{ width: `${Math.round(bucket * 100)}%`, background: color }}
        />
      </span>
      <span style={{ color }}>{display}</span>
    </span>
  );
}

function PathCell({ path }: { path: string[] }) {
  const [open, setOpen] = useState(false);
  if (path.length === 0) return <span className="text-[#8CA0AC]">--</span>;
  const joined = path.join(" › ");
  return (
    <button
      onClick={() => setOpen(!open)}
      className={
        open
          ? "text-left text-xs text-[#0B2026] whitespace-normal break-words hover:text-[#FF3621]"
          : "text-left text-xs text-[#8CA0AC] truncate max-w-[220px] hover:text-[#FF3621]"
      }
      title={joined}
    >
      {joined}
    </button>
  );
}

function usePredictions(schemaName: string, source?: string) {
  return useQuery({
    queryKey: ["predictions", schemaName, source ?? "all"],
    queryFn: () => {
      const qs = new URLSearchParams({ limit: "5000" });
      if (source) qs.set("source", source);
      return apiFetch<PredictionRow[]>(`/predictions/${schemaName}?${qs.toString()}`);
    },
    enabled: !!schemaName,
  });
}

function useSummary(schemaName: string) {
  return useQuery({
    queryKey: ["predictions", "summary", schemaName],
    queryFn: () => apiFetch<PredictionsSummary>(`/predictions/${schemaName}/summary`),
    enabled: !!schemaName,
  });
}

function useAccuracy(schemaName: string) {
  return useQuery({
    queryKey: ["analytics", "accuracy", schemaName],
    queryFn: () => apiFetch<AccuracyResponse>(`/analytics/accuracy/${schemaName}`),
    enabled: !!schemaName,
  });
}

function SourceCard({ s }: { s: SourceSummary }) {
  const conf = s.avg_confidence;
  const pct = conf == null ? null : conf <= 1 ? Math.round(conf * 100) : null;
  return (
    <div className="bg-white rounded-lg border border-[#E5EBF0] p-4">
      <p className="text-xs text-[#8CA0AC] uppercase tracking-wide mb-1">{s.source}</p>
      <p className="text-2xl font-bold text-[#0B2026]">{s.count.toLocaleString()}</p>
      <p className="text-xs text-[#8CA0AC] mb-2">predictions</p>
      <div className="flex items-baseline justify-between text-xs">
        <span className="text-[#8CA0AC]">avg conf</span>
        <span className="font-mono text-[#0B2026]">
          {conf == null ? "n/a" : pct != null ? `${pct}%` : conf.toFixed(2)}
        </span>
      </div>
      <div className="flex items-baseline justify-between text-xs">
        <span className="text-[#8CA0AC]">w/ conf</span>
        <span className="font-mono text-[#0B2026]">
          {s.with_confidence.toLocaleString()} / {s.count.toLocaleString()}
        </span>
      </div>
    </div>
  );
}

function AccuracySummary({ schemaName }: { schemaName: string }) {
  const { data, isLoading } = useAccuracy(schemaName);
  if (isLoading) return null;
  if (!data || data.total_classified === 0) {
    return (
      <div className="bg-[#F7F9FA] rounded-lg border border-[#E5EBF0] p-4 text-sm text-[#8CA0AC]">
        No ground-truth accuracy available for this schema. Confidence stats above; submit corrections from
        Spend Review to build a feedback signal.
      </div>
    );
  }
  return (
    <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
      <div className="bg-white rounded-lg border border-[#E5EBF0] p-4">
        <p className="text-xs text-[#8CA0AC] uppercase tracking-wide mb-1">accuracy</p>
        <p className="text-2xl font-bold text-[#0B2026]">{(data.overall_accuracy * 100).toFixed(1)}%</p>
        <p className="text-xs text-[#8CA0AC]">
          {data.total_correct}/{data.total_classified} correct
        </p>
      </div>
      <div className="bg-white rounded-lg border border-[#E5EBF0] p-4">
        <p className="text-xs text-[#8CA0AC] uppercase tracking-wide mb-1">precision (macro)</p>
        <p className="text-2xl font-bold text-[#0B2026]">{(data.overall_precision * 100).toFixed(1)}%</p>
      </div>
      <div className="bg-white rounded-lg border border-[#E5EBF0] p-4">
        <p className="text-xs text-[#8CA0AC] uppercase tracking-wide mb-1">recall (macro)</p>
        <p className="text-2xl font-bold text-[#0B2026]">{(data.overall_recall * 100).toFixed(1)}%</p>
      </div>
      <div className="bg-white rounded-lg border border-[#E5EBF0] p-4">
        <p className="text-xs text-[#8CA0AC] uppercase tracking-wide mb-1">f1 (macro)</p>
        <p className="text-2xl font-bold text-[#0B2026]">{(data.overall_f1 * 100).toFixed(1)}%</p>
      </div>
    </div>
  );
}

function Classifications() {
  const { schemaName, current } = useSchema();
  const navigate = useNavigate();
  const [search, setSearch] = useState("");
  const [sourceFilter, setSourceFilter] = useState<string>("");
  const [selectedIds, setSelectedIds] = useState<Set<string>>(new Set());

  const { data: rows, isLoading } = usePredictions(schemaName, sourceFilter || undefined);
  const { data: summary } = useSummary(schemaName);
  const { add: addToQueue, queue } = useReviewQueue();

  const sources = useMemo(() => {
    if (!rows) return [];
    return Array.from(new Set(rows.map((r) => r.source).filter(Boolean) as string[])).sort();
  }, [rows]);

  const filtered = useMemo(() => {
    if (!rows) return [];
    const s = search.toLowerCase();
    if (!s) return rows;
    return rows.filter((r) =>
      [r.order_id, r.code, r.label, ...(r.level_path ?? [])]
        .filter(Boolean)
        .some((v) => String(v).toLowerCase().includes(s)),
    );
  }, [rows, search]);

  const toggle = (id: string) =>
    setSelectedIds((prev) => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return next;
    });

  const toggleAll = () =>
    setSelectedIds(
      selectedIds.size === filtered.length
        ? new Set()
        : new Set(filtered.map((r) => r.order_id).filter(Boolean) as string[]),
    );

  const queueSelected = () => {
    const selected = filtered.filter((r) => r.order_id && selectedIds.has(r.order_id));
    if (selected.length === 0) return;
    const items: QueueItem[] = selected.map((r) => ({
      order_id: r.order_id,
      schema_name: r.schema_name,
      code: r.code,
      label: r.label,
      level_path: r.level_path,
      confidence: r.confidence,
      source: r.source,
    }));
    addToQueue(items);
    setSelectedIds(new Set());
    navigate({ to: "/review", search: () => ({ tab: "review" }) }).catch(() => {
      navigate({ to: "/review" });
    });
  };

  return (
    <div className="max-w-7xl mx-auto px-6 py-8 font-sans">
      <div className="flex flex-wrap items-baseline justify-between gap-3 mb-2">
        <h1 className="text-3xl font-bold text-[#0B2026]">Classifications</h1>
        <SchemaBadge />
      </div>
      {current?.description && (
        <p className="text-sm text-[#8CA0AC] mb-6">{current.description}</p>
      )}

      {/* Accuracy summary */}
      <div className="mb-4">
        <AccuracySummary schemaName={schemaName} />
      </div>

      {/* Source / confidence summary cards */}
      {summary && (summary.sources?.length ?? 0) > 0 && (
        <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4 mb-6">
          {summary.sources.map((s) => (
            <SourceCard key={s.source} s={s} />
          ))}
        </div>
      )}

      {/* Filters */}
      <div className="flex flex-wrap items-end gap-3 mb-3">
        <input
          type="text"
          placeholder="Filter by order_id, code, label, path…"
          value={search}
          onChange={(e) => setSearch(e.target.value)}
          className="flex-1 min-w-[260px] px-4 py-2 border border-[#E5EBF0] rounded-md text-[#0B2026] placeholder:text-[#8CA0AC] focus:outline-none focus:ring-2 focus:ring-[#FF3621]/40"
        />
        <select
          value={sourceFilter}
          onChange={(e) => setSourceFilter(e.target.value)}
          className="px-3 py-2 border border-[#E5EBF0] rounded-md text-sm text-[#0B2026]"
        >
          <option value="">All sources</option>
          {sources.map((s) => (
            <option key={s} value={s}>
              {s}
            </option>
          ))}
        </select>
        <span className="text-sm text-[#8CA0AC]">
          {filtered.length.toLocaleString()} / {(rows?.length ?? 0).toLocaleString()} predictions
        </span>
      </div>

      {/* Action bar */}
      <div className="flex items-center justify-between bg-[#F7F9FA] border border-[#E5EBF0] rounded-t-lg px-4 py-2">
        <p className="text-sm text-[#0B2026]">
          {selectedIds.size > 0
            ? `${selectedIds.size} selected`
            : "Select rows to queue them for review"}
          {queue.length > 0 && (
            <span className="ml-3 text-xs text-[#8CA0AC]">
              ({queue.length} already in queue)
            </span>
          )}
        </p>
        <Button onClick={queueSelected} disabled={selectedIds.size === 0}>
          Queue for Review ({selectedIds.size})
        </Button>
      </div>

      {/* Predictions table — scrollable */}
      <div className="rounded-b-lg border-x border-b border-[#E5EBF0] overflow-hidden bg-white mb-8">
        {isLoading && <p className="p-6 text-[#8CA0AC]">Loading…</p>}
        {!isLoading && filtered.length === 0 && (
          <div className="p-6 text-[#8CA0AC] space-y-2">
            <p>
              No predictions for schema{" "}
              <code className="px-1.5 py-0.5 rounded bg-[#F7F9FA]">{schemaName}</code>.
            </p>
            <p className="text-xs">Run notebooks 0–3 to populate <code>cat_predictions</code>.</p>
          </div>
        )}
        {!isLoading && filtered.length > 0 && (
          <div className="overflow-auto max-h-[60vh]">
            <table className="w-full text-sm">
              <thead className="bg-[#F7F9FA] border-b border-[#E5EBF0] sticky top-0 z-10">
                <tr>
                  <th className="text-left p-3">
                    <input
                      type="checkbox"
                      checked={selectedIds.size === filtered.length && filtered.length > 0}
                      onChange={toggleAll}
                      className="rounded border-[#E5EBF0]"
                    />
                  </th>
                  <th className="text-left p-3 font-semibold text-[#0B2026]">order_id</th>
                  <th className="text-left p-3 font-semibold text-[#0B2026]">code</th>
                  <th className="text-left p-3 font-semibold text-[#0B2026]">label</th>
                  <th className="text-left p-3 font-semibold text-[#0B2026]">path</th>
                  <th className="text-left p-3 font-semibold text-[#0B2026]">source</th>
                  <th className="text-right p-3 font-semibold text-[#0B2026]">conf</th>
                </tr>
              </thead>
              <tbody>
                {filtered.map((r, i) => {
                  const id = r.order_id ?? `row-${i}`;
                  return (
                    <tr key={`${id}-${r.source}-${i}`} className="border-b border-[#E5EBF0] hover:bg-[#F7F9FA]">
                      <td className="p-3">
                        <input
                          type="checkbox"
                          checked={selectedIds.has(id)}
                          onChange={() => toggle(id)}
                          className="rounded border-[#E5EBF0]"
                        />
                      </td>
                      <td className="p-3 font-mono text-xs text-[#0B2026]">{r.order_id}</td>
                      <td className="p-3 font-mono text-xs text-[#0B2026]">{r.code ?? "--"}</td>
                      <td className="p-3 text-[#0B2026] max-w-[260px] truncate">{r.label ?? "--"}</td>
                      <td className="p-3 align-top">
                        <PathCell path={r.level_path ?? []} />
                      </td>
                      <td className="p-3 text-[#0B2026]">
                        <span className="px-2 py-0.5 rounded text-xs bg-[#F7F9FA] border border-[#E5EBF0]">
                          {r.source ?? "--"}
                        </span>
                      </td>
                      <td className="p-3 text-right text-[#0B2026]">
                        {r.confidence != null ? (
                          <ConfidenceBadge value={r.confidence} source={r.source} />
                        ) : (
                          <span className="text-[#8CA0AC] text-xs italic">n/a</span>
                        )}
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        )}
      </div>
    </div>
  );
}
