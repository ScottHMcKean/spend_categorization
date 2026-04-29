import { apiFetch } from "@/lib/fetch";
import { Button } from "@/components/ui/button";
import { SchemaBadge } from "@/components/apx/schema-selector";
import { useSchema } from "@/lib/schema-context";
import { useReviewQueue, type QueueItem } from "@/lib/review-queue";
import { createFileRoute } from "@tanstack/react-router";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { useMemo, useState } from "react";

export const Route = createFileRoute("/review")({
  component: Review,
});

interface Invoice {
  order_id?: string;
  supplier?: string;
  description?: string;
  total?: number;
  code?: string;
  label?: string;
  level_path?: string[];
  pred_level_1?: string;
  pred_level_2?: string;
  confidence?: number;
}

interface TaxonomyLeaf {
  code: string;
  label: string;
  level_path: string[];
  description?: string;
}

interface AgentReview {
  order_id: string;
  schema_name: string;
  bootstrap: {
    code?: string | null;
    label?: string | null;
    level_path?: string[] | null;
    confidence?: number | null;
    rationale?: string | null;
    source?: string | null;
  };
  suggested_code?: string | null;
  suggested_label?: string | null;
  suggested_level_path?: string[] | null;
  confidence?: number | null;
  agrees_with_bootstrap?: boolean | null;
  rationale: string;
  tool_calls: { name: string; args: Record<string, unknown>; result?: unknown }[];
  error?: string | null;
  latency_seconds: number;
}

function useInvoices(search: string) {
  return useQuery({
    queryKey: ["invoices", search],
    queryFn: () => apiFetch<Invoice[]>(`/invoices?search=${encodeURIComponent(search)}&limit=200`),
  });
}

function useFlagged(schemaName: string, threshold: number, enabled: boolean) {
  return useQuery({
    queryKey: ["invoices", "flagged", schemaName, threshold],
    queryFn: () =>
      apiFetch<Invoice[]>(
        `/invoices/flagged?schema_name=${encodeURIComponent(schemaName)}&threshold=${threshold}&limit=50`,
      ),
    enabled,
  });
}

function useTaxonomy(schemaName: string) {
  return useQuery({
    queryKey: ["taxonomy", schemaName],
    queryFn: () =>
      apiFetch<TaxonomyLeaf[]>(`/schemas/${schemaName}/taxonomy?limit=200000`),
    enabled: !!schemaName,
    staleTime: 5 * 60_000,
  });
}

interface SchemaPrediction {
  code?: string;
  label?: string;
  level_path?: string[];
  confidence?: number;
  source?: string;
  rationale?: string;
}

function useAllSchemaPredictions(orderId: string | undefined) {
  return useQuery({
    queryKey: ["predictions", "by-order", orderId],
    queryFn: () =>
      apiFetch<Record<string, SchemaPrediction[]>>(
        `/invoices/${encodeURIComponent(orderId!)}/predictions`,
      ),
    enabled: !!orderId,
    staleTime: 30_000,
  });
}

function useSubmitReview() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: (payload: {
      order_id: string;
      reviewer?: string;
      reviewed_code?: string;
      reviewed_label?: string;
      reviewed_level_path?: string[];
      original_level_1?: string;
      original_level_2?: string;
      original_level_3?: string;
      comments?: string;
    }[]) =>
      apiFetch("/reviews", {
        method: "POST",
        body: JSON.stringify(payload),
      }),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["classifications"] });
    },
  });
}

function useRunAgent() {
  return useMutation({
    mutationFn: ({ schemaName, orderId }: { schemaName: string; orderId: string }) =>
      apiFetch<AgentReview>(
        `/agent/review/${encodeURIComponent(schemaName)}/${encodeURIComponent(orderId)}`,
        { method: "POST" },
      ),
  });
}

type Tab = "search" | "flagged" | "agent" | "review";

interface LeafPickerProps {
  value: TaxonomyLeaf | null;
  onChange: (leaf: TaxonomyLeaf | null) => void;
  taxonomy: TaxonomyLeaf[] | undefined;
  loading: boolean;
}

function LeafPicker({ value, onChange, taxonomy, loading }: LeafPickerProps) {
  const [query, setQuery] = useState("");
  const [open, setOpen] = useState(false);

  const matches = useMemo(() => {
    if (!taxonomy) return [];
    const q = query.trim().toLowerCase();
    if (!q) return taxonomy.slice(0, 20);
    return taxonomy
      .filter((l) => {
        const path = (l.level_path ?? []).join(" > ").toLowerCase();
        return (
          (l.code ?? "").toLowerCase().includes(q) ||
          (l.label ?? "").toLowerCase().includes(q) ||
          path.includes(q)
        );
      })
      .slice(0, 50);
  }, [taxonomy, query]);

  return (
    <div className="relative">
      <input
        type="text"
        placeholder={loading ? "Loading taxonomy…" : "Search by code, label, or path…"}
        value={query}
        onChange={(e) => {
          setQuery(e.target.value);
          setOpen(true);
        }}
        onFocus={() => setOpen(true)}
        onBlur={() => setTimeout(() => setOpen(false), 150)}
        className="w-full px-4 py-2 border border-[#E5EBF0] rounded-md text-[#0B2026] placeholder:text-[#8CA0AC] focus:outline-none focus:ring-2 focus:ring-[#FF3621]/40"
      />
      {value && (
        <div className="mt-1 text-xs text-[#0B2026] flex items-baseline gap-2">
          <span className="font-mono text-[#FF3621]">{value.code}</span>
          <span>{value.label}</span>
          <button
            onClick={() => onChange(null)}
            className="ml-auto text-[#8CA0AC] hover:text-[#FF3621]"
          >
            clear
          </button>
        </div>
      )}
      {open && matches.length > 0 && (
        <ul className="absolute z-10 mt-1 w-full max-h-72 overflow-auto bg-white border border-[#E5EBF0] rounded-md shadow-lg text-sm">
          {matches.map((leaf) => (
            <li
              key={leaf.code}
              onMouseDown={(e) => {
                e.preventDefault();
                onChange(leaf);
                setQuery("");
                setOpen(false);
              }}
              className="px-3 py-2 cursor-pointer hover:bg-[#F7F9FA] flex flex-col gap-0.5"
            >
              <div className="flex items-baseline gap-2">
                <span className="font-mono text-[#FF3621] text-xs">{leaf.code}</span>
                <span className="font-medium text-[#0B2026]">{leaf.label}</span>
              </div>
              {leaf.level_path && leaf.level_path.length > 0 && (
                <span className="text-xs text-[#8CA0AC]">{leaf.level_path.join(" › ")}</span>
              )}
            </li>
          ))}
        </ul>
      )}
    </div>
  );
}

function AllSchemasPanel({ orderId, activeSchema }: { orderId: string | undefined; activeSchema: string }) {
  const { data, isLoading } = useAllSchemaPredictions(orderId);
  if (!orderId) return null;
  if (isLoading) {
    return <p className="text-xs text-[#8CA0AC]">Loading predictions across schemas…</p>;
  }
  const entries = data ? Object.entries(data) : [];
  if (entries.length === 0) {
    return (
      <p className="text-xs text-[#8CA0AC]">No predictions for this invoice in any schema yet.</p>
    );
  }
  return (
    <div className="space-y-2">
      <p className="text-xs uppercase tracking-wide text-[#8CA0AC]">
        Predictions across schemas
      </p>
      {entries.map(([schema, preds]) => (
        <div
          key={schema}
          className={`rounded-md border p-3 ${
            schema === activeSchema
              ? "border-[#FF3621] bg-[#FF3621]/5"
              : "border-[#E5EBF0] bg-[#F7F9FA]"
          }`}
        >
          <div className="flex items-baseline justify-between mb-1">
            <span className="font-semibold text-sm text-[#0B2026]">
              {schema}
              {schema === activeSchema && (
                <span className="ml-2 text-xs font-normal text-[#FF3621]">active</span>
              )}
            </span>
            <span className="text-xs text-[#8CA0AC]">
              {preds.length} prediction{preds.length === 1 ? "" : "s"}
            </span>
          </div>
          <ul className="space-y-1 text-xs">
            {preds.map((p, i) => (
              <li key={`${schema}-${i}`} className="flex items-start gap-2">
                <span className="text-[#8CA0AC] w-20 shrink-0">{p.source ?? "?"}</span>
                <span className="font-mono text-[#FF3621] shrink-0">{p.code ?? "—"}</span>
                <span className="text-[#0B2026]">{p.label ?? "—"}</span>
                {p.level_path && p.level_path.length > 0 && (
                  <span className="text-[#8CA0AC] truncate" title={p.level_path.join(" › ")}>
                    {p.level_path.join(" › ")}
                  </span>
                )}
                {p.confidence != null && (
                  <span className="font-mono text-[#0B2026] ml-auto shrink-0">
                    {p.confidence <= 1
                      ? `${(p.confidence * 100).toFixed(0)}%`
                      : p.confidence.toFixed(2)}
                  </span>
                )}
              </li>
            ))}
          </ul>
        </div>
      ))}
    </div>
  );
}

interface AgentPanelProps {
  schemaName: string;
  invoice: Invoice | null;
  variant?: "card" | "inline";
}

function AgentPanel({ schemaName, invoice, variant = "card" }: AgentPanelProps) {
  const runAgent = useRunAgent();
  const result = runAgent.data;

  if (!invoice?.order_id) {
    return null;
  }

  return (
    <div className={
      variant === "inline"
        ? "rounded-lg border border-[#E5EBF0] bg-white p-4"
        : "mt-6 rounded-lg border border-[#E5EBF0] bg-[#F7F9FA] p-4"
    }>
      <div className="flex items-center justify-between mb-3">
        <div>
          <h4 className="font-semibold text-[#0B2026]">Agent Review</h4>
          <p className="text-xs text-[#8CA0AC]">
            {invoice.order_id} · {invoice.supplier ?? "—"}
          </p>
        </div>
        <Button
          onClick={() => runAgent.mutate({ schemaName, orderId: invoice.order_id! })}
          disabled={runAgent.isPending}
          variant="secondary"
        >
          {runAgent.isPending ? "Reviewing…" : "Run Agent Review"}
        </Button>
      </div>
      {runAgent.isPending && (
        <p className="text-sm text-[#8CA0AC]">
          Calling the agent — this drives the same toolset and LLM as notebook 4.
        </p>
      )}
      {runAgent.isError && (
        <p className="text-sm text-[#FF3621]">
          {(runAgent.error as Error).message || "Agent run failed."}
        </p>
      )}
      {result && (
        <div className="space-y-3 text-sm">
          {result.error && (
            <p className="text-[#FF3621]">Agent error: {result.error}</p>
          )}
          <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
            <div className="bg-white p-3 rounded border border-[#E5EBF0]">
              <p className="text-xs text-[#8CA0AC] mb-1">Bootstrap</p>
              <p className="font-mono text-xs text-[#FF3621]">{result.bootstrap.code ?? "—"}</p>
              <p className="font-medium text-[#0B2026]">{result.bootstrap.label ?? "—"}</p>
              <p className="text-xs text-[#8CA0AC]">
                {(result.bootstrap.level_path ?? []).join(" › ") || "—"}
              </p>
              <p className="text-xs text-[#8CA0AC] mt-1">
                conf={result.bootstrap.confidence ?? "—"} · src={result.bootstrap.source ?? "—"}
              </p>
            </div>
            <div className="bg-white p-3 rounded border border-[#E5EBF0]">
              <p className="text-xs text-[#8CA0AC] mb-1">
                Suggested ({result.agrees_with_bootstrap ? "agrees" : "disagrees"})
              </p>
              <p className="font-mono text-xs text-[#FF3621]">{result.suggested_code ?? "—"}</p>
              <p className="font-medium text-[#0B2026]">{result.suggested_label ?? "—"}</p>
              <p className="text-xs text-[#8CA0AC]">
                {(result.suggested_level_path ?? []).join(" › ") || "—"}
              </p>
              <p className="text-xs text-[#8CA0AC] mt-1">
                conf={result.confidence ?? "—"}/5 · {result.latency_seconds}s
              </p>
            </div>
          </div>
          {result.rationale && (
            <div className="bg-white p-3 rounded border border-[#E5EBF0]">
              <p className="text-xs text-[#8CA0AC] mb-1">Rationale</p>
              <p className="text-[#0B2026]">{result.rationale}</p>
            </div>
          )}
          {result.tool_calls.length > 0 && (
            <details className="bg-white p-3 rounded border border-[#E5EBF0]">
              <summary className="text-xs text-[#8CA0AC] cursor-pointer">
                Tool trace ({result.tool_calls.length} calls)
              </summary>
              <ol className="mt-2 space-y-1 text-xs font-mono">
                {result.tool_calls.map((tc, i) => (
                  <li key={i} className="text-[#0B2026]">
                    <span className="text-[#FF3621]">{tc.name}</span>(
                    <span className="text-[#8CA0AC]">{JSON.stringify(tc.args)}</span>)
                  </li>
                ))}
              </ol>
            </details>
          )}
        </div>
      )}
    </div>
  );
}

function Review() {
  const [tab, setTab] = useState<Tab>("search");
  const [search, setSearch] = useState("");
  const [queueIndex, setQueueIndex] = useState(0);
  const [selectedLeaf, setSelectedLeaf] = useState<Record<string, TaxonomyLeaf | null>>({});
  const [reviewComment, setReviewComment] = useState("");
  const [selectedSearch, setSelectedSearch] = useState<Set<string>>(new Set());
  const [selectedFlagged, setSelectedFlagged] = useState<Set<string>>(new Set());
  const [flaggedLoaded, setFlaggedLoaded] = useState(false);
  const [agentTarget, setAgentTarget] = useState<Invoice | null>(null);

  const { schemaName } = useSchema();
  const { queue: reviewQueue, add: addToQueue, remove: removeFromQueue, clear: clearQueue } =
    useReviewQueue();
  const { data: invoices, isLoading: invoicesLoading } = useInvoices(search);
  const { data: flagged, isLoading: flaggedLoading } = useFlagged(schemaName, 3.5, flaggedLoaded);
  const { data: taxonomy, isLoading: taxonomyLoading } = useTaxonomy(schemaName);
  const submitReview = useSubmitReview();
  const { data: meData } = useQuery({
    queryKey: ["me"],
    queryFn: () => apiFetch<{ username: string }>("/me"),
  });

  const loadFlagged = () => setFlaggedLoaded(true);

  const invoiceToQueueItem = (inv: Invoice): QueueItem => ({
    order_id: inv.order_id ?? "",
    schema_name: schemaName,
    supplier: inv.supplier,
    description: inv.description,
    total: inv.total,
    code: inv.code,
    label: inv.label,
    level_path: inv.level_path,
    confidence: inv.confidence,
  });

  const addSearchToQueue = () => {
    const toAdd = (invoices ?? []).filter((inv) =>
      selectedSearch.has(inv.order_id ?? "")
    );
    if (toAdd.length === 0) return;
    addToQueue(toAdd.map(invoiceToQueueItem));
    setSelectedSearch(new Set());
    setTab("review");
  };

  const addFlaggedToQueue = () => {
    const toAdd = (flagged ?? []).filter((inv) =>
      selectedFlagged.has(inv.order_id ?? "")
    );
    if (toAdd.length === 0) return;
    addToQueue(toAdd.map(invoiceToQueueItem));
    setSelectedFlagged(new Set());
    setTab("review");
  };

  const toggleSearch = (id: string) => {
    setSelectedSearch((prev) => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return next;
    });
  };

  const toggleFlagged = (id: string) => {
    setSelectedFlagged((prev) => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return next;
    });
  };

  const currentItem = reviewQueue[queueIndex] as QueueItem | undefined;
  const progress = reviewQueue.length
    ? `${queueIndex + 1} / ${reviewQueue.length}`
    : "0 / 0";

  const currentSelected = currentItem?.order_id
    ? selectedLeaf[currentItem.order_id]
    : null;

  const handlePrevious = () => {
    if (queueIndex > 0) setQueueIndex(queueIndex - 1);
  };

  const handleSkip = () => {
    if (!currentItem) return;
    removeFromQueue(currentItem.order_id, currentItem.schema_name);
    setQueueIndex((i) => Math.min(i, Math.max(0, reviewQueue.length - 2)));
  };

  const handleSubmit = () => {
    if (!currentItem) return;
    const id = currentItem.order_id;
    const leaf = selectedLeaf[id];
    if (!leaf) return;
    submitReview.mutate(
      [
        {
          order_id: id,
          reviewer: meData?.username ?? "user",
          reviewed_code: leaf.code,
          reviewed_label: leaf.label,
          reviewed_level_path: leaf.level_path ?? [],
          original_level_1: currentItem.level_path?.[0] ?? "",
          original_level_2: currentItem.level_path?.[1] ?? "",
          original_level_3: currentItem.level_path?.[2] ?? currentItem.label ?? "",
          comments: reviewComment || undefined,
        },
      ],
      {
        onSuccess: () => {
          setReviewComment("");
          removeFromQueue(currentItem.order_id, currentItem.schema_name);
          setQueueIndex((i) => Math.min(i, Math.max(0, reviewQueue.length - 2)));
        },
      }
    );
  };

  const handleFinish = () => {
    clearQueue();
    setQueueIndex(0);
    setTab("search");
  };

  const displayFlagged = flaggedLoaded ? flagged ?? [] : [];

  return (
    <div className="max-w-7xl mx-auto px-6 py-8 font-sans">
      <div className="flex flex-wrap items-baseline justify-between gap-3 mb-6">
        <h1 className="text-3xl font-bold text-[#0B2026]">Spend Review</h1>
        <SchemaBadge />
      </div>

      <div className="flex gap-2 border-b border-[#E5EBF0] mb-6">
        {(["search", "flagged", "agent", "review"] as const).map((t) => (
          <button
            key={t}
            onClick={() => setTab(t)}
            className={`px-4 py-2 text-sm font-semibold transition-colors border-b-2 -mb-px capitalize flex items-center gap-2 ${
              tab === t
                ? "text-[#FF3621] border-[#FF3621]"
                : "text-[#8CA0AC] border-transparent hover:text-[#0B2026]"
            }`}
          >
            {t}
            {t === "review" && reviewQueue.length > 0 && (
              <span className="bg-[#FF3621] text-white text-xs px-1.5 py-0.5 rounded-full">
                {reviewQueue.length}
              </span>
            )}
          </button>
        ))}
      </div>

      {tab === "search" && (
        <div>
          <div className="mb-4 flex gap-4">
            <input
              type="text"
              placeholder="Search invoices..."
              value={search}
              onChange={(e) => setSearch(e.target.value)}
              className="flex-1 max-w-md px-4 py-2 border border-[#E5EBF0] rounded-md text-[#0B2026] placeholder:text-[#8CA0AC] focus:outline-none focus:ring-2 focus:ring-[#FF3621]/50"
            />
            <Button onClick={addSearchToQueue} disabled={selectedSearch.size === 0}>
              Add to Review Queue ({selectedSearch.size})
            </Button>
          </div>
          {invoicesLoading && <p className="text-[#8CA0AC]">Loading...</p>}
          {!invoicesLoading && invoices && (
            <div className="rounded-lg border border-[#E5EBF0] overflow-hidden">
              <table className="w-full text-sm">
                <thead className="bg-[#F7F9FA] border-b border-[#E5EBF0]">
                  <tr>
                    <th className="text-left p-3 w-10"></th>
                    <th className="text-left p-3 font-semibold text-[#0B2026]">order_id</th>
                    <th className="text-left p-3 font-semibold text-[#0B2026]">supplier</th>
                    <th className="text-left p-3 font-semibold text-[#0B2026]">description</th>
                    <th className="text-left p-3 font-semibold text-[#0B2026]">total</th>
                  </tr>
                </thead>
                <tbody>
                  {invoices.map((inv, i) => {
                    const id = inv.order_id ?? `inv-${i}`;
                    return (
                      <tr key={id} className="border-b border-[#E5EBF0] hover:bg-[#F7F9FA]">
                        <td className="p-3">
                          <input
                            type="checkbox"
                            checked={selectedSearch.has(id)}
                            onChange={() => toggleSearch(id)}
                            className="rounded border-[#E5EBF0]"
                          />
                        </td>
                        <td className="p-3 text-[#0B2026]">{inv.order_id}</td>
                        <td className="p-3 text-[#0B2026]">{inv.supplier}</td>
                        <td className="p-3 text-[#0B2026] max-w-[200px] truncate">
                          {inv.description}
                        </td>
                        <td className="p-3 text-[#0B2026]">{inv.total}</td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          )}
        </div>
      )}

      {tab === "flagged" && (
        <div>
          <div className="mb-4">
            <Button onClick={loadFlagged} disabled={flaggedLoaded && flaggedLoading}>
              {flaggedLoaded && flaggedLoading ? "Loading..." : "Load Flagged"}
            </Button>
            <Button
              onClick={addFlaggedToQueue}
              disabled={selectedFlagged.size === 0}
              variant="secondary"
              className="ml-2"
            >
              Add to Review Queue ({selectedFlagged.size})
            </Button>
          </div>
          {displayFlagged.length === 0 && !flaggedLoading && (
            <p className="text-[#8CA0AC]">
              {flaggedLoaded ? "No flagged invoices." : "Click Load Flagged to fetch low-confidence invoices."}
            </p>
          )}
          {displayFlagged.length > 0 && (
            <div className="rounded-lg border border-[#E5EBF0] overflow-hidden">
              <table className="w-full text-sm">
                <thead className="bg-[#F7F9FA] border-b border-[#E5EBF0]">
                  <tr>
                    <th className="text-left p-3 w-10"></th>
                    <th className="text-left p-3 font-semibold text-[#0B2026]">order_id</th>
                    <th className="text-left p-3 font-semibold text-[#0B2026]">supplier</th>
                    <th className="text-left p-3 font-semibold text-[#0B2026]">predicted</th>
                    <th className="text-left p-3 font-semibold text-[#0B2026]">confidence</th>
                    <th className="text-left p-3 font-semibold text-[#0B2026]">agent</th>
                  </tr>
                </thead>
                <tbody>
                  {displayFlagged.map((inv, i) => {
                    const id = inv.order_id ?? `inv-${i}`;
                    return (
                      <tr key={id} className="border-b border-[#E5EBF0] hover:bg-[#F7F9FA]">
                        <td className="p-3">
                          <input
                            type="checkbox"
                            checked={selectedFlagged.has(id)}
                            onChange={() => toggleFlagged(id)}
                            className="rounded border-[#E5EBF0]"
                          />
                        </td>
                        <td className="p-3 text-[#0B2026]">{inv.order_id}</td>
                        <td className="p-3 text-[#0B2026]">{inv.supplier}</td>
                        <td className="p-3 text-[#0B2026]">
                          {inv.code ? (
                            <span>
                              <span className="font-mono text-xs text-[#FF3621]">{inv.code}</span>{" "}
                              <span className="text-[#0B2026]">{inv.label}</span>
                            </span>
                          ) : (
                            "—"
                          )}
                        </td>
                        <td className="p-3 text-[#0B2026]">{inv.confidence != null ? inv.confidence : "--"}</td>
                        <td className="p-3">
                          <button
                            onClick={() => {
                              setAgentTarget(inv);
                              setTab("agent");
                            }}
                            className="text-xs text-[#FF3621] hover:underline"
                          >
                            Run agent →
                          </button>
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          )}
        </div>
      )}

      {tab === "agent" && (
        <div className="space-y-4">
          <div className="rounded-lg border border-[#E5EBF0] bg-[#F7F9FA] p-4">
            <p className="text-sm text-[#0B2026] mb-3">
              Pick any invoice and run the review agent. Same toolset, prompt,
              and Foundation Models endpoint as notebook 4 — the agent walks
              the active taxonomy ({schemaName}) and reports a suggested code
              with a 1–5 confidence and a tool-call trace.
            </p>
            <div className="flex flex-wrap gap-2">
              <Button onClick={loadFlagged} variant="secondary" disabled={flaggedLoaded && flaggedLoading}>
                {flaggedLoaded && flaggedLoading ? "Loading…" : "Load Flagged Invoices"}
              </Button>
              {(flagged ?? []).slice(0, 8).map((inv) => {
                const id = inv.order_id ?? "";
                return (
                  <button
                    key={id}
                    onClick={() => setAgentTarget(inv)}
                    className={`px-3 py-1.5 rounded text-xs border transition-colors ${
                      agentTarget?.order_id === id
                        ? "border-[#FF3621] text-[#FF3621] bg-white"
                        : "border-[#E5EBF0] text-[#0B2026] hover:border-[#FF3621]"
                    }`}
                  >
                    {id}
                  </button>
                );
              })}
            </div>
            {!flaggedLoaded && (
              <p className="text-xs text-[#8CA0AC] mt-2">
                Click "Load Flagged Invoices" to populate quick-pick buttons, or queue any invoice from
                the Search tab and switch back here.
              </p>
            )}
          </div>
          {agentTarget ? (
            <AgentPanel schemaName={schemaName} invoice={agentTarget} variant="inline" />
          ) : (
            <p className="text-sm text-[#8CA0AC]">Select an invoice above to start an agent review.</p>
          )}
        </div>
      )}

      {tab === "review" && (
        <div>
          <div className="flex items-center justify-between mb-4">
            <p className="text-sm text-[#8CA0AC]">Progress: {progress}</p>
            {reviewQueue.length > 0 && (
              <button
                onClick={() => clearQueue()}
                className="text-xs text-[#8CA0AC] hover:text-[#FF3621]"
              >
                Clear queue
              </button>
            )}
          </div>
          {reviewQueue.length === 0 ? (
            <p className="text-[#8CA0AC]">
              No items in review queue. Add from Search, Flagged, or the Classifications page.
            </p>
          ) : (
            <div className="rounded-lg border border-[#E5EBF0] bg-white p-6 max-w-3xl">
              <h3 className="font-semibold text-[#0B2026] mb-2">
                {currentItem?.order_id} · {currentItem?.supplier ?? "—"}
              </h3>
              {currentItem?.description && (
                <p className="text-sm text-[#8CA0AC] mb-2">{currentItem.description}</p>
              )}
              {currentItem?.total != null && (
                <p className="text-sm text-[#0B2026] mb-4">Total: {currentItem.total}</p>
              )}

              <AllSchemasPanel orderId={currentItem?.order_id} activeSchema={schemaName} />

              <div className="mb-4 mt-4">
                <label className="block text-sm font-medium text-[#0B2026] mb-1">
                  Reviewed Category for <span className="font-mono">{schemaName}</span>
                </label>
                <LeafPicker
                  value={currentSelected ?? null}
                  onChange={(leaf) =>
                    setSelectedLeaf((prev) => ({
                      ...prev,
                      [currentItem?.order_id ?? ""]: leaf,
                    }))
                  }
                  taxonomy={taxonomy}
                  loading={taxonomyLoading}
                />
              </div>
              <div className="mb-6">
                <label className="block text-sm font-medium text-[#0B2026] mb-1">Comment</label>
                <input
                  type="text"
                  value={reviewComment}
                  onChange={(e) => setReviewComment(e.target.value)}
                  placeholder="Optional"
                  className="w-full px-4 py-2 border border-[#E5EBF0] rounded-md text-[#0B2026] placeholder:text-[#8CA0AC] focus:outline-none focus:ring-2 focus:ring-[#FF3621]/50"
                />
              </div>
              <div className="flex gap-2">
                <Button
                  variant="outline"
                  onClick={handlePrevious}
                  disabled={queueIndex === 0}
                >
                  Previous
                </Button>
                <Button variant="outline" onClick={handleSkip}>
                  Skip
                </Button>
                <Button onClick={handleSubmit} disabled={!currentSelected}>
                  Submit
                </Button>
                <Button variant="secondary" onClick={handleFinish}>
                  Finish
                </Button>
              </div>

              <AgentPanel schemaName={schemaName} invoice={currentItem ?? null} />
            </div>
          )}
        </div>
      )}
    </div>
  );
}
