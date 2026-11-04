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
  confidence?: number;
}

interface TaxonomyLeaf {
  code: string;
  label: string;
  level_path: string[];
  description?: string;
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

// POST /reviews — one review event per invoice, matching the backend ReviewIn.
function useSubmitReview() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: (payload: {
      order_id: string;
      schema_name: string;
      reviewed_source?: string;
      action?: string;
      code?: string;
      label?: string;
      level_path?: string[];
      reviewer?: string;
      comment?: string;
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

type Tab = "search" | "flagged" | "review";

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

function Review() {
  const [tab, setTab] = useState<Tab>("search");
  const [search, setSearch] = useState("");
  const [queueIndex, setQueueIndex] = useState(0);
  const [selectedLeaf, setSelectedLeaf] = useState<Record<string, TaxonomyLeaf | null>>({});
  const [reviewComment, setReviewComment] = useState("");
  const [selectedSearch, setSelectedSearch] = useState<Set<string>>(new Set());
  const [selectedFlagged, setSelectedFlagged] = useState<Set<string>>(new Set());
  const [flaggedLoaded, setFlaggedLoaded] = useState(false);

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
    const approves =
      !!currentItem.code && currentItem.code === leaf.code;
    submitReview.mutate(
      [
        {
          order_id: id,
          schema_name: currentItem.schema_name ?? schemaName,
          reviewed_source: "llm",
          action: approves ? "approved" : "corrected",
          code: leaf.code,
          label: leaf.label,
          level_path: leaf.level_path ?? [],
          reviewer: meData?.username ?? "user",
          comment: reviewComment || "",
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
        {(["search", "flagged", "review"] as const).map((t) => (
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
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
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
            </div>
          )}
        </div>
      )}
    </div>
  );
}
