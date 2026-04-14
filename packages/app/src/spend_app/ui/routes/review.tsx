import { apiFetch } from "@/lib/fetch";
import { Button } from "@/components/ui/button";
import { createFileRoute } from "@tanstack/react-router";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { useState } from "react";

export const Route = createFileRoute("/review")({
  component: Review,
});

interface Invoice {
  order_id?: string;
  supplier?: string;
  description?: string;
  total?: number;
  pred_level_1?: string;
  pred_level_2?: string;
  confidence?: number;
}

function useInvoices(search: string) {
  return useQuery({
    queryKey: ["invoices", search],
    queryFn: () => apiFetch<Invoice[]>(`/invoices?search=${encodeURIComponent(search)}&limit=200`),
  });
}

function useFlagged(threshold: number, enabled: boolean) {
  return useQuery({
    queryKey: ["invoices", "flagged", threshold],
    queryFn: () =>
      apiFetch<Invoice[]>(`/invoices/flagged?threshold=${threshold}&limit=50`),
    enabled,
  });
}

function useCategories() {
  return useQuery({
    queryKey: ["categories"],
    queryFn: () => apiFetch<string[]>("/categories"),
  });
}

function useSubmitReview() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: (payload: { order_id: string; reviewer?: string; reviewed_level_2?: string; comments?: string }[]) =>
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

function Review() {
  const [tab, setTab] = useState<Tab>("search");
  const [search, setSearch] = useState("");
  const [reviewQueue, setReviewQueue] = useState<Invoice[]>([]);
  const [queueIndex, setQueueIndex] = useState(0);
  const [reviewedL2, setReviewedL2] = useState<Record<string, string>>({});
  const [reviewComment, setReviewComment] = useState("");
  const [selectedSearch, setSelectedSearch] = useState<Set<string>>(new Set());
  const [selectedFlagged, setSelectedFlagged] = useState<Set<string>>(new Set());
  const [flaggedLoaded, setFlaggedLoaded] = useState(false);

  const { data: invoices, isLoading: invoicesLoading } = useInvoices(search);
  const { data: flagged, isLoading: flaggedLoading } = useFlagged(3.5, flaggedLoaded);
  const { data: categories } = useCategories();
  const submitReview = useSubmitReview();
  const { data: meData } = useQuery({
    queryKey: ["me"],
    queryFn: () => apiFetch<{ username: string }>("/me"),
  });

  const loadFlagged = () => setFlaggedLoaded(true);

  const addSearchToQueue = () => {
    const toAdd = (invoices ?? []).filter((inv) =>
      selectedSearch.has(inv.order_id ?? "")
    );
    if (toAdd.length === 0) return;
    setReviewQueue((prev) => [...prev, ...toAdd]);
    setSelectedSearch(new Set());
    setTab("review");
  };

  const addFlaggedToQueue = () => {
    const toAdd = (flagged ?? []).filter((inv) =>
      selectedFlagged.has(inv.order_id ?? "")
    );
    if (toAdd.length === 0) return;
    setReviewQueue((prev) => [...prev, ...toAdd]);
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

  const currentItem = reviewQueue[queueIndex];
  const progress = reviewQueue.length
    ? `${queueIndex + 1} / ${reviewQueue.length}`
    : "0 / 0";

  const handlePrevious = () => {
    if (queueIndex > 0) setQueueIndex(queueIndex - 1);
  };

  const handleSkip = () => {
    const next = reviewQueue.filter((_, i) => i !== queueIndex);
    setReviewQueue(next);
    setQueueIndex(Math.min(queueIndex, Math.max(0, next.length - 1)));
  };

  const handleSubmit = () => {
    if (!currentItem) return;
    const id = currentItem.order_id ?? "";
    const l2 = reviewedL2[id] ?? currentItem.pred_level_2 ?? "";
    submitReview.mutate(
      [{ order_id: id, reviewer: meData?.username ?? "user", reviewed_level_2: l2, comments: reviewComment || undefined }],
      {
        onSuccess: () => {
          setReviewedL2((prev) => ({ ...prev, [id]: l2 }));
          setReviewComment("");
          if (queueIndex < reviewQueue.length - 1) {
            setQueueIndex(queueIndex + 1);
          } else {
            setReviewQueue([]);
            setQueueIndex(0);
          }
        },
      }
    );
  };

  const handleFinish = () => {
    setReviewQueue([]);
    setQueueIndex(0);
    setTab("search");
  };

  const displayFlagged = flaggedLoaded ? flagged ?? [] : [];

  return (
    <div className="max-w-7xl mx-auto px-6 py-8 font-sans">
      <h1 className="text-3xl font-bold text-[#0B2026] mb-6">Spend Review</h1>

      {/* Tabs */}
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

      {/* Search tab */}
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

      {/* Flagged tab */}
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

      {/* Review tab */}
      {tab === "review" && (
        <div>
          <p className="text-sm text-[#8CA0AC] mb-4">Progress: {progress}</p>
          {reviewQueue.length === 0 ? (
            <p className="text-[#8CA0AC]">No items in review queue. Add from Search or Flagged.</p>
          ) : (
            <div className="rounded-lg border border-[#E5EBF0] bg-white p-6 max-w-2xl">
              <h3 className="font-semibold text-[#0B2026] mb-2">
                {currentItem?.order_id} · {currentItem?.supplier}
              </h3>
              <p className="text-sm text-[#8CA0AC] mb-4">{currentItem?.description}</p>
              <p className="text-sm text-[#0B2026] mb-4">Total: {currentItem?.total}</p>
              <div className="mb-4">
                <label className="block text-sm font-medium text-[#0B2026] mb-1">
                  L2 Category
                </label>
                <select
                  value={
                    reviewedL2[currentItem?.order_id ?? ""] ?? currentItem?.pred_level_2 ?? ""
                  }
                  onChange={(e) =>
                    setReviewedL2((prev) => ({
                      ...prev,
                      [currentItem?.order_id ?? ""]: e.target.value,
                    }))
                  }
                  className="w-full px-4 py-2 border border-[#E5EBF0] rounded-md text-[#0B2026] focus:outline-none focus:ring-2 focus:ring-[#FF3621]/50"
                >
                  <option value="">Select...</option>
                  {categories?.map((c: string) => (
                    <option key={c} value={c}>
                      {c}
                    </option>
                  ))}
                </select>
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
                <Button onClick={handleSubmit}>Submit</Button>
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
