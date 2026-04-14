import { apiFetch } from "@/lib/fetch";
import { Button } from "@/components/ui/button";
import { createFileRoute } from "@tanstack/react-router";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { useState, useMemo } from "react";

export const Route = createFileRoute("/classifications")({
  component: Classifications,
});

type Source = "bootstrap" | "catboost" | "vectorsearch" | "reviews" | "performance";

interface ClassificationRow {
  order_id?: string;
  supplier?: string;
  description?: string;
  total?: number;
  pred_level_1?: string;
  pred_level_2?: string;
  confidence?: number;
  reviewer?: string;
  original_level_2?: string;
  reviewed_level_2?: string;
  review_status?: string;
  comments?: string;
}

interface ReviewPayload {
  order_id: string;
  reviewer?: string;
  reviewed_level_1?: string;
  reviewed_level_2?: string;
  comments?: string;
}

function useCategories() {
  return useQuery({
    queryKey: ["categories"],
    queryFn: () => apiFetch<string[]>("/categories"),
  });
}

function useClassifications(source: Source) {
  return useQuery({
    queryKey: ["classifications", source],
    queryFn: () => apiFetch<ClassificationRow[]>(`/classifications/${source}?limit=500`),
    enabled: !!source,
  });
}

function useSubmitReviews() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: (payload: ReviewPayload[]) =>
      apiFetch("/reviews", {
        method: "POST",
        body: JSON.stringify(payload),
      }),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["classifications"] });
    },
  });
}

interface CategoryAccuracy {
  category: string;
  precision: number;
  recall: number;
  f1: number;
  spend: number;
  count: number;
}

interface AccuracyData {
  overall_accuracy: number;
  overall_precision: number;
  overall_recall: number;
  overall_f1: number;
  total_classified: number;
  total_correct: number;
  source: string;
  categories: CategoryAccuracy[];
}

type ClassifierSource = "bootstrap" | "catboost" | "vectorsearch";

function useAccuracy(source: ClassifierSource) {
  return useQuery({
    queryKey: ["accuracy", source],
    queryFn: () => apiFetch<AccuracyData>(`/analytics/accuracy/${source}`),
  });
}

const SOURCES: { id: Source; label: string }[] = [
  { id: "bootstrap", label: "Bootstrap" },
  { id: "catboost", label: "CatBoost" },
  { id: "vectorsearch", label: "Vector Search" },
  { id: "reviews", label: "Human Reviews" },
  { id: "performance", label: "Model Performance" },
];

const METRIC_INFO: Record<string, string> = {
  Accuracy: "Fraction of all predictions that exactly match the ground truth category.",
  Precision: "Of all items predicted as this category, the fraction that actually belong to it.",
  Recall: "Of all items that actually belong to this category, the fraction correctly predicted.",
  "F1 Score": "Harmonic mean of precision and recall.",
};

function InfoHover({ label }: { label: string }) {
  const [show, setShow] = useState(false);
  const info = METRIC_INFO[label];
  if (!info) return null;
  return (
    <span
      className="relative inline-flex items-center ml-1"
      onMouseEnter={() => setShow(true)}
      onMouseLeave={() => setShow(false)}
    >
      <span className="w-4 h-4 rounded-full bg-[#E5EBF0] text-[#8CA0AC] text-xs flex items-center justify-center cursor-help font-bold">
        ?
      </span>
      {show && (
        <span className="absolute bottom-full left-1/2 -translate-x-1/2 mb-2 px-3 py-2 bg-[#0B2026] text-white text-xs rounded-lg shadow-lg w-56 z-50 leading-relaxed">
          {info}
        </span>
      )}
    </span>
  );
}

function Classifications() {
  const [activeSource, setActiveSource] = useState<Source>("bootstrap");
  const [search, setSearch] = useState("");
  const [selectedIds, setSelectedIds] = useState<Set<string>>(new Set());
  const [newL2, setNewL2] = useState("");
  const [comment, setComment] = useState("");
  const [modelSource, setModelSource] = useState<ClassifierSource>("catboost");

  const { data: categories } = useCategories();
  const { data: rows, isLoading } = useClassifications(activeSource);
  const bootstrap = useClassifications("bootstrap");
  const catboost = useClassifications("catboost");
  const vectorsearch = useClassifications("vectorsearch");
  const reviews = useClassifications("reviews");
  const submitReviews = useSubmitReviews();
  const { data: meData } = useQuery({
    queryKey: ["me"],
    queryFn: () => apiFetch<{ username: string }>("/me"),
  });
  const bootstrapAcc = useAccuracy("bootstrap");
  const catboostAcc = useAccuracy("catboost");
  const vectorsearchAcc = useAccuracy("vectorsearch");

  const counts: Record<Source, number> = {
    bootstrap: bootstrap.data?.length ?? 0,
    catboost: catboost.data?.length ?? 0,
    vectorsearch: vectorsearch.data?.length ?? 0,
    reviews: reviews.data?.length ?? 0,
  };

  const filteredRows = useMemo(() => {
    if (!rows) return [];
    const s = search.toLowerCase();
    return rows.filter((r) => {
      const orderId = (r.order_id ?? "").toLowerCase();
      const supplier = (r.supplier ?? "").toLowerCase();
      const desc = (r.description ?? "").toLowerCase();
      return orderId.includes(s) || supplier.includes(s) || desc.includes(s);
    });
  }, [rows, search]);

  const toggleSelect = (id: string) => {
    setSelectedIds((prev) => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return next;
    });
  };

  const toggleAll = () => {
    if (selectedIds.size === filteredRows.length) {
      setSelectedIds(new Set());
    } else {
      setSelectedIds(new Set(filteredRows.map((r) => r.order_id ?? "").filter(Boolean)));
    }
  };

  const handleSubmitCorrections = () => {
    if (selectedIds.size === 0 || !newL2) return;
    const payload: ReviewPayload[] = Array.from(selectedIds).map((order_id) => ({
      order_id,
      reviewer: meData?.username ?? "user",
      reviewed_level_2: newL2,
      comments: comment || undefined,
    }));
    submitReviews.mutate(payload, {
      onSuccess: () => {
        setSelectedIds(new Set());
        setNewL2("");
        setComment("");
      },
    });
  };

  const isReview = activeSource === "reviews";
  const isPerformance = activeSource === "performance";
  const isDataTab = !isPerformance;

  const fmtPct = (v: number) => `${(v * 100).toFixed(0)}%`;
  const fmtDollar = (v: number) =>
    new Intl.NumberFormat("en-US", { style: "currency", currency: "USD", minimumFractionDigits: 0, maximumFractionDigits: 0 }).format(v);

  const activeAccuracy = modelSource === "bootstrap" ? bootstrapAcc.data
    : modelSource === "catboost" ? catboostAcc.data
    : vectorsearchAcc.data;

  return (
    <div className="max-w-7xl mx-auto px-6 py-8 font-sans">
      <h1 className="text-3xl font-bold text-[#0B2026] mb-6">Classifications</h1>

      {/* Tabs */}
      <div className="flex gap-2 border-b border-[#E5EBF0] mb-6">
        {SOURCES.map(({ id, label }) => (
          <button
            key={id}
            onClick={() => setActiveSource(id)}
            className={`px-4 py-2 text-sm font-semibold transition-colors border-b-2 -mb-px ${
              activeSource === id
                ? "text-[#FF3621] border-[#FF3621]"
                : "text-[#8CA0AC] border-transparent hover:text-[#0B2026]"
            }`}
          >
            {label}
          </button>
        ))}
      </div>

      {isDataTab && (
        <>
          {/* Accuracy summary */}
          {(() => {
            const accData: { label: string; acc: AccuracyData | undefined }[] = [
              { label: "Bootstrap", acc: bootstrapAcc.data },
              { label: "CatBoost", acc: catboostAcc.data },
              { label: "Vector Search", acc: vectorsearchAcc.data },
            ];
            return (
              <div className="grid grid-cols-3 gap-4 mb-6">
                {accData.map(({ label, acc }) => (
                  <div key={label} className="rounded-lg border border-[#E5EBF0] bg-white p-4">
                    <p className="text-sm font-semibold text-[#0B2026] mb-2">{label} vs. Ground Truth</p>
                    {acc && acc.total_classified > 0 ? (
                      <div className="space-y-1">
                        <div className="flex justify-between">
                          <span className="text-sm text-[#8CA0AC]">Accuracy</span>
                          <span className="text-sm font-bold text-[#0B2026]">{(acc.overall_accuracy * 100).toFixed(1)}%</span>
                        </div>
                        <div className="w-full bg-[#E5EBF0] rounded-full h-2">
                          <div
                            className="h-2 rounded-full"
                            style={{
                              width: `${acc.overall_accuracy * 100}%`,
                              backgroundColor: acc.overall_accuracy >= 0.9 ? "#22C55E" : acc.overall_accuracy >= 0.7 ? "#F59E0B" : "#FF3621",
                            }}
                          />
                        </div>
                        <div className="flex justify-between text-xs text-[#8CA0AC] pt-1">
                          <span>Precision: {(acc.overall_precision * 100).toFixed(1)}%</span>
                          <span>Recall: {(acc.overall_recall * 100).toFixed(1)}%</span>
                          <span>F1: {(acc.overall_f1 * 100).toFixed(1)}%</span>
                        </div>
                        <p className="text-xs text-[#8CA0AC]">{acc.total_correct.toLocaleString()} / {acc.total_classified.toLocaleString()} correct</p>
                      </div>
                    ) : (
                      <p className="text-sm text-[#8CA0AC]">No data</p>
                    )}
                  </div>
                ))}
              </div>
            );
          })()}

          {/* Row counts */}
          <div className="grid grid-cols-4 gap-4 mb-6">
            {SOURCES.filter((s) => s.id !== "performance").map(({ id, label }) => (
              <div
                key={id}
                className={`rounded-lg border p-4 ${
                  activeSource === id ? "border-[#FF3621] bg-[#FF3621]/5" : "border-[#E5EBF0] bg-white"
                }`}
              >
                <p className="text-sm text-[#8CA0AC]">{label}</p>
                <p className="text-2xl font-bold text-[#0B2026]">{counts[id as keyof typeof counts] ?? 0}</p>
              </div>
            ))}
          </div>

          {/* Search */}
          <div className="mb-4">
            <input
              type="text"
              placeholder="Filter by order_id, supplier, description..."
              value={search}
              onChange={(e) => setSearch(e.target.value)}
              className="w-full max-w-md px-4 py-2 border border-[#E5EBF0] rounded-md text-[#0B2026] placeholder:text-[#8CA0AC] focus:outline-none focus:ring-2 focus:ring-[#FF3621]/50"
            />
          </div>

          {/* Data table */}
          <div className="rounded-lg border border-[#E5EBF0] overflow-hidden bg-white mb-8">
            {isLoading && <p className="p-6 text-[#8CA0AC]">Loading...</p>}
            {!isLoading && filteredRows.length === 0 && (
              <p className="p-6 text-[#8CA0AC]">No classifications found.</p>
            )}
            {!isLoading && filteredRows.length > 0 && (
              <div className="overflow-x-auto">
                <table className="w-full text-sm">
                  <thead className="bg-[#F7F9FA] border-b border-[#E5EBF0]">
                    <tr>
                      <th className="text-left p-3">
                        <input
                          type="checkbox"
                          checked={selectedIds.size === filteredRows.length && filteredRows.length > 0}
                          onChange={toggleAll}
                          className="rounded border-[#E5EBF0]"
                        />
                      </th>
                      <th className="text-left p-3 font-semibold text-[#0B2026]">order_id</th>
                      {!isReview && (
                        <>
                          <th className="text-left p-3 font-semibold text-[#0B2026]">supplier</th>
                          <th className="text-left p-3 font-semibold text-[#0B2026]">description</th>
                          <th className="text-left p-3 font-semibold text-[#0B2026]">total</th>
                          <th className="text-left p-3 font-semibold text-[#0B2026]">pred_level_1</th>
                          <th className="text-left p-3 font-semibold text-[#0B2026]">pred_level_2</th>
                          <th className="text-left p-3 font-semibold text-[#0B2026]">confidence</th>
                        </>
                      )}
                      {isReview && (
                        <>
                          <th className="text-left p-3 font-semibold text-[#0B2026]">reviewer</th>
                          <th className="text-left p-3 font-semibold text-[#0B2026]">original_level_2</th>
                          <th className="text-left p-3 font-semibold text-[#0B2026]">reviewed_level_2</th>
                          <th className="text-left p-3 font-semibold text-[#0B2026]">review_status</th>
                          <th className="text-left p-3 font-semibold text-[#0B2026]">comments</th>
                        </>
                      )}
                    </tr>
                  </thead>
                  <tbody>
                    {filteredRows.map((r, i) => {
                      const id = r.order_id ?? `row-${i}`;
                      return (
                        <tr key={id} className="border-b border-[#E5EBF0] hover:bg-[#F7F9FA]">
                          <td className="p-3">
                            <input
                              type="checkbox"
                              checked={selectedIds.has(id)}
                              onChange={() => toggleSelect(id)}
                              className="rounded border-[#E5EBF0]"
                            />
                          </td>
                          <td className="p-3 text-[#0B2026]">{r.order_id}</td>
                          {!isReview && (
                            <>
                              <td className="p-3 text-[#0B2026]">{r.supplier}</td>
                              <td className="p-3 text-[#0B2026] max-w-[200px] truncate">
                                {r.description}
                              </td>
                              <td className="p-3 text-[#0B2026]">{r.total}</td>
                              <td className="p-3 text-[#0B2026]">{r.pred_level_1}</td>
                              <td className="p-3 text-[#0B2026]">{r.pred_level_2}</td>
                              <td className="p-3 text-[#0B2026]">{r.confidence != null ? r.confidence : "--"}</td>
                            </>
                          )}
                          {isReview && (
                            <>
                              <td className="p-3 text-[#0B2026]">{r.reviewer}</td>
                              <td className="p-3 text-[#0B2026]">{r.original_level_2}</td>
                              <td className="p-3 text-[#0B2026]">{r.reviewed_level_2}</td>
                              <td className="p-3 text-[#0B2026]">{r.review_status}</td>
                              <td className="p-3 text-[#0B2026] max-w-[150px] truncate">{r.comments}</td>
                            </>
                          )}
                        </tr>
                      );
                    })}
                  </tbody>
                </table>
              </div>
            )}
          </div>

          {/* Inline correction form */}
          <div className="rounded-lg border border-[#E5EBF0] bg-[#F7F9FA] p-6">
            <h3 className="font-semibold text-[#0B2026] mb-4">Submit corrections</h3>
            <p className="text-sm text-[#8CA0AC] mb-4">
              Select rows above, choose a new L2 category, add an optional comment, and submit.
            </p>
            <div className="flex flex-wrap gap-4 items-end">
              <div>
                <label className="block text-sm font-medium text-[#0B2026] mb-1">New L2 category</label>
                <select
                  value={newL2}
                  onChange={(e) => setNewL2(e.target.value)}
                  className="px-4 py-2 border border-[#E5EBF0] rounded-md text-[#0B2026] focus:outline-none focus:ring-2 focus:ring-[#FF3621]/50"
                >
                  <option value="">Select...</option>
                  {categories?.map((c) => (
                    <option key={c} value={c}>
                      {c}
                    </option>
                  ))}
                </select>
              </div>
              <div className="flex-1 min-w-[200px]">
                <label className="block text-sm font-medium text-[#0B2026] mb-1">Comment</label>
                <input
                  type="text"
                  value={comment}
                  onChange={(e) => setComment(e.target.value)}
                  placeholder="Optional comment"
                  className="w-full px-4 py-2 border border-[#E5EBF0] rounded-md text-[#0B2026] placeholder:text-[#8CA0AC] focus:outline-none focus:ring-2 focus:ring-[#FF3621]/50"
                />
              </div>
              <Button
                onClick={handleSubmitCorrections}
                disabled={selectedIds.size === 0 || !newL2 || submitReviews.isPending}
              >
                {submitReviews.isPending ? "Submitting..." : `Submit (${selectedIds.size} selected)`}
              </Button>
            </div>
          </div>
        </>
      )}

      {isPerformance && (
        <div className="space-y-8">
          <div className="flex items-center gap-4">
            <span className="text-sm font-medium text-[#0B2026]">Classification source:</span>
            {(["catboost", "bootstrap", "vectorsearch"] as const).map((s) => (
              <button
                key={s}
                onClick={() => setModelSource(s)}
                className={`px-3 py-1.5 rounded-md text-sm font-semibold transition-colors ${
                  modelSource === s
                    ? "bg-[#FF3621] text-white"
                    : "bg-[#F7F9FA] text-[#0B2026] hover:bg-[#E5EBF0]"
                }`}
              >
                {s === "catboost" ? "CatBoost" : s === "bootstrap" ? "Bootstrap" : "Vector Search"}
              </button>
            ))}
          </div>

          {activeAccuracy && (
            <>
              <div className="rounded-lg border border-[#E5EBF0] bg-white p-6">
                <div className="flex items-center justify-between mb-4">
                  <h3 className="font-semibold text-[#0B2026] text-lg">Overall Metrics</h3>
                  <span className="text-sm text-[#8CA0AC]">
                    {activeAccuracy.total_correct.toLocaleString()} / {activeAccuracy.total_classified.toLocaleString()} correct
                  </span>
                </div>
                <div className="grid grid-cols-4 gap-4">
                  {[
                    { label: "Accuracy", value: activeAccuracy.overall_accuracy },
                    { label: "Precision", value: activeAccuracy.overall_precision },
                    { label: "Recall", value: activeAccuracy.overall_recall },
                    { label: "F1 Score", value: activeAccuracy.overall_f1 },
                  ].map(({ label, value }) => (
                    <div key={label} className="rounded-lg border border-[#E5EBF0] p-4 text-center">
                      <p className="text-sm text-[#8CA0AC] flex items-center justify-center">
                        {label}
                        <InfoHover label={label} />
                      </p>
                      <p className="text-3xl font-bold text-[#0B2026] mt-1">{fmtPct(value)}</p>
                    </div>
                  ))}
                </div>
              </div>

              <div className="rounded-lg border border-[#E5EBF0] bg-white p-6">
                <h3 className="font-semibold text-[#0B2026] text-lg mb-4">
                  Top 10 Categories by Spend
                </h3>
                {activeAccuracy.categories.length > 0 ? (
                  <div className="overflow-x-auto">
                    <table className="w-full text-sm">
                      <thead className="bg-[#F7F9FA] border-b border-[#E5EBF0]">
                        <tr>
                          <th className="text-right p-3 font-semibold text-[#0B2026] w-[40%]">Category</th>
                          <th className="text-center p-3 font-semibold text-[#0B2026]">
                            <span className="flex items-center justify-center">Precision<InfoHover label="Precision" /></span>
                          </th>
                          <th className="text-center p-3 font-semibold text-[#0B2026]">
                            <span className="flex items-center justify-center">Recall<InfoHover label="Recall" /></span>
                          </th>
                          <th className="text-center p-3 font-semibold text-[#0B2026]">
                            <span className="flex items-center justify-center">F1<InfoHover label="F1 Score" /></span>
                          </th>
                          <th className="text-right p-3 font-semibold text-[#0B2026]">Spend</th>
                        </tr>
                      </thead>
                      <tbody>
                        {activeAccuracy.categories.map((cat) => (
                          <tr key={cat.category} className="border-b border-[#E5EBF0] hover:bg-[#F7F9FA]">
                            <td className="p-3 text-right font-medium text-[#0B2026]">{cat.category}</td>
                            <td className="p-3 text-center text-[#0B2026]">{cat.precision.toFixed(2)}</td>
                            <td className="p-3 text-center text-[#0B2026]">{cat.recall.toFixed(2)}</td>
                            <td className="p-3 text-center text-[#0B2026]">{cat.f1.toFixed(2)}</td>
                            <td className="p-3 text-right text-[#0B2026]">{fmtDollar(cat.spend)}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                ) : (
                  <p className="text-[#8CA0AC] py-4 text-center">No accuracy data available for this source.</p>
                )}
              </div>

              <div className="rounded-lg border border-[#E5EBF0] p-4 bg-white">
                <h3 className="font-semibold text-[#0B2026] mb-4">Model Coverage</h3>
                <div className="grid grid-cols-4 gap-4">
                  <div className="rounded border border-[#E5EBF0] p-4 text-center">
                    <p className="text-sm text-[#8CA0AC]">Bootstrap</p>
                    <p className="text-2xl font-bold text-[#FF3621]">{counts.bootstrap.toLocaleString()}</p>
                  </div>
                  <div className="rounded border border-[#E5EBF0] p-4 text-center">
                    <p className="text-sm text-[#8CA0AC]">CatBoost</p>
                    <p className="text-2xl font-bold text-[#2e6e8a]">{counts.catboost.toLocaleString()}</p>
                  </div>
                  <div className="rounded border border-[#E5EBF0] p-4 text-center">
                    <p className="text-sm text-[#8CA0AC]">Vector Search</p>
                    <p className="text-2xl font-bold text-[#3fa8c8]">{counts.vectorsearch.toLocaleString()}</p>
                  </div>
                  <div className="rounded border border-[#E5EBF0] p-4 text-center">
                    <p className="text-sm text-[#8CA0AC]">Human Reviews</p>
                    <p className="text-2xl font-bold text-[#22C55E]">{counts.reviews.toLocaleString()}</p>
                  </div>
                </div>
              </div>
            </>
          )}
          {!activeAccuracy && (
            <p className="text-[#8CA0AC]">Loading metrics...</p>
          )}
        </div>
      )}
    </div>
  );
}
