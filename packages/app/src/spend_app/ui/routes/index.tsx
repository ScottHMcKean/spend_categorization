import { apiFetch } from "@/lib/fetch";
import { useSchema } from "@/lib/schema-context";
import { SchemaBadge } from "@/components/apx/schema-selector";
import { createFileRoute, Link } from "@tanstack/react-router";
import { useQuery } from "@tanstack/react-query";
import { useMemo } from "react";
import Plot from "react-plotly.js";

export const Route = createFileRoute("/")({
  component: Index,
});

interface TableStatus {
  table: string;
  description: string;
  rows: number;
  columns: number;
  column_names: string;
  status: "ok" | "empty" | "missing";
}

interface StatusResponse {
  tables: TableStatus[];
  backend: string;
  catalog: string;
  schema_name: string;
}

interface AnalyticsSummary {
  total_spend: number;
  invoice_count: number;
  prediction_count: number;
  review_count: number;
  predictions_by_source: { source: string; count: number }[];
}

function useStatus() {
  return useQuery({
    queryKey: ["status"],
    queryFn: () => apiFetch<StatusResponse>("/status"),
  });
}

function useSummary(schemaName: string) {
  return useQuery({
    queryKey: ["analytics", "summary", schemaName, "sankey"],
    queryFn: () =>
      apiFetch<AnalyticsSummary>(`/analytics/summary?schema_name=${encodeURIComponent(schemaName)}`),
    enabled: !!schemaName,
  });
}

const SOURCE_COLORS: Record<string, string> = {
  ai_classify: "#FF3621",
  ai_classify_chat: "#FF7A56",
  catboost: "#2e6e8a",
  pgvector: "#3fa8c8",
  tsvector: "#3fa8c8",
  agent_review: "#22C55E",
  ai_query_bootstrap: "#FF3621",
};

/**
 * Schema-aware Sankey:
 *   Invoices ─┬─► <source 1> (n)
 *             ├─► <source 2> (n)
 *             └─► <source N> (n)        ─► cat_predictions (sum) ─► Reviewed
 *                                                                ▲
 *                                                Human Reviews (n)┘
 */
function buildSankey(
  invoices: number,
  reviews: number,
  bySource: { source: string; count: number }[],
) {
  const sources = bySource.filter((s) => s.count > 0).sort((a, b) => b.count - a.count);
  const totalPreds = sources.reduce((acc, s) => acc + s.count, 0);

  const invoicesIdx = 0;
  const predIdx = 1 + sources.length;
  const reviewsIdx = predIdx + 1;
  const reviewedIdx = predIdx + 2;

  const labels: string[] = [
    `Invoices (${invoices.toLocaleString()})`,
    ...sources.map((s) => `${s.source} (${s.count.toLocaleString()})`),
    `cat_predictions (${totalPreds.toLocaleString()})`,
    `Human Reviews (${reviews.toLocaleString()})`,
    "Reviewed Spend",
  ];
  const colors: string[] = [
    "#0B2026",
    ...sources.map((s) => SOURCE_COLORS[s.source] ?? "#8CA0AC"),
    "#FF3621",
    "#22C55E",
    "#0B2026",
  ];

  const source: number[] = [];
  const target: number[] = [];
  const value: number[] = [];

  sources.forEach((s, i) => {
    source.push(invoicesIdx); target.push(1 + i); value.push(s.count);
    source.push(1 + i); target.push(predIdx); value.push(s.count);
  });
  if (sources.length === 0) {
    source.push(invoicesIdx); target.push(predIdx); value.push(1);
  }
  source.push(predIdx); target.push(reviewedIdx); value.push(totalPreds || 1);
  source.push(reviewsIdx); target.push(reviewedIdx); value.push(reviews || 1);

  return { labels, colors, source, target, value };
}

function Index() {
  const { data: status, isLoading, error } = useStatus();
  const { schemaName, current } = useSchema();
  const { data: summary } = useSummary(schemaName);
  const sankey = useMemo(() => {
    if (!status) return null;
    const invoices = status.tables.find((t) => t.table === "invoices")?.rows ?? 0;
    const reviews = status.tables.find((t) => t.table === "cat_reviews")?.rows ?? 0;
    const bySource = (summary?.predictions_by_source ?? []) as { source: string; count: number }[];
    return buildSankey(invoices, reviews, bySource);
  }, [status, summary]);

  return (
    <div className="font-sans">
      {/* Hero */}
      <section className="bg-[#0B2026] text-white py-16 px-6">
        <div className="max-w-7xl mx-auto text-center">
          <h1 className="text-4xl md:text-5xl font-bold mb-4">
            Classify procurement spend with Databricks
          </h1>
          <p className="text-lg text-[#8CA0AC] max-w-2xl mx-auto">
            A unified pipeline combining LLM bootstrap, CatBoost models, Vector Search RAG, and human review to categorize spend across your organization.
          </p>
        </div>
      </section>

      {/* Sankey */}
      <section className="py-12 px-6 bg-white">
        <div className="max-w-7xl mx-auto">
          <div className="flex flex-wrap items-baseline justify-between gap-3 mb-6">
            <div>
              <h2 className="text-2xl font-bold text-[#0B2026]">Pipeline Flow</h2>
              <p className="text-sm text-[#8CA0AC]">
                Sources contributing to <strong>{current?.display_name ?? schemaName}</strong> predictions
              </p>
            </div>
            <SchemaBadge />
          </div>
          <div className="bg-white rounded-lg border border-[#E5EBF0] p-4">
            {sankey ? (
              <Plot
                data={[
                  {
                    type: "sankey",
                    node: {
                      pad: 15,
                      thickness: 20,
                      line: { color: "#E5EBF0", width: 0.5 },
                      label: sankey.labels,
                      color: sankey.colors,
                    },
                    link: {
                      source: sankey.source,
                      target: sankey.target,
                      value: sankey.value,
                      color: "rgba(139, 160, 172, 0.3)",
                    },
                  },
                ]}
                layout={{
                  paper_bgcolor: "white",
                  plot_bgcolor: "white",
                  font: { family: "DM Sans", size: 12 },
                  margin: { t: 20, b: 20, l: 20, r: 20 },
                  height: 400,
                }}
                config={{ responsive: true }}
                useResizeHandler
                className="w-full"
              />
            ) : (
              <p className="text-[#8CA0AC] p-6">Loading pipeline data...</p>
            )}
          </div>
        </div>
      </section>

      {/* Table Status */}
      <section className="py-12 px-6 bg-[#F7F9FA]">
        <div className="max-w-7xl mx-auto">
          <h2 className="text-2xl font-bold text-[#0B2026] mb-6">Table Status</h2>
          {isLoading && <p className="text-[#8CA0AC]">Loading...</p>}
          {error && (
            <p className="text-[#FF3621]">Failed to load status: {(error as Error).message}</p>
          )}
          {status && (
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
              {status.tables.map((t) => (
                <div
                  key={t.table}
                  className="bg-white rounded-lg border border-[#E5EBF0] p-4 flex items-center justify-between"
                >
                  <div>
                    <p className="font-semibold text-[#0B2026]">{t.table}</p>
                    <p className="text-xs text-[#8CA0AC] mb-1">{t.description}</p>
                    <p className="text-sm text-[#8CA0AC]">
                      {t.rows.toLocaleString()} rows · {t.columns} cols
                    </p>
                  </div>
                  <span
                    className={`px-2 py-1 rounded text-xs font-medium ${
                      t.status === "ok"
                        ? "bg-[#22C55E]/20 text-[#22C55E]"
                        : t.status === "empty"
                          ? "bg-amber-200 text-amber-800"
                          : "bg-[#FF3621]/20 text-[#FF3621]"
                    }`}
                  >
                    {t.status}
                  </span>
                </div>
              ))}
            </div>
          )}
          {status && (
            <p className="mt-4 text-sm text-[#8CA0AC]">
              Backend: {status.backend} · Catalog: {status.catalog} · Schema: {status.schema_name}
            </p>
          )}
        </div>
      </section>

      {/* How It Works */}
      <section className="py-12 px-6 bg-white">
        <div className="max-w-7xl mx-auto">
          <h2 className="text-2xl font-bold text-[#0B2026] mb-8">How It Works</h2>
          <ol className="space-y-6">
            {[
              {
                step: 0,
                title: "Define taxonomies",
                desc: "Register one or more classification schemas (3-level Direct/Indirect, GL accounts, UNSPSC). Each schema lands as a normalized taxonomy table; the app dropdown discovers them automatically.",
              },
              {
                step: 1,
                title: "Ingest raw invoices",
                desc: "Raw procurement data is loaded into staging tables with supplier, description, and amount fields.",
              },
              {
                step: 2,
                title: "LLM bootstrap labels",
                desc: "Hierarchical ai_classify generates initial category labels per schema and writes them to the unified cat_predictions table.",
              },
              {
                step: 3,
                title: "Train CatBoost and index for Vector Search",
                desc: "CatBoost learns from bootstrap labels. Lakebase pgvector + tsvector hybrid search powers UNSPSC's 150k commodity codes.",
              },
              {
                step: 4,
                title: "Human review low-confidence predictions",
                desc: "Invoices flagged with low confidence are queued for an agent reviewer and human review. Corrections feed back into the model.",
              },
              {
                step: 5,
                title: "Classified spend analytics",
                desc: "Final categorized spend is available for reporting, dashboards, and downstream analytics through vw_spend_by_<schema> views.",
              },
            ].map(({ step, title, desc }) => (
              <li key={step} className="flex gap-4">
                <span className="flex-shrink-0 w-8 h-8 rounded-full bg-[#FF3621] text-white font-bold flex items-center justify-center text-sm">
                  {step}
                </span>
                <div>
                  <h3 className="font-semibold text-[#0B2026]">{title}</h3>
                  <p className="text-[#8CA0AC]"> {desc}</p>
                </div>
              </li>
            ))}
          </ol>
        </div>
      </section>

      {/* Navigation Cards */}
      <section className="py-12 px-6 bg-[#F7F9FA]">
        <div className="max-w-7xl mx-auto">
          <h2 className="text-2xl font-bold text-[#0B2026] mb-6">Explore</h2>
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
            <Link to="/classifications">
              <div className="bg-white rounded-lg border border-[#E5EBF0] p-6 hover:border-[#FF3621] transition-colors cursor-pointer">
                <h3 className="font-semibold text-[#0B2026] mb-2">Classifications</h3>
                <p className="text-sm text-[#8CA0AC]">
                  Browse classifications, model performance, and submit corrections
                </p>
              </div>
            </Link>
            <Link to="/review">
              <div className="bg-white rounded-lg border border-[#E5EBF0] p-6 hover:border-[#FF3621] transition-colors cursor-pointer">
                <h3 className="font-semibold text-[#0B2026] mb-2">Spend Review</h3>
                <p className="text-sm text-[#8CA0AC]">
                  Search, flag, and review low-confidence invoices
                </p>
              </div>
            </Link>
            <Link to="/analytics">
              <div className="bg-white rounded-lg border border-[#E5EBF0] p-6 hover:border-[#FF3621] transition-colors cursor-pointer">
                <h3 className="font-semibold text-[#0B2026] mb-2">Spend Analytics</h3>
                <p className="text-sm text-[#8CA0AC]">
                  Spend breakdowns, trends, top suppliers, and regional analysis
                </p>
              </div>
            </Link>
            <Link to="/comparison">
              <div className="bg-white rounded-lg border border-[#E5EBF0] p-6 hover:border-[#FF3621] transition-colors cursor-pointer">
                <h3 className="font-semibold text-[#0B2026] mb-2">Walkthrough</h3>
                <p className="text-sm text-[#8CA0AC]">
                  Architecture, pipeline stages, and how each schema is classified
                </p>
              </div>
            </Link>
          </div>
        </div>
      </section>
    </div>
  );
}
