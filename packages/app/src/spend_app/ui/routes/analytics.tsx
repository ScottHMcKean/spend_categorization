import { apiFetch } from "@/lib/fetch";
import { SchemaBadge } from "@/components/apx/schema-selector";
import { useSchema } from "@/lib/schema-context";
import { createFileRoute } from "@tanstack/react-router";
import { useQuery } from "@tanstack/react-query";
import { useEffect, useState } from "react";
import Plot from "react-plotly.js";

export const Route = createFileRoute("/analytics")({
  component: Analytics,
});

interface AnalyticsSummary {
  total_spend: number;
  invoice_count: number;
  supplier_count: number;
  prediction_count: number;
  review_count: number;
  predictions_by_source: Record<string, unknown>[];
  spend_by_l1: Record<string, unknown>[];
  spend_by_l2: Record<string, unknown>[];
  monthly_trend: Record<string, unknown>[];
  top_suppliers: Record<string, unknown>[];
  region_category: Record<string, unknown>[];
}

function useAnalyticsSummary(schemaName: string, l1Filter: string) {
  return useQuery({
    queryKey: ["analytics", "summary", schemaName, l1Filter || "all"],
    queryFn: () => {
      const qs = new URLSearchParams({ schema_name: schemaName });
      if (l1Filter) qs.set("l1_filter", l1Filter);
      return apiFetch<AnalyticsSummary>(`/analytics/summary?${qs.toString()}`);
    },
  });
}

function useL1Options(schemaName: string) {
  // Fetch the unfiltered summary just to populate the L1 dropdown so
  // selecting an L1 doesn't shrink the available options.
  return useQuery({
    queryKey: ["analytics", "summary", schemaName, "all"],
    queryFn: () =>
      apiFetch<AnalyticsSummary>(`/analytics/summary?schema_name=${encodeURIComponent(schemaName)}`),
  });
}

function Analytics() {
  const { schemaName } = useSchema();
  const [l1Filter, setL1Filter] = useState<string>("");

  // Reset the L1 filter when the schema changes — stale L1s from another
  // taxonomy would zero-out every metric.
  useEffect(() => {
    setL1Filter("");
  }, [schemaName]);

  const { data: summary, isLoading } = useAnalyticsSummary(schemaName, l1Filter);
  const { data: optionsSummary } = useL1Options(schemaName);

  const l1Options = Array.from(
    new Set(
      (optionsSummary?.spend_by_l1 ?? [])
        .map((s) => String(s.category_level_1 ?? ""))
        .filter(Boolean)
    )
  );

  const treemapLabels: string[] = [];
  const treemapParents: string[] = [];
  const treemapValues: number[] = [];

  (summary?.spend_by_l2 ?? []).forEach((s) => {
    const l1 = String(s.category_level_1 ?? "Unknown");
    const l2 = String(s.category_level_2 ?? "Unknown");
    const total = Number(s.total ?? 0);
    if (l1Filter && l1 !== l1Filter) return;
    treemapLabels.push(l2);
    treemapParents.push(l1);
    treemapValues.push(total);
  });

  const monthlyData = summary?.monthly_trend ?? [];
  const topSuppliers = (summary?.top_suppliers ?? []).slice(0, 15);

  const plotlyLayout = {
    paper_bgcolor: "white",
    plot_bgcolor: "white",
    font: { family: "DM Sans", size: 12 },
    margin: { t: 20, b: 40, l: 60, r: 20 },
  };

  const fmtDollar = (v: number) =>
    new Intl.NumberFormat("en-US", {
      style: "currency",
      currency: "USD",
      minimumFractionDigits: 0,
      maximumFractionDigits: 0,
    }).format(v);

  return (
    <div className="max-w-7xl mx-auto px-6 py-8 font-sans flex gap-6">
      <aside className="w-56 flex-shrink-0">
        <div className="sticky top-20 space-y-4">
          <h3 className="font-semibold text-[#0B2026]">Filters</h3>
          <div>
            <label className="block text-sm font-medium text-[#0B2026] mb-1">
              Category L1
            </label>
            <select
              value={l1Filter}
              onChange={(e) => setL1Filter(e.target.value)}
              className="w-full px-3 py-2 border border-[#E5EBF0] rounded-md text-[#0B2026] focus:outline-none focus:ring-2 focus:ring-[#FF3621]/50"
            >
              <option value="">All</option>
              {l1Options.map((l) => (
                <option key={l} value={l}>
                  {l}
                </option>
              ))}
            </select>
          </div>
        </div>
      </aside>

      <div className="flex-1 min-w-0">
        <div className="flex flex-wrap items-baseline justify-between gap-3 mb-6">
          <h1 className="text-3xl font-bold text-[#0B2026]">Spend Analytics</h1>
          <SchemaBadge />
        </div>

        <div className="grid grid-cols-3 gap-4 mb-8">
          <div className="rounded-lg border border-[#E5EBF0] bg-white p-4">
            <p className="text-sm text-[#8CA0AC]">Total Spend</p>
            <p className="text-2xl font-bold text-[#0B2026]">
              {summary?.total_spend != null ? fmtDollar(summary.total_spend) : "-"}
            </p>
          </div>
          <div className="rounded-lg border border-[#E5EBF0] bg-white p-4">
            <p className="text-sm text-[#8CA0AC]">Invoices</p>
            <p className="text-2xl font-bold text-[#0B2026]">
              {summary?.invoice_count?.toLocaleString() ?? "-"}
            </p>
          </div>
          <div className="rounded-lg border border-[#E5EBF0] bg-white p-4">
            <p className="text-sm text-[#8CA0AC]">Unique Suppliers</p>
            <p className="text-2xl font-bold text-[#0B2026]">
              {summary?.supplier_count ?? "-"}
            </p>
          </div>
        </div>

        {isLoading && <p className="text-[#8CA0AC]">Loading...</p>}

        {!isLoading && (
          <div className="space-y-8">
            <div className="rounded-lg border border-[#E5EBF0] p-4 bg-white">
              <h3 className="font-semibold text-[#0B2026] mb-4">Spend by Category</h3>
              {treemapLabels.length > 0 ? (
                <Plot
                  data={[
                    {
                      type: "treemap",
                      labels: treemapLabels,
                      parents: treemapParents,
                      values: treemapValues,
                      marker: {
                        colors: ["#0B2026", "#FF3621", "#2e6e8a", "#3fa8c8", "#22C55E", "#8CA0AC"],
                      },
                    },
                  ]}
                  layout={{ ...plotlyLayout, height: 400 }}
                  config={{ responsive: true }}
                  useResizeHandler
                  className="w-full"
                />
              ) : (
                <p className="text-[#8CA0AC] py-8 text-center">No spend data available.</p>
              )}
            </div>

            <div className="rounded-lg border border-[#E5EBF0] p-4 bg-white">
              <h3 className="font-semibold text-[#0B2026] mb-4">Monthly Trend</h3>
              {monthlyData.length > 0 ? (
                <Plot
                  data={[
                    {
                      type: "scatter",
                      mode: "lines",
                      x: monthlyData.map((d) => String(d.month ?? "")),
                      y: monthlyData.map((d) => Number(d.total ?? 0)),
                      fill: "tozeroy",
                      line: { color: "#FF3621" },
                      fillcolor: "rgba(255, 54, 33, 0.2)",
                    },
                  ]}
                  layout={{
                    ...plotlyLayout,
                    height: 300,
                    xaxis: { title: { text: "Month" } },
                    yaxis: { title: { text: "Spend" } },
                  }}
                  config={{ responsive: true }}
                  useResizeHandler
                  className="w-full"
                />
              ) : (
                <p className="text-[#8CA0AC] py-8 text-center">No trend data available.</p>
              )}
            </div>

            <div className="rounded-lg border border-[#E5EBF0] p-4 bg-white">
              <h3 className="font-semibold text-[#0B2026] mb-4">Top 15 Suppliers</h3>
              {topSuppliers.length > 0 ? (
                <Plot
                  data={[
                    {
                      type: "bar",
                      x: topSuppliers.map((s) => Number(s.total ?? 0)),
                      y: topSuppliers.map((s) => String(s.supplier ?? "")),
                      orientation: "h" as const,
                      marker: { color: "#2e6e8a" },
                    },
                  ]}
                  layout={{
                    ...plotlyLayout,
                    height: 400,
                    xaxis: { title: { text: "Spend" } },
                    yaxis: { autorange: "reversed" as const },
                  }}
                  config={{ responsive: true }}
                  useResizeHandler
                  className="w-full"
                />
              ) : (
                <p className="text-[#8CA0AC] py-8 text-center">No supplier data available.</p>
              )}
            </div>

            <div className="rounded-lg border border-[#E5EBF0] p-4 bg-white">
              <h3 className="font-semibold text-[#0B2026] mb-4">Region x Category</h3>
              {(summary?.region_category ?? []).length > 0 ? (
                (() => {
                  const rc = summary?.region_category ?? [];
                  const regions = Array.from(new Set(rc.map((r) => String(r.region ?? "")).filter(Boolean)));
                  const cats = Array.from(new Set(rc.map((r) => String(r.category_level_1 ?? "")).filter(Boolean)));
                  const z = regions.map((reg) =>
                    cats.map((cat) => Number(rc.find((r) => String(r.region) === reg && String(r.category_level_1) === cat)?.total ?? 0))
                  );
                  return (
                    <Plot
                      data={[{ type: "heatmap", x: cats, y: regions, z, colorscale: [[0, "#F7F9FA"], [0.5, "#3fa8c8"], [1, "#0B2026"]] }]}
                      layout={{ ...plotlyLayout, height: 300 }}
                      config={{ responsive: true }}
                      useResizeHandler
                      className="w-full"
                    />
                  );
                })()
              ) : (
                <p className="text-[#8CA0AC] py-8 text-center">No regional data available.</p>
              )}
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
