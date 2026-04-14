import { createFileRoute } from "@tanstack/react-router";

export const Route = createFileRoute("/comparison")({
  component: Comparison,
});

const STRENGTHS = [
  {
    title: "Delta Lake ACID",
    description:
      "Delta Lake provides ACID transactions on your data lake, enabling reliable concurrent writes and time travel. Your procurement data stays consistent and auditable across the pipeline.",
  },
  {
    title: "Unity Catalog Governance",
    description:
      "Unity Catalog delivers fine-grained access control, lineage tracking, and data discovery. Classifications and spend data are governed end-to-end with column-level security.",
  },
  {
    title: "Foundation Models (AI_QUERY)",
    description:
      "Leverage Databricks Foundation Model APIs for zero-shot classification and embedding generation. Bootstrap labels and vector search run natively on the platform without managing model endpoints.",
  },
  {
    title: "MLflow Experiment Tracking",
    description:
      "Track CatBoost training runs, hyperparameters, and metrics in MLflow. Compare model versions and promote the best performers to production with a single click.",
  },
  {
    title: "Vector Search RAG",
    description:
      "Databricks Vector Search indexes embeddings for semantic similarity. Find similar invoices and propagate labels from existing classifications without retraining.",
  },
  {
    title: "Databricks Jobs Orchestration",
    description:
      "Schedule and orchestrate the full pipeline—ingestion, bootstrap, training, indexing, and sync—with Databricks Jobs. Dependencies, retries, and alerts are built in.",
  },
];

const FUTURE_PROJECTS = [
  {
    title: "Real-time streaming classification",
    description:
      "Classify invoices as they arrive via Kafka or Event Hubs. Low-latency predictions with streaming tables and model serving.",
  },
  {
    title: "Multi-language NLP",
    description:
      "Extend the pipeline to handle invoices in multiple languages using multilingual embeddings and translation models.",
  },
  {
    title: "Supplier risk scoring",
    description:
      "Combine spend data with external signals to score supplier risk. Alert on concentration, payment delays, and compliance gaps.",
  },
  {
    title: "Automated PO matching",
    description:
      "Match purchase orders to invoices automatically. Reduce manual reconciliation and flag discrepancies for review.",
  },
];

function Comparison() {
  return (
    <div className="max-w-7xl mx-auto px-6 py-8 font-sans">
      <h1 className="text-4xl font-bold text-[#0B2026] mb-12">Why Databricks?</h1>

      {/* Strength cards */}
      <section className="mb-16">
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
          {STRENGTHS.map((s) => (
            <div
              key={s.title}
              className="rounded-lg border border-[#E5EBF0] bg-white p-6 hover:border-[#FF3621]/50 transition-colors"
            >
              <h3 className="font-semibold text-[#0B2026] text-lg mb-2">{s.title}</h3>
              <p className="text-[#8CA0AC] text-sm leading-relaxed">{s.description}</p>
            </div>
          ))}
        </div>
      </section>

      {/* Future Projects */}
      <section className="mb-16">
        <h2 className="text-2xl font-bold text-[#0B2026] mb-6">Future Projects</h2>
        <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
          {FUTURE_PROJECTS.map((p) => (
            <div
              key={p.title}
              className="rounded-lg border border-[#E5EBF0] bg-[#F7F9FA] p-6"
            >
              <h3 className="font-semibold text-[#0B2026] mb-2">{p.title}</h3>
              <p className="text-[#8CA0AC] text-sm leading-relaxed">{p.description}</p>
            </div>
          ))}
        </div>
      </section>

      {/* TCO comparison */}
      <section>
        <h2 className="text-2xl font-bold text-[#0B2026] mb-6">TCO Comparison</h2>
        <div className="rounded-lg border border-[#E5EBF0] bg-white overflow-hidden">
          <table className="w-full text-sm">
            <thead className="bg-[#F7F9FA] border-b border-[#E5EBF0]">
              <tr>
                <th className="text-left p-4 font-semibold text-[#0B2026]">Factor</th>
                <th className="text-left p-4 font-semibold text-[#0B2026]">Traditional Stack</th>
                <th className="text-left p-4 font-semibold text-[#0B2026]">Databricks</th>
              </tr>
            </thead>
            <tbody>
              <tr className="border-b border-[#E5EBF0]">
                <td className="p-4 text-[#0B2026]">Data lake + warehouse</td>
                <td className="p-4 text-[#8CA0AC]">Separate systems, ETL between them</td>
                <td className="p-4 text-[#0B2026]">Unified lakehouse, single copy of data</td>
              </tr>
              <tr className="border-b border-[#E5EBF0]">
                <td className="p-4 text-[#0B2026]">ML / AI infrastructure</td>
                <td className="p-4 text-[#8CA0AC]">Standalone ML platform, data movement</td>
                <td className="p-4 text-[#0B2026]">Native MLflow, models where data lives</td>
              </tr>
              <tr className="border-b border-[#E5EBF0]">
                <td className="p-4 text-[#0B2026]">Governance & security</td>
                <td className="p-4 text-[#8CA0AC]">Point solutions, fragmented policies</td>
                <td className="p-4 text-[#0B2026]">Unity Catalog, centralized governance</td>
              </tr>
              <tr>
                <td className="p-4 text-[#0B2026]">Operational overhead</td>
                <td className="p-4 text-[#8CA0AC]">Multiple vendors, complex integrations</td>
                <td className="p-4 text-[#0B2026]">Single platform, serverless options</td>
              </tr>
            </tbody>
          </table>
        </div>
      </section>
    </div>
  );
}
