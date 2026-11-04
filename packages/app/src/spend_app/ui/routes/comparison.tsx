import { createFileRoute } from "@tanstack/react-router";

export const Route = createFileRoute("/comparison")({
  component: Platform,
});

const PIPELINE_STAGES = [
  {
    n: 0,
    nb: "0_define_taxonomies",
    title: "Define taxonomies",
    description:
      "Each schema (three_level, gl_map, unspsc) loads its raw asset (CSV/Excel) via a pluggable ClassificationSchema subclass and emits a normalized {code, label, level_path[], description} table. The schema_registry table records every active taxonomy so the app dropdown discovers them automatically.",
  },
  {
    n: 1,
    nb: "1_ingest",
    title: "Ingest invoices",
    description:
      "Synthetic / source invoices land in shm.spend.invoices with supplier, description, cost_centre, total. One Delta table powers all schemas — invoice rows are never duplicated per taxonomy.",
  },
  {
    n: 2,
    nb: "2_embed",
    title: "Embed examples into pgvector",
    description:
      "For every schema, embeds taxonomy leaves and labelled ground-truth invoices — through Unity Gateway — into one Lakebase pgvector table (examples). This is the single store the LLM classifier retrieves against. Very large taxonomies (UNSPSC) skip taxonomy embeddings and rely on ground truth once it exists.",
  },
  {
    n: 3,
    nb: "3_classify",
    title: "Classify (LLM + pgvector)",
    description:
      "One method for every schema: embed the invoice, retrieve the most similar examples from pgvector, and ask the LLM (via Unity Gateway) to pick a code from those candidates. Scales to any taxonomy size — the model only ever sees K candidates, never the whole tree. Each row lands in the unified predictions table with source='llm', a 0–1 confidence, a rationale, a reason, and an MLflow trace_id.",
  },
  {
    n: 4,
    nb: "4_train_catboost",
    title: "Train CatBoost (optional)",
    description:
      "Trains a CatBoost classifier on the ground_truth table and predicts every invoice, writing source='catboost' rows with predict_proba confidence. The supervised half of the feedback loop — it gets better as ground truth grows, and is skipped cleanly when there isn't enough yet.",
  },
  {
    n: 5,
    nb: "5_analytics",
    title: "Classified spend analytics",
    description:
      "Categorized spend rolls up into one view per schema (vw_spend_by_<schema>). With a unified 0–1 confidence column, the views rank the llm and catboost sources with a single ORDER BY. The app's /analytics/summary endpoint joins predictions to invoices for schema-aware spend-by-L1/L2, monthly trend, top suppliers, and the home-page Sankey.",
  },
  {
    n: 6,
    nb: "6_evaluate + 7_sync",
    title: "Score and serve",
    description:
      "6_evaluate prints the scorecard — coverage and accuracy vs ground_truth, plus the reason breakdown that tells you which tuning lever to pull. 7_sync mirrors the Delta tables into Lakebase (synced read tables + a native writable reviews table) so the app serves fresh data.",
  },
];

const SCHEMAS = [
  {
    name: "three_level",
    leaves: "102",
    method: "LLM + pgvector",
    notes:
      "Internal Direct/Indirect 3-level hierarchy. Taxonomy leaves are embedded, so it classifies from cold start.",
  },
  {
    name: "gl_map",
    leaves: "292",
    method: "LLM + pgvector",
    notes:
      "GL chart of accounts. Taxonomy leaves embedded; ground-truth examples sharpen it over time. Asset: assets/new_gl_map.xlsx.",
  },
  {
    name: "unspsc",
    leaves: "~150k",
    method: "LLM + pgvector (ground truth)",
    notes:
      "UN UNSPSC commodity codes. Too large to embed every leaf, so it relies on ground-truth examples — seed some and it starts classifying.",
  },
];

const SOURCES = [
  { source: "llm", schemas: "all schemas", confidence: "model self-reported 1–5 (normalized 0–1)" },
  { source: "catboost", schemas: "any with ground truth (optional)", confidence: "predict_proba.max() per row" },
];

const STORAGE = [
  {
    layer: "Unity Catalog (Delta)",
    contents:
      "shm.spend.{invoices, predictions, reviews, ground_truth, schema_registry, three_level_taxonomy, gl_map_taxonomy, unspsc_taxonomy}",
    why: "Source of truth — ACID writes, time travel, lineage, governance. Notebooks read/write here.",
  },
  {
    layer: "Lakebase (Postgres) — synced",
    contents: "*_synced tables (SNAPSHOT mode) for invoices, predictions, ground_truth, taxonomies, schema_registry",
    why: "Low-latency reads for the React app. ms-level lookups vs. warehouse round-trips.",
  },
  {
    layer: "Lakebase (Postgres) — native",
    contents: "spend.reviews",
    why: "Writable Postgres table for human review events. App POSTs straight to it.",
  },
  {
    layer: "Lakebase (Postgres) — pgvector",
    contents: "spend.examples (embedding column, HNSW index)",
    why: "The retrieval store for the LLM classifier — taxonomy leaves + ground-truth invoices, searched at request time.",
  },
];

const APP_STACK = [
  {
    layer: "Frontend",
    tech: "React 19 + TanStack Router + TanStack Query + Plotly + Tailwind",
    notes:
      "Schema selector in shared context; review queue persisted in localStorage so Classifications can hand items off to Spend Review. Type-checked TypeScript bundle served from /__dist__.",
  },
  {
    layer: "Backend",
    tech: "FastAPI + uvicorn (Databricks Apps)",
    notes:
      "Mounted at /api. Lifespan reads tables from Lakebase into pandas at startup; reload via /api/data/reload. Human review events POST to the native reviews table.",
  },
  {
    layer: "LLM + embeddings",
    tech: "Unity Gateway (databricks-claude-sonnet-4-5, databricks-gte-large-en)",
    notes:
      "All chat and embedding calls route through Unity Gateway via src.llm — governed, logged, auditable. Classification is wrapped in MLflow tracing so every prediction carries a trace_id.",
  },
];

const BENEFITS = [
  {
    title: "Unified prediction table",
    body: "Both classifiers — llm and catboost — land in predictions keyed on (order_id, schema_name, source), with status/reason/rationale/trace_id on every row. One MERGE, one query surface, fully auditable.",
  },
  {
    title: "Pluggable schemas",
    body: "Adding a new taxonomy = subclass ClassificationSchema, point at an asset, register it. The app dropdown, the agent, and the analytics views pick it up automatically.",
  },
  {
    title: "Confidence everywhere",
    body: "Each strategy emits a confidence in the same 0–1 column. The UI bars, low-confidence flagging, and human-review queue all read one signal regardless of how the prediction was made.",
  },
  {
    title: "Explainable by construction",
    body: "Every prediction records why: a reason code (ok / low_confidence / no_candidates / out_of_taxonomy), the model's rationale, the retrieved candidates, and an MLflow trace_id you can open to see the full call.",
  },
  {
    title: "Lakebase for app latency",
    body: "Synced Postgres cuts the read path to single-digit ms vs. warehouse cold starts. Writable native tables mean the app POSTs reviews without round-tripping through ETL.",
  },
  {
    title: "Asset-bundle deploy",
    body: "Notebooks, jobs, app source, and Postgres instance shipped as one Databricks Asset Bundle — `databricks bundle deploy` provisions everything.",
  },
];

const FUTURE = [
  {
    title: "Multi-schema agent reconciliation",
    body: "When three_level and UNSPSC disagree on the same invoice, run a second agent that proposes a unified mapping and flags the schema with the weaker confidence.",
  },
  {
    title: "Active learning loop",
    body: "Corrections append to reviews and update ground_truth; re-embed + retrain CatBoost nightly so both classifiers improve. Track accuracy lift with the scorecard and MLflow.",
  },
  {
    title: "Real-time streaming classification",
    body: "Replace the batch ingest with Auto Loader + Lakeflow pipelines. The LLM + pgvector classifier runs per-record; new ground truth re-embeds incrementally.",
  },
  {
    title: "Genie / NL exploration",
    body: "Wire Databricks Genie on top of the analytics views so procurement leaders can ask spend questions in natural language.",
  },
];

function Platform() {
  return (
    <div className="max-w-7xl mx-auto px-6 py-8 font-sans space-y-16">
      <header>
        <h1 className="text-4xl font-bold text-[#0B2026] mb-3">Walkthrough</h1>
        <p className="text-[#8CA0AC] max-w-3xl leading-relaxed">
          How this app actually works. Seven pipeline notebooks, three pluggable schemas,
          one unified prediction table, a retrieval-augmented LLM strategy for large
          taxonomies, and a generic agent reviewer that works for any schema depth.
        </p>
      </header>

      {/* Pipeline */}
      <section>
        <h2 className="text-2xl font-bold text-[#0B2026] mb-1">Pipeline</h2>
        <p className="text-sm text-[#8CA0AC] mb-6">
          notebooks/ runs as a Databricks Job (resources/jobs.yml). Each task is one
          notebook; downstream tasks depend on the prior task's success.
        </p>
        <ol className="space-y-3">
          {PIPELINE_STAGES.map(({ n, nb, title, description }) => (
            <li
              key={n}
              className="flex gap-4 rounded-lg border border-[#E5EBF0] bg-white p-5"
            >
              <span className="flex-shrink-0 w-8 h-8 rounded-full bg-[#FF3621] text-white font-bold flex items-center justify-center text-sm">
                {n}
              </span>
              <div>
                <h3 className="font-semibold text-[#0B2026]">
                  {title}
                  <code className="ml-2 text-xs text-[#8CA0AC] font-mono">
                    {nb}.ipynb
                  </code>
                </h3>
                <p className="text-sm text-[#8CA0AC] leading-relaxed mt-1">
                  {description}
                </p>
              </div>
            </li>
          ))}
        </ol>
      </section>

      {/* Schemas */}
      <section>
        <h2 className="text-2xl font-bold text-[#0B2026] mb-1">Schemas</h2>
        <p className="text-sm text-[#8CA0AC] mb-6">
          Three taxonomies live side-by-side; the active one is chosen via the
          schema dropdown in the header.
        </p>
        <div className="rounded-lg border border-[#E5EBF0] bg-white overflow-hidden">
          <table className="w-full text-sm">
            <thead className="bg-[#F7F9FA] border-b border-[#E5EBF0]">
              <tr>
                <th className="text-left p-4 font-semibold text-[#0B2026]">name</th>
                <th className="text-left p-4 font-semibold text-[#0B2026]">leaves</th>
                <th className="text-left p-4 font-semibold text-[#0B2026]">classify method</th>
                <th className="text-left p-4 font-semibold text-[#0B2026]">notes</th>
              </tr>
            </thead>
            <tbody>
              {SCHEMAS.map((s) => (
                <tr key={s.name} className="border-b border-[#E5EBF0] last:border-0 align-top">
                  <td className="p-4 font-mono text-[#FF3621]">{s.name}</td>
                  <td className="p-4 font-mono text-[#0B2026]">{s.leaves}</td>
                  <td className="p-4 text-[#0B2026]">{s.method}</td>
                  <td className="p-4 text-[#8CA0AC]">{s.notes}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </section>

      {/* Sources / confidence */}
      <section>
        <h2 className="text-2xl font-bold text-[#0B2026] mb-1">
          Prediction sources
        </h2>
        <p className="text-sm text-[#8CA0AC] mb-6">
          Every row in <code>predictions</code> tags its source. The
          confidence column is normalized to 0–1 so the UI bars work uniformly.
        </p>
        <div className="rounded-lg border border-[#E5EBF0] bg-white overflow-hidden">
          <table className="w-full text-sm">
            <thead className="bg-[#F7F9FA] border-b border-[#E5EBF0]">
              <tr>
                <th className="text-left p-4 font-semibold text-[#0B2026]">source</th>
                <th className="text-left p-4 font-semibold text-[#0B2026]">used by</th>
                <th className="text-left p-4 font-semibold text-[#0B2026]">confidence</th>
              </tr>
            </thead>
            <tbody>
              {SOURCES.map((s) => (
                <tr key={s.source} className="border-b border-[#E5EBF0] last:border-0">
                  <td className="p-4 font-mono text-[#FF3621]">{s.source}</td>
                  <td className="p-4 font-mono text-[#0B2026]">{s.schemas}</td>
                  <td className="p-4 text-[#8CA0AC]">{s.confidence}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </section>

      {/* Storage */}
      <section>
        <h2 className="text-2xl font-bold text-[#0B2026] mb-1">Storage layout</h2>
        <p className="text-sm text-[#8CA0AC] mb-6">
          Delta is the source of truth; Lakebase Postgres is the app-facing copy.
        </p>
        <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
          {STORAGE.map((s) => (
            <div
              key={s.layer}
              className="rounded-lg border border-[#E5EBF0] bg-white p-5"
            >
              <h3 className="font-semibold text-[#0B2026]">{s.layer}</h3>
              <p className="text-xs font-mono text-[#FF3621] mt-1 break-all">
                {s.contents}
              </p>
              <p className="text-sm text-[#8CA0AC] leading-relaxed mt-2">{s.why}</p>
            </div>
          ))}
        </div>
      </section>

      {/* App stack */}
      <section>
        <h2 className="text-2xl font-bold text-[#0B2026] mb-1">App stack</h2>
        <p className="text-sm text-[#8CA0AC] mb-6">
          Built and deployed as a Databricks App via{" "}
          <code>databricks bundle deploy</code>. apx wraps the React+FastAPI build.
        </p>
        <div className="space-y-3">
          {APP_STACK.map((s) => (
            <div
              key={s.layer}
              className="rounded-lg border border-[#E5EBF0] bg-white p-5 grid grid-cols-1 md:grid-cols-[160px_1fr] gap-4"
            >
              <div>
                <p className="text-xs uppercase tracking-wide text-[#8CA0AC]">
                  {s.layer}
                </p>
                <p className="font-semibold text-[#0B2026] mt-1">{s.tech}</p>
              </div>
              <p className="text-sm text-[#8CA0AC] leading-relaxed">{s.notes}</p>
            </div>
          ))}
        </div>
      </section>

      {/* Why Databricks */}
      <section>
        <h2 className="text-2xl font-bold text-[#0B2026] mb-1">
          What this gets you
        </h2>
        <p className="text-sm text-[#8CA0AC] mb-6">
          Concrete properties of the architecture above, not generic platform pitches.
        </p>
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
          {BENEFITS.map((b) => (
            <div
              key={b.title}
              className="rounded-lg border border-[#E5EBF0] bg-white p-5 hover:border-[#FF3621]/50 transition-colors"
            >
              <h3 className="font-semibold text-[#0B2026] mb-2">{b.title}</h3>
              <p className="text-sm text-[#8CA0AC] leading-relaxed">{b.body}</p>
            </div>
          ))}
        </div>
      </section>

      {/* Future */}
      <section>
        <h2 className="text-2xl font-bold text-[#0B2026] mb-1">Roadmap</h2>
        <p className="text-sm text-[#8CA0AC] mb-6">Things this codebase is set up for.</p>
        <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
          {FUTURE.map((f) => (
            <div
              key={f.title}
              className="rounded-lg border border-[#E5EBF0] bg-[#F7F9FA] p-5"
            >
              <h3 className="font-semibold text-[#0B2026] mb-2">{f.title}</h3>
              <p className="text-sm text-[#8CA0AC] leading-relaxed">{f.body}</p>
            </div>
          ))}
        </div>
      </section>
    </div>
  );
}
