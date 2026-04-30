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
    nb: "2_bootstrap",
    title: "LLM bootstrap labels",
    description:
      "three_level uses ai_query with a STRUCT<l1, l2, confidence> response parsed via from_json — single SQL call per row. gl_map uses parallel chat completions through src.llm.chat_with_retry (full-jitter exponential backoff, retry-after honored). Both write into the unified cat_predictions table keyed on (order_id, schema_name, source).",
  },
  {
    n: 3,
    nb: "3_train_and_index",
    title: "Train CatBoost & index for vector search",
    description:
      "CatBoost classifier learns from the bootstrap labels and writes back per-row predict_proba as confidence. UNSPSC's 150k commodity codes are indexed in Lakebase Postgres with a tsvector column; classification is OR-token full-text search ranked by ts_rank. pgvector hybrid retrieval is available behind a flag.",
  },
  {
    n: 4,
    nb: "4_review",
    title: "Human + agent review",
    description:
      "Low-confidence predictions are queued for review. An LLM agent (TaxonomyReviewAgent) walks the active taxonomy with five generic tools — list_root_categories, list_children, search_taxonomy, get_leaf, validate_path — and submits a structured classification via a forced submit_classification tool call. Same code drives the in-app review panel and the offline notebook.",
  },
  {
    n: 5,
    nb: "5_analytics",
    title: "Classified spend analytics",
    description:
      "Categorized spend rolls up into views (vw_spend_by_<schema>) for reporting. The app's /analytics/summary endpoint joins cat_predictions to invoices to produce schema-aware spend-by-L1, spend-by-L2, monthly trend, top suppliers, and a fan-out Sankey on the home page.",
  },
];

const SCHEMAS = [
  {
    name: "three_level",
    leaves: "102",
    method: "ai_query (structured output)",
    notes:
      "Internal Direct/Indirect 3-level hierarchy. Single ai_query call per row returns {l1, l2, confidence:1-5} parsed with from_json. Confidence normalized to 0–1.",
  },
  {
    name: "gl_map",
    leaves: "292",
    method: "ai_classify_chat (parallel)",
    notes:
      "GL chart of accounts (1–5 levels). 4 concurrent OpenAI-compatible chat workers with retry/backoff; model returns {code, confidence}. Asset: assets/new_gl_map.xlsx.",
  },
  {
    name: "unspsc",
    leaves: "~150k",
    method: "tsvector OR-token + pgvector",
    notes:
      "UN UNSPSC commodity codes. tsvector OR-token query in Lakebase Postgres returns top-K with ts_rank as confidence. pgvector hybrid retrieval available behind run_pgvector flag. Synced to Postgres for app browsing.",
  },
];

const SOURCES = [
  { source: "ai_classify", schemas: "three_level", confidence: "model self-reported 1–5 (normalized 0–1)" },
  { source: "ai_classify_chat", schemas: "gl_map", confidence: "model self-reported 1–5 (normalized 0–1)" },
  { source: "catboost", schemas: "three_level", confidence: "predict_proba.max() per row" },
  { source: "tsvector", schemas: "unspsc", confidence: "ts_rank (raw)" },
  { source: "agent_review", schemas: "any", confidence: "agent's 1–5 audit score (normalized 0–1)" },
];

const STORAGE = [
  {
    layer: "Unity Catalog (Delta)",
    contents:
      "shm.spend.{invoices, cat_predictions, schema_registry, three_level_taxonomy, gl_map_taxonomy, unspsc_taxonomy}",
    why: "Source of truth — ACID writes, time travel, lineage, governance. Notebooks read/write here.",
  },
  {
    layer: "Lakebase (Postgres) — synced",
    contents: "*_synced tables (SNAPSHOT mode) for invoices, cat_predictions, taxonomies, schema_registry",
    why: "Low-latency reads for the React app. ms-level lookups vs. warehouse round-trips.",
  },
  {
    layer: "Lakebase (Postgres) — native",
    contents: "spend.cat_reviews",
    why: "Writable Postgres table for human corrections. App POSTs straight to it.",
  },
  {
    layer: "Lakebase (Postgres) — pgvector + tsvector",
    contents: "spend.unspsc_text (tsv column + optional embedding)",
    why: "150k UNSPSC commodity index. Hybrid retrieval at request time.",
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
      "Mounted at /api. Lifespan reads tables from Lakebase into pandas at startup; reload via /api/data/reload. Agent endpoint wraps src.agents.review_agent.TaxonomyReviewAgent.",
  },
  {
    layer: "LLM",
    tech: "Foundation Models (databricks-claude-sonnet-4-5)",
    notes:
      "Accessed via WorkspaceClient().serving_endpoints.get_open_ai_client(). Shared retry helper src.llm.chat_with_retry handles 429/5xx/connection drops with full-jitter exponential backoff.",
  },
  {
    layer: "Agent toolset",
    tech: "TaxonomyToolset (5 generic tools) + forced submit_classification",
    notes:
      "Schema-agnostic — operates on level_path arrays, so the same agent reviews three_level (3 levels), gl_map (1–5 levels), and UNSPSC (4 levels) without per-schema code.",
  },
];

const BENEFITS = [
  {
    title: "Unified prediction table",
    body: "Every classification — ai_query, chat, CatBoost, tsvector, pgvector, agent_review — lands in cat_predictions keyed on (order_id, schema_name, source). One MERGE, one query surface.",
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
    title: "One agent for any taxonomy",
    body: "Five generic level_path tools mean the reviewer in notebook 4 and the in-app /agent/review endpoint share code with zero per-schema branches.",
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
    body: "Feed cat_reviews corrections back into CatBoost retraining nightly. Track precision/recall lift in MLflow.",
  },
  {
    title: "Real-time streaming classification",
    body: "Replace the batch ingest with Auto Loader + DLT. Bootstrap and CatBoost run as streaming tasks; UNSPSC tsvector lookups stay request-time.",
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
          How this app actually works. Six pipeline notebooks, three pluggable schemas,
          one unified prediction table, a generic LLM-agent reviewer, and a React app
          backed by Lakebase Postgres for sub-second reads.
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
          Every row in <code>cat_predictions</code> tags its source. The
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
