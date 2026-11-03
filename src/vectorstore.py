"""pgvector example store — the retrieval layer for the LLM classifier.

One Postgres table, `examples`, holds everything the LLM classifier
retrieves against. Each row is a short text plus its embedding and the
code it maps to. Two kinds of rows live here:

  * kind='taxonomy'      — one row per taxonomy leaf (used at cold start,
                           before any ground truth exists).
  * kind='ground_truth'  — one row per labelled invoice (few-shot examples
                           that get stronger as corrections accumulate).

Notebook 2 fills this table; notebook 3 searches it. pgvector is the app's
backend too, so there is only ever one store to understand.

Everything here is plain SQL over a psycopg2 connection — no ORM, no
cleverness. The table lives in the Postgres schema named like the Unity
Catalog schema (e.g. "spend").
"""

from __future__ import annotations

from psycopg2.extras import execute_values


def _vec(embedding: list[float]) -> str:
    """Format an embedding as a pgvector literal, e.g. '[0.1,0.2,0.3]'.

    We pass this string and cast it with `::vector` in SQL, which avoids
    depending on pgvector's Python type adapters.
    """
    return "[" + ",".join(str(float(x)) for x in embedding) + "]"


def ensure_table(conn, pg_schema: str, dim: int) -> None:
    """Create the `examples` table (and its vector index) if missing."""
    with conn.cursor() as cur:
        cur.execute("CREATE EXTENSION IF NOT EXISTS vector")
        cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{pg_schema}"')
        cur.execute(f'''
            CREATE TABLE IF NOT EXISTS "{pg_schema}".examples (
                id          BIGSERIAL PRIMARY KEY,
                schema_name TEXT,
                kind        TEXT,          -- 'taxonomy' | 'ground_truth'
                code        TEXT,
                label       TEXT,
                level_path  TEXT[],
                text        TEXT,
                embedding   vector({dim})
            )
        ''')
        cur.execute(f'''
            CREATE INDEX IF NOT EXISTS examples_embedding_idx
            ON "{pg_schema}".examples USING hnsw (embedding vector_cosine_ops)
        ''')
    conn.commit()


def replace_examples(conn, pg_schema: str, schema_name: str, rows: list[dict],
                     embeddings: list[list[float]]) -> int:
    """Delete this schema's existing examples and insert a fresh set.

    `rows` and `embeddings` line up positionally. Each row is a dict with
    keys: kind, code, label, level_path, text.
    """
    with conn.cursor() as cur:
        cur.execute(f'DELETE FROM "{pg_schema}".examples WHERE schema_name = %s', (schema_name,))
        values = [
            (schema_name, r["kind"], r["code"], r["label"],
             list(r["level_path"] or []), r["text"], _vec(emb))
            for r, emb in zip(rows, embeddings)
        ]
        execute_values(
            cur,
            f'INSERT INTO "{pg_schema}".examples '
            f'(schema_name, kind, code, label, level_path, text, embedding) VALUES %s',
            values,
            template="(%s,%s,%s,%s,%s,%s,%s::vector)",
        )
    conn.commit()
    return len(values)


def search(conn, pg_schema: str, schema_name: str, embedding: list[float], k: int) -> list[dict]:
    """Return the `k` nearest examples for one schema, closest first.

    Ground-truth examples are preferred over taxonomy rows at equal
    distance, so real labelled invoices win once they exist.
    """
    with conn.cursor() as cur:
        cur.execute(
            f'''
            SELECT code, label, level_path, text, kind,
                   embedding <=> %s::vector AS distance
            FROM "{pg_schema}".examples
            WHERE schema_name = %s
            ORDER BY (kind = 'ground_truth') DESC, distance ASC
            LIMIT %s
            ''',
            (_vec(embedding), schema_name, k),
        )
        return [
            {"code": r[0], "label": r[1], "level_path": list(r[2] or []),
             "text": r[3], "kind": r[4], "distance": float(r[5])}
            for r in cur.fetchall()
        ]


def grant_select(conn, pg_schema: str, role: str) -> None:
    """Let the app's service principal read the examples table."""
    with conn.cursor() as cur:
        cur.execute(f'GRANT USAGE ON SCHEMA "{pg_schema}" TO "{role}"')
        cur.execute(f'GRANT SELECT ON "{pg_schema}".examples TO "{role}"')
    conn.commit()
