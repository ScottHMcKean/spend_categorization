import { apiFetch } from "@/lib/fetch";
import { SchemaBadge } from "@/components/apx/schema-selector";
import { useSchema } from "@/lib/schema-context";
import { createFileRoute } from "@tanstack/react-router";
import { useQuery } from "@tanstack/react-query";
import { useMemo, useState } from "react";

export const Route = createFileRoute("/taxonomy")({
  component: Taxonomy,
});

interface TaxonomyRow {
  code: string;
  label: string;
  level_path?: string[];
  [key: string]: unknown;
}

interface TreeNode {
  name: string;
  fullPath: string[];
  children: Map<string, TreeNode>;
  leaves: TaxonomyRow[];
}

function useTaxonomy(schemaName: string) {
  return useQuery({
    queryKey: ["taxonomy", schemaName],
    queryFn: () =>
      apiFetch<TaxonomyRow[]>(`/schemas/${schemaName}/taxonomy?limit=200000`),
    enabled: !!schemaName,
    staleTime: 5 * 60_000,
  });
}

function buildTree(rows: TaxonomyRow[]): TreeNode {
  const root: TreeNode = { name: "", fullPath: [], children: new Map(), leaves: [] };
  for (const r of rows) {
    const path = (r.level_path ?? []).map(String).filter(Boolean);
    let cur = root;
    for (let i = 0; i < path.length; i++) {
      const seg = path[i];
      if (!cur.children.has(seg)) {
        cur.children.set(seg, {
          name: seg,
          fullPath: path.slice(0, i + 1),
          children: new Map(),
          leaves: [],
        });
      }
      cur = cur.children.get(seg)!;
    }
    cur.leaves.push(r);
  }
  return root;
}

function leafCount(n: TreeNode): number {
  let c = n.leaves.length;
  for (const child of n.children.values()) c += leafCount(child);
  return c;
}

interface TreeProps {
  node: TreeNode;
  depth: number;
  expanded: Set<string>;
  toggle: (key: string) => void;
  search: string;
}

function matchesSearch(node: TreeNode, query: string): boolean {
  if (!query) return true;
  const q = query.toLowerCase();
  if (node.name.toLowerCase().includes(q)) return true;
  for (const l of node.leaves) {
    if (
      l.code?.toLowerCase().includes(q) ||
      l.label?.toLowerCase().includes(q) ||
      l.level_path?.some((p) => String(p).toLowerCase().includes(q))
    ) return true;
  }
  for (const c of node.children.values()) if (matchesSearch(c, q)) return true;
  return false;
}

function TreeView({ node, depth, expanded, toggle, search }: TreeProps) {
  const key = node.fullPath.join("›") || "__root__";
  const isOpen = depth === 0 || expanded.has(key) || (search.length > 0);

  if (depth > 0 && search && !matchesSearch(node, search)) return null;

  const children = Array.from(node.children.values()).sort((a, b) => a.name.localeCompare(b.name));
  const totalLeaves = leafCount(node);

  return (
    <div className={depth === 0 ? "" : "pl-4 border-l border-[#E5EBF0]"}>
      {depth > 0 && (
        <button
          onClick={() => toggle(key)}
          className="flex items-center gap-2 py-1 text-sm text-[#0B2026] hover:text-[#FF3621] w-full text-left"
        >
          <span className={`w-4 inline-block text-[#8CA0AC] transition-transform ${isOpen ? "rotate-90" : ""}`}>
            {children.length > 0 || node.leaves.length > 0 ? "▸" : "•"}
          </span>
          <span className="font-medium">{node.name}</span>
          <span className="text-xs text-[#8CA0AC]">({totalLeaves})</span>
        </button>
      )}
      {isOpen && (
        <div>
          {children.map((c) => (
            <TreeView
              key={c.fullPath.join("›")}
              node={c}
              depth={depth + 1}
              expanded={expanded}
              toggle={toggle}
              search={search}
            />
          ))}
          {depth > 0 &&
            node.leaves
              .filter((l) =>
                !search ||
                l.code?.toLowerCase().includes(search.toLowerCase()) ||
                l.label?.toLowerCase().includes(search.toLowerCase()),
              )
              .map((l, i) => (
                <div key={`${l.code}-${i}`} className="pl-6 py-0.5 text-xs flex items-baseline gap-2">
                  <span className="font-mono text-[#FF3621]">{l.code}</span>
                  <span className="text-[#0B2026]">{l.label}</span>
                </div>
              ))}
        </div>
      )}
    </div>
  );
}

function Taxonomy() {
  const { schemaName, current } = useSchema();
  const { data, isLoading } = useTaxonomy(schemaName);
  const [search, setSearch] = useState("");
  const [expanded, setExpanded] = useState<Set<string>>(new Set());

  const tree = useMemo(() => buildTree(data ?? []), [data]);

  const toggle = (key: string) =>
    setExpanded((prev) => {
      const next = new Set(prev);
      if (next.has(key)) next.delete(key);
      else next.add(key);
      return next;
    });

  const expandAll = () => {
    const all = new Set<string>();
    const walk = (n: TreeNode) => {
      if (n.fullPath.length) all.add(n.fullPath.join("›"));
      for (const c of n.children.values()) walk(c);
    };
    walk(tree);
    setExpanded(all);
  };

  const total = data?.length ?? 0;

  return (
    <div className="max-w-7xl mx-auto px-6 py-8 font-sans">
      <div className="flex flex-wrap items-baseline justify-between gap-3 mb-2">
        <h1 className="text-3xl font-bold text-[#0B2026]">Taxonomy</h1>
        <SchemaBadge />
      </div>
      {current?.description && (
        <p className="text-sm text-[#8CA0AC] mb-6">{current.description}</p>
      )}

      <div className="flex flex-wrap items-end gap-3 mb-4">
        <input
          type="text"
          placeholder="Search code, label, or hierarchy…"
          value={search}
          onChange={(e) => setSearch(e.target.value)}
          className="flex-1 min-w-[260px] px-4 py-2 border border-[#E5EBF0] rounded-md text-[#0B2026] placeholder:text-[#8CA0AC] focus:outline-none focus:ring-2 focus:ring-[#FF3621]/40"
        />
        <button
          onClick={expandAll}
          className="px-3 py-2 border border-[#E5EBF0] rounded-md text-sm text-[#0B2026] hover:border-[#FF3621]"
        >
          Expand all
        </button>
        <button
          onClick={() => setExpanded(new Set())}
          className="px-3 py-2 border border-[#E5EBF0] rounded-md text-sm text-[#0B2026] hover:border-[#FF3621]"
        >
          Collapse
        </button>
        <span className="text-sm text-[#8CA0AC]">{total.toLocaleString()} leaves</span>
      </div>

      <div className="rounded-lg border border-[#E5EBF0] bg-white p-4 overflow-auto">
        {isLoading && <p className="text-[#8CA0AC]">Loading…</p>}
        {!isLoading && total === 0 && (
          <p className="text-[#8CA0AC]">
            No taxonomy loaded for <code>{schemaName}</code>. Run notebook 0.
          </p>
        )}
        {!isLoading && total > 0 && (
          <TreeView
            node={tree}
            depth={0}
            expanded={expanded}
            toggle={toggle}
            search={search}
          />
        )}
      </div>
    </div>
  );
}
