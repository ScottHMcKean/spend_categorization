import { useSchema } from "@/lib/schema-context";

export function SchemaSelector({ compact = false }: { compact?: boolean }) {
  const { schemas, schemaName, setSchemaName, loading } = useSchema();

  if (loading) {
    return (
      <span className="text-xs text-[#8CA0AC] px-2">Loading schemas…</span>
    );
  }
  if (!schemas.length) {
    return (
      <span className="text-xs text-[#8CA0AC] px-2">No schemas registered</span>
    );
  }

  const id = "schema-selector";
  return (
    <div className={`flex items-center ${compact ? "gap-2" : "gap-3"}`}>
      {!compact && (
        <label htmlFor={id} className="text-xs font-semibold uppercase tracking-wide text-[#8CA0AC]">
          Schema
        </label>
      )}
      <select
        id={id}
        value={schemaName}
        onChange={(e) => setSchemaName(e.target.value)}
        className="px-3 py-1.5 text-sm font-medium bg-white border border-[#E5EBF0] rounded-md text-[#0B2026] focus:outline-none focus:ring-2 focus:ring-[#FF3621]/40 hover:border-[#8CA0AC]"
      >
        {schemas.map((s) => (
          <option key={s.name} value={s.name}>
            {s.display_name}
            {s.leaf_count != null ? ` (${s.leaf_count.toLocaleString()})` : ""}
            {s.loaded === false ? " — not loaded" : ""}
          </option>
        ))}
      </select>
    </div>
  );
}

/** Small read-only badge — handy on pages where the dropdown lives elsewhere. */
export function SchemaBadge() {
  const { current, schemaName } = useSchema();
  return (
    <span className="inline-flex items-center gap-1.5 px-2.5 py-1 text-xs font-semibold rounded-full bg-[#F7F9FA] border border-[#E5EBF0] text-[#0B2026]">
      <span className="w-1.5 h-1.5 rounded-full bg-[#FF3621]" />
      {current?.display_name ?? schemaName}
    </span>
  );
}
