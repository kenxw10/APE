export type StatusTone = "green" | "red" | "amber" | "muted";

export interface StatusRow {
  label: string;
  value: string;
  tone?: StatusTone;
  detail?: string;
}

export function StatusPanel({
  title,
  rows,
  note
}: {
  title: string;
  rows: readonly StatusRow[];
  note?: string;
}) {
  return (
    <section className="panel status-panel">
      <div className="panel-heading">
        <h2>{title}</h2>
      </div>
      <div className="status-rows">
        {rows.map((row) => (
          <div key={row.label} className="status-row">
            <span>{row.label}</span>
            <b className={row.tone ? `tone-${row.tone}` : undefined}>{row.value}</b>
            {row.detail ? <small>{row.detail}</small> : null}
          </div>
        ))}
      </div>
      {note ? <p className="panel-note">{note}</p> : null}
    </section>
  );
}
