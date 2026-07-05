"use client";

import type { PositionRow } from "../lib/scaffold-data";
import { formatDateButton, formatEasternDateKey, formatEasternDateTime, shiftDateKey } from "../lib/time";

interface PositionsTablesProps {
  openPositions: readonly PositionRow[];
  closedPositions: readonly PositionRow[];
  selectedDate: string;
  onSelectedDateChange: (dateKey: string) => void;
  note: string;
}

const openColumns = [
  "Time Entered (ET)",
  "Market",
  "Side",
  "Entry Price",
  "Current Price",
  "Entry Cost",
  "Current Value",
  "Qty",
  "P/L ($)",
  "P/L (%)",
  "Status",
  "Resolution"
] as const;

const closedColumns = [
  "Time Entered (ET)",
  "Time Closed (ET)",
  "Market",
  "Side",
  "Entry Price",
  "Exit Price",
  "Entry Cost",
  "Exit Value",
  "Qty",
  "P/L ($)",
  "P/L (%)",
  "Status",
  "Resolution"
] as const;

export function PositionsTables({
  openPositions,
  closedPositions,
  selectedDate,
  onSelectedDateChange,
  note
}: PositionsTablesProps) {
  const visibleClosedPositions = closedPositions.filter((position) => {
    if (position.closedAtMs === null) {
      return false;
    }

    return formatEasternDateKey(new Date(position.closedAtMs)) === selectedDate;
  });

  return (
    <>
      <section className="panel table-panel">
        <div className="panel-heading">
          <h2>OPEN POSITIONS</h2>
        </div>
        <TableShell columns={openColumns}>
          {openPositions.length === 0 ? (
            <tr>
              <td colSpan={openColumns.length} className="table-empty">
                <b>NO OPEN POSITIONS</b>
                <span>OBSERVER HAS NOT TAKEN ANY POSITIONS.</span>
              </td>
            </tr>
          ) : (
            openPositions.map((position) => (
              <tr key={position.id}>
                <td>{formatMaybeTime(position.enteredAtMs)}</td>
                <td>{position.market}</td>
                <td>{position.side}</td>
                <td>{formatPrice(position.entryPrice)}</td>
                <td>{formatPrice(position.currentPrice)}</td>
                <td>{formatCurrency(position.entryCost)}</td>
                <td>{formatCurrency(position.currentValue)}</td>
                <td>{position.qty ?? "--"}</td>
                <td>{formatSignedCurrency(position.plDollars)}</td>
                <td>{formatSignedPercent(position.plPercent)}</td>
                <td>{position.status}</td>
                <td>{position.resolution}</td>
              </tr>
            ))
          )}
        </TableShell>
      </section>

      <section className="panel table-panel">
        <div className="panel-heading table-heading">
          <h2>CLOSED POSITIONS</h2>
          <div className="closed-controls">
            <button type="button" onClick={() => onSelectedDateChange(shiftDateKey(selectedDate, -1))}>
              Previous
            </button>
            <button type="button" onClick={() => onSelectedDateChange(formatEasternDateKey())}>
              Today
            </button>
            <label className="date-picker-label">
              <span>{formatDateButton(selectedDate)}</span>
              <input
                aria-label="Closed positions date"
                type="date"
                value={selectedDate}
                onChange={(event) => onSelectedDateChange(event.target.value)}
              />
            </label>
          </div>
        </div>
        <TableShell columns={closedColumns}>
          {visibleClosedPositions.length === 0 ? (
            <tr>
              <td colSpan={closedColumns.length} className="table-empty">
                <b>NO CLOSED POSITIONS FOR SELECTED DATE</b>
                <span>{formatDateButton(selectedDate)} - {note}</span>
              </td>
            </tr>
          ) : (
            visibleClosedPositions.map((position) => (
              <tr key={position.id}>
                <td>{formatMaybeTime(position.enteredAtMs)}</td>
                <td>{formatMaybeTime(position.closedAtMs)}</td>
                <td>{position.market}</td>
                <td>{position.side}</td>
                <td>{formatPrice(position.entryPrice)}</td>
                <td>{formatPrice(position.exitPrice)}</td>
                <td>{formatCurrency(position.entryCost)}</td>
                <td>{formatCurrency(position.exitValue)}</td>
                <td>{position.qty ?? "--"}</td>
                <td>{formatSignedCurrency(position.plDollars)}</td>
                <td>{formatSignedPercent(position.plPercent)}</td>
                <td>{position.status}</td>
                <td>{position.resolution}</td>
              </tr>
            ))
          )}
        </TableShell>
      </section>
    </>
  );
}

function TableShell({
  columns,
  children
}: {
  columns: readonly string[];
  children: React.ReactNode;
}) {
  return (
    <div className="table-wrap">
      <table className="terminal-table">
        <thead>
          <tr>
            {columns.map((column) => (
              <th key={column}>{column}</th>
            ))}
          </tr>
        </thead>
        <tbody>{children}</tbody>
      </table>
    </div>
  );
}

function formatMaybeTime(value: number | null): string {
  return value === null ? "--" : formatEasternDateTime(value);
}

function formatPrice(value: number | null): string {
  if (value === null) {
    return "--";
  }

  return `${Math.round(value * 100)}c`;
}

function formatCurrency(value: number | null): string {
  if (value === null) {
    return "--";
  }

  return new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: "USD",
    minimumFractionDigits: 2,
    maximumFractionDigits: 2
  }).format(value);
}

function formatSignedCurrency(value: number | null): string {
  if (value === null) {
    return "--";
  }

  const formatted = formatCurrency(Math.abs(value));
  return `${value >= 0 ? "+" : "-"}${formatted}`;
}

function formatSignedPercent(value: number | null): string {
  if (value === null) {
    return "--";
  }

  return `${value >= 0 ? "+" : ""}${value.toFixed(2)}%`;
}
