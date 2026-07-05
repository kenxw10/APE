"use client";

import { useMemo, useState, useTransition } from "react";
import { useRouter } from "next/navigation";

import { type OperationalSnapshot } from "../lib/api";
import { type PortfolioRange } from "../lib/chart";
import { type ScaffoldDashboardData } from "../lib/scaffold-data";
import { formatDateButton, formatEasternDateKey, formatEasternDateTime, formatEasternTime } from "../lib/time";
import { PortfolioValueChart } from "./PortfolioValueChart";
import { PositionsTables } from "./PositionsTables";
import { ReferencePriceChart } from "./ReferencePriceChart";
import { StatusPanel, type StatusRow } from "./StatusPanel";

interface DashboardShellProps {
  snapshot: OperationalSnapshot;
  scaffold: ScaffoldDashboardData;
}

export function DashboardShell({ snapshot, scaffold }: DashboardShellProps) {
  const router = useRouter();
  const [isPending, startTransition] = useTransition();
  const [portfolioRange, setPortfolioRange] = useState<PortfolioRange>("12h");
  const [selectedClosedDate, setSelectedClosedDate] = useState(() =>
    formatEasternDateKey(new Date(snapshot.fetchedAt))
  );
  const nowMs = new Date(snapshot.fetchedAt).getTime();

  const apiConnected = snapshot.health.ok && snapshot.health.data?.status === "ok";
  const dbReady =
    snapshot.database.data?.status === "ok" || snapshot.readiness.data?.database.status === "ok";
  const safety = snapshot.safety.data ?? snapshot.health.data?.safety ?? snapshot.readiness.data?.safety ?? null;
  const mode = safety?.mode ?? snapshot.health.data?.app_mode ?? "UNKNOWN";
  const tradingLabel = safety?.trading_enabled === false ? "DISABLED" : "UNKNOWN";
  const executeLabel = safety?.execute === false ? "FALSE" : "UNKNOWN";

  const statusSections = useMemo(
    () => createStatusSections(snapshot, scaffold, apiConnected, dbReady),
    [snapshot, scaffold, apiConnected, dbReady]
  );

  const refresh = () => {
    startTransition(() => {
      router.refresh();
    });
  };

  return (
    <main className="dashboard-shell">
      <header className="terminal-header">
        <div className="brand-block">
          <h1>APE</h1>
          <p>BTC15 Kalshi Momentum Observer</p>
        </div>
        <div className="header-strip" aria-label="APE operational state">
          <span>
            Mode: <b className="tone-amber">{mode}</b>
          </span>
          <span>
            Trading: <b className={tradingLabel === "DISABLED" ? "tone-green" : "tone-amber"}>{tradingLabel}</b>
          </span>
          <span>
            Execute: <b className={executeLabel === "FALSE" ? "tone-green" : "tone-amber"}>{executeLabel}</b>
          </span>
          <span>
            API: <b className={apiConnected ? "tone-green" : "tone-red"}>{apiConnected ? "CONNECTED" : "DISCONNECTED"}</b>
          </span>
          <span>
            DB: <b className={dbReady ? "tone-green" : "tone-red"}>{dbReady ? "READY" : "NOT READY"}</b>
          </span>
          <button type="button" className="refresh-button" onClick={refresh} disabled={isPending}>
            {isPending ? "Refreshing" : "Refresh"}
          </button>
          <span>Last update: {formatEasternDateTime(snapshot.fetchedAt)}</span>
        </div>
      </header>

      <section className="operator-banner">
        OBSERVER ONLY: NO TRADING, NO EXECUTION, NO KALSHI SECRETS IN DASHBOARD.
      </section>

      <PortfolioValueChart
        points={scaffold.portfolio.series.points}
        openedAtMs={scaffold.portfolio.openedAtMs}
        startValue={scaffold.portfolio.startValue}
        currentValue={scaffold.portfolio.currentValue}
        plPercent={scaffold.portfolio.plPercent}
        note={scaffold.portfolio.series.note}
        range={portfolioRange}
        onRangeChange={setPortfolioRange}
        nowMs={nowMs}
      />

      <section className="metrics-grid" aria-label="Summary metrics">
        <MetricCard label="Win Rate" value="--" detail="No settled ledger data yet" />
        <MetricCard label="P/L" value="$0.00" detail="Observer scaffold only" />
        <MetricCard label="Record" value="0-0-0" detail="Wins-losses-pushes" />
      </section>

      <section className="reference-grid">
        <ReferencePriceChart
          points={scaffold.reference.series.points}
          intervalOpenPrice={scaffold.reference.intervalOpenPrice}
          intervalStartMs={scaffold.reference.intervalStartMs}
          intervalEndMs={scaffold.reference.intervalEndMs}
          note={scaffold.reference.series.note}
        />
        <BenchmarkSummary scaffold={scaffold} />
      </section>

      <PositionsTables
        openPositions={scaffold.positions.open}
        closedPositions={scaffold.positions.closed}
        selectedDate={selectedClosedDate}
        onSelectedDateChange={setSelectedClosedDate}
        note={scaffold.positions.note}
      />

      <section className="status-grid">
        <StatusPanel title="Source Status" rows={statusSections.source} />
        <StatusPanel title="System Status" rows={statusSections.system} />
        <StatusPanel title="Data & Streaming" rows={statusSections.streaming} />
        <StatusPanel title="Engine Status" rows={statusSections.engine} />
        <StatusPanel title="Safety" rows={statusSections.safety} />
      </section>
    </main>
  );
}

function MetricCard({ label, value, detail }: { label: string; value: string; detail: string }) {
  return (
    <section className="panel metric-card">
      <span>{label}</span>
      <strong>{value}</strong>
      <small>{detail}</small>
    </section>
  );
}

function BenchmarkSummary({ scaffold }: { scaffold: ScaffoldDashboardData }) {
  const reference = scaffold.reference;
  const move = ((reference.currentPrice - reference.intervalOpenPrice) / reference.intervalOpenPrice) * 100;
  const rows = [
    [
      "Visible Window",
      `${formatEasternTime(reference.intervalStartMs)}-${formatEasternTime(reference.intervalEndMs)} ET`
    ],
    ["Interval Open", formatUsd(reference.intervalOpenPrice)],
    ["Settlement Source", "CF/BRTI"],
    ["Current CF/BRTI", formatUsd(reference.currentPrice)],
    ["Move Since Open", `${move >= 0 ? "+" : ""}${move.toFixed(2)}%`],
    ["Ref Spread", reference.refSpreadBps === null ? "--" : `${reference.refSpreadBps.toFixed(1)} bps`],
    ["Source Coverage", "1 / 1"],
    ["Status", "SCAFFOLD"]
  ];

  return (
    <section className="panel benchmark-panel">
      <div className="panel-heading">
        <h2>BENCHMARK SUMMARY</h2>
      </div>
      <dl className="summary-table">
        {rows.map(([label, value]) => (
          <div key={label}>
            <dt>{label}</dt>
            <dd className={label === "Status" ? "tone-amber" : undefined}>{value}</dd>
          </div>
        ))}
      </dl>
      <p className="panel-note">Date control: {formatDateButton(formatEasternDateKey(new Date(scaffold.createdAt)))}</p>
    </section>
  );
}

function createStatusSections(
  snapshot: OperationalSnapshot,
  scaffold: ScaffoldDashboardData,
  apiConnected: boolean,
  dbReady: boolean
): Record<"source" | "system" | "streaming" | "engine" | "safety", StatusRow[]> {
  const safety = snapshot.safety.data ?? snapshot.health.data?.safety ?? snapshot.readiness.data?.safety ?? null;

  return {
    source: [
      { label: "CF/BRTI", value: "NOT IMPLEMENTED", tone: "amber" },
      { label: "Age", value: "--", tone: "muted" },
      { label: "Latency", value: "--", tone: "muted" },
      { label: "Provenance", value: scaffold.reference.series.provenance.toUpperCase(), tone: "amber" }
    ],
    system: [
      { label: "Backend API", value: apiConnected ? "HEALTHY" : "UNREACHABLE", tone: apiConnected ? "green" : "red" },
      { label: "Database", value: dbReady ? "READY" : "NOT READY", tone: dbReady ? "green" : "red" },
      {
        label: "Dashboard Sync",
        value: snapshot.health.ok && snapshot.safety.ok && snapshot.database.ok && snapshot.readiness.ok ? "HEALTHY" : "PARTIAL",
        tone: snapshot.health.ok && snapshot.safety.ok && snapshot.database.ok && snapshot.readiness.ok ? "green" : "amber"
      }
    ],
    streaming: [
      { label: "SSE Connection", value: "NOT IMPLEMENTED", tone: "amber" },
      { label: "Updates / Min", value: "--", tone: "muted" },
      { label: "Chart Points", value: `${scaffold.reference.series.points.length} / 600 max`, tone: "green" }
    ],
    engine: [
      { label: "Market Resolver", value: "DISABLED", tone: "amber" },
      { label: "Signal Engine", value: "DISABLED", tone: "green" },
      { label: "Execution", value: "DISABLED", tone: "green" }
    ],
    safety: [
      { label: "Mode", value: safety?.mode ?? "UNKNOWN", tone: safety?.mode === "OBSERVER" ? "green" : "amber" },
      {
        label: "Trading",
        value: safety?.trading_enabled === false ? "DISABLED" : "UNKNOWN",
        tone: safety?.trading_enabled === false ? "green" : "amber"
      },
      {
        label: "Execute",
        value: safety?.execute === false ? "FALSE" : "UNKNOWN",
        tone: safety?.execute === false ? "green" : "amber"
      }
    ]
  };
}

function formatUsd(value: number): string {
  return new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: "USD",
    minimumFractionDigits: 2,
    maximumFractionDigits: 2
  }).format(value);
}
