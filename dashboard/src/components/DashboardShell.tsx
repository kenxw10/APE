"use client";

import { useEffect, useMemo, useState, useTransition } from "react";
import { useRouter } from "next/navigation";

import {
  type BrtiReferenceSeriesPointResponse,
  type BrtiReferenceStatusResponse,
  type OperationalSnapshot,
  type StrategyStatusResponse
} from "../lib/api";
import {
  MAX_REFERENCE_CHART_POINTS,
  capPoints,
  selectFixedIntervalReferencePoints,
  type PortfolioRange
} from "../lib/chart";
import { type ReferencePricePoint, type ScaffoldDashboardData } from "../lib/scaffold-data";
import { formatDateButton, formatEasternDateKey, formatEasternDateTime, formatEasternTime } from "../lib/time";
import { PortfolioValueChart } from "./PortfolioValueChart";
import { PositionsTables } from "./PositionsTables";
import { ReferencePriceChart } from "./ReferencePriceChart";
import { StatusPanel, type StatusRow } from "./StatusPanel";

interface DashboardShellProps {
  snapshot: OperationalSnapshot;
  scaffold: ScaffoldDashboardData;
}

interface ReferenceChartData {
  points: readonly ReferencePricePoint[];
  intervalStartMs: number;
  intervalEndMs: number;
  intervalOpenPrice: number;
  currentPrice: number;
  refSpreadBps: number | null;
  status: "live" | "stale" | "fallback";
  note: string;
  sourceAgeLabel: string;
  backendAgeLabel: string;
  pointCount: number;
  maxPoints: number;
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

  const referenceChart = useMemo(
    () => createReferenceChartData(snapshot, scaffold),
    [snapshot, scaffold]
  );
  const statusSections = useMemo(
    () => createStatusSections(snapshot, apiConnected, dbReady, referenceChart),
    [snapshot, apiConnected, dbReady, referenceChart]
  );

  useEffect(() => {
    const intervalId = window.setInterval(() => {
      router.refresh();
    }, 1000);
    return () => window.clearInterval(intervalId);
  }, [router]);

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
          points={referenceChart.points}
          intervalOpenPrice={referenceChart.intervalOpenPrice}
          intervalStartMs={referenceChart.intervalStartMs}
          intervalEndMs={referenceChart.intervalEndMs}
          note={referenceChart.note}
          provenanceLabel={referenceChartProvenanceLabel(referenceChart)}
          sourceAgeLabel={referenceChart.sourceAgeLabel}
          backendAgeLabel={referenceChart.backendAgeLabel}
        />
        <BenchmarkSummary scaffold={scaffold} referenceChart={referenceChart} />
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

function BenchmarkSummary({
  scaffold,
  referenceChart
}: {
  scaffold: ScaffoldDashboardData;
  referenceChart: ReferenceChartData;
}) {
  const reference = referenceChart;
  const move =
    reference.intervalOpenPrice === 0
      ? 0
      : ((reference.currentPrice - reference.intervalOpenPrice) / reference.intervalOpenPrice) * 100;
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
    ["Source Coverage", referenceChartCoverageLabel(reference)],
    ["Status", reference.status.toUpperCase()]
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
            <dd className={label === "Status" ? referenceChartToneClass(reference) : undefined}>{value}</dd>
          </div>
        ))}
      </dl>
      <p className="panel-note">Date control: {formatDateButton(formatEasternDateKey(new Date(scaffold.createdAt)))}</p>
    </section>
  );
}

function createStatusSections(
  snapshot: OperationalSnapshot,
  apiConnected: boolean,
  dbReady: boolean,
  referenceChart: ReferenceChartData
): Record<"source" | "system" | "streaming" | "engine" | "safety", StatusRow[]> {
  const safety = snapshot.safety.data ?? snapshot.health.data?.safety ?? snapshot.readiness.data?.safety ?? null;
  const wsStatus = snapshot.wsStatus.data;
  const brtiStatus = snapshot.brtiStatus.data;
  const strategyStatus = snapshot.strategyStatus.data;
  const wsConnected = wsStatus?.connection_state === "subscribed" && !wsStatus.stale;
  const wsTone = !snapshot.wsStatus.ok
    ? "red"
    : !wsStatus?.enabled
      ? "amber"
      : wsConnected
        ? "green"
        : wsStatus.connection_state === "error"
          ? "red"
          : "amber";
  const brtiTone = !snapshot.brtiStatus.ok
    ? "red"
    : !brtiStatus?.enabled
      ? "muted"
      : brtiStatus.connection_state === "subscribed" && !brtiStatus.stale
        ? "green"
        : brtiStatus.connection_state === "error"
          ? "red"
          : "amber";

  return {
    source: [
      {
        label: "BRTI Transport",
        value: brtiTransportLabel(brtiStatus),
        tone: brtiTone,
        detail: formatReferenceValue(brtiStatus?.latest_parsed_value ?? null)
      },
      {
        label: "Kalshi WS",
        value: wsStatus ? wsStatus.connection_state.toUpperCase() : "UNREACHABLE",
        tone: wsTone
      },
      {
        label: "BRTI Age",
        value: formatSourceAge(brtiStatus),
        tone: brtiStatus?.latest_tick_received_at && !brtiStatus.transport_stale ? "green" : "muted"
      },
      {
        label: "BRTI Source Age",
        value: brtiSourceAgeStatusLabel(brtiStatus),
        tone: brtiSourceAgeStatusTone(brtiStatus),
        detail: formatDurationMs(brtiStatus?.source_age_ms ?? null)
      },
      {
        label: "BRTI Persistence",
        value: brtiStatus?.persistence_stale ? "STALE" : brtiStatus?.last_persisted_at ? "LIVE" : "--",
        tone: brtiStatus?.persistence_stale ? "amber" : brtiStatus?.last_persisted_at ? "green" : "muted"
      },
      {
        label: "Final Avg",
        value: brtiStatus?.final_minute_average_status?.toUpperCase() ?? "--",
        tone: brtiStatus?.final_minute_average_status === "present" ? "green" : "muted"
      },
      {
        label: "Provenance",
        value: referenceChartCoverageLabel(referenceChart),
        tone: referenceChartTone(referenceChart)
      }
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
      {
        label: "WS Channels",
        value: wsStatus?.subscribed_channels.length ? `${wsStatus.subscribed_channels.length} ACTIVE` : "--",
        tone: wsConnected ? "green" : "muted"
      },
      {
        label: "Orderbook Age",
        value: formatAge(snapshot.fetchedAt, wsStatus?.last_orderbook_at ?? null),
        tone: wsStatus?.last_orderbook_at && !wsStatus.stale ? "green" : "muted"
      },
      {
        label: "Trade Age",
        value: formatAge(snapshot.fetchedAt, wsStatus?.last_trade_at ?? null),
        tone: wsStatus?.last_trade_at ? "green" : "muted"
      },
      {
        label: "Chart Points",
        value: `${referenceChart.pointCount} / ${referenceChart.maxPoints} max`,
        tone: referenceChartTone(referenceChart),
        detail: `active 15m ${referenceChartProvenanceLabel(referenceChart)}`
      }
    ],
    engine: [
      {
        label: "Strategy Observer",
        value: strategyObserverLabel(snapshot.strategyStatus.ok, strategyStatus),
        tone: strategyObserverTone(snapshot.strategyStatus.ok, strategyStatus)
      },
      {
        label: "Latest Decision",
        value: strategyStatus?.latest_decision_state ?? "--",
        tone: strategyDecisionTone(strategyStatus),
        detail: strategyStatus?.latest_primary_reason ?? undefined
      },
      {
        label: "Candidate",
        value: strategyStatus?.candidate_side ?? "--",
        tone: strategyStatus?.candidate_side ? "green" : "muted",
        detail: strategyDistanceLabel(strategyStatus)
      },
      {
        label: "Seconds Left",
        value: strategyStatus?.seconds_left === null || strategyStatus?.seconds_left === undefined
          ? "--"
          : `${strategyStatus.seconds_left}s`,
        tone: strategyStatus?.seconds_left === null || strategyStatus?.seconds_left === undefined ? "muted" : "green"
      },
      {
        label: "Decision Age",
        value: strategyStatus?.decision_age_seconds === null || strategyStatus?.decision_age_seconds === undefined
          ? "--"
          : `${Math.round(strategyStatus.decision_age_seconds)}s`,
        tone: strategyStatus?.stale ? "amber" : strategyStatus?.latest_decision_id ? "green" : "muted"
      },
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

function formatAge(fetchedAt: string, value: string | null): string {
  if (!value) {
    return "--";
  }

  const ageSeconds = Math.max(0, Math.round((Date.parse(fetchedAt) - Date.parse(value)) / 1000));
  if (!Number.isFinite(ageSeconds)) {
    return "--";
  }
  if (ageSeconds < 60) {
    return `${ageSeconds}s`;
  }

  return `${Math.round(ageSeconds / 60)}m`;
}

function formatReferenceValue(value: string | number | null): string | undefined {
  if (value === null) {
    return undefined;
  }
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) {
    return undefined;
  }
  return formatUsd(numeric);
}

function formatSourceAge(brtiStatus: BrtiReferenceStatusResponse | null): string {
  if (!brtiStatus) {
    return "--";
  }
  if (brtiStatus.latest_tick_received_at) {
    return formatAge(brtiStatus.checked_at, brtiStatus.latest_tick_received_at);
  }
  return "--";
}

function strategyObserverLabel(
  endpointOk: boolean,
  strategyStatus: StrategyStatusResponse | null
): string {
  if (!endpointOk || !strategyStatus) {
    return "UNREACHABLE";
  }
  if (!strategyStatus.enabled) {
    return "DISABLED";
  }
  if (strategyStatus.blockers.length > 0 || !strategyStatus.is_safe) {
    return "BLOCKED";
  }
  if (strategyStatus.stale) {
    return "STALE";
  }
  return "RUNNING";
}

function strategyObserverTone(
  endpointOk: boolean,
  strategyStatus: StrategyStatusResponse | null
): StatusRow["tone"] {
  const label = strategyObserverLabel(endpointOk, strategyStatus);
  if (label === "RUNNING" || label === "DISABLED") {
    return "green";
  }
  if (label === "UNREACHABLE" || label === "BLOCKED") {
    return "red";
  }
  return "amber";
}

function strategyDecisionTone(strategyStatus: StrategyStatusResponse | null): StatusRow["tone"] {
  if (!strategyStatus?.latest_decision_state) {
    return "muted";
  }
  if (strategyStatus.latest_decision_state === "OBSERVE_ONLY_MARKET") {
    return "green";
  }
  if (
    strategyStatus.latest_decision_state === "LIVE_GUARD_BLOCKED" ||
    strategyStatus.latest_decision_state === "BOOK_UNUSABLE"
  ) {
    return "red";
  }
  return "amber";
}

function strategyDistanceLabel(strategyStatus: StrategyStatusResponse | null): string | undefined {
  if (strategyStatus?.distance_bps === null || strategyStatus?.distance_bps === undefined) {
    return undefined;
  }
  const distance = Number(strategyStatus.distance_bps);
  if (!Number.isFinite(distance)) {
    return undefined;
  }
  return `${distance.toFixed(2)} bps`;
}

function createReferenceChartData(
  snapshot: OperationalSnapshot,
  scaffold: ScaffoldDashboardData
): ReferenceChartData {
  const series = snapshot.brtiSeries.ok ? snapshot.brtiSeries.data : null;
  const brtiStatus = snapshot.brtiStatus.data;
  const generatedAtMs = series
    ? Date.parse(series.generated_at) || Date.parse(snapshot.fetchedAt)
    : Date.parse(snapshot.fetchedAt);
  const livePoints = series ? liveBrtiReferencePoints(series.points) : [];
  const intervalSelection = selectFixedIntervalReferencePoints(livePoints, generatedAtMs);

  if (
    series &&
    intervalSelection.points.length > 0 &&
    intervalSelection.intervalOpenPrice !== null &&
    intervalSelection.currentPrice !== null
  ) {
    const cappedPoints = capPoints(
      intervalSelection.points,
      Math.min(series.max_points, MAX_REFERENCE_CHART_POINTS)
    );
    const latestPoint = cappedPoints[cappedPoints.length - 1];
    const status = isLiveBrtiStatus(brtiStatus) ? "live" : "stale";

    return {
      points: cappedPoints,
      intervalStartMs: intervalSelection.domain.startMs,
      intervalEndMs: intervalSelection.domain.endMs,
      intervalOpenPrice: intervalSelection.intervalOpenPrice,
      currentPrice: intervalSelection.currentPrice,
      refSpreadBps: null,
      status,
      note:
        status === "live"
          ? "Current Kalshi 15-minute interval from /reference/brti/series. Raw payload excluded."
          : "Current-interval /reference/brti/series points are shown, but BRTI status is not live.",
      sourceAgeLabel: formatDurationMs(
        brtiStatus?.source_age_ms ?? latestSeriesSourceAge(intervalSelection.points)
      ),
      backendAgeLabel: formatAge(
        snapshot.fetchedAt,
        brtiStatus?.latest_tick_received_at ?? new Date(latestPoint.tsMs).toISOString()
      ),
      pointCount: cappedPoints.length,
      maxPoints: Math.min(series.max_points, MAX_REFERENCE_CHART_POINTS)
    };
  }

  const fallbackPoints = capPoints(scaffold.reference.series.points, MAX_REFERENCE_CHART_POINTS);
  return {
    points: fallbackPoints,
    intervalStartMs: scaffold.reference.intervalStartMs,
    intervalEndMs: scaffold.reference.intervalEndMs,
    intervalOpenPrice: scaffold.reference.intervalOpenPrice,
    currentPrice: scaffold.reference.currentPrice,
    refSpreadBps: scaffold.reference.refSpreadBps,
    status: "fallback",
    note: scaffold.reference.series.note,
    sourceAgeLabel: "--",
    backendAgeLabel: "--",
    pointCount: fallbackPoints.length,
    maxPoints: MAX_REFERENCE_CHART_POINTS
  };
}

function liveBrtiReferencePoints(
  points: readonly BrtiReferenceSeriesPointResponse[]
): BrtiReferencePointWithAge[] {
  return points
    .map((point) => {
      const tsMs = Date.parse(point.received_at);
      const value = Number(point.parsed_value);
      if (
        !Number.isFinite(tsMs) ||
        !Number.isFinite(value) ||
        point.parse_status !== "valid"
      ) {
        return null;
      }

      return {
        tsMs,
        value,
        source: "CF/BRTI" as const,
        sourceAgeMs: point.source_age_ms
      };
    })
    .filter((point): point is BrtiReferencePointWithAge => point !== null)
    .sort((left, right) => left.tsMs - right.tsMs);
}

function latestSeriesSourceAge(points: readonly BrtiReferencePointWithAge[]): number | null {
  const latest = points[points.length - 1];
  return latest?.sourceAgeMs ?? null;
}

interface BrtiReferencePointWithAge extends ReferencePricePoint {
  sourceAgeMs: number | null;
}

function formatDurationMs(value: number | null | undefined): string {
  if (value === null || value === undefined || !Number.isFinite(value)) {
    return "--";
  }
  const ms = Math.max(0, Math.round(value));
  if (ms < 1000) {
    return `${ms}ms`;
  }
  const seconds = Math.round(ms / 1000);
  if (seconds < 60) {
    return `${seconds}s`;
  }
  return `${Math.round(seconds / 60)}m`;
}

function brtiTransportLabel(brtiStatus: BrtiReferenceStatusResponse | null): string {
  if (!brtiStatus) {
    return "UNREACHABLE";
  }
  if (!brtiStatus.enabled) {
    return "DISABLED";
  }
  if (brtiStatus.connection_state === "error") {
    return "ERROR";
  }
  if (brtiStatus.transport_stale) {
    return "STALE";
  }
  if (brtiStatus.connection_state === "subscribed") {
    return "LIVE";
  }
  return brtiStatus.connection_state.toUpperCase();
}

function isLiveBrtiStatus(brtiStatus: BrtiReferenceStatusResponse | null): boolean {
  return (
    brtiStatus !== null &&
    brtiStatus.enabled &&
    brtiStatus.connection_state === "subscribed" &&
    !brtiStatus.stale &&
    !brtiStatus.transport_stale &&
    !brtiStatus.persistence_stale &&
    brtiStatus.last_error_type === null &&
    brtiStatus.last_error_message === null &&
    brtiStatus.blockers.length === 0
  );
}

function brtiSourceAgeStatusLabel(brtiStatus: BrtiReferenceStatusResponse | null): string {
  if (!brtiStatus || brtiStatus.source_age_ms === null || brtiStatus.source_age_ms === undefined) {
    return "--";
  }
  return brtiStatus.source_stale ? "LAGGING" : "FRESH";
}

function brtiSourceAgeStatusTone(brtiStatus: BrtiReferenceStatusResponse | null): StatusRow["tone"] {
  if (!brtiStatus || brtiStatus.source_age_ms === null || brtiStatus.source_age_ms === undefined) {
    return "muted";
  }
  return brtiStatus.source_stale ? "amber" : "green";
}

function referenceChartCoverageLabel(referenceChart: ReferenceChartData): string {
  if (referenceChart.status === "live") {
    return "LIVE BRTI";
  }
  if (referenceChart.status === "stale") {
    return "STALE BRTI";
  }
  return "FALLBACK";
}

function referenceChartProvenanceLabel(
  referenceChart: ReferenceChartData
): "live BRTI" | "stale BRTI" | "fallback scaffold" {
  if (referenceChart.status === "live") {
    return "live BRTI";
  }
  if (referenceChart.status === "stale") {
    return "stale BRTI";
  }
  return "fallback scaffold";
}

function referenceChartTone(referenceChart: ReferenceChartData): StatusRow["tone"] {
  return referenceChart.status === "live" ? "green" : "amber";
}

function referenceChartToneClass(referenceChart: ReferenceChartData): string {
  return referenceChart.status === "live" ? "tone-green" : "tone-amber";
}
