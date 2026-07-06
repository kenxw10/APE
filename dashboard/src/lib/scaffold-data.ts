import { MAX_REFERENCE_CHART_POINTS, type TimeValuePoint } from "./chart";
import { currentEasternInterval } from "./time";

export type DashboardDataProvenance = "dashboard-scaffold";
export type FutureDataSurface = "future-ledger" | "future-reference" | "future-position";

export interface ScaffoldSeries<T> {
  provenance: DashboardDataProvenance;
  futureSurface: FutureDataSurface;
  note: string;
  points: T[];
}

export interface PortfolioScaffold {
  startValue: number;
  currentValue: number | null;
  plPercent: number | null;
  openedAtMs: number;
  series: ScaffoldSeries<TimeValuePoint>;
}

export interface ReferencePricePoint extends TimeValuePoint {
  source: "CF/BRTI";
}

export interface ReferenceScaffold {
  source: "CF/BRTI";
  intervalStartMs: number;
  intervalEndMs: number;
  intervalOpenPrice: number;
  currentPrice: number;
  refSpreadBps: number | null;
  status: "scaffold";
  series: ScaffoldSeries<ReferencePricePoint>;
}

export interface PositionRow {
  id: string;
  enteredAtMs: number | null;
  closedAtMs: number | null;
  market: string;
  side: string;
  entryPrice: number | null;
  currentPrice: number | null;
  exitPrice: number | null;
  entryCost: number | null;
  currentValue: number | null;
  exitValue: number | null;
  qty: number | null;
  plDollars: number | null;
  plPercent: number | null;
  status: string;
  resolution: string;
}

export interface PositionsScaffold {
  provenance: DashboardDataProvenance;
  futureSurface: "future-position";
  note: string;
  open: PositionRow[];
  closed: PositionRow[];
}

export interface ScaffoldDashboardData {
  createdAt: string;
  portfolio: PortfolioScaffold;
  reference: ReferenceScaffold;
  positions: PositionsScaffold;
}

export function createScaffoldDashboardData(nowIso: string): ScaffoldDashboardData {
  const nowMs = new Date(nowIso).getTime();
  const portfolioOpenedAtMs = nowMs - 48 * 60 * 60 * 1000;
  const portfolioPoints = createPortfolioScaffoldPoints(portfolioOpenedAtMs, nowMs);
  const interval = currentEasternInterval(nowMs, 15);
  const referencePoints = createReferenceScaffoldPoints(interval.startMs, Math.min(nowMs, interval.endMs));
  const currentReferencePrice = referencePoints[referencePoints.length - 1]?.value ?? 0;
  const intervalOpenPrice = referencePoints[0]?.value ?? currentReferencePrice;

  return {
    createdAt: nowIso,
    portfolio: {
      startValue: 500,
      currentValue: null,
      plPercent: null,
      openedAtMs: portfolioOpenedAtMs,
      series: {
        provenance: "dashboard-scaffold",
        futureSurface: "future-ledger",
        note: "Scaffold sample only. APE does not have a live portfolio ledger endpoint yet.",
        points: portfolioPoints
      }
    },
    reference: {
      source: "CF/BRTI",
      intervalStartMs: interval.startMs,
      intervalEndMs: interval.endMs,
      intervalOpenPrice,
      currentPrice: currentReferencePrice,
      refSpreadBps: null,
      status: "scaffold",
      series: {
        provenance: "dashboard-scaffold",
        futureSurface: "future-reference",
        note: "Fallback scaffold sample only. Live CF/BRTI reference data is unavailable.",
        points: referencePoints
      }
    },
    positions: {
      provenance: "dashboard-scaffold",
      futureSurface: "future-position",
      note: "No open or closed positions are available because PR 4 adds no trading, paper trading, or execution.",
      open: [],
      closed: []
    }
  };
}

function createPortfolioScaffoldPoints(startMs: number, endMs: number): TimeValuePoint[] {
  const values = [
    500,
    501.2,
    502.1,
    501.4,
    498.8,
    497.5,
    500.2,
    503.8,
    505.4,
    504.9,
    507.3,
    509.8,
    508.9,
    512.4,
    514.7,
    513.8,
    512.6,
    510.5,
    509.3
  ];
  const step = (endMs - startMs) / Math.max(values.length - 1, 1);

  return values.map((value, index) => ({
    tsMs: Math.round(startMs + step * index),
    value
  }));
}

function createReferenceScaffoldPoints(startMs: number, endMs: number): ReferencePricePoint[] {
  const pointCount = Math.min(MAX_REFERENCE_CHART_POINTS, 90);
  const span = Math.max(endMs - startMs, 1);

  return Array.from({ length: pointCount }, (_, index) => {
    const ratio = index / Math.max(pointCount - 1, 1);
    const drift = ratio * 92;
    const wave = Math.sin(index / 7) * 7 + Math.cos(index / 13) * 3;

    return {
      tsMs: Math.round(startMs + span * ratio),
      value: 62612.46 + drift + wave,
      source: "CF/BRTI"
    };
  });
}
