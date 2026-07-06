import { EASTERN_TIME_ZONE } from "./time";

export const MAX_REFERENCE_CHART_POINTS = 16_000;
export const REFERENCE_CHART_WINDOW_MS = 15 * 60 * 1000;

export const CHART_PLOT = {
  left: 7,
  right: 97,
  top: 10,
  bottom: 82
} as const;

export const REFERENCE_OPEN_LABEL_LEFT_PERCENT = CHART_PLOT.right + 0.7;

export type PortfolioRange = "today" | "12h" | "1d" | "1w" | "1m" | "all";

export const PORTFOLIO_RANGE_OPTIONS: readonly { value: PortfolioRange; label: string }[] = [
  { value: "today", label: "Today" },
  { value: "12h", label: "12H" },
  { value: "1d", label: "1D" },
  { value: "1w", label: "1W" },
  { value: "1m", label: "1M" },
  { value: "all", label: "All" }
];

export interface TimeValuePoint {
  tsMs: number;
  value: number;
}

export interface ChartDomain {
  min: number;
  max: number;
}

export interface TimeDomain {
  startMs: number;
  endMs: number;
}

function finiteValues(values: readonly number[]): number[] {
  return values.filter((value) => Number.isFinite(value));
}

export function calculatePaddedDomain(
  values: readonly number[],
  extraValues: readonly number[] = []
): ChartDomain {
  const allValues = finiteValues([...values, ...extraValues]);

  if (allValues.length === 0) {
    return { min: 0, max: 1 };
  }

  let min = Math.min(...allValues);
  let max = Math.max(...allValues);

  if (min === max) {
    const basePadding = Math.max(Math.abs(min) * 0.0002, 1);
    min -= basePadding;
    max += basePadding;
  }

  const range = max - min;
  const midpoint = (max + min) / 2;
  const padding = Math.max(range * 0.08, Math.abs(midpoint) * 0.00002, 0.5);

  return {
    min: min - padding,
    max: max + padding
  };
}

export function calculateReferencePriceDomain(
  points: readonly TimeValuePoint[],
  intervalOpenPrice: number | null
): ChartDomain {
  return calculatePaddedDomain(
    points.map((point) => point.value),
    intervalOpenPrice === null ? [] : [intervalOpenPrice]
  );
}

export function getYPercent(value: number, domain: ChartDomain): number {
  const range = Math.max(domain.max - domain.min, 1);
  const plotHeight = CHART_PLOT.bottom - CHART_PLOT.top;
  const rawY = CHART_PLOT.bottom - ((value - domain.min) / range) * plotHeight;

  return Math.min(CHART_PLOT.bottom, Math.max(CHART_PLOT.top, rawY));
}

export function getXPercent(tsMs: number, domain: TimeDomain): number {
  const span = Math.max(domain.endMs - domain.startMs, 1);
  const plotWidth = CHART_PLOT.right - CHART_PLOT.left;
  const ratio = Math.min(1, Math.max(0, (tsMs - domain.startMs) / span));

  return CHART_PLOT.left + ratio * plotWidth;
}

export function createPolylinePoints(
  points: readonly TimeValuePoint[],
  yDomain: ChartDomain,
  xDomain: TimeDomain
): string {
  return points
    .map((point) => `${getXPercent(point.tsMs, xDomain).toFixed(2)},${getYPercent(point.value, yDomain).toFixed(2)}`)
    .join(" ");
}

export function capPoints<T>(points: readonly T[], maxPoints: number): T[] {
  if (points.length <= maxPoints) {
    return [...points];
  }

  const step = Math.ceil(points.length / maxPoints);
  const sampled = points.filter((_, index) => index % step === 0);
  const last = points[points.length - 1];

  if (sampled[sampled.length - 1] !== last) {
    sampled.push(last);
  }

  return sampled.slice(-maxPoints);
}

export interface FixedIntervalReferenceSelection<T extends TimeValuePoint> {
  domain: TimeDomain;
  points: T[];
  intervalOpenPrice: number | null;
  currentPrice: number | null;
}

export function getFixedIntervalDomain(
  nowMs: number,
  intervalMs = REFERENCE_CHART_WINDOW_MS
): TimeDomain {
  const span = Math.max(intervalMs, 1);
  const startMs = Math.floor(nowMs / span) * span;
  return {
    startMs,
    endMs: startMs + span
  };
}

export function selectFixedIntervalReferencePoints<T extends TimeValuePoint>(
  points: readonly T[],
  nowMs: number,
  intervalMs = REFERENCE_CHART_WINDOW_MS
): FixedIntervalReferenceSelection<T> {
  const domain = getFixedIntervalDomain(nowMs, intervalMs);
  const visibleEndMs = Math.min(nowMs, domain.endMs);
  const intervalPoints = points
    .filter((point) => point.tsMs >= domain.startMs && point.tsMs <= visibleEndMs)
    .sort((left, right) => left.tsMs - right.tsMs);
  const intervalOpen = intervalPoints[0] ?? null;
  const current = intervalPoints[intervalPoints.length - 1] ?? null;

  return {
    domain,
    points: intervalPoints,
    intervalOpenPrice: intervalOpen?.value ?? null,
    currentPrice: current?.value ?? null
  };
}

export function getPortfolioWindow(
  range: PortfolioRange,
  nowMs: number,
  portfolioStartMs: number
): TimeDomain {
  const hourMs = 60 * 60 * 1000;
  const trailingWindows: Partial<Record<PortfolioRange, number>> = {
    "12h": 12 * hourMs,
    "1d": 24 * hourMs,
    "1w": 7 * 24 * hourMs,
    "1m": 30 * 24 * hourMs
  };

  if (range === "all") {
    return { startMs: portfolioStartMs, endMs: nowMs };
  }

  if (range === "today") {
    return {
      startMs: easternMidnightMs(nowMs),
      endMs: nowMs
    };
  }

  return {
    startMs: nowMs - (trailingWindows[range] ?? 24 * hourMs),
    endMs: nowMs
  };
}

export function filterPointsForWindow(
  points: readonly TimeValuePoint[],
  domain: TimeDomain
): TimeValuePoint[] {
  const visible = points.filter((point) => point.tsMs >= domain.startMs && point.tsMs <= domain.endMs);

  if (visible.length >= 2) {
    return visible;
  }

  return points.slice(-2);
}

export function portfolioSegmentTone(
  previousValue: number,
  currentValue: number,
  startingValue: number
): "green" | "red" {
  if (currentValue > previousValue) {
    return "green";
  }

  if (currentValue < previousValue) {
    return "red";
  }

  return currentValue >= startingValue ? "green" : "red";
}

export function createTimeAxisLabels(domain: TimeDomain, count = 5): { label: string; left: number }[] {
  const labels: { label: string; left: number }[] = [];
  const labelCount = Math.max(2, count);
  const includeSeconds = domain.endMs - domain.startMs < 5 * 60 * 1000;

  for (let index = 0; index < labelCount; index += 1) {
    const ratio = index / (labelCount - 1);
    const tsMs = domain.startMs + (domain.endMs - domain.startMs) * ratio;
    labels.push({
      label: new Intl.DateTimeFormat("en-US", {
        timeZone: EASTERN_TIME_ZONE,
        hour: "numeric",
        minute: "2-digit",
        second: includeSeconds ? "2-digit" : undefined
      }).format(new Date(tsMs)),
      left: CHART_PLOT.left + ratio * (CHART_PLOT.right - CHART_PLOT.left)
    });
  }

  return labels;
}

export function createYAxisLabels(domain: ChartDomain, formatter: (value: number) => string): string[] {
  return [domain.max, (domain.max + domain.min) / 2, domain.min].map(formatter);
}

function getTimeZoneOffsetMs(date: Date, timeZone: string): number {
  const parts = new Intl.DateTimeFormat("en-US", {
    timeZone,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hourCycle: "h23"
  }).formatToParts(date);
  const values = Object.fromEntries(parts.map((part) => [part.type, part.value]));
  const zonedAsUtc = Date.UTC(
    Number(values.year),
    Number(values.month) - 1,
    Number(values.day),
    Number(values.hour),
    Number(values.minute),
    Number(values.second)
  );

  return zonedAsUtc - date.getTime();
}

function easternMidnightMs(nowMs: number): number {
  const now = new Date(nowMs);
  const parts = new Intl.DateTimeFormat("en-US", {
    timeZone: EASTERN_TIME_ZONE,
    year: "numeric",
    month: "2-digit",
    day: "2-digit"
  }).formatToParts(now);
  const values = Object.fromEntries(parts.map((part) => [part.type, part.value]));
  const utcGuess = Date.UTC(Number(values.year), Number(values.month) - 1, Number(values.day), 0, 0, 0);

  return utcGuess - getTimeZoneOffsetMs(new Date(utcGuess), EASTERN_TIME_ZONE);
}
