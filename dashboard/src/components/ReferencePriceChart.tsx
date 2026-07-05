"use client";

import {
  CHART_PLOT,
  MAX_REFERENCE_CHART_POINTS,
  calculateReferencePriceDomain,
  capPoints,
  createPolylinePoints,
  createTimeAxisLabels,
  getYPercent
} from "../lib/chart";
import type { ReferencePricePoint } from "../lib/scaffold-data";
import { formatEasternTime } from "../lib/time";
import { Grid } from "./PortfolioValueChart";

interface ReferencePriceChartProps {
  points: readonly ReferencePricePoint[];
  intervalOpenPrice: number;
  intervalStartMs: number;
  intervalEndMs: number;
  note: string;
}

export function ReferencePriceChart({
  points,
  intervalOpenPrice,
  intervalStartMs,
  intervalEndMs,
  note
}: ReferencePriceChartProps) {
  const cappedPoints = capPoints(points, MAX_REFERENCE_CHART_POINTS);
  const xDomain = { startMs: intervalStartMs, endMs: intervalEndMs };
  const yDomain = calculateReferencePriceDomain(cappedPoints, intervalOpenPrice);
  const openY = getYPercent(intervalOpenPrice, yDomain);
  const yLabels = [yDomain.max, (yDomain.max + yDomain.min) / 2, yDomain.min].map((value) =>
    formatUsd(value, 0)
  );
  const xLabels = createTimeAxisLabels(xDomain, 4);
  const linePoints = createPolylinePoints(cappedPoints, yDomain, xDomain);
  const latest = cappedPoints[cappedPoints.length - 1] ?? null;

  return (
    <section className="panel reference-panel">
      <div className="panel-heading">
        <div>
          <h2>REFERENCE PRICE (USD) - CF/BRTI</h2>
          <div className="reference-legend">
            <span>
              <i className="legend-gold" /> CF/BRTI
            </span>
            <small>Rolling 15-minute window. Max {MAX_REFERENCE_CHART_POINTS} points.</small>
          </div>
        </div>
        <strong className="latest-price">{latest ? formatUsd(latest.value, 2) : "--"}</strong>
      </div>
      <div className="chart-shell reference-chart" aria-label="CF/BRTI reference price scaffold chart">
        <svg viewBox="0 0 100 100" preserveAspectRatio="none" role="img">
          <Grid />
          <line
            x1={CHART_PLOT.left}
            x2={CHART_PLOT.right}
            y1={openY}
            y2={openY}
            className="chart-open-line"
            vectorEffect="non-scaling-stroke"
          />
          <polyline
            points={linePoints}
            className="chart-line chart-line-gold"
            vectorEffect="non-scaling-stroke"
          />
        </svg>
        <div className="chart-y-axis" aria-hidden="true">
          {yLabels.map((label) => (
            <span key={label}>{label}</span>
          ))}
        </div>
        <div className="chart-x-axis" aria-hidden="true">
          {xLabels.map((label) => (
            <span key={`${label.label}:${label.left}`} style={{ left: `${label.left}%` }}>
              {label.label}
            </span>
          ))}
        </div>
        <span className="open-label" style={{ top: `${openY}%` }}>
          Open
        </span>
      </div>
      <p className="chart-note">
        {formatEasternTime(intervalStartMs)}-{formatEasternTime(intervalEndMs)} ET. {note}
      </p>
    </section>
  );
}

function formatUsd(value: number, maximumFractionDigits: number): string {
  return new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: "USD",
    maximumFractionDigits
  }).format(value);
}
