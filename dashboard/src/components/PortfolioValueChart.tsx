"use client";

import {
  CHART_PLOT,
  PORTFOLIO_RANGE_OPTIONS,
  calculatePaddedDomain,
  createTimeAxisLabels,
  filterPointsForWindow,
  getPortfolioWindow,
  getXPercent,
  getYPercent,
  portfolioSegmentTone,
  type PortfolioRange,
  type TimeValuePoint
} from "../lib/chart";

interface PortfolioValueChartProps {
  points: readonly TimeValuePoint[];
  openedAtMs: number;
  startValue: number;
  currentValue: number | null;
  plPercent: number | null;
  note: string;
  range: PortfolioRange;
  onRangeChange: (range: PortfolioRange) => void;
  nowMs: number;
}

export function PortfolioValueChart({
  points,
  openedAtMs,
  startValue,
  currentValue,
  plPercent,
  note,
  range,
  onRangeChange,
  nowMs
}: PortfolioValueChartProps) {
  const windowDomain = getPortfolioWindow(range, nowMs, openedAtMs);
  const visiblePoints = filterPointsForWindow(points, windowDomain);
  const effectiveDomain = visiblePoints.length >= 2 ? windowDomain : {
    startMs: visiblePoints[0]?.tsMs ?? openedAtMs,
    endMs: visiblePoints[visiblePoints.length - 1]?.tsMs ?? nowMs
  };
  const yDomain = calculatePaddedDomain(
    visiblePoints.map((point) => point.value),
    [startValue]
  );
  const yLabels = createPortfolioYAxisLabels(yDomain);
  const xLabels = createTimeAxisLabels(effectiveDomain, 5);
  const startY = getYPercent(startValue, yDomain);
  const formattedValue = currentValue === null ? "--" : formatCurrency(currentValue);
  const formattedPercent = plPercent === null ? "--" : `${plPercent >= 0 ? "+" : ""}${plPercent.toFixed(2)}%`;

  return (
    <section className="panel portfolio-panel">
      <div className="panel-heading portfolio-heading">
        <div>
          <h2>PORTFOLIO VALUE</h2>
          <div className="metric-inline">
            <strong>{formattedValue}</strong>
            <span className="tone-muted">{formattedPercent}</span>
          </div>
        </div>
        <div className="segmented-controls" aria-label="Portfolio chart range">
          {PORTFOLIO_RANGE_OPTIONS.map((option) => (
            <button
              key={option.value}
              type="button"
              className={option.value === range ? "active" : undefined}
              onClick={() => onRangeChange(option.value)}
            >
              {option.label}
            </button>
          ))}
        </div>
      </div>
      <div className="chart-shell portfolio-chart" aria-label="Portfolio value scaffold chart">
        <svg viewBox="0 0 100 100" preserveAspectRatio="none" role="img">
          <Grid />
          <line
            x1={CHART_PLOT.left}
            x2={CHART_PLOT.right}
            y1={startY}
            y2={startY}
            className="chart-guide-line"
            vectorEffect="non-scaling-stroke"
          />
          {visiblePoints.slice(1).map((point, index) => {
            const previous = visiblePoints[index];
            const tone = portfolioSegmentTone(previous.value, point.value, startValue);
            const pointsAttr = [
              `${getXPercent(previous.tsMs, effectiveDomain).toFixed(2)},${getYPercent(previous.value, yDomain).toFixed(2)}`,
              `${getXPercent(point.tsMs, effectiveDomain).toFixed(2)},${getYPercent(point.value, yDomain).toFixed(2)}`
            ].join(" ");

            return (
              <polyline
                key={`${point.tsMs}:${point.value}`}
                points={pointsAttr}
                className={`chart-line chart-line-${tone}`}
                vectorEffect="non-scaling-stroke"
              />
            );
          })}
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
        <span className="start-label" style={{ top: `${startY}%` }}>
          Start {formatCurrency(startValue)}
        </span>
      </div>
      <p className="chart-note">{note}</p>
    </section>
  );
}

export function Grid() {
  const horizontalLines = [CHART_PLOT.top, (CHART_PLOT.top + CHART_PLOT.bottom) / 2, CHART_PLOT.bottom];
  const verticalLines = [
    CHART_PLOT.left + (CHART_PLOT.right - CHART_PLOT.left) / 3,
    CHART_PLOT.left + ((CHART_PLOT.right - CHART_PLOT.left) * 2) / 3
  ];

  return (
    <g className="chart-grid">
      {horizontalLines.map((y) => (
        <line
          key={`y:${y}`}
          x1={CHART_PLOT.left}
          x2={CHART_PLOT.right}
          y1={y}
          y2={y}
          vectorEffect="non-scaling-stroke"
        />
      ))}
      {verticalLines.map((x) => (
        <line
          key={`x:${x}`}
          x1={x}
          x2={x}
          y1={CHART_PLOT.top}
          y2={CHART_PLOT.bottom}
          vectorEffect="non-scaling-stroke"
        />
      ))}
    </g>
  );
}

function createPortfolioYAxisLabels(domain: { min: number; max: number }): string[] {
  return [domain.max, (domain.max + domain.min) / 2, domain.min].map((value) =>
    new Intl.NumberFormat("en-US", {
      style: "currency",
      currency: "USD",
      maximumFractionDigits: 0
    }).format(value)
  );
}

function formatCurrency(value: number): string {
  return new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: "USD",
    minimumFractionDigits: 2,
    maximumFractionDigits: 2
  }).format(value);
}
