import assert from "node:assert/strict";
import test from "node:test";

import {
  calculateReferencePriceDomain,
  CHART_PLOT,
  REFERENCE_OPEN_LABEL_LEFT_PERCENT,
  getFixedIntervalDomain,
  getYPercent,
  portfolioSegmentTone,
  selectFixedIntervalReferencePoints,
  type TimeValuePoint
} from "../src/lib/chart";

function point(value: number): TimeValuePoint {
  return { tsMs: 1_000, value };
}

test("reference domain includes interval open when prices are far above it", () => {
  const domain = calculateReferencePriceDomain([point(63_000), point(63_100)], 61_000);

  assert.ok(domain.min < 61_000);
  assert.ok(domain.max > 63_100);
  assert.ok(getYPercent(61_000, domain) < 82);
  assert.ok(getYPercent(61_000, domain) > 10);
});

test("reference domain includes interval open when prices are far below it", () => {
  const domain = calculateReferencePriceDomain([point(60_000), point(60_100)], 62_000);

  assert.ok(domain.min < 60_000);
  assert.ok(domain.max > 62_000);
  assert.ok(getYPercent(62_000, domain) < 82);
  assert.ok(getYPercent(62_000, domain) > 10);
});

test("portfolio segment color follows movement and starting-value plateau rules", () => {
  assert.equal(portfolioSegmentTone(500, 501, 500), "green");
  assert.equal(portfolioSegmentTone(501, 500, 500), "red");
  assert.equal(portfolioSegmentTone(500, 500, 500), "green");
  assert.equal(portfolioSegmentTone(499, 499, 500), "red");
});

test("reference open label sits outside the plotted grid", () => {
  assert.ok(REFERENCE_OPEN_LABEL_LEFT_PERCENT > CHART_PLOT.right);
  assert.ok(REFERENCE_OPEN_LABEL_LEFT_PERCENT < 100);
});

test("fixed interval reference selection keeps interval open fixed", () => {
  const nowMs = Date.parse("2026-07-05T15:08:30.000Z");
  const domain = getFixedIntervalDomain(nowMs);
  const selection = selectFixedIntervalReferencePoints(
    [
      { tsMs: domain.startMs - 1_000, value: 61_990 },
      { tsMs: domain.startMs + 1_000, value: 62_000 },
      { tsMs: domain.startMs + 120_000, value: 62_050 },
      { tsMs: domain.endMs + 1_000, value: 62_200 }
    ],
    nowMs
  );

  assert.equal(selection.domain.startMs, domain.startMs);
  assert.equal(selection.domain.endMs, domain.endMs);
  assert.equal(selection.intervalOpenPrice, 62_000);
  assert.equal(selection.currentPrice, 62_050);
  assert.deepEqual(
    selection.points.map((item) => item.value),
    [62_000, 62_050]
  );
});

test("fixed interval reference selection resets at interval boundary", () => {
  const previousNowMs = Date.parse("2026-07-05T15:14:59.000Z");
  const nextNowMs = Date.parse("2026-07-05T15:15:02.000Z");
  const nextDomain = getFixedIntervalDomain(nextNowMs);
  const selection = selectFixedIntervalReferencePoints(
    [
      { tsMs: previousNowMs, value: 62_100 },
      { tsMs: nextDomain.startMs + 1_000, value: 62_200 }
    ],
    nextNowMs
  );

  assert.equal(selection.domain.startMs, nextDomain.startMs);
  assert.equal(selection.intervalOpenPrice, 62_200);
  assert.equal(selection.currentPrice, 62_200);
  assert.deepEqual(
    selection.points.map((item) => item.value),
    [62_200]
  );
});
