import assert from "node:assert/strict";
import test from "node:test";

import {
  calculateReferencePriceDomain,
  getYPercent,
  portfolioSegmentTone,
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
