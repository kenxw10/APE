import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import path from "node:path";
import test from "node:test";

type VercelConfig = {
  framework?: string;
  buildCommand?: string;
  installCommand?: string;
  outputDirectory?: string;
};

function readVercelConfig(): VercelConfig {
  const configPath = path.resolve("vercel.json");
  return JSON.parse(readFileSync(configPath, "utf8")) as VercelConfig;
}

test("Vercel config forces the dashboard Next.js build path", () => {
  const config = readVercelConfig();

  assert.equal(config.framework, "nextjs");
  assert.equal(config.buildCommand, "npm run build");
  assert.equal(config.installCommand, "npm install");
  assert.notEqual(config.outputDirectory, "public");
  assert.equal(Object.hasOwn(config, "outputDirectory"), false);
});
