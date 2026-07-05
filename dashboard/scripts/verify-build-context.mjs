import { existsSync } from "node:fs";
import { basename, join } from "node:path";

const cwd = process.cwd();
const requiredFiles = ["package.json", join("src", "app", "page.tsx")];

if (basename(cwd) !== "dashboard") {
  console.error(`APE dashboard build must run from dashboard/. Current directory: ${cwd}`);
  process.exit(1);
}

for (const requiredFile of requiredFiles) {
  if (!existsSync(join(cwd, requiredFile))) {
    console.error(`APE dashboard build context missing ${requiredFile} in ${cwd}`);
    process.exit(1);
  }
}

console.log("APE_DASHBOARD_BUILD_PATH_CONFIRMED");
