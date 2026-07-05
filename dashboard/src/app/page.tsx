import { DashboardShell } from "../components/DashboardShell";
import { fetchOperationalSnapshot } from "../lib/api";
import { createScaffoldDashboardData } from "../lib/scaffold-data";

export const dynamic = "force-dynamic";

export default async function Page() {
  const snapshot = await fetchOperationalSnapshot();
  const scaffold = createScaffoldDashboardData(snapshot.fetchedAt);

  return <DashboardShell snapshot={snapshot} scaffold={scaffold} />;
}
