export const EASTERN_TIME_ZONE = "America/New_York";

function easternFormatter(options: Intl.DateTimeFormatOptions): Intl.DateTimeFormat {
  return new Intl.DateTimeFormat("en-US", {
    timeZone: EASTERN_TIME_ZONE,
    ...options
  });
}

export function formatEasternTime(tsMs: number, includeSeconds = false): string {
  return easternFormatter({
    hour: "numeric",
    minute: "2-digit",
    second: includeSeconds ? "2-digit" : undefined
  }).format(new Date(tsMs));
}

export function formatEasternDateTime(value: string | number | Date | null | undefined): string {
  if (value === null || value === undefined) {
    return "--";
  }

  const date = value instanceof Date ? value : new Date(value);
  if (Number.isNaN(date.getTime())) {
    return "--";
  }

  return easternFormatter({
    month: "short",
    day: "2-digit",
    year: "numeric",
    hour: "numeric",
    minute: "2-digit",
    timeZoneName: "short"
  }).format(date);
}

export function formatEasternDateKey(date: Date = new Date()): string {
  const parts = new Intl.DateTimeFormat("en-CA", {
    timeZone: EASTERN_TIME_ZONE,
    year: "numeric",
    month: "2-digit",
    day: "2-digit"
  }).formatToParts(date);
  const values = Object.fromEntries(parts.map((part) => [part.type, part.value]));

  return `${values.year}-${values.month}-${values.day}`;
}

export function shiftDateKey(dateKey: string, days: number): string {
  const [year, month, day] = dateKey.split("-").map(Number);
  const shifted = new Date(Date.UTC(year, month - 1, day + days, 12, 0, 0));

  return shifted.toISOString().slice(0, 10);
}

export function formatDateButton(dateKey: string): string {
  const [year, month, day] = dateKey.split("-").map(Number);
  const date = new Date(Date.UTC(year, month - 1, day, 12, 0, 0));

  if (Number.isNaN(date.getTime())) {
    return dateKey.toUpperCase();
  }

  return new Intl.DateTimeFormat("en-US", {
    timeZone: "UTC",
    month: "short",
    day: "2-digit",
    year: "numeric"
  })
    .format(date)
    .toUpperCase();
}

export function currentEasternInterval(nowMs: number, intervalMinutes = 15) {
  const intervalMs = intervalMinutes * 60 * 1000;
  const startMs = Math.floor(nowMs / intervalMs) * intervalMs;
  const endMs = startMs + intervalMs;

  return { startMs, endMs };
}
