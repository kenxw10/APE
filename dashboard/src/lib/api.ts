export const DEFAULT_API_BASE_URL = "https://ape-api-production.up.railway.app";

export type BackendDataProvenance = "backend-operational";

export interface SafetyResponse {
  mode: string;
  trading_enabled: boolean;
  execute: boolean;
  is_safe: boolean;
  blockers: string[];
  warnings: string[];
}

export interface HealthResponse {
  status: string;
  service: string;
  environment: string;
  app_mode: string;
  safety: SafetyResponse;
  version: string | null;
}

export interface DatabaseStatusResponse {
  status: string;
  configured: boolean;
}

export interface ReadinessResponse {
  status: string;
  ready: boolean;
  safety: SafetyResponse;
  database: DatabaseStatusResponse;
}

export interface EndpointResult<T> {
  path: string;
  ok: boolean;
  data: T | null;
  error: string | null;
}

export interface OperationalSnapshot {
  provenance: BackendDataProvenance;
  apiBaseUrl: string;
  fetchedAt: string;
  health: EndpointResult<HealthResponse>;
  safety: EndpointResult<SafetyResponse>;
  database: EndpointResult<DatabaseStatusResponse>;
  readiness: EndpointResult<ReadinessResponse>;
}

export function getApiBaseUrl(): string {
  return (process.env.NEXT_PUBLIC_API_BASE_URL || DEFAULT_API_BASE_URL).replace(/\/+$/, "");
}

async function fetchEndpoint<T>(apiBaseUrl: string, path: string): Promise<EndpointResult<T>> {
  try {
    const response = await fetch(`${apiBaseUrl}${path}`, {
      cache: "no-store",
      headers: {
        accept: "application/json"
      }
    });

    if (!response.ok) {
      return {
        path,
        ok: false,
        data: null,
        error: `${response.status} ${response.statusText}`
      };
    }

    const data = (await response.json()) as T;
    return { path, ok: true, data, error: null };
  } catch (error) {
    return {
      path,
      ok: false,
      data: null,
      error: error instanceof Error ? error.message : "Unknown API error"
    };
  }
}

export async function fetchOperationalSnapshot(): Promise<OperationalSnapshot> {
  const apiBaseUrl = getApiBaseUrl();
  const [health, safety, database, readiness] = await Promise.all([
    fetchEndpoint<HealthResponse>(apiBaseUrl, "/health"),
    fetchEndpoint<SafetyResponse>(apiBaseUrl, "/safety"),
    fetchEndpoint<DatabaseStatusResponse>(apiBaseUrl, "/db/status"),
    fetchEndpoint<ReadinessResponse>(apiBaseUrl, "/ready")
  ]);

  return {
    provenance: "backend-operational",
    apiBaseUrl,
    fetchedAt: new Date().toISOString(),
    health,
    safety,
    database,
    readiness
  };
}
