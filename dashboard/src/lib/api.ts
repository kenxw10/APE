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

export interface WebSocketStatusResponse {
  configured: boolean;
  enabled: boolean;
  signer_ready: boolean;
  endpoint_host: string;
  endpoint_path: string;
  connection_state: string;
  active_market_ticker: string | null;
  subscribed_channels: string[];
  subscription_ids: Record<string, number>;
  last_connected_at: string | null;
  last_message_at: string | null;
  last_ticker_at: string | null;
  last_orderbook_at: string | null;
  last_trade_at: string | null;
  latest_orderbook_received_at: string | null;
  latest_trade_received_at: string | null;
  reconnect_count: number;
  last_error_type: string | null;
  last_error_message: string | null;
  warnings: string[];
  blockers: string[];
  stale: boolean;
  checked_at: string;
}

export interface BrtiReferenceStatusResponse {
  configured: boolean;
  enabled: boolean;
  signer_ready: boolean;
  source: string;
  index_ids: string[];
  subscription_id: number | null;
  connection_state: string;
  latest_tick_received_at: string | null;
  latest_source_ts: string | null;
  latest_parsed_value: string | number | null;
  latest_trailing_60s_avg: string | number | null;
  latest_trailing_60s_window_size: number | null;
  latest_final_minute_average: string | number | null;
  final_minute_average_status: string | null;
  source_age_ms: number | null;
  stale: boolean;
  last_message_at: string | null;
  last_persisted_at: string | null;
  last_error_type: string | null;
  last_error_message: string | null;
  warnings: string[];
  blockers: string[];
  checked_at: string;
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
  wsStatus: EndpointResult<WebSocketStatusResponse>;
  brtiStatus: EndpointResult<BrtiReferenceStatusResponse>;
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
  const [health, safety, database, readiness, wsStatus, brtiStatus] = await Promise.all([
    fetchEndpoint<HealthResponse>(apiBaseUrl, "/health"),
    fetchEndpoint<SafetyResponse>(apiBaseUrl, "/safety"),
    fetchEndpoint<DatabaseStatusResponse>(apiBaseUrl, "/db/status"),
    fetchEndpoint<ReadinessResponse>(apiBaseUrl, "/ready"),
    fetchEndpoint<WebSocketStatusResponse>(apiBaseUrl, "/ws/status"),
    fetchEndpoint<BrtiReferenceStatusResponse>(apiBaseUrl, "/reference/brti/status")
  ]);

  return {
    provenance: "backend-operational",
    apiBaseUrl,
    fetchedAt: new Date().toISOString(),
    health,
    safety,
    database,
    readiness,
    wsStatus,
    brtiStatus
  };
}
