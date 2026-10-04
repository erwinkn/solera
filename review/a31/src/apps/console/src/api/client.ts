import { lock, session } from "@/lib/session";

export class ApiError extends Error {
  constructor(
    readonly status: number,
    message: string,
  ) {
    super(message);
    this.name = "ApiError";
  }
}

type Query = Record<string, string | number | boolean | null | undefined | (string | number)[]>;

/** `/api…` with the query string built from `query`, dropping empty values. */
export function url(path: string, query?: Query): string {
  const params = new URLSearchParams();
  for (const [name, value] of Object.entries(query ?? {})) {
    for (const v of Array.isArray(value) ? value : [value]) {
      if (v !== undefined && v !== null && v !== "") params.append(name, String(v));
    }
  }
  const qs = params.toString();
  return `/api${path}${qs ? `?${qs}` : ""}`;
}

interface Options {
  method?: "GET" | "POST" | "DELETE";
  body?: unknown;
  query?: Query;
  signal?: AbortSignal;
  headers?: Record<string, string>;
}

async function send(path: string, { method, body, query, signal, headers }: Options): Promise<Response> {
  const token = session.get().token;
  const response = await fetch(url(path, query), {
    method: method ?? (body === undefined ? "GET" : "POST"),
    signal,
    headers: {
      ...(token ? { Authorization: `Bearer ${token}` } : {}),
      ...(body === undefined ? {} : { "Content-Type": "application/json" }),
      ...headers,
    },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  if (response.ok) return response;
  if (response.status === 401) lock();
  const data: unknown = await response.json().catch(() => null);
  const detail = data && typeof data === "object" && "detail" in data ? data.detail : null;
  throw new ApiError(
    response.status,
    typeof detail === "string"
      ? detail
      : detail
        ? JSON.stringify(detail)
        : `${response.status} ${response.statusText}`,
  );
}

export async function api<T>(path: string, options: Options = {}): Promise<T> {
  const response = await send(path, options);
  return (response.status === 204 ? null : await response.json()) as T;
}

export async function apiText(path: string, options: Options = {}): Promise<string> {
  return (await send(path, options)).text();
}
