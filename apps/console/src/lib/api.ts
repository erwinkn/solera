import { useCallback, useEffect, useRef, useState } from "react";

export class ApiError extends Error {
  constructor(
    public status: number,
    message: string,
  ) {
    super(message);
  }
}

export function token() {
  return sessionStorage.getItem("solera-token");
}

export async function request<T>(
  path: string,
  options: { body?: unknown; headers?: Record<string, string> } = {},
  signal?: AbortSignal,
): Promise<T> {
  const auth = token();
  const response = await fetch(`/api${path}`, {
    method: options.body === undefined ? "GET" : "POST",
    signal,
    headers: {
      ...(auth ? { Authorization: `Bearer ${auth}` } : {}),
      ...(options.body === undefined
        ? {}
        : { "Content-Type": "application/json" }),
      ...options.headers,
    },
    body: options.body === undefined ? undefined : JSON.stringify(options.body),
  });
  const data: unknown = await response.json().catch(() => null);
  if (!response.ok) {
    const detail =
      data && typeof data === "object" && "detail" in data
        ? data.detail
        : "Request failed";
    throw new ApiError(
      response.status,
      typeof detail === "string" ? detail : JSON.stringify(detail),
    );
  }
  return data as T;
}

export async function requestText(
  path: string,
  signal?: AbortSignal,
): Promise<string> {
  const auth = token();
  const response = await fetch(`/api${path}`, {
    signal,
    headers: auth ? { Authorization: `Bearer ${auth}` } : {},
  });
  if (!response.ok) throw new ApiError(response.status, "Request failed");
  return response.text();
}

export function useQueryText(path: string | null, interval = 2000) {
  const [data, setData] = useState<string | null>(null);
  const [error, setError] = useState<Error | null>(null);
  useEffect(() => {
    if (path === null) return;
    const controller = new AbortController();
    let busy = false;
    const load = async () => {
      if (busy) return;
      busy = true;
      try {
        const value = await requestText(path, controller.signal);
        if (!controller.signal.aborted) {
          setData(value);
          setError(null);
        }
      } catch (failure) {
        if (!controller.signal.aborted)
          setError(
            failure instanceof Error ? failure : new Error(String(failure)),
          );
      } finally {
        busy = false;
      }
    };
    void load();
    const timer = window.setInterval(() => {
      void load();
    }, interval);
    return () => {
      controller.abort();
      window.clearInterval(timer);
    };
  }, [path, interval]);
  return { data, error };
}

export function useQuery<T>(path: string | null, interval = 2000) {
  const [data, setData] = useState<T | null>(null);
  const [error, setError] = useState<Error | null>(null);
  const [revision, setRevision] = useState(0);
  const refresh = useCallback(() => setRevision((value) => value + 1), []);
  const pathRef = useRef(path);
  pathRef.current = path;
  useEffect(() => {
    if (path === null) return;
    const controller = new AbortController();
    let busy = false;
    const load = async () => {
      if (busy) return;
      busy = true;
      try {
        const value = await request<T>(path, {}, controller.signal);
        if (!controller.signal.aborted && pathRef.current === path) {
          setData(value);
          setError(null);
        }
      } catch (failure) {
        if (!controller.signal.aborted)
          setError(
            failure instanceof Error ? failure : new Error(String(failure)),
          );
      } finally {
        busy = false;
      }
    };
    void load();
    const timer = window.setInterval(() => {
      void load();
    }, interval);
    return () => {
      controller.abort();
      window.clearInterval(timer);
    };
  }, [path, interval, revision]);
  return { data, error, refresh };
}

export function useAction() {
  const [pending, setPending] = useState(false);
  const [error, setError] = useState<string | null>(null);
  async function run<T>(operation: () => Promise<T>): Promise<T | undefined> {
    setPending(true);
    setError(null);
    try {
      return await operation();
    } catch (failure) {
      setError(failure instanceof Error ? failure.message : String(failure));
    } finally {
      setPending(false);
    }
  }
  return { pending, error, run };
}
