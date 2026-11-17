import { useSyncExternalStore } from "react"

/* One fetch wrapper for the whole console. It adds the token, turns an error
 * body into a message, and raises the token prompt when the server says 401. */

const TOKEN = "solera.console.token"

export class ApiError extends Error {
  constructor(
    readonly status: number,
    message: string,
  ) {
    super(message)
  }
}

let needsToken = false
const listeners = new Set<() => void>()
function setNeedsToken(value: boolean) {
  if (needsToken === value) return
  needsToken = value
  for (const listener of listeners) listener()
}

/** True while the server wants a token the console does not have (or has wrong). */
export function useNeedsToken(): boolean {
  return useSyncExternalStore(
    (listener) => {
      listeners.add(listener)
      return () => listeners.delete(listener)
    },
    () => needsToken,
  )
}

export function hasToken(): boolean {
  return sessionStorage.getItem(TOKEN) !== null
}

/** Keep the token for this browser session and let the waiting requests retry. */
export function saveToken(token: string) {
  sessionStorage.setItem(TOKEN, token)
  setNeedsToken(false)
}

export function forgetToken() {
  sessionStorage.removeItem(TOKEN)
}

export async function request(path: string, init: RequestInit = {}): Promise<Response> {
  const token = sessionStorage.getItem(TOKEN)
  const headers = new Headers(init.headers)
  if (token) headers.set("Authorization", `Bearer ${token}`)
  if (init.body) headers.set("Content-Type", "application/json")
  const response = await fetch(path, { ...init, headers })
  if (response.status === 401) {
    setNeedsToken(true)
    throw new ApiError(401, "Authentication required")
  }
  if (!response.ok) {
    let message = `${response.status} ${response.statusText}`
    try {
      const body = await response.json()
      if (typeof body.detail === "string") message = body.detail
      else if (Array.isArray(body.detail)) message = body.detail.map((d: { msg?: string }) => d.msg).join("; ")
    } catch {
      // not JSON: the status line will do
    }
    throw new ApiError(response.status, message)
  }
  return response
}

export async function get<T>(path: string, signal?: AbortSignal): Promise<T> {
  return (await request(path, { signal })).json()
}

export async function post<T>(path: string, body?: unknown): Promise<T> {
  const response = await request(path, { method: "POST", body: body === undefined ? undefined : JSON.stringify(body) })
  return response.status === 204 ? (undefined as T) : response.json()
}

/** A query string from the fields that are set; arrays repeat the field. */
export function query(fields: Record<string, string | number | boolean | null | undefined | string[]>): string {
  const params = new URLSearchParams()
  for (const [name, value] of Object.entries(fields)) {
    if (value === null || value === undefined || value === "") continue
    if (Array.isArray(value)) for (const v of value) params.append(name, v)
    else params.set(name, String(value))
  }
  const text = params.toString()
  return text ? `?${text}` : ""
}
