import { createStore, useStore } from "./store";

/**
 * The API token, kept for the browser session only. `locked` turns true when
 * the server answers 401: the shell then asks for a token instead of
 * rendering the page.
 */
const KEY = "solera.token";

function read(): string | null {
  try {
    return sessionStorage.getItem(KEY);
  } catch {
    return null;
  }
}

export const session = createStore({ token: read(), locked: false });

export function connect(token: string) {
  try {
    sessionStorage.setItem(KEY, token);
  } catch {
    /* private mode: the token lives for this page only */
  }
  session.set({ token, locked: false });
}

export function disconnect() {
  try {
    sessionStorage.removeItem(KEY);
  } catch {
    /* nothing stored */
  }
  session.set({ token: null, locked: true });
}

export function lock() {
  if (!session.get().locked) session.set({ ...session.get(), locked: true });
}

export const useSession = () => useStore(session);
