import { Toast } from "@base-ui/react/toast";

/** One toast manager for the app, usable outside React (mutation callbacks). */
export const toasts = Toast.createToastManager();

export function notify(title: string, description?: string) {
  toasts.add({ title, description, type: "info" });
}

export function complain(title: string, error: unknown) {
  toasts.add({
    title,
    description: error instanceof Error ? error.message : String(error),
    type: "error",
    timeout: 8000,
  });
}
