import { Toast } from "@base-ui/react/toast";
import { X } from "lucide-react";
import { cn } from "@/lib/cn";
import { toasts } from "@/lib/toasts";
import { StatusIcon } from "@/ui/status";

export function Toasts() {
  return (
    <Toast.Provider toastManager={toasts} limit={4}>
      <Toast.Portal>
        <Toast.Viewport className="fixed right-4 bottom-4 z-[60] flex w-[min(360px,calc(100vw-2rem))] flex-col">
          <ToastList />
        </Toast.Viewport>
      </Toast.Portal>
    </Toast.Provider>
  );
}

function ToastList() {
  const { toasts: list } = Toast.useToastManager();
  return list.map((toast) => (
    <Toast.Root
      key={toast.id}
      toast={toast}
      className={cn(
        "absolute right-0 bottom-0 w-full rounded-md border-theme border-line-strong bg-surface shadow-3",
        "[--gap:0.5rem] [--offset-y:calc(var(--toast-offset-y)*-1+var(--toast-index)*var(--gap)*-1)]",
        "z-[calc(100-var(--toast-index))] [transform:translateY(var(--offset-y))] transition-[transform,opacity] motion-2",
        "data-[starting-style]:[transform:translateY(120%)] data-[ending-style]:opacity-0 data-[limited]:opacity-0",
      )}
    >
      <Toast.Content className="flex items-start gap-2.5 p-3">
        <StatusIcon tone={toast.type === "error" ? "fail" : "ok"} className="mt-0.5 size-4" />
        <div className="flex min-w-0 flex-1 flex-col gap-0.5">
          <Toast.Title className="text-sm font-medium text-fg" />
          <Toast.Description className="text-xs break-words text-fg-muted" />
        </div>
        <Toast.Close
          aria-label="Dismiss"
          className="grid size-5 place-items-center rounded-xs text-fg-subtle hover:text-fg [&_svg]:size-3.5"
        >
          <X />
        </Toast.Close>
      </Toast.Content>
    </Toast.Root>
  ));
}
