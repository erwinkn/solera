import type { ReactElement, ReactNode } from "react";
import { AlertDialog } from "@base-ui/react/alert-dialog";
import { Dialog as BaseDialog } from "@base-ui/react/dialog";
import { Menu as BaseMenu } from "@base-ui/react/menu";
import { Popover as BasePopover } from "@base-ui/react/popover";
import { Tooltip as BaseTooltip } from "@base-ui/react/tooltip";
import { X } from "lucide-react";
import { cn } from "@/lib/cn";
import { Button, IconButton } from "./button";

const popup =
  "rounded-md border-theme border-line-strong bg-surface text-fg shadow-3 outline-none " +
  "origin-[var(--transform-origin)] transition-[transform,opacity] motion-2 " +
  "data-[starting-style]:scale-95 data-[starting-style]:opacity-0 data-[ending-style]:scale-95 data-[ending-style]:opacity-0";

const backdrop =
  "fixed inset-0 bg-overlay transition-opacity motion-2 data-[starting-style]:opacity-0 data-[ending-style]:opacity-0";

export function Tooltip({ content, children }: { content: ReactNode; children: ReactElement }) {
  return (
    <BaseTooltip.Root>
      <BaseTooltip.Trigger render={children} />
      <BaseTooltip.Portal>
        <BaseTooltip.Positioner sideOffset={6} className="z-50">
          <BaseTooltip.Popup
            className={cn(
              "max-w-80 rounded-sm bg-fg px-2 py-1 text-xs text-fg-inverse shadow-2",
              "origin-[var(--transform-origin)] transition-[transform,opacity] motion-1",
              "data-[starting-style]:scale-95 data-[starting-style]:opacity-0 data-[ending-style]:opacity-0",
            )}
          >
            {content}
          </BaseTooltip.Popup>
        </BaseTooltip.Positioner>
      </BaseTooltip.Portal>
    </BaseTooltip.Root>
  );
}

export const TooltipProvider = BaseTooltip.Provider;

export function Dialog({
  open,
  onOpenChange,
  trigger,
  title,
  description,
  children,
  className,
}: {
  open?: boolean;
  onOpenChange?: (open: boolean) => void;
  trigger?: ReactElement;
  title: ReactNode;
  description?: ReactNode;
  children: ReactNode;
  className?: string;
}) {
  return (
    <BaseDialog.Root open={open} onOpenChange={onOpenChange}>
      {trigger && <BaseDialog.Trigger render={trigger} />}
      <BaseDialog.Portal>
        <BaseDialog.Backdrop className={cn(backdrop, "z-40")} />
        <BaseDialog.Popup
          className={cn(
            popup,
            "fixed top-[8vh] left-1/2 z-50 flex max-h-[84vh] w-[min(560px,calc(100vw-2rem))] -translate-x-1/2 flex-col",
            className,
          )}
        >
          <div className="flex items-start justify-between gap-4 border-b border-line px-5 pt-4 pb-3">
            <div className="flex flex-col gap-1">
              <BaseDialog.Title className="font-display text-lg">{title}</BaseDialog.Title>
              {description && (
                <BaseDialog.Description className="text-sm text-fg-muted">
                  {description}
                </BaseDialog.Description>
              )}
            </div>
            <BaseDialog.Close render={<IconButton label="Close" />}>
              <X />
            </BaseDialog.Close>
          </div>
          <div className="min-h-0 overflow-y-auto">{children}</div>
        </BaseDialog.Popup>
      </BaseDialog.Portal>
    </BaseDialog.Root>
  );
}

export const DialogClose = BaseDialog.Close;

/** Asks before an action that can't be undone. */
export function Confirm({
  trigger,
  title,
  description,
  action,
  onConfirm,
  danger,
}: {
  trigger: ReactElement;
  title: ReactNode;
  description: ReactNode;
  action: string;
  onConfirm: () => void;
  danger?: boolean;
}) {
  return (
    <AlertDialog.Root>
      <AlertDialog.Trigger render={trigger} />
      <AlertDialog.Portal>
        <AlertDialog.Backdrop className={cn(backdrop, "z-40")} />
        <AlertDialog.Popup
          className={cn(
            popup,
            "fixed top-1/3 left-1/2 z-50 flex w-[min(440px,calc(100vw-2rem))] -translate-x-1/2 -translate-y-1/2 flex-col gap-4 p-5",
          )}
        >
          <div className="flex flex-col gap-1.5">
            <AlertDialog.Title className="font-display text-lg">{title}</AlertDialog.Title>
            <AlertDialog.Description className="text-sm text-fg-muted">{description}</AlertDialog.Description>
          </div>
          <div className="flex justify-end gap-2">
            <AlertDialog.Close render={<Button variant="ghost">Keep it</Button>} />
            <AlertDialog.Close
              render={
                <Button variant={danger ? "danger" : "primary"} onClick={onConfirm}>
                  {action}
                </Button>
              }
            />
          </div>
        </AlertDialog.Popup>
      </AlertDialog.Portal>
    </AlertDialog.Root>
  );
}

export function Menu({
  trigger,
  children,
  align = "end",
}: {
  trigger: ReactElement;
  children: ReactNode;
  align?: "start" | "end";
}) {
  return (
    <BaseMenu.Root>
      <BaseMenu.Trigger render={trigger} />
      <BaseMenu.Portal>
        <BaseMenu.Positioner sideOffset={6} align={align} className="z-50">
          <BaseMenu.Popup className={cn(popup, "min-w-44 p-1")}>{children}</BaseMenu.Popup>
        </BaseMenu.Positioner>
      </BaseMenu.Portal>
    </BaseMenu.Root>
  );
}

export function MenuItem({
  children,
  onClick,
  danger,
  icon,
}: {
  children: ReactNode;
  onClick?: () => void;
  danger?: boolean;
  icon?: ReactNode;
}) {
  return (
    <BaseMenu.Item
      onClick={onClick}
      className={cn(
        "flex h-8 cursor-default items-center gap-2 rounded-sm px-2.5 text-sm outline-none select-none [&_svg]:size-3.5",
        "data-[highlighted]:bg-accent-soft data-[disabled]:opacity-50",
        danger ? "text-fail-fg" : "text-fg",
      )}
    >
      {icon}
      {children}
    </BaseMenu.Item>
  );
}

export function Popover({
  trigger,
  children,
  className,
}: {
  trigger: ReactElement;
  children: ReactNode;
  className?: string;
}) {
  return (
    <BasePopover.Root>
      <BasePopover.Trigger render={trigger} />
      <BasePopover.Portal>
        <BasePopover.Positioner sideOffset={6} align="start" className="z-50">
          <BasePopover.Popup className={cn(popup, "p-3", className)}>{children}</BasePopover.Popup>
        </BasePopover.Positioner>
      </BasePopover.Portal>
    </BasePopover.Root>
  );
}
