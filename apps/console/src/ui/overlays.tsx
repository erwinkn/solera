import {
  AlertDialog,
  AlertDialogBackdrop,
  AlertDialogClose,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogTitle,
  createConfirm,
} from "@ui-sdk/react/alert-dialog"
import { Command, CommandEmpty, CommandGroup, CommandGroupLabel, CommandInput, CommandItem, CommandList } from "@ui-sdk/react/command"
import { Dialog, DialogBackdrop, DialogClose, DialogContent, DialogDescription, DialogTitle } from "@ui-sdk/react/dialog"
import { Drawer, DrawerBackdrop, DrawerClose, DrawerContent, DrawerTitle } from "@ui-sdk/react/drawer"
import { Menu, MenuContent, MenuItem, MenuSeparator, MenuTrigger } from "@ui-sdk/react/menu"
import { Select, SelectContent, SelectItem, SelectItemIndicator, SelectTrigger, SelectValue } from "@ui-sdk/react/select"
import { Toast, ToastClose, ToastDescription, ToastRegion, ToastTitle, toast } from "@ui-sdk/react/toast"
import { Check, ChevronsUpDown, Search, X } from "lucide-react"
import { Fragment, type ReactNode } from "react"
import type { Option, Slot } from "./controls"

export { toast }

/* -- menu --------------------------------------------------------------------- */

export type Action = { value: string; label: string; icon?: ReactNode; danger?: boolean; disabled?: boolean; note?: string; run: () => unknown }

/** A button that opens a list of actions. `null` in the list draws a separator. */
export function ActionMenu({ trigger, actions, align = "end" }: { trigger: Slot; actions: (Action | null)[]; align?: "start" | "end" }) {
  const byValue = new Map(actions.filter((a): a is Action => a !== null).map((a) => [a.value, a]))
  return (
    <Menu onAction={(value) => byValue.get(value)?.run()}>
      <MenuTrigger render={trigger} />
      <MenuContent className="popup menu" align={align} sideOffset={6}>
        {actions.map((action, i) =>
          action === null ? (
            <MenuSeparator key={`separator-${i}`} className="menu-separator" />
          ) : (
            <MenuItem key={action.value} value={action.value} className="menu-item" destructive={action.danger} disabled={action.disabled}>
              {action.icon}
              <span className="grow">{action.label}</span>
              {action.note && <span className="faint">{action.note}</span>}
            </MenuItem>
          ),
        )}
      </MenuContent>
    </Menu>
  )
}

/* -- select ------------------------------------------------------------------- */

type PickProps<T extends string> = { label: string; options: Option<T>[]; placeholder?: string; size?: "s" | "m" }

function PickList<T extends string>({ options }: { options: Option<T>[] }) {
  return (
    <SelectContent className="popup menu" sideOffset={6}>
      {options.map((option) => (
        <SelectItem key={option.value} value={option.value} className="menu-item">
          <span className="grow">
            {option.label}
            {option.note && <span className="menu-note">{option.note}</span>}
          </span>
          <SelectItemIndicator className="menu-check">
            <Check size={14} />
          </SelectItemIndicator>
        </SelectItem>
      ))}
    </SelectContent>
  )
}

export function Pick<T extends string>({ value, onChange, label, options, placeholder, size = "m" }: PickProps<T> & { value: T | null; onChange: (value: T) => void }) {
  return (
    <Select items={options} value={value} onValueChange={(next) => next !== null && onChange(next as T)} placeholder={placeholder}>
      <SelectTrigger className="btn pick" data-size={size} aria-label={label}>
        <SelectValue className="truncate" />
        <ChevronsUpDown size={13} className="faint" aria-hidden="true" />
      </SelectTrigger>
      <PickList options={options} />
    </Select>
  )
}

export function PickMany<T extends string>({ value, onChange, label, options, placeholder, size = "m" }: PickProps<T> & { value: T[]; onChange: (value: T[]) => void }) {
  return (
    <Select multiple items={options} value={value} onValueChange={(next) => onChange((next ?? []) as T[])} placeholder={placeholder}>
      <SelectTrigger className="btn pick" data-size={size} aria-label={label}>
        <SelectValue className="truncate">
          {(state) => (state.placeholder ? (placeholder ?? label) : state.labels.length > 2 ? `${state.labels[0]} and ${state.labels.length - 1} more` : state.labels.join(", "))}
        </SelectValue>
        <ChevronsUpDown size={13} className="faint" aria-hidden="true" />
      </SelectTrigger>
      <PickList options={options} />
    </Select>
  )
}

/* -- dialogs ------------------------------------------------------------------ */

type ModalProps = { open: boolean; onOpenChange: (open: boolean) => void; title: string; description?: ReactNode; children: ReactNode; footer?: ReactNode; wide?: boolean; /** A selector for what takes focus when it opens. Without it, the first control does. */ focus?: string }

export function Modal({ open, onOpenChange, title, description, children, footer, wide, focus }: ModalProps) {
  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogBackdrop className="backdrop" />
      <DialogContent className="dialog" data-wide={wide ? "" : undefined} initialFocus={focus ?? "auto"}>
        <header className="dialog-head">
          <div className="grow">
            <DialogTitle className="dialog-title">{title}</DialogTitle>
            {description && <DialogDescription className="muted">{description}</DialogDescription>}
          </div>
          <DialogClose className="btn" data-variant="quiet" data-icon-only="" aria-label="Close">
            <X size={15} />
          </DialogClose>
        </header>
        <div className="dialog-body">{children}</div>
        {footer && <footer className="dialog-foot">{footer}</footer>}
      </DialogContent>
    </Dialog>
  )
}

/** A panel that slides in from an edge: the menu and the inspector on a phone. */
export function Sheet({ open, onOpenChange, title, side, children }: { open: boolean; onOpenChange: (open: boolean) => void; title: string; side: "left" | "right" | "bottom"; children: ReactNode }) {
  return (
    <Drawer open={open} onOpenChange={onOpenChange} side={side}>
      <DrawerBackdrop className="backdrop" />
      <DrawerContent className="sheet" data-edge={side}>
        <header className="dialog-head">
          <DrawerTitle className="dialog-title grow">{title}</DrawerTitle>
          <DrawerClose className="btn" data-variant="quiet" data-icon-only="" aria-label="Close">
            <X size={15} />
          </DrawerClose>
        </header>
        <div className="sheet-body">{children}</div>
      </DrawerContent>
    </Drawer>
  )
}

type Question = { title: string; description?: ReactNode; confirmLabel: string; danger?: boolean }
const ask = createConfirm<Question>()

/** Ask before something that cannot be undone. Resolves true on yes. */
export const confirm = (question: Question) => ask(question)

/** Mounted once at the root: the confirm dialog and the toasts. */
export function Overlays() {
  return (
    <>
      <AlertDialog controller={ask.controller}>
        <AlertDialogBackdrop className="backdrop" />
        <AlertDialogContent<Question> className="dialog" data-alert="">
          {(state) => (
            <>
              <div className="dialog-body stack">
                <AlertDialogTitle className="dialog-title">{state.payload?.title}</AlertDialogTitle>
                {state.payload?.description && <AlertDialogDescription className="muted">{state.payload.description}</AlertDialogDescription>}
              </div>
              <footer className="dialog-foot">
                <AlertDialogClose className="btn" data-variant="default" data-size="m">
                  Go back
                </AlertDialogClose>
                <AlertDialogClose className="btn" data-variant={state.payload?.danger ? "danger" : "primary"} data-size="m" value="confirm">
                  {state.payload?.confirmLabel}
                </AlertDialogClose>
              </footer>
            </>
          )}
        </AlertDialogContent>
      </AlertDialog>
      <ToastRegion className="toasts">
        {() => (
          <Toast className="toast">
            <div className="grow">
              <ToastTitle className="toast-title" />
              <ToastDescription className="muted" />
            </div>
            <ToastClose className="btn" data-variant="quiet" data-icon-only="" data-size="s" aria-label="Dismiss">
              <X size={14} />
            </ToastClose>
          </Toast>
        )}
      </ToastRegion>
    </>
  )
}

/** Run an action and say how it went. The message of a refusal is the engine's own. */
export async function report<T>(work: Promise<T>, done: string | ((result: T) => string)): Promise<T | undefined> {
  try {
    const result = await work
    toast.success(typeof done === "string" ? done : done(result))
    return result
  } catch (error) {
    toast.error("That did not work", { description: (error as Error).message })
    return undefined
  }
}

/* -- command palette ---------------------------------------------------------- */

export type Command_ = { value: string; label: string; group: string; note?: string; keywords?: string[]; icon?: ReactNode; run: () => void }

export function Palette({ open, onOpenChange, commands }: { open: boolean; onOpenChange: (open: boolean) => void; commands: Command_[] }) {
  const groups = [...new Set(commands.map((c) => c.group))]
  const byValue = new Map(commands.map((c) => [c.value, c]))
  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogBackdrop className="backdrop" />
      <DialogContent className="dialog palette">
        <DialogTitle className="sr-only">Go to or do anything</DialogTitle>
        <Command
          onAction={(value) => {
            onOpenChange(false)
            byValue.get(value)?.run()
          }}
        >
          <div className="palette-input">
            <Search size={16} className="faint" aria-hidden="true" />
            <CommandInput className="grow" aria-label="Search pages, assets, runs and actions" placeholder="Search pages, assets and actions" />
            <kbd className="kbd">esc</kbd>
          </div>
          <CommandEmpty className="palette-empty muted">Nothing matches.</CommandEmpty>
          <CommandList className="palette-list">
            {groups.map((group) => (
              <Fragment key={group}>
                <CommandGroup>
                  <CommandGroupLabel className="menu-label">{group}</CommandGroupLabel>
                  {commands
                    .filter((c) => c.group === group)
                    .map((c) => (
                      <CommandItem key={c.value} value={c.value} label={c.label} keywords={c.keywords} className="menu-item">
                        {c.icon}
                        <span className="grow truncate">{c.label}</span>
                        {c.note && <span className="faint">{c.note}</span>}
                      </CommandItem>
                    ))}
                </CommandGroup>
              </Fragment>
            ))}
          </CommandList>
        </Command>
      </DialogContent>
    </Dialog>
  )
}
