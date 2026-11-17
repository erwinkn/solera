import { Button as SdkButton } from "@ui-sdk/react/button"
import { Checkbox, CheckboxIndicator } from "@ui-sdk/react/checkbox"
import { Collapsible, CollapsiblePanel, CollapsibleTrigger } from "@ui-sdk/react/collapsible"
import { Field, FieldDescription, FieldError, FieldLabel } from "@ui-sdk/react/field"
import { Input } from "@ui-sdk/react/input"
import { SegmentedControl, SegmentedControlIndicator, SegmentedControlItem } from "@ui-sdk/react/segmented-control"
import { Switch, SwitchThumb } from "@ui-sdk/react/switch"
import { Tabs as SdkTabs, TabsIndicator, TabsList, TabsPanel, TabsTab } from "@ui-sdk/react/tabs"
import { Textarea } from "@ui-sdk/react/textarea"
import { Tooltip, TooltipContent, TooltipTrigger } from "@ui-sdk/react/tooltip"
import { Check, ChevronRight } from "lucide-react"
import { useState, type ComponentProps, type CSSProperties, type ReactElement, type ReactNode } from "react"

/* The console's controls. Behaviour (keyboard, focus, ARIA) comes from the
 * UI SDK's parts; the look comes from ui.css, which reads only theme tokens. */

/** An element a UI SDK part renders as: the part adds its behaviour to it. */
export type Slot = ReactElement<Record<string, unknown>>

type ButtonProps = Omit<ComponentProps<typeof SdkButton>, "className"> & {
  variant?: "default" | "primary" | "quiet" | "danger"
  size?: "s" | "m"
  icon?: ReactNode
  className?: string
}

export function Button({ variant = "default", size = "m", icon, children, className, ...props }: ButtonProps) {
  return (
    <SdkButton className={`btn ${className ?? ""}`} data-variant={variant} data-size={size} data-icon-only={children ? undefined : ""} {...props}>
      {icon}
      {children as ReactNode}
    </SdkButton>
  )
}

/** A hint on hover and focus. It names an icon-only trigger and describes any other. */
export function Tip({ text, children, side = "top", align = "center" }: { text: ReactNode; children: Slot; side?: "top" | "bottom" | "left" | "right"; align?: "start" | "center" | "end" }) {
  return (
    <Tooltip delay={350}>
      <TooltipTrigger render={children} />
      <TooltipContent className="tip" side={side} align={align} sideOffset={6}>
        {text}
      </TooltipContent>
    </Tooltip>
  )
}

/** One hint shared by the many small marks of a chart, where a tooltip for
 * each would be hundreds of them. Call `over` as the pointer enters a mark and
 * `out` as it leaves the chart, and render `tip` anywhere inside it. */
export function useChartTip() {
  const [shown, setShown] = useState<{ content: ReactNode; style: CSSProperties } | null>(null)
  return {
    over: (mark: Element, content: ReactNode) => {
      const box = mark.getBoundingClientRect()
      setShown({ content, style: { left: box.left + box.width / 2, top: box.top } })
    },
    out: () => setShown(null),
    tip: shown && (
      <div className="tip chart-tip" style={shown.style}>
        {shown.content}
      </div>
    ),
  }
}

export function IconButton({ label, children, ...props }: Omit<ButtonProps, "children" | "icon"> & { label: string; children: ReactNode }) {
  return (
    <Tip text={label}>
      <Button variant="quiet" aria-label={label} icon={children} {...props} />
    </Tip>
  )
}

export type Option<T extends string = string> = { value: T; label: string; note?: string }

export function Segmented<T extends string>({ value, onChange, options, label }: { value: T; onChange: (value: T) => void; options: Option<T>[]; label: string }) {
  return (
    <SegmentedControl className="segmented" aria-label={label} value={value} onValueChange={(next) => next && onChange(next as T)}>
      <SegmentedControlIndicator className="segmented-indicator" />
      {options.map((option) => (
        <SegmentedControlItem key={option.value} value={option.value} className="segmented-item">
          {option.label}
        </SegmentedControlItem>
      ))}
    </SegmentedControl>
  )
}

export function Toggle({ checked, onChange, label, disabled }: { checked: boolean; onChange: (checked: boolean) => void; label: string; disabled?: boolean }) {
  return (
    <Switch className="switch" aria-label={label} checked={checked} onCheckedChange={onChange} disabled={disabled}>
      <SwitchThumb className="switch-thumb" />
    </Switch>
  )
}

export function Checkmark({ checked, onChange, children }: { checked: boolean; onChange: (checked: boolean) => void; children: ReactNode }) {
  return (
    <label className="check">
      <Checkbox className="check-box" checked={checked} onCheckedChange={(next) => onChange(next === true)}>
        <CheckboxIndicator className="check-mark" aria-hidden="true">
          <Check size={12} strokeWidth={3} />
        </CheckboxIndicator>
      </Checkbox>
      <span>{children}</span>
    </label>
  )
}

type TextProps = {
  label: string
  value: string
  onChange: (value: string) => void
  description?: ReactNode
  error?: string | null
  placeholder?: string
  mono?: boolean
  rows?: number
  type?: string
  autoFocus?: boolean
  hideLabel?: boolean
  name?: string
}

export function TextField({ label, value, onChange, description, error, mono, rows, hideLabel, ...input }: TextProps) {
  const shared = { className: `input ${mono ? "mono" : ""}`, value, "aria-invalid": error ? true : undefined, spellCheck: false, ...input }
  return (
    <Field className="field" invalid={Boolean(error)}>
      <FieldLabel className={hideLabel ? "sr-only" : "field-label"}>{label}</FieldLabel>
      {rows ? (
        <Textarea {...shared} rows={rows} onChange={(event) => onChange(event.currentTarget.value)} />
      ) : (
        <Input {...shared} onChange={(event) => onChange(event.currentTarget.value)} />
      )}
      {description && <FieldDescription className="field-note">{description}</FieldDescription>}
      {error && <FieldError className="field-error">{error}</FieldError>}
    </Field>
  )
}

export type TabSpec = { value: string; label: ReactNode; content: ReactNode }

export function Tabs({ tabs, value, onChange, label }: { tabs: TabSpec[]; value: string; onChange: (value: string) => void; label: string }) {
  return (
    <SdkTabs className="tabs" value={value} onValueChange={(next) => next && onChange(String(next))}>
      <TabsList className="tabs-list" aria-label={label}>
        {tabs.map((tab) => (
          <TabsTab key={tab.value} value={tab.value} className="tabs-tab">
            {tab.label}
          </TabsTab>
        ))}
        <TabsIndicator className="tabs-indicator" />
      </TabsList>
      {tabs.map((tab) => (
        <TabsPanel key={tab.value} value={tab.value} className="tabs-panel">
          {tab.content}
        </TabsPanel>
      ))}
    </SdkTabs>
  )
}

export function Disclosure({ summary, children, defaultOpen }: { summary: ReactNode; children: ReactNode; defaultOpen?: boolean }) {
  return (
    <Collapsible className="disclosure" defaultOpen={defaultOpen}>
      <CollapsibleTrigger className="disclosure-trigger">
        <ChevronRight size={14} className="disclosure-chevron" aria-hidden="true" />
        {summary}
      </CollapsibleTrigger>
      <CollapsiblePanel className="disclosure-panel">{children}</CollapsiblePanel>
    </Collapsible>
  )
}
