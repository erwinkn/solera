import { Menu, MenuContent, MenuGroup, MenuGroupLabel, MenuItem, MenuItemIndicator, MenuRadioGroup, MenuRadioItem, MenuSeparator, MenuTrigger } from "@ui-sdk/react/menu"
import { Check, Copy, Palette as PaletteIcon, Trash2, Upload } from "lucide-react"
import { useEffect, useRef, useState } from "react"
import { useLocation } from "wouter"
import { forgetToken, hasToken } from "../api/client"
import { BUILT_IN, starterTheme } from "../theme/themes"
import { currentTheme, paint, resolve, setLook, themes, useLook } from "../theme/store"
import { parseCustomTheme, TOKENS, TOKEN_NAMES, type CustomTheme, type Problem, type Theme } from "../theme/tokens"
import { Button, Disclosure, TextField } from "../ui/controls"
import { confirm, toast } from "../ui/overlays"
import { Notice, Panel } from "./atoms"

/* How the console looks: which theme. It is remembered in the browser and
 * changes at once, without a reload. */

/** The quick switch beside the health status. */
export function LookMenu() {
  const look = useLook()
  const [, navigate] = useLocation()
  return (
    <Menu>
      <MenuTrigger render={<Button variant="quiet" icon={<PaletteIcon size={15} />} aria-label="Change the theme" />} />
      <MenuContent className="popup menu look-menu" align="end" sideOffset={6}>
        <MenuGroup>
          <MenuGroupLabel className="menu-label">Theme</MenuGroupLabel>
          <MenuRadioGroup value={currentTheme(look).id} onValueChange={(value) => setLook({ theme: value })}>
            {themes(look).map((theme) => (
              <MenuRadioItem key={theme.id} value={theme.id} className="menu-item" closeOnSelect={false}>
                <Swatch theme={theme} />
                <span className="grow">
                  {theme.name}
                  <span className="menu-note">{theme.note}</span>
                </span>
                <MenuItemIndicator className="menu-check">
                  <Check size={14} />
                </MenuItemIndicator>
              </MenuRadioItem>
            ))}
          </MenuRadioGroup>
        </MenuGroup>
        <MenuSeparator className="menu-separator" />
        <MenuItem className="menu-item" onSelect={() => navigate("/settings")}>
          Make a theme
        </MenuItem>
      </MenuContent>
    </Menu>
  )
}

/** A theme at a glance. The swatch shows another theme's colours, so it is the
 * one place values are set inline rather than read from the page's tokens. */
function Swatch({ theme }: { theme: Theme }) {
  const t = theme.tokens
  return (
    <span className="swatch" aria-hidden="true" style={{ background: t["color-bg"], borderColor: t["color-border-strong"], borderRadius: t["radius-m"] }}>
      <span style={{ background: t["color-text"] }} />
      <span style={{ background: t["color-ok"] }} />
      <span style={{ background: t["color-running"] }} />
      <span style={{ background: t["color-failed"] }} />
    </span>
  )
}

export function LookSettings() {
  const look = useLook()
  const active = currentTheme(look)
  return (
    <div className="stack settings">
      <Panel title="Theme">
        <div className="choices" role="radiogroup" aria-label="Theme">
          {themes(look).map((theme) => (
            <button key={theme.id} role="radio" aria-checked={active.id === theme.id} className="choice" onClick={() => setLook({ theme: theme.id })}>
              <Swatch theme={theme} />
              <span className="name">{theme.name}</span>
              <span className="muted">{theme.note}</span>
            </button>
          ))}
        </div>
      </Panel>
      <CustomThemes />
      <SessionPanel />
    </div>
  )
}

function CustomThemes() {
  const look = useLook()
  const [editing, setEditing] = useState<string | "new" | null>(null)
  const edited = look.custom.find((t) => t.id === editing)
  return (
    <Panel
      title="Your themes"
      actions={
        editing === null && (
          <Button size="s" onClick={() => setEditing("new")}>
            New theme
          </Button>
        )
      }
    >
      <p className="muted settings-lead">
        Kept in this browser. THEMES.md in the console's folder lists every token.
      </p>
      {look.custom.length > 0 && editing === null && (
        <ul className="plain-list custom-list">
          {look.custom.map((theme) => (
            <li key={theme.id} className="row">
              <Swatch theme={resolve(theme)} />
              <span className="name grow">{theme.name}</span>
              <span className="muted">{Object.keys(theme.tokens).length} tokens changed</span>
              <Button size="s" onClick={() => setEditing(theme.id)}>
                Edit
              </Button>
              <Button
                size="s"
                variant="quiet"
                aria-label={`Copy ${theme.name} as JSON`}
                icon={<Copy size={13} />}
                onClick={async () => {
                  await navigator.clipboard.writeText(JSON.stringify({ name: theme.name, extends: theme.extends, tokens: theme.tokens }, null, 2))
                  toast.success("Copied as JSON")
                }}
              />
              <Button
                size="s"
                variant="quiet"
                aria-label={`Delete ${theme.name}`}
                icon={<Trash2 size={13} />}
                onClick={async () => {
                  if (!(await confirm({ title: `Delete the theme ${theme.name}?`, confirmLabel: "Delete", danger: true }))) return
                  setLook({ custom: look.custom.filter((t) => t.id !== theme.id), theme: look.theme === theme.id ? theme.extends : look.theme })
                }}
              />
            </li>
          ))}
        </ul>
      )}
      {editing !== null && <ThemeEditor key={editing} existing={edited} close={() => setEditing(null)} />}
    </Panel>
  )
}

function ThemeEditor({ existing, close }: { existing?: CustomTheme; close: () => void }) {
  const look = useLook()
  const base = BUILT_IN.find((t) => t.id === currentTheme(look).id)?.id ?? BUILT_IN[0]!.id
  const [text, setText] = useState(() => (existing ? JSON.stringify({ name: existing.name, extends: existing.extends, tokens: existing.tokens }, null, 2) : starterTheme(base)))
  const file = useRef<HTMLInputElement>(null)
  const parsed = parseCustomTheme(text, BUILT_IN.map((t) => t.id))
  const problems: Problem[] = "problems" in parsed ? parsed.problems : []
  const draft = "theme" in parsed ? parsed.theme : null

  // paint the draft as it is typed, and put the chosen theme back on the way out
  const serialized = draft ? JSON.stringify(draft) : null
  useEffect(() => {
    if (serialized) paint(resolve({ id: "draft", ...JSON.parse(serialized) }))
    return () => paint()
  }, [serialized])

  const save = () => {
    if (!draft) return
    const id = existing?.id ?? `custom-${Date.now().toString(36)}`
    const theme: CustomTheme = { id, ...draft }
    setLook({ custom: [...look.custom.filter((t) => t.id !== id), theme], theme: id })
    toast.success(`${theme.name} saved and applied`)
    close()
  }
  const fillAll = () => {
    const from = BUILT_IN.find((t) => t.id === (draft?.extends ?? base)) ?? BUILT_IN[0]!
    setText(JSON.stringify({ name: draft?.name ?? "My theme", extends: from.id, tokens: { ...from.tokens, ...draft?.tokens } }, null, 2))
  }

  return (
    <div className="stack theme-editor">
      <TextField label="Theme as JSON" value={text} onChange={setText} rows={14} mono description="Previewed as you type." />
      {problems.length > 0 && (
        <Notice tone="failed">
          <ul className="plain-list">
            {problems.map((problem) => (
              <li key={problem.path + problem.message}>
                {problem.path && <span className="mono">{problem.path}: </span>}
                {problem.message}
              </li>
            ))}
          </ul>
        </Notice>
      )}
      <div className="row wrap">
        <Button variant="primary" disabled={!draft} onClick={save}>
          Save theme
        </Button>
        <Button onClick={close}>Discard</Button>
        <span className="grow" />
        <Button variant="quiet" onClick={fillAll}>
          List every token
        </Button>
        <Button variant="quiet" icon={<Upload size={14} />} onClick={() => file.current?.click()}>
          Open a file
        </Button>
        <input
          ref={file}
          type="file"
          accept="application/json,.json"
          className="sr-only"
          tabIndex={-1}
          aria-label="Theme file"
          onChange={async (event) => {
            const chosen = event.currentTarget.files?.[0]
            if (chosen) setText(await chosen.text())
            event.currentTarget.value = ""
          }}
        />
      </div>
      <Disclosure summary={`What each of the ${TOKEN_NAMES.length} tokens does`}>
        <dl className="token-list">
          {TOKEN_NAMES.map((name) => (
            <div key={name}>
              <dt className="mono">{name}</dt>
              <dd className="muted">{TOKENS[name]}</dd>
            </div>
          ))}
        </dl>
      </Disclosure>
    </div>
  )
}

function SessionPanel() {
  if (!hasToken()) return null
  return (
    <Panel title="This session">
      <div className="row wrap">
        <span className="grow muted">The API token is kept in this tab until you close it.</span>
        <Button
          onClick={() => {
            forgetToken()
            window.location.reload()
          }}
        >
          Forget the token
        </Button>
      </div>
    </Panel>
  )
}
