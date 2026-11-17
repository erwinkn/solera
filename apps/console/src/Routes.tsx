import { Route, Switch } from "wouter"
import { StartRunDialog } from "./blocks/start-run"
import { useLook } from "./theme/store"
import { briefing } from "./variations/briefing"
import { deck } from "./variations/deck"
import { CommandPalette, type Layout } from "./variations/shared"
import { workbench } from "./variations/workbench"

const LAYOUTS: Record<string, Layout> = { workbench, deck, briefing }

/** The same addresses in every layout: a link keeps working when the layout changes. */
export function Routes() {
  const look = useLook()
  const L = LAYOUTS[look.variation] ?? workbench
  return (
    <>
      <L.Shell>
        <Switch>
          <Route path="/" component={L.Home} />
          <Route path="/runs" component={L.Runs} />
          <Route path="/runs/:id">{(params) => <L.Run key={params.id} id={params.id} />}</Route>
          <Route path="/assets" component={L.Assets} />
          <Route path="/assets/:name">{(params) => <L.Asset key={params.name} name={params.name} />}</Route>
          <Route path="/sources/:name">{(params) => <L.Source key={params.name} name={params.name} />}</Route>
          <Route path="/automations" component={L.Automations} />
          <Route path="/sensors/:name">{(params) => <L.Sensor key={params.name} name={params.name} />}</Route>
          <Route path="/engine" component={L.Engine} />
          <Route path="/keys" component={L.Keys} />
          <Route path="/settings" component={L.Settings} />
          <Route component={L.NotFound} />
        </Switch>
      </L.Shell>
      <StartRunDialog />
      <CommandPalette />
    </>
  )
}
