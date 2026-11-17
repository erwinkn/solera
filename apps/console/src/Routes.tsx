import { Route, Switch } from "wouter"
import { StartRunDialog } from "./blocks/start-run"
import * as P from "./pages"
import { CommandPalette } from "./pages/chrome"

export function Routes() {
  return (
    <>
      <P.Shell>
        <Switch>
          <Route path="/" component={P.Home} />
          <Route path="/runs" component={P.Runs} />
          <Route path="/runs/:id">{(params) => <P.Run key={params.id} id={params.id} />}</Route>
          <Route path="/assets" component={P.Assets} />
          <Route path="/assets/:name">{(params) => <P.Asset key={params.name} name={params.name} />}</Route>
          <Route path="/sources/:name">{(params) => <P.Source key={params.name} name={params.name} />}</Route>
          <Route path="/automations" component={P.Automations} />
          <Route path="/sensors/:name">{(params) => <P.Sensor key={params.name} name={params.name} />}</Route>
          <Route path="/engine" component={P.Engine} />
          <Route path="/keys" component={P.Keys} />
          <Route path="/settings" component={P.Settings} />
          <Route component={P.NotFound} />
        </Switch>
      </P.Shell>
      <StartRunDialog />
      <CommandPalette />
    </>
  )
}
