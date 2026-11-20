import { QueryClient, QueryClientProvider, useQueryClient } from "@tanstack/react-query"
import { useState, type FormEvent } from "react"
import { hasToken, saveToken, useNeedsToken } from "./api/client"
import { ProjectContext, useDiagnostics, useMe } from "./api/queries"
import { Routes } from "./Routes"
import { Button, TextField } from "./ui/controls"
import { Overlays } from "./ui/overlays"

const client = new QueryClient({
  defaultOptions: {
    queries: {
      staleTime: 1000,
      refetchOnWindowFocus: true,
      retry: (count, error) => (error as { status?: number }).status === undefined && count < 2,
    },
  },
})

export function App() {
  return (
    <QueryClientProvider client={client}>
      <Gate />
      <Overlays />
    </QueryClientProvider>
  )
}

/** Nothing renders until the console knows which project the server runs and who it acts for, and has a token if the server wants one. */
function Gate() {
  const needsToken = useNeedsToken()
  const diagnostics = useDiagnostics()
  const me = useMe()
  if (needsToken) return <TokenPrompt />
  if (diagnostics.data && me.data)
    return (
      <ProjectContext.Provider value={diagnostics.data.project}>
        <Routes />
      </ProjectContext.Provider>
    )
  return (
    <main className="gate">
      <div className="gate-card">
        <h1 className="gate-title">Solera</h1>
        {diagnostics.isError || me.isError ? (
          <>
            <p>The console cannot reach the server.</p>
            <p className="muted">{((diagnostics.error ?? me.error) as Error).message}</p>
            <Button onClick={() => (diagnostics.isError ? diagnostics.refetch() : me.refetch())}>Try again</Button>
          </>
        ) : (
          <p className="muted">Connecting…</p>
        )}
      </div>
    </main>
  )
}

function TokenPrompt() {
  const queries = useQueryClient()
  const [token, setToken] = useState("")
  const refused = hasToken()
  const submit = (event: FormEvent) => {
    event.preventDefault()
    if (!token.trim()) return
    saveToken(token.trim())
    setToken("")
    queries.invalidateQueries()
  }
  return (
    <main className="gate">
      <form className="gate-card" onSubmit={submit}>
        <h1 className="gate-title">Solera</h1>
        <p>This server asks for a token. It is the value of SOLERA_API_TOKEN where the server runs.</p>
        <TextField
          label="API token"
          type="password"
          name="token"
          value={token}
          onChange={setToken}
          autoFocus
          error={refused ? "The server did not accept that token." : null}
          description="Kept in this browser tab until you close it."
        />
        <Button variant="primary" type="submit">
          Open the console
        </Button>
      </form>
    </main>
  )
}
