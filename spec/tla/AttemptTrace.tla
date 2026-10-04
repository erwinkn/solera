---------------------------- MODULE AttemptTrace ----------------------------
(***************************************************************************)
(* Trace validation: the control-file requests of one partition's          *)
(* attempts in a simulation run (exported by spec/tla/check-trace.py into  *)
(* AttemptTraceLog) are a behaviour of Attempt.tla.                       *)
(*                                                                         *)
(* A request: [e: "engine" | "zombie" | "worker", i: the attempt, in the  *)
(* order its file was created, c: the worker's copy, obj: "spec" |        *)
(* "control", req: "create" | "get" | "swap" | "delete", out: how it      *)
(* ended, st: the state a write wrote]. The fenced store's acquire and    *)
(* writes, and the engine's journal decisions, make no object request:    *)
(* they are steps any request may follow. TLC searches for a behaviour    *)
(* that explains every request: `NotDone` is violated when it finds one.  *)
(***************************************************************************)
EXTENDS Attempt, AttemptTraceLog

VARIABLE j   \* how many requests are explained

Ev == Trace[j + 1]
W(ev) == <<ev.i, ev.c>>

\* The steps that make no object request, of the attempt the next request
\* is about or the one before it (whose decision the next claim waits for).
Near(i) == {x \in Attempts : x = Ev.i \/ x = Ev.i - 1}
Silent ==
    \/ \E i \in Near(Ev.i) : Launch(i) \/ Abandon(i) \/ Decide(i) \/ Request(i)
    \/ Restart
    \/ \E w \in Workers : w[1] \in Near(Ev.i) /\ (Acquire(w) \/ Drain(w) \/ NothingToWrite(w) \/ Write(w))

\* The step that makes request ev, seeing what it saw.
Makes(ev) ==
    LET i == ev.i  w == W(ev) IN
    CASE ev.obj = "control" /\ ev.req = "create" /\ ev.out \in {"ok", "lost"} -> Create(i)
      [] ev.obj = "spec" /\ ev.req = "get" /\ ev.e = "worker" ->
           (ev.out = "missing" <=> i \in purged) /\ Boot(w)
      [] ev.obj = "control" /\ ev.req = "get" /\ ev.e = "worker" /\ wpc[w] = "read" ->
           (ev.out = "missing" <=> control[i] = Missing) /\ ReadControl(w)
      \* The read-back of a swap whose answer was lost, finding its own body:
      \* the swap landed, and the worker goes on.
      [] ev.obj = "control" /\ ev.req = "get" /\ ev.e = "worker" /\ wpc[w] \in {"own", "gate", "seal"}
         /\ ev.out = "ok" /\ control[i] = Writes(w) ->
           Own(w) \/ Gate(w) \/ Seal(w)
      [] ev.obj = "control" /\ ev.req = "swap" /\ ev.e = "worker" /\ ev.out \in {"ok", "refused"} ->
           /\ wpc[w] \in {"own", "gate", "seal"} /\ Writes(w).state = ev.st
           /\ (ev.out = "ok" <=> control[i] \in {wseen[w], Writes(w)})
           /\ Own(w) \/ Gate(w) \/ Seal(w)
      [] ev.obj = "control" /\ ev.req = "swap" /\ ev.e = "worker" /\ ev.out = "lost" ->
           wpc[w] \in {"own", "gate", "seal"} /\ Writes(w).state = ev.st /\ SwapUnheard(w)
      [] ev.obj = "control" /\ ev.req = "get" /\ ev.e = "engine" -> Read(i)
      [] ev.obj = "control" /\ ev.req = "swap" /\ ev.e = "engine" /\ ev.out = "ok" ->
           control[i] = eseen[i] /\ End(i)
      [] ev.obj = "control" /\ ev.req = "swap" /\ ev.e = "zombie" /\ ev.out = "ok" -> ZombieEnd(i)
      [] ev.obj = "control" /\ ev.req = "delete" -> Purge(i)
      [] OTHER -> FALSE

\* A request the spec folds into another step, or makes no step for.
Checks(ev) ==
    \/ ev.obj = "spec" /\ ev.e # "worker"                \* the engine writes the spec
    \/ ev.out \in {"error", "lost"} /\ ev.req \in {"get", "delete"}
    \/ ev.obj = "control" /\ ev.req = "get"               \* a read-back after a swap, or a look
    \/ ev.obj = "control" /\ ev.req = "swap" /\ ev.out \in {"refused", "error"} /\ ev.e # "worker"

TInit == Init /\ j = 0

TNext ==
    /\ j < Len(Trace)
    /\ \/ Silent /\ UNCHANGED j
       \/ Makes(Ev) /\ j' = j + 1
       \/ Checks(Ev) /\ UNCHANGED vars /\ j' = j + 1
    /\ IF j' > TLCGet(1) THEN TLCSet(1, j') /\ PrintT(<<"explained", j'>>) ELSE TRUE

ASSUME TLCSet(1, 0)

NotDone == j < Len(Trace)

=============================================================================
