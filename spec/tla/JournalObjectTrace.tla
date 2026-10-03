------------------------- MODULE JournalObjectTrace -------------------------
(***************************************************************************)
(* Trace validation: the journal requests of one simulation run           *)
(* (exported by spec/tla/check-trace.py into JournalObjectTraceLog) are a  *)
(* behaviour of JournalObject.tla.                                         *)
(*                                                                         *)
(* Each logged request is a step of its engine that makes that request    *)
(* and sees what the code saw, or a check with no step for a request the  *)
(* spec folds into another (the read-back of a create that found its own  *)
(* object). Before each request, its engine may take the steps that make  *)
(* no request: start, begin an append or a checkpoint, close, delete      *)
(* nothing more. TLC searches for a behaviour that explains every         *)
(* request: `NotDone` is violated when it finds one.                      *)
(*                                                                         *)
(* A request: [e: the engine, req: "get" | "readback" | "swap" | "list" | *)
(* "create" | "delete", obj: "journal" | "checkpoints", n: a checkpoint's *)
(* name <<engine, k>> (for a write of the journal, the checkpoint its     *)
(* body names; else <<>>), out: "ok" | "missing" |                        *)
(* "exists" | "refused" | "lost" | "error", listed: the names a LIST      *)
(* returned].                                                              *)
(***************************************************************************)
EXTENDS JournalObject, JournalObjectTraceLog

VARIABLE i   \* how many requests are explained

Ev == Trace[i + 1]

\* A write of the journal is logged with the checkpoint its body names.
Names(e) == IF CpOf(Pending(e)) = None THEN <<>> ELSE CpOf(Pending(e))

\* The steps that make no request.
Silent(e) ==
    \/ Start(e) \/ BeginAppend(e) \/ BeginCheckpoint(e) \/ Close(e) \/ ReadOnly(e)
    \/ GC(e)
    \/ Delete(e) /\ doomed[e] = {}
    \* A checkpoint dropped with no request of its own failing: a create
    \* whose answer was lost, or a clean shutdown cancelling the flusher.
    \/ pc[e] \in {"create", "verify", "move", "delete"} /\ GiveUp(e)

\* The step of engine e that makes request ev, seeing what ev saw.
Makes(e, ev) ==
    LET n == ev.n  ok == ev.out = "ok" IN
    CASE ev.req = "get" /\ ev.obj = "journal" /\ ev.out \in {"ok", "missing"} ->
           /\ (journal = None <=> ev.out = "missing")
           /\ GetJournal(e) \/ AskStep(e)
      \* The GET that settles a swap fails: the engine writes again.
      [] ev.req = "get" /\ ev.obj = "journal" /\ ev.out \in {"lost", "error"} -> AskFails(e)
      [] ev.req = "get" /\ ev.obj = "checkpoints" /\ ev.out \in {"ok", "missing"} ->
           /\ (n \in DOMAIN checkpoints <=> ok)
           /\ \/ pc[e] = "load" /\ n = seen[e].cp /\ GetCheckpoint(e)
              \/ pc[e] = "verify" /\ n = LastCp(e) /\ VerifyCheckpoint(e)
      \* Refused: 412, or (no journal yet: a create) the object exists.
      [] ev.req = "swap" /\ ev.out \in {"ok", "refused", "exists"} ->
           /\ pc[e] \in Writing /\ n = Names(e)
           /\ (journal = seen[e] <=> ok)
           /\ Swap(e)
      [] ev.req = "swap" /\ ev.out = "lost" -> pc[e] \in Writing /\ n = Names(e) /\ SwapUnheard(e)
      \* Refused before reaching the store: like a 409, the journal unchanged.
      [] ev.req = "swap" /\ ev.out = "error" -> pc[e] \in Writing /\ n = Names(e) /\ SwapConflict(e)
      [] ev.req = "list" /\ ok ->
           /\ ev.listed = DOMAIN checkpoints
           /\ List(e)
      [] ev.req = "create" /\ ev.out \in {"ok", "lost"} ->
           /\ n = NextCp(e) /\ n \notin DOMAIN checkpoints
           /\ CreateCheckpoint(e)
      [] ev.req = "delete" /\ ev.out \in {"ok", "missing"} ->
           /\ (n \in DOMAIN checkpoints <=> ok)
           /\ pc[e] = "delete" /\ n \in doomed[e]
           /\ checkpoints' = Drop(checkpoints, n)
           /\ doomed' = [doomed EXCEPT ![e] = @ \ {n}]
           /\ UNCHANGED <<journal, torn, pc, state, seen, mine, made, listed, fence, closing, ghost>>
      [] OTHER -> FALSE

\* A request the spec makes no step for, and what it must have seen.
Checks(e, ev) ==
    CASE ev.out = "error" /\ ev.req # "swap" -> TRUE    \* it never reached the store
      [] ev.out = "lost" /\ ev.req \in {"get", "readback"} -> TRUE
      \* A create that found an object in its way reads it back: its own
      \* earlier try (a fresh name has no other writer).
      [] ev.req = "create" /\ ev.out = "exists" -> ev.n \in DOMAIN checkpoints
      [] ev.req = "readback" -> ev.n \in DOMAIN checkpoints
      [] OTHER -> FALSE

\* A failed request of a checkpoint or its cleanup ends them (GiveUp).
Fails(e, ev) ==
    \/ ev.out = "error" /\ ev.req \in {"list", "create", "get", "delete"} /\ GiveUp(e)

TInit == Init /\ i = 0

TNext ==
    /\ i < Len(Trace)
    /\ LET e == Ev.e IN
       \/ Silent(e) /\ UNCHANGED i
       \/ (Makes(e, Ev) \/ Fails(e, Ev)) /\ i' = i + 1
       \/ Checks(e, Ev) /\ UNCHANGED vars /\ i' = i + 1
    \* The furthest request explained so far, for the report.
    /\ IF i' > TLCGet(1) THEN TLCSet(1, i') /\ PrintT(<<"explained", i'>>) ELSE TRUE

ASSUME TLCSet(1, 0)

\* Violated once every request is explained: the trace is a behaviour.
NotDone == i < Len(Trace)

=============================================================================
