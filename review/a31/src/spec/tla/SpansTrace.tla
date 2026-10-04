----------------------------- MODULE SpansTrace -----------------------------
(***************************************************************************)
(* Trace validation: one key index's lifecycle in a simulation run         *)
(* (exported by spec/tla/check-trace.py into SpansTraceLog) is a behaviour *)
(* of Spans.tla, the code as built.                                        *)
(*                                                                         *)
(* A record: [act: what happened, e: "serving" | "zombie" | "worker", c:  *)
(* the consumer, k: the claim's number, land: its landing point, pass: a  *)
(* pass's batch, id: the span (Spans.tla's file number), ids: a merge's   *)
(* inputs, b: a commit's last commit, empty: it has no files, bounds: the *)
(* segment starts a merge kept, any: a merge never published, whose       *)
(* starts are unknown, ok: a read found the file]. check-trace.py decides *)
(* each step's parameters from the journal and the requests, so the       *)
(* search is a walk: a merge may keep more starts than the endpoints this *)
(* spec knows (a pattern change's), never fewer.                           *)
(***************************************************************************)
EXTENDS Spans, SpansTraceLog, TLC

VARIABLE j   \* how many records are explained

Ev == Trace[j + 1]

Upload(ev) ==
    LET ids == ev.ids IN
    /\ nfile + 1 = ev.id
    /\ IF ev.any
       THEN \E bs \in SUBSET Inner(ids) : Required(ids) \subseteq bs /\ StartMergeWith(ids, bs)
       ELSE Required(ids) \subseteq ev.bounds /\ ev.bounds \subseteq Inner(ids) /\ StartMergeWith(ids, ev.bounds)

Ends(ev, Step(_, _)) == \E x \in att[ev.c] : x.k = ev.k /\ Step(ev.c, x)

Makes(ev) ==
    CASE ev.act = "commit" -> nfile + 1 = ev.id /\ CommitTo(ev.b, ev.empty)
      [] ev.act = "claim" ->
           Claim(ev.c) /\ \E x \in att'[ev.c] : x.k = ev.k /\ x.land = ev.land /\ x.pass = ev.pass
      [] ev.act = "settle" -> Ends(ev, Settle)
      [] ev.act = "batch" -> Ends(ev, Batch)
      [] ev.act = "fail" -> Ends(ev, Fail)
      [] ev.act = "upload" -> Upload(ev)
      [] ev.act = "publish" -> \E x \in jobs : x.out = ev.id /\ PublishMem(x) /\ ev.id \in mem'
      [] ev.act = "flush" -> Flush
      [] ev.act = "refuse" -> \E x \in jobs : x.out = ev.id /\ PublishMem(x) /\ ev.id \notin mem'
      [] ev.act = "crash" -> \E x \in jobs : x.out = ev.id /\ Crash(x)
      [] ev.act = "list" /\ ev.e = "serving" -> ServingLists
      [] ev.act = "list" /\ ev.e = "zombie" -> ZombieLists
      [] ev.act = "delete" /\ ev.e = "serving" ->
           (\E g \in garbage : g[1] = ev.id /\ CollectGarbage(g)) \/ ServingDeletes(ev.id)
      [] ev.act = "delete" /\ ev.e = "zombie" ->
           (\E g \in garbage : g[1] = ev.id /\ ZombieCollectsGarbage(g)) \/ ZombieDeletes(ev.id)
      [] ev.act = "reset" -> Reset
      [] ev.act = "takeover" -> Takeover
      [] OTHER -> FALSE

\* A read of a span still stored: no step. A read that found nothing, or
\* of a span the spec deleted, is explained by nothing.
Checks(ev) == ev.act = "read" /\ ev.ok /\ Stored(ev.id)

TInit == Init /\ j = 0

TNext ==
    /\ j < Len(Trace)
    /\ \/ Makes(Ev) /\ j' = j + 1
       \/ Checks(Ev) /\ UNCHANGED vars /\ j' = j + 1
    /\ IF j' > TLCGet(1) THEN TLCSet(1, j') /\ PrintT(<<"explained", j'>>) ELSE TRUE

ASSUME TLCSet(1, 0)

NotDone == j < Len(Trace)

=============================================================================
