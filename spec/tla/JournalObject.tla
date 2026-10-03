---------------------------- MODULE JournalObject -----------------------------
(***************************************************************************)
(* The journal as one object, rewritten with If-Match on every flush      *)
(* (docs/object-store-state.md §10; docs/verification.md, "Formal model:  *)
(* the journal object"). Journal.tla models the numbered segments it      *)
(* replaces.                                                               *)
(*                                                                         *)
(* `control/journal.json` holds the engine id of its writer, the name of  *)
(* the current checkpoint and the events since that checkpoint.           *)
(* Checkpoints are immutable objects with unique names. An engine opens   *)
(* by GETting the journal, then its checkpoint, and fences by rewriting   *)
(* the journal under its own id with If-Match. Every later write (an      *)
(* append, or a move to a new checkpoint) is an If-Match on the journal   *)
(* it last wrote, so a newer engine's write makes the old engine's next   *)
(* one fail.                                                               *)
(*                                                                         *)
(* An ETag is a function of the body (S3 and R2: its MD5), so the model   *)
(* compares bodies: If-Match succeeds when the journal equals the body    *)
(* the engine last read or wrote.                                          *)
(*                                                                         *)
(* Every step is one object request, atomic. Engines crash between any    *)
(* two steps; a restart is the next engine of `Engines` starting.         *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets, TLC

CONSTANTS
    Engines,          \* engine processes, each started at most once
    MaxWrites,        \* journal writes are bounded: the model's MaxSeq
    None,             \* no journal yet; a journal with no checkpoint
    Nobody,           \* the engine a body without an engine id names
    Overlap,          \* an engine may start while another still runs
    LostAnswers,      \* a journal write may land with its answer lost
    Conflicts,        \* a journal write may fail without landing (S3's 409)
    Unreadable,       \* one checkpoint may be written that cannot be parsed
    \* The design has every switch below TRUE. Each one off removes a rule,
    \* to check that the model finds what the rule is for.
    EngineId,         \* the journal names its writer: a random id per process
    AskJournal,       \* a refused write GETs the journal and compares it
    ReGet,            \* an opener whose checkpoint is gone GETs the journal again
    Verify,           \* a checkpoint is read back before the journal names it
    ListFirst         \* cleanup deletes what it LISTed before its move

VARIABLES
    \* The object store.
    journal,      \* the journal's body, or None
    checkpoints,  \* name -> the state it holds
    torn,         \* the checkpoints that cannot be parsed (at most one)
    \* Each engine.
    pc,           \* where it is
    state,        \* its state: the events folded into it, in order
    seen,         \* the journal body it last read or wrote: its If-Match
    mine,         \* the body of its last journal write, heard or not
    made,         \* how many checkpoints it has created (names them)
    listed,       \* the checkpoints its cleanup LISTed
    doomed,       \* the checkpoints its cleanup has yet to delete
    fence,        \* the serial of its fence write, 0 before it has one
    closing,      \* it is shutting down cleanly
    \* History, for the properties only: no engine reads it.
    writes,       \* how many journal writes have landed: the serial of the last
    history,      \* the state the journal holds: its checkpoint, then its events
    acked,        \* the appends an engine saw succeed
    took          \* the fences engines serve under

store == <<journal, checkpoints, torn>>
local == <<pc, state, seen, mine, made, listed, doomed, fence, closing>>
ghost == <<writes, history, acked, took>>
vars == <<store, local, ghost>>

-----------------------------------------------------------------------------
Event(e) == [by |-> e]
Body(e, cp, events) == [by |-> IF EngineId THEN e ELSE Nobody, cp |-> cp, events |-> events]

Put(m, k, v) == [x \in DOMAIN m \cup {k} |-> IF x = k THEN v ELSE m[x]]
Drop(m, k) == [x \in DOMAIN m \ {k} |-> m[x]]

Readable == DOMAIN checkpoints \ torn

\* What the journal names, None included.
CpOf(h) == IF h = None THEN None ELSE h.cp
EventsOf(h) == IF h = None THEN <<>> ELSE h.events

\* The engine's next checkpoint.
NextCp(e) == <<e, made[e] + 1>>
LastCp(e) == <<e, made[e]>>

\* The journal write an engine at a writing step makes, and the state the
\* journal then holds. `fence`: the journal it read, under its own id;
\* `append`: one more event; `move`: its new checkpoint, no events.
Writing == {"fence", "append", "move"}
Pending(e) ==
    CASE pc[e] = "fence"  -> Body(e, CpOf(seen[e]), EventsOf(seen[e]))
      [] pc[e] = "append" -> Body(e, seen[e].cp, Append(seen[e].events, Event(e)))
      [] pc[e] = "move"  -> Body(e, LastCp(e), <<>>)
PendingState(e) ==
    IF pc[e] = "append" THEN Append(state[e], Event(e)) ELSE state[e]

\* After a write whose answer was lost (or a 409), the engine GETs the journal.
Asking == [fence |-> "fence?", append |-> "append?", move |-> "move?"]
KindOf(p) == CHOOSE k \in Writing : Asking[k] = p

Terminal == {"dead", "stopped", "failed", "read"}

-----------------------------------------------------------------------------
Init ==
    /\ journal = None
    /\ checkpoints = <<>>
    /\ torn = {}
    /\ pc = [e \in Engines |-> "idle"]
    /\ state = [e \in Engines |-> <<>>]
    /\ seen = [e \in Engines |-> None]
    /\ mine = [e \in Engines |-> None]
    /\ made = [e \in Engines |-> 0]
    /\ listed = [e \in Engines |-> {}]
    /\ doomed = [e \in Engines |-> {}]
    /\ fence = [e \in Engines |-> 0]
    /\ closing = [e \in Engines |-> FALSE]
    /\ writes = 0
    /\ history = <<>>
    /\ acked = {}
    /\ took = {}

Goto(e, where) == pc' = [pc EXCEPT ![e] = where]

\* Stop for good, forgetting everything but what names its checkpoints.
Halt(e, how) ==
    /\ Goto(e, how)
    /\ state' = [state EXCEPT ![e] = <<>>]
    /\ seen' = [seen EXCEPT ![e] = None]
    /\ mine' = [mine EXCEPT ![e] = None]
    /\ listed' = [listed EXCEPT ![e] = {}]
    /\ doomed' = [doomed EXCEPT ![e] = {}]
    /\ fence' = [fence EXCEPT ![e] = 0]
    /\ closing' = [closing EXCEPT ![e] = FALSE]
    /\ UNCHANGED made

AfterCleanup(e) == IF closing[e] THEN "stopped" ELSE "serve"

-----------------------------------------------------------------------------
(* Opening.                                                               *)

Start(e) ==
    /\ pc[e] = "idle"
    /\ Overlap \/ \A f \in Engines \ {e} : pc[f] \in {"idle"} \cup Terminal
    /\ Goto(e, "open")
    /\ UNCHANGED <<store, state, seen, mine, made, listed, doomed, fence, closing, ghost>>

\* GET the journal. None, or no checkpoint: its events are the whole state.
GetJournal(e) ==
    /\ pc[e] = "open"
    /\ seen' = [seen EXCEPT ![e] = journal]
    /\ IF CpOf(journal) = None
       THEN /\ state' = [state EXCEPT ![e] = EventsOf(journal)]
            /\ Goto(e, "fence")
       ELSE /\ Goto(e, "load")
            /\ UNCHANGED state
    /\ UNCHANGED <<store, mine, made, listed, doomed, fence, closing, ghost>>

\* GET the checkpoint the journal named. Gone: a writer moved past it and
\* cleaned it up since; GET the journal again.
GetCheckpoint(e) ==
    /\ pc[e] = "load"
    /\ LET c == seen[e].cp IN
       CASE c \in Readable ->
              /\ state' = [state EXCEPT ![e] = checkpoints[c] \o seen[e].events]
              /\ Goto(e, "fence")
              /\ UNCHANGED <<seen, mine, listed, doomed, fence, closing>>
         [] c \notin DOMAIN checkpoints /\ ReGet ->
              /\ Goto(e, "open")
              /\ UNCHANGED <<state, seen, mine, listed, doomed, fence, closing>>
         [] OTHER -> Halt(e, "failed")
    /\ UNCHANGED <<store, made, ghost>>

\* A read-only open ends here, with the state loaded.
ReadOnly(e) ==
    /\ pc[e] = "fence"
    /\ Goto(e, "read")
    /\ UNCHANGED <<store, state, seen, mine, made, listed, doomed, fence, closing, ghost>>

-----------------------------------------------------------------------------
(* Journal writes: the fence, an append, a move to a new checkpoint. Each *)
(* is a PUT with If-Match on `seen` (If-None-Match: * when there is no    *)
(* journal yet).                                                           *)

\* What a write that the engine knows landed does, at serial `serial`.
Succeed(e, kind, body, serial) ==
    /\ seen' = [seen EXCEPT ![e] = body]
    /\ CASE kind = "fence" ->
              /\ fence' = [fence EXCEPT ![e] = serial]
              /\ took' = took \cup {[serial |-> serial, by |-> e]}
              /\ Goto(e, "serve")
              /\ UNCHANGED <<state, acked>>
         [] kind = "append" ->
              /\ state' = [state EXCEPT ![e] = Append(@, Event(e))]
              /\ acked' = acked \cup {[pos |-> Len(state[e]) + 1, by |-> e,
                                       serial |-> serial, fence |-> fence[e]]}
              /\ Goto(e, "serve")
              /\ UNCHANGED <<fence, took>>
         [] kind = "move" ->
              /\ Goto(e, "gc")
              /\ UNCHANGED <<state, fence, acked, took>>

\* The write lands.
Land(e) ==
    /\ journal' = Pending(e)
    /\ writes' = writes + 1
    /\ history' = PendingState(e)

\* The write was refused (412, or 409) without landing.
Refused(e) ==
    IF AskJournal
    THEN /\ Goto(e, Asking[pc[e]])
         /\ mine' = [mine EXCEPT ![e] = Pending(e)]
         /\ UNCHANGED <<state, seen, made, listed, doomed, fence, closing>>
    ELSE IF pc[e] = "fence"
    THEN /\ Goto(e, "open")
         /\ UNCHANGED <<state, seen, mine, made, listed, doomed, fence, closing>>
    ELSE Halt(e, "stopped")

Swap(e) ==
    /\ pc[e] \in Writing
    /\ writes < MaxWrites
    /\ IF journal = seen[e]
       THEN /\ Land(e)
            /\ Succeed(e, pc[e], Pending(e), writes + 1)
            /\ mine' = [mine EXCEPT ![e] = Pending(e)]
            /\ UNCHANGED <<made, listed, doomed, closing>>
       ELSE \* 412: another write landed since this engine's last.
            Refused(e) /\ UNCHANGED <<journal, ghost>>
    /\ UNCHANGED <<checkpoints, torn>>

\* The same write, landing with its answer lost: the engine tries again.
SwapUnheard(e) ==
    /\ LostAnswers
    /\ pc[e] \in Writing
    /\ writes < MaxWrites
    /\ journal = seen[e]
    /\ Land(e)
    /\ mine' = [mine EXCEPT ![e] = Pending(e)]
    /\ UNCHANGED <<checkpoints, torn, pc, state, seen, made, listed, doomed, fence, closing,
                   acked, took>>

\* The same write, refused without landing although the journal matched:
\* S3 answers 409 to one of two concurrent conditional writes.
SwapConflict(e) ==
    /\ Conflicts
    /\ pc[e] \in Writing
    /\ writes < MaxWrites
    /\ journal = seen[e]
    /\ Refused(e)
    /\ UNCHANGED <<store, ghost>>

\* GET the journal after a refused write. Its own body: the write landed (an
\* earlier try whose answer was lost). The journal it wrote on: nothing
\* landed, write again. Anything else: another engine wrote. An opener
\* opens again; a writer was fenced, and stops.
AskStep(e) ==
    /\ pc[e] \in {Asking[k] : k \in Writing}
    /\ LET kind == KindOf(pc[e]) IN
       CASE journal = mine[e] ->
              Succeed(e, kind, mine[e], writes)
              /\ UNCHANGED <<mine, made, listed, doomed, closing>>
         [] journal = seen[e] ->
              /\ Goto(e, kind)
              /\ UNCHANGED <<state, seen, mine, made, listed, doomed, fence, closing, acked, took>>
         [] kind = "fence" ->
              /\ Goto(e, "open")
              /\ UNCHANGED <<state, seen, mine, made, listed, doomed, fence, closing, acked, took>>
         [] OTHER -> Halt(e, "stopped") /\ UNCHANGED <<acked, took>>
    /\ UNCHANGED <<store, writes, history>>

-----------------------------------------------------------------------------
(* Serving.                                                               *)

BeginAppend(e) ==
    /\ pc[e] = "serve"
    /\ writes < MaxWrites
    /\ Goto(e, "append")
    /\ UNCHANGED <<store, state, seen, mine, made, listed, doomed, fence, closing, ghost>>

\* A checkpoint, whenever events have been written since the last one: the
\* cadence (bytes since the last checkpoint) changes nothing here.
BeginCheckpoint(e) ==
    /\ pc[e] = "serve"
    /\ seen[e].events # <<>>
    /\ Goto(e, "list")
    /\ UNCHANGED <<store, state, seen, mine, made, listed, doomed, fence, closing, ghost>>

\* A clean shutdown: a last checkpoint unless the journal has no events.
Close(e) ==
    /\ pc[e] = "serve"
    /\ closing' = [closing EXCEPT ![e] = TRUE]
    /\ Goto(e, IF seen[e].events = <<>> THEN "stopped" ELSE "list")
    /\ UNCHANGED <<store, state, seen, mine, made, listed, doomed, fence, ghost>>

\* LIST checkpoints/, before anything else: cleanup deletes only these,
\* and only once the move has landed. (Without ListFirst, cleanup LISTs
\* after the move instead: `GC`.)
List(e) ==
    /\ pc[e] = "list"
    /\ listed' = [listed EXCEPT ![e] = IF ListFirst THEN DOMAIN checkpoints ELSE {}]
    /\ Goto(e, "create")
    /\ UNCHANGED <<store, state, seen, mine, made, doomed, fence, closing, ghost>>

\* PUT the checkpoint under a fresh name: the state as of the journal this
\* engine last wrote. With Unreadable, the first checkpoint written may turn
\* out unparseable to every reader (a bug in the snapshot, say).
CreateCheckpoint(e) ==
    /\ pc[e] = "create"
    /\ checkpoints' = Put(checkpoints, NextCp(e), state[e])
    /\ \/ UNCHANGED torn
       \/ Unreadable /\ torn = {} /\ torn' = {NextCp(e)}
    /\ made' = [made EXCEPT ![e] = @ + 1]
    /\ Goto(e, "verify")
    /\ UNCHANGED <<journal, state, seen, mine, listed, doomed, fence, closing, ghost>>

\* GET it back and parse it. Unreadable (or gone: a newer engine's cleanup
\* took it, so this one is fenced): no move; cleanup deletes it later.
VerifyCheckpoint(e) ==
    /\ pc[e] = "verify"
    /\ Goto(e, IF Verify /\ LastCp(e) \notin Readable THEN AfterCleanup(e) ELSE "move")
    /\ UNCHANGED <<store, state, seen, mine, made, listed, doomed, fence, closing, ghost>>

\* The move landed: delete what the LIST before it showed, the checkpoint
\* moved from included. Without ListFirst: LIST now, and delete every
\* checkpoint but the one this engine's journal names.
GC(e) ==
    /\ pc[e] = "gc"
    /\ doomed' = [doomed EXCEPT ![e] =
                     IF ListFirst THEN listed[e] ELSE DOMAIN checkpoints \ {seen[e].cp}]
    /\ listed' = [listed EXCEPT ![e] = {}]
    /\ Goto(e, "delete")
    /\ UNCHANGED <<store, state, seen, mine, made, fence, closing, ghost>>

\* DELETE the doomed checkpoints one by one, in any order.
Delete(e) ==
    /\ pc[e] = "delete"
    /\ IF doomed[e] = {}
       THEN /\ Goto(e, AfterCleanup(e))
            /\ UNCHANGED <<checkpoints, doomed>>
       ELSE \E c \in doomed[e] :
              /\ checkpoints' = Drop(checkpoints, c)
              /\ doomed' = [doomed EXCEPT ![e] = @ \ {c}]
              /\ UNCHANGED pc
    /\ UNCHANGED <<journal, torn, state, seen, mine, made, listed, fence, closing, ghost>>

\* The process dies between two requests: what landed stays.
Crash(e) ==
    /\ pc[e] \notin {"idle"} \cup Terminal
    /\ Halt(e, "dead")
    /\ UNCHANGED <<store, ghost>>

-----------------------------------------------------------------------------
\* The steps an engine takes on its own once it has started; fairness
\* applies to these only. Nothing forces a start, an append, a checkpoint,
\* a close, a crash, a lost answer or a conflict.
Progress(e) ==
    \/ GetJournal(e) \/ GetCheckpoint(e) \/ Swap(e) \/ AskStep(e)
    \/ List(e) \/ CreateCheckpoint(e) \/ VerifyCheckpoint(e) \/ GC(e) \/ Delete(e)

Step(e) ==
    \/ Start(e) \/ Progress(e)
    \/ ReadOnly(e) \/ SwapUnheard(e) \/ SwapConflict(e)
    \/ BeginAppend(e) \/ BeginCheckpoint(e) \/ Close(e) \/ Crash(e)

Spec == Init /\ [][\E e \in Engines : Step(e)]_vars

FairSpec == Spec /\ \A e \in Engines : WF_vars(Progress(e))

Symmetry == Permutations(Engines)

-----------------------------------------------------------------------------
(* Properties: Journal.tla's, restated for one object.                   *)

TypeOK ==
    /\ writes \in 0..MaxWrites
    /\ \A e \in Engines : state[e] \in Seq([by: Engines])
    /\ Cardinality(torn) <= 1

\* What an engine opening right now, with nothing in its way, would load:
\* the journal's checkpoint, then the journal's events. A journal naming a
\* checkpoint that is gone or unreadable loads nothing: the state is lost.
Recovered ==
    CASE CpOf(journal) = None -> EventsOf(journal)
      [] journal.cp \in Readable -> checkpoints[journal.cp] \o journal.events
      [] OTHER -> <<>>

IsPrefix(st) == Len(st) <= Len(history) /\ SubSeq(history, 1, Len(st)) = st

Holds(st, a) == a.pos <= Len(st) /\ st[a.pos] = Event(a.by)

\* No acknowledged event is ever lost: an engine opening now recovers it.
NoAckedLoss == \A a \in acked : Holds(Recovered, a)

\* At most one engine appends successfully at a time: no engine's fence
\* lands between another engine's fence and one of its acknowledged appends.
OneWriter ==
    \A a \in acked, t \in took :
        (t.by # a.by /\ t.serial < a.serial) => t.serial < a.fence

\* An engine that fenced holds every event acknowledged before its fence.
FencedSeesAcked ==
    \A e \in Engines, a \in acked :
        fence[e] > 0 /\ a.serial < fence[e] => Holds(state[e], a)

\* Every state an engine acts on, every checkpoint and what an opener would
\* recover are prefixes of the one history, so position n names the same
\* event everywhere.
StatesArePrefixes ==
    /\ \A e \in Engines : fence[e] > 0 \/ pc[e] = "read" => IsPrefix(state[e])
    /\ \A c \in DOMAIN checkpoints : IsPrefix(checkpoints[c])
    /\ IsPrefix(Recovered)

\* The journal names a checkpoint that is there and readable: the one
\* invariant cleanup has to keep (Journal.tla's CleanupCovered).
JournalResolves == CpOf(journal) = None \/ journal.cp \in Readable

\* Opening never gives up.
OpensNeverFail == \A e \in Engines : pc[e] # "failed"

\* The journal's state only grows, and so does a serving engine's.
Monotonic ==
    [][/\ Len(history') >= Len(history) /\ SubSeq(history', 1, Len(history)) = history
       /\ \A e \in Engines :
            fence[e] > 0 /\ pc'[e] \notin Terminal =>
                /\ Len(state'[e]) >= Len(state[e])
                /\ SubSeq(state'[e], 1, Len(state[e])) = state[e]]_vars

\* Liveness, engines one at a time (Overlap = FALSE): an engine that starts
\* opens, and an append it begins succeeds, unless it crashes or the
\* bound on writes is reached.
OpensAlone ==
    \A e \in Engines :
        pc[e] = "open" ~> (pc[e] \in {"serve", "read", "dead"} \/ writes = MaxWrites)
AppendsAlone ==
    \A e \in Engines :
        pc[e] = "append" ~> (pc[e] \in {"serve", "dead"} \/ writes = MaxWrites)

=============================================================================
