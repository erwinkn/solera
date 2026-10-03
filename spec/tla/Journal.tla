------------------------------- MODULE Journal -------------------------------
(***************************************************************************)
(* The journal: the engine's state as an event log on an object store     *)
(* (docs/object-store-state.md §3 and §10, python/solera_server/journal.py)*)
(*                                                                         *)
(* Segments `journal/{seq}` and checkpoints `checkpoints/{seq}` are        *)
(* create-only objects. An engine opens the journal by loading the newest  *)
(* checkpoint, replaying the segments after it and creating its fence      *)
(* segment at the next seq; then it appends one segment per flush at       *)
(* seq + 1, checkpoints, and cleans up. A create that finds another        *)
(* engine's segment in its slot means a newer engine fenced this one.      *)
(*                                                                         *)
(* Every step is one object request, atomic: S3's strong consistency.      *)
(* Engines crash between any two steps; a restart is the next engine of    *)
(* `Engines` starting. docs/verification.md, "Formal model: the journal",  *)
(* says what is modeled, what is abstracted and why.                       *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets, TLC

CONSTANTS
    Engines,          \* engine processes, each started at most once
    MaxSeq,           \* segments are numbered 1..MaxSeq
    Nobody,           \* the engine a fence without a nonce names
    Overlap,          \* an engine may start while another still runs
    LostAnswers,      \* a create may land with its answer lost
    \* The design as built has every switch below TRUE. Each one off puts a
    \* fix back out, to check that the model finds what the fix fixed.
    FixF7,            \* 3c23397: an opener that finds a segment missing
                      \* where a checkpoint covers it opens again
    KeepFences,       \* 0b3e226: cleanup keeps every fence segment
    FenceNonce,       \* f300500: a fence carries its engine's random nonce
    OwnBytes,         \* solera.objects.create: a create that finds its own
                      \* bytes succeeded (always so for segments)
    \* Candidate fixes for what this spec found; FALSE is the design as built.
    \* A fence segment at s is a hole's (created where cleanup had deleted
    \* a segment) exactly when a checkpoint at or past s exists and the
    \* newest does not list s among its fences.
    FixF14,           \* the engine that created a fence tells a hole's so
    FixF15            \* an opener that reads a fence tells a hole's so

VARIABLES
    \* The object store: two maps, one per prefix.
    journal,      \* seq -> segment
    checkpoints,  \* seq -> the state it holds
    \* Each engine.
    pc,           \* where it is
    state,        \* its state: the segments folded into it, in seq order
    listed,       \* what its last LIST returned and it has yet to read
    known,        \* the checkpoints it knows of (Journal._checkpoints)
    fence,        \* the seq of its fence segment, 0 before it has one
    doomed,       \* the objects its cleanup has yet to delete
    closing,      \* it is shutting down cleanly
    \* History, for the properties only: no engine reads it.
    history,      \* seq -> the first segment that landed there
    acked,        \* the segments whose create an engine saw succeed
    took          \* <<seq, engine>>: the fences engines serve under

store == <<journal, checkpoints>>
local == <<pc, state, listed, known, fence, doomed, closing>>
ghost == <<history, acked, took>>
vars == <<store, local, ghost>>

-----------------------------------------------------------------------------
(* Segments. Each holds one event: the batching of events into segments  *)
(* changes nothing here, so the event counter is the seq.                 *)

Event(e) == [by |-> e, fence |-> FALSE]
Fence(e) == [by |-> IF FenceNonce THEN e ELSE Nobody, fence |-> TRUE]
Segment == [by: Engines \cup {Nobody}, fence: BOOLEAN]

Max(S) == CHOOSE x \in S : \A y \in S : y <= x
Min(S) == CHOOSE x \in S : \A y \in S : x <= y

\* A map with one more key, or one less.
Put(m, k, v) == [x \in DOMAIN m \cup {k} |-> IF x = k THEN v ELSE m[x]]
Drop(m, k) == [x \in DOMAIN m \ {k} |-> m[x]]

\* The last seq folded into an engine's state: its event counter.
At(e) == Len(state[e])
Next(e) == At(e) + 1

FencesIn(st) == {i \in 1..Len(st) : st[i].fence}

\* `_behind`: a checkpoint at or past `s` exists, so a missing segment `s`
\* was written, then cleaned up.
Covered(s) == \E c \in DOMAIN checkpoints : c >= s

NewestCheckpoint ==
    IF DOMAIN checkpoints = {} THEN 0 ELSE Max(DOMAIN checkpoints)

\* A fence at `s` that cleanup's hole holds, not history: covered by a
\* checkpoint, and not among the newest checkpoint's fences.
HoleFence(s) == Covered(s) /\ s \notin FencesIn(checkpoints[NewestCheckpoint])

\* Whether an opener takes segment `s` as it finds it.
Takes(s) == ~(FixF15 /\ journal[s].fence /\ HoleFence(s))

Terminal == {"dead", "stopped", "failed", "read"}

-----------------------------------------------------------------------------
Init ==
    /\ journal = <<>>
    /\ checkpoints = <<>>
    /\ pc = [e \in Engines |-> "idle"]
    /\ state = [e \in Engines |-> <<>>]
    /\ listed = [e \in Engines |-> {}]
    /\ known = [e \in Engines |-> {}]
    /\ fence = [e \in Engines |-> 0]
    /\ doomed = [e \in Engines |-> {}]
    /\ closing = [e \in Engines |-> FALSE]
    /\ history = <<>>
    /\ acked = {}
    /\ took = {}

\* A create-only PUT of segment `s` that lands.
Land(s, seg) ==
    /\ journal' = Put(journal, s, seg)
    /\ history' = IF s \in DOMAIN history THEN history ELSE Put(history, s, seg)

Goto(e, where) == pc' = [pc EXCEPT ![e] = where]

\* Stop for good, forgetting everything: what a stopped process knew no
\* longer matters, and forgetting it keeps the state space small.
Halt(e, how) ==
    /\ Goto(e, how)
    /\ state' = [state EXCEPT ![e] = <<>>]
    /\ listed' = [listed EXCEPT ![e] = {}]
    /\ known' = [known EXCEPT ![e] = {}]
    /\ fence' = [fence EXCEPT ![e] = 0]
    /\ doomed' = [doomed EXCEPT ![e] = {}]
    /\ closing' = [closing EXCEPT ![e] = FALSE]

\* A segment `s` an opener needs is missing: a gap in the listing, a GET
\* that finds nothing (with FixF15, a hole's fence). With FixF7, a
\* checkpoint covering it means it was cleaned up, and the engine opens
\* again; otherwise opening fails.
Behind(e, s) ==
    IF FixF7 /\ Covered(s)
    THEN /\ Goto(e, "open")
         /\ UNCHANGED <<state, listed, known, fence, doomed, closing>>
    ELSE Halt(e, "failed")

-----------------------------------------------------------------------------
(* Opening: `Journal.open`.                                               *)

Start(e) ==
    /\ pc[e] = "idle"
    /\ Overlap \/ \A f \in Engines \ {e} : pc[f] \in {"idle"} \cup Terminal
    /\ Goto(e, "open")
    /\ UNCHANGED <<store, state, listed, known, fence, doomed, closing, ghost>>

\* LIST checkpoints/.
ListCheckpoints(e) ==
    /\ pc[e] = "open"
    /\ listed' = [listed EXCEPT ![e] = DOMAIN checkpoints]
    /\ Goto(e, "load")
    /\ UNCHANGED <<store, state, known, fence, doomed, closing, ghost>>

\* GET the newest listed checkpoint still there (or, with none, start
\* from nothing). Every listed one becomes known, read or not.
Load(e) ==
    /\ pc[e] = "load"
    /\ LET there == listed[e] \cap DOMAIN checkpoints
       IN  state' = [state EXCEPT ![e] =
                        IF there = {} THEN <<>> ELSE checkpoints[Max(there)]]
    /\ known' = [known EXCEPT ![e] = listed[e]]
    /\ Goto(e, "list")
    /\ UNCHANGED <<store, listed, fence, doomed, closing, ghost>>

\* LIST journal/ after the loaded seq.
ListJournal(e) ==
    /\ pc[e] = "list"
    /\ LET after == {s \in DOMAIN journal : s > At(e)}
       IN  /\ listed' = [listed EXCEPT ![e] = after]
           /\ Goto(e, IF after = {} THEN "fence" ELSE "replay")
    /\ UNCHANGED <<store, state, known, fence, doomed, closing, ghost>>

\* GET the next listed segment and apply it.
Replay(e) ==
    /\ pc[e] = "replay"
    /\ LET s == Next(e) IN
       IF Min(listed[e]) = s /\ s \in DOMAIN journal /\ Takes(s)
       THEN /\ state' = [state EXCEPT ![e] = Append(@, journal[s])]
            /\ listed' = [listed EXCEPT ![e] = @ \ {s}]
            /\ Goto(e, IF listed[e] = {s} THEN "fence" ELSE "replay")
            /\ UNCHANGED <<known, fence, doomed, closing>>
       ELSE Behind(e, s)
    /\ UNCHANGED <<store, ghost>>

\* A read-only open (`writer=False`) ends here, with the state replayed.
ReadOnly(e) ==
    /\ pc[e] = "fence"
    /\ Goto(e, "read")
    /\ UNCHANGED <<store, state, listed, known, fence, doomed, closing, ghost>>

\* Create the fence segment at the next seq. If another engine's segment
\* is there, read it next and try the seq after.
CreateFence(e) ==
    /\ pc[e] = "fence"
    /\ Next(e) <= MaxSeq
    /\ LET s == Next(e) IN
       IF s \notin DOMAIN journal
       THEN Land(s, Fence(e)) /\ Goto(e, "fenceCheck")
       ELSE /\ IF OwnBytes /\ journal[s] = Fence(e)
               THEN Goto(e, "fenceCheck")
               ELSE Goto(e, "fenceRead")
            /\ UNCHANGED <<journal, history>>
    /\ UNCHANGED <<checkpoints, state, listed, known, fence, doomed, closing,
                   acked, took>>

\* The same create, landing with its answer lost: the engine tries again.
CreateFenceUnheard(e) ==
    /\ LostAnswers
    /\ pc[e] = "fence"
    /\ Next(e) <= MaxSeq
    /\ Next(e) \notin DOMAIN journal
    /\ Land(Next(e), Fence(e))
    /\ UNCHANGED <<checkpoints, local, acked, took>>

\* GET the segment that was in the fence's way.
FenceRead(e) ==
    /\ pc[e] = "fenceRead"
    /\ LET s == Next(e) IN
       IF s \in DOMAIN journal /\ Takes(s)
       THEN /\ state' = [state EXCEPT ![e] = Append(@, journal[s])]
            /\ Goto(e, "fence")
            /\ UNCHANGED <<listed, known, fence, doomed, closing>>
       ELSE Behind(e, s)
    /\ UNCHANGED <<store, ghost>>

\* LIST checkpoints/: a checkpoint at or past the fence means it landed in
\* a hole cleanup left, after segments this engine never read (as built).
\* With FixF14, also GET the newest: if it lists this fence, a newer engine
\* read the fence and moved past it, so the fence stays.
FenceCheck(e) ==
    /\ pc[e] = "fenceCheck"
    /\ LET s == Next(e) IN
       IF FixF7 /\ Covered(s)
       THEN /\ IF FixF14 /\ ~HoleFence(s)
               THEN Goto(e, "open")
               ELSE Goto(e, "unfence")
            /\ UNCHANGED <<state, fence, took>>
       ELSE /\ state' = [state EXCEPT ![e] = Append(@, Fence(e))]
            /\ fence' = [fence EXCEPT ![e] = s]
            /\ took' = took \cup {<<s, e>>}
            /\ Goto(e, "serve")
    /\ UNCHANGED <<store, listed, known, doomed, closing, history, acked>>

\* DELETE the fence created in the hole, and open again.
Unfence(e) ==
    /\ pc[e] = "unfence"
    /\ journal' = Drop(journal, Next(e))
    /\ Goto(e, "open")
    /\ UNCHANGED <<checkpoints, state, listed, known, fence, doomed, closing,
                   ghost>>

-----------------------------------------------------------------------------
(* Serving: append, checkpoint, clean up, close.                          *)

BeginAppend(e) ==
    /\ pc[e] = "serve"
    /\ Next(e) <= MaxSeq
    /\ Goto(e, "append")
    /\ UNCHANGED <<store, state, listed, known, fence, doomed, closing, ghost>>

\* Create the next segment. Success acknowledges its event; a checkpoint
\* may then be due. Another engine's segment there means this engine was
\* fenced: it stops.
AppendSegment(e) ==
    /\ pc[e] = "append"
    /\ LET s == Next(e)
           ack == /\ state' = [state EXCEPT ![e] = Append(@, Event(e))]
                  /\ acked' = acked \cup {[seq |-> s, by |-> e, fence |-> fence[e]]}
                  /\ \E then \in {"serve", "checkpoint"} : Goto(e, then)
                  /\ UNCHANGED <<listed, known, fence, doomed, closing>>
       IN
       IF s \notin DOMAIN journal
       THEN Land(s, Event(e)) /\ ack
       ELSE IF OwnBytes /\ journal[s] = Event(e)
       THEN ack /\ UNCHANGED <<journal, history>>
       ELSE Halt(e, "stopped") /\ UNCHANGED <<journal, history, acked>>
    /\ UNCHANGED <<checkpoints, took>>

\* The same create, landing with its answer lost: the engine tries again.
AppendUnheard(e) ==
    /\ LostAnswers
    /\ pc[e] = "append"
    /\ Next(e) \notin DOMAIN journal
    /\ Land(Next(e), Event(e))
    /\ UNCHANGED <<checkpoints, local, acked, took>>

\* A clean shutdown: a last checkpoint unless this seq has one, then stop.
Close(e) ==
    /\ pc[e] = "serve"
    /\ closing' = [closing EXCEPT ![e] = TRUE]
    /\ Goto(e, IF At(e) \in known[e] THEN "stopped" ELSE "checkpoint")
    /\ UNCHANGED <<store, state, listed, known, fence, doomed, ghost>>

AfterCleanup(e) == IF closing[e] THEN "stopped" ELSE "serve"

\* Create the checkpoint at this engine's seq. One already there with other
\* bytes: no cleanup this time.
WriteCheckpoint(e) ==
    /\ pc[e] = "checkpoint"
    /\ LET c == At(e) IN
       IF c \notin DOMAIN checkpoints \/ checkpoints[c] = state[e]
       THEN /\ checkpoints' = Put(checkpoints, c, state[e])
            /\ known' = [known EXCEPT ![e] = @ \cup {c}]
            /\ Goto(e, "cleanup")
       ELSE /\ Goto(e, AfterCleanup(e))
            /\ UNCHANGED <<checkpoints, known>>
    /\ UNCHANGED <<journal, state, listed, fence, doomed, closing, ghost>>

\* Journal cleanup (`_collect`), LIST journal/: keep the newest known
\* checkpoint and the one before it (`previous`), and every segment past
\* `previous`; doom the older checkpoints and every segment at or below
\* `previous` but the fences this engine's state holds.
Cleanup(e) ==
    /\ pc[e] = "cleanup"
    /\ IF Cardinality(known[e]) < 2
       THEN /\ Goto(e, AfterCleanup(e))
            /\ UNCHANGED <<known, doomed>>
       ELSE LET previous == Max(known[e] \ {Max(known[e])})
                keep == IF KeepFences THEN FencesIn(state[e]) ELSE {}
            IN  /\ doomed' = [doomed EXCEPT ![e] =
                       {<<"checkpoint", c>> : c \in {c \in known[e] : c < previous}}
                       \cup {<<"segment", s>> :
                               s \in {s \in DOMAIN journal : s <= previous /\ s \notin keep}}]
                /\ known' = [known EXCEPT ![e] = {c \in @ : c >= previous}]
                /\ Goto(e, "delete")
    /\ UNCHANGED <<store, state, listed, fence, closing, ghost>>

\* DELETE the doomed objects one by one, in any order.
Delete(e) ==
    /\ pc[e] = "delete"
    /\ IF doomed[e] = {}
       THEN /\ Goto(e, AfterCleanup(e))
            /\ UNCHANGED <<store, doomed>>
       ELSE \E p \in doomed[e] :
              /\ IF p[1] = "segment"
                 THEN /\ journal' = Drop(journal, p[2])
                      /\ UNCHANGED checkpoints
                 ELSE /\ checkpoints' = Drop(checkpoints, p[2])
                      /\ UNCHANGED journal
              /\ doomed' = [doomed EXCEPT ![e] = @ \ {p}]
              /\ UNCHANGED pc
    /\ UNCHANGED <<state, listed, known, fence, closing, ghost>>

\* The process dies between two requests: what landed stays.
Crash(e) ==
    /\ pc[e] \notin {"idle"} \cup Terminal
    /\ Halt(e, "dead")
    /\ UNCHANGED <<store, ghost>>

-----------------------------------------------------------------------------
\* The steps an engine takes on its own once it has started; fairness
\* applies to these only. An engine is not forced to start, to append or to
\* close, and nothing forces a crash or a lost answer.
Progress(e) ==
    \/ ListCheckpoints(e) \/ Load(e) \/ ListJournal(e) \/ Replay(e)
    \/ CreateFence(e) \/ FenceRead(e) \/ FenceCheck(e) \/ Unfence(e)
    \/ AppendSegment(e) \/ WriteCheckpoint(e) \/ Cleanup(e) \/ Delete(e)

Step(e) ==
    \/ Start(e) \/ Progress(e)
    \/ ReadOnly(e) \/ CreateFenceUnheard(e) \/ BeginAppend(e) \/ AppendUnheard(e)
    \/ Close(e) \/ Crash(e)

Spec == Init /\ [][\E e \in Engines : Step(e)]_vars

FairSpec == Spec /\ \A e \in Engines : WF_vars(Progress(e))

\* Engines are interchangeable: TLC checks one ordering of them.
Symmetry == Permutations(Engines)

-----------------------------------------------------------------------------
(* Properties.                                                            *)

TypeOK ==
    /\ journal \in [DOMAIN journal -> Segment] /\ DOMAIN journal \subseteq 1..MaxSeq
    /\ DOMAIN checkpoints \subseteq 1..MaxSeq
    /\ \A e \in Engines : At(e) <= MaxSeq /\ state[e] \in Seq(Segment)

\* What an engine opening right now, with nothing in its way, would load:
\* the newest checkpoint, then every segment after it, in order.
RECURSIVE Extend(_)
Extend(st) ==
    IF Len(st) + 1 \in DOMAIN journal
    THEN Extend(Append(st, journal[Len(st) + 1]))
    ELSE st
Recovered ==
    Extend(IF NewestCheckpoint = 0 THEN <<>> ELSE checkpoints[NewestCheckpoint])

IsPrefix(st) ==
    \A i \in 1..Len(st) : i \in DOMAIN history /\ st[i] = history[i]

Holds(st, a) == a.seq <= Len(st) /\ st[a.seq] = Event(a.by)

\* No acknowledged event is ever lost: an engine opening now recovers it.
NoAckedLoss == \A a \in acked : Holds(Recovered, a)

\* At most one engine appends successfully at a time: every acknowledged
\* segment is past its engine's fence and before any newer engine's fence.
OneWriter ==
    \A a \in acked, t \in took :
        (t[2] # a.by /\ t[1] <= a.seq) => t[1] < a.fence

\* An engine that opened and fenced holds every event acknowledged before
\* its fence.
FencedSeesAcked ==
    \A e \in Engines, a \in acked :
        fence[e] > 0 /\ a.seq < fence[e] => Holds(state[e], a)

\* Event counters agree and are dense: every state an engine acts on — once
\* it has fenced, or what a read-only open returns — and every checkpoint
\* is a prefix of the one history, so counter n names the same event
\* everywhere; nothing lands at n before n - 1 has. (An opener mid-replay
\* may hold a segment it will find out about before it acts.)
StatesArePrefixes ==
    /\ \A e \in Engines : fence[e] > 0 \/ pc[e] = "read" => IsPrefix(state[e])
    /\ \A c \in DOMAIN checkpoints :
          Len(checkpoints[c]) = c /\ IsPrefix(checkpoints[c])
CountersDense ==
    \A s \in DOMAIN history : s = 1 \/ s - 1 \in DOMAIN history

\* Cleanup never deletes a segment an opener still needs: a segment that
\* landed and is gone is covered by a checkpoint still there, which holds
\* its event (StatesArePrefixes); an opener that finds it missing loads
\* that checkpoint instead.
CleanupCovered ==
    \A s \in DOMAIN history \ DOMAIN journal : Covered(s)

\* Fence segments are kept for good.
FencesStay == \A t \in took : t[1] \in DOMAIN journal /\ journal[t[1]] = Fence(t[2])

\* Opening never gives up on a journal another engine is cleaning up.
OpensNeverFail == \A e \in Engines : pc[e] # "failed"

\* The literal reading of "cleanup never deletes a segment an opener still
\* needs": a segment an opener has listed is still there when it reads it.
\* The design does not promise this (the opener copes instead); TLC shows
\* why in one trace. Not checked by default.
ListedStayUntilRead ==
    \A e \in Engines : pc[e] = "replay" => listed[e] \subseteq DOMAIN journal

\* Event counters are monotonic: once an engine serves, its state only
\* grows, and the newest checkpoint only moves forward.
Monotonic ==
    [][/\ \A e \in Engines :
            fence[e] > 0 /\ pc'[e] \notin Terminal =>
                /\ Len(state'[e]) >= Len(state[e])
                /\ SubSeq(state'[e], 1, Len(state[e])) = state[e]
       /\ NewestCheckpoint' >= NewestCheckpoint]_vars

\* Liveness, with engines one at a time (Overlap = FALSE): an engine that
\* starts opens (or ends a read-only open), and an append it begins
\* succeeds, unless it crashes.
OpensAlone ==
    \A e \in Engines :
        pc[e] = "open" ~> (pc[e] \in {"serve", "read", "dead"} \/ Next(e) > MaxSeq)
AppendsAlone ==
    \A e \in Engines :
        pc[e] = "append" ~> pc[e] \in {"serve", "checkpoint", "dead"}

=============================================================================
