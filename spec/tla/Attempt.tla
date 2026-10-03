------------------------------- MODULE Attempt --------------------------------
(***************************************************************************)
(* The attempt control file (docs/lifecycle.md §2): one object per        *)
(* attempt, `runs/{run}/{attempt}.control`, swapped with If-Match, in     *)
(* place of the claim (`.worker`), the gate (`.writing`) and the result   *)
(* (`.result`). The engine creates it `open` before the launch; the first *)
(* worker to swap it `owned` owns the attempt, marks it `writing` before  *)
(* its first store mutation, and seals its result in it; the engine ends  *)
(* it instead when it gives up.                                            *)
(* Every change is one swap on the body its writer last read, so a seal   *)
(* and an end that race on one version cannot both land.                  *)
(*                                                                         *)
(* One output partition on a fenced store, its attempts one after another *)
(* (the claim), each run by one worker process or two (a duplicate).      *)
(* Workers pause anywhere for any time and crash anywhere; the engine     *)
(* asks a worker to cancel (it drains) and ends attempts at any time (a   *)
(* forced cancel, a timeout, a lost worker),                              *)
(* crashes and restarts, and an old engine fenced out of the journal may  *)
(* still end attempts (a zombie). Retention deletes an ended attempt's    *)
(* files at any time after its end is durable.                            *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets, TLC

CONSTANTS
    N,           \* attempts of the partition, launched one after another
    Copies,      \* worker processes per attempt: {1} or {1, 2}
    Zombie,      \* an old engine may still end attempts
    MaxRestarts, \* engine restarts, over the whole behaviour
    LostAnswers, \* a worker's swap may land with its answer lost
    Missing,     \* no object
    Unread,      \* nothing read yet
    \* The design has every switch below TRUE. Each one off removes a rule,
    \* to check that the model finds what the rule is for.
    PreCreate,   \* the engine creates the control file before the launch,
                 \* and nobody else ever creates it
    TakeWriting, \* a worker marks the file `writing` before its first write
    EngineSwaps, \* the engine ends an attempt with If-Match, not a blind PUT
    Classify     \* ended from `writing`, its write evidence is `writing`

Attempts == 1..N               \* an attempt's number is its generation
Workers == Attempts \X Copies  \* <<attempt, copy>>

VARIABLES
    \* The object store.
    control,     \* [Attempts -> body or Missing]
    purged,      \* the attempts whose files retention deleted
    \* The fenced store (lifecycle.md §9.7): the newest generation acquired, by whom.
    fence, fenceBy,
    \* The journal, as far as it matters here.
    launched,    \* attempts whose AttemptLaunched is durable
    decided,     \* [Attempts -> what AttemptFinished says of its writes]
    requested,   \* attempts asked to cancel (the beat's answer; latched)
    \* The engine's memory.
    eseen,       \* [Attempts -> the body it last read, Missing, or Unread]
    restarts,    \* how many times it restarted
    \* Each worker process.
    wpc,         \* where it is
    wseen,       \* the body it last read or wrote: its If-Match
    wrote,       \* what it knows of its own store write
    \* History, for the properties only.
    landed,      \* the attempts a store write of which landed
    last,        \* the generation of the last write that landed
    inOrder      \* no write landed after a newer generation's

vars == <<control, purged, fence, fenceBy, launched, decided, requested, eseen, restarts,
          wpc, wseen, wrote, landed, last, inOrder>>

-----------------------------------------------------------------------------
Att(w) == w[1]

\* Bodies. `by` names the writer, so no two writes have the same bytes:
\* a worker, or an engine (the serving one, or the zombie).
TheEngine == <<0, 0>>
TheZombie == <<0, 1>>
Open == [state |-> "open", by |-> TheEngine, ev |-> "none"]
Owned(w) == [state |-> "owned", by |-> w, ev |-> "none"]
Writing(w) == [state |-> "writing", by |-> w, ev |-> "none"]
Sealed(w, ev) == [state |-> "sealed", by |-> w, ev |-> ev]
Ended(who, ev) == [state |-> "ended", by |-> who, ev |-> ev]

Live(b) == b # Missing /\ b.state \in {"open", "owned", "writing"}
Final(b) == b # Missing /\ b.state \in {"sealed", "ended"}

\* What ending an attempt whose file reads `b` establishes of its writes
\* (lifecycle.md §2.3): it never marked `writing`, so it never wrote and
\* now never can; or it did, and they may land.
Evidence(b) == IF b # Missing /\ b.state = "writing" /\ Classify THEN "writing" ELSE "none"

Evidences == {"undecided", "none", "writing", "complete"}

WorkerDone == {"done", "stopped", "dead"}

-----------------------------------------------------------------------------
Init ==
    /\ control = [i \in Attempts |-> Missing]
    /\ purged = {}
    /\ fence = 0
    /\ fenceBy = Missing
    /\ launched = {}
    /\ decided = [i \in Attempts |-> "undecided"]
    /\ requested = {}
    /\ eseen = [i \in Attempts |-> Unread]
    /\ restarts = 0
    /\ wpc = [w \in Workers |-> "idle"]
    /\ wseen = [w \in Workers |-> Missing]
    /\ wrote = [w \in Workers |-> "none"]
    /\ landed = {}
    /\ last = 0
    /\ inOrder = TRUE

engine == <<launched, decided, requested, eseen, restarts>>
worker == <<wpc, wseen, wrote>>
ghost == <<landed, last, inOrder>>

Set(f, i, v) == [f EXCEPT ![i] = v]

-----------------------------------------------------------------------------
(* The engine.                                                            *)

\* Claim the partition and launch attempt i once the one before it has
\* ended: create its control file `open` (with PreCreate), then make
\* AttemptLaunched durable. (A crash between the two leaves an `open` file
\* no worker is launched for; one step here.)
Launch(i) ==
    /\ i \notin launched
    /\ \A j \in Attempts : j < i => j \in launched /\ decided[j] # "undecided"
    /\ launched' = launched \cup {i}
    /\ control' = IF PreCreate THEN Set(control, i, Open) ELSE control
    /\ UNCHANGED <<purged, fence, fenceBy, decided, requested, eseen, restarts, worker, ghost>>

\* GET the control file of an attempt the engine is about to end.
Read(i) ==
    /\ i \in launched /\ decided[i] = "undecided"
    /\ eseen' = Set(eseen, i, control[i])
    /\ UNCHANGED <<control, purged, fence, fenceBy, launched, decided, requested, restarts, worker, ghost>>

\* A cancel is requested (a user cancel, a timeout): the worker hears it in
\* a beat's answer and drains. Forcing it is End.
Request(i) ==
    /\ i \in launched /\ decided[i] = "undecided" /\ i \notin requested
    /\ requested' = requested \cup {i}
    /\ UNCHANGED <<control, purged, fence, fenceBy, launched, decided, eseen, restarts, worker, ghost>>

\* End it, on the body read: `ended`, with what that body establishes.
\* Refused (the worker moved, or another engine ended it): read again.
\* Without PreCreate, a file not there yet is created `ended` instead.
End(i) ==
    /\ i \in launched /\ decided[i] = "undecided"
    /\ eseen[i] # Unread
    /\ Live(eseen[i]) \/ (~PreCreate /\ eseen[i] = Missing)
    /\ IF control[i] = eseen[i] \/ (~EngineSwaps /\ control[i] # Missing)
       THEN control' = Set(control, i, Ended(TheEngine, Evidence(eseen[i])))
       ELSE control' = control
    /\ eseen' = Set(eseen, i, Unread)
    /\ UNCHANGED <<purged, fence, fenceBy, launched, decided, requested, restarts, worker, ghost>>

\* Settle: the file is final, sealed or ended, by whoever. AttemptFinished
\* records what it establishes, durable.
Decide(i) ==
    /\ i \in launched /\ decided[i] = "undecided"
    /\ Final(control[i])
    /\ decided' = Set(decided, i, control[i].ev)
    /\ UNCHANGED <<control, purged, fence, fenceBy, launched, requested, eseen, restarts, worker,
                   ghost>>

\* An old engine, fenced out of the journal, ends an attempt on what it
\* reads. Its own swap is atomic here: one that is refused changes nothing.
ZombieEnd(i) ==
    /\ Zombie
    /\ Live(control[i]) \/ (~PreCreate /\ control[i] = Missing /\ i \in launched)
    /\ control' = Set(control, i, Ended(TheZombie, Evidence(control[i])))
    /\ UNCHANGED <<purged, fence, fenceBy, engine, worker, ghost>>

\* The engine restarts: it forgets what it read.
Restart ==
    /\ restarts < MaxRestarts
    /\ restarts' = restarts + 1
    /\ eseen' = [i \in Attempts |-> Unread]
    /\ UNCHANGED <<control, purged, fence, fenceBy, launched, decided, requested, worker, ghost>>

\* Retention deletes the attempt's spec and control file, any time after
\* its end is durable. Nothing is kept.
Purge(i) ==
    /\ decided[i] # "undecided" /\ i \notin purged
    /\ purged' = purged \cup {i}
    /\ control' = Set(control, i, Missing)
    /\ UNCHANGED <<fence, fenceBy, engine, worker, ghost>>

-----------------------------------------------------------------------------
(* A worker process w of attempt Att(w).                                  *)

\* Boot: GET the spec. Gone, the worker stops.
Boot(w) ==
    /\ wpc[w] = "idle" /\ Att(w) \in launched
    /\ wpc' = Set(wpc, w, IF Att(w) \in purged THEN "stopped" ELSE "read")
    /\ UNCHANGED <<control, purged, fence, fenceBy, engine, wseen, wrote, ghost>>

\* GET the control file. `open`: claim it. Missing: stop, never create it
\* (without PreCreate, create it). Anything else: another worker owns the
\* attempt (wait for it, writing nothing), or it is over.
ReadControl(w) ==
    /\ wpc[w] = "read"
    /\ LET b == control[Att(w)] IN
       /\ wseen' = Set(wseen, w, b)
       /\ wpc' = Set(wpc, w,
              IF b = Missing THEN (IF PreCreate THEN "stopped" ELSE "own")
              ELSE IF b.state = "open" THEN "own"
              ELSE "stopped")
    /\ UNCHANGED <<control, purged, fence, fenceBy, engine, wrote, ghost>>

\* A swap on `wseen`: it lands if the file is still what the worker last
\* read or wrote (without PreCreate, a create where it read nothing).
Swap(w, body, then) ==
    CASE control[Att(w)] = wseen[w] ->
           /\ control' = Set(control, Att(w), body)
           /\ wseen' = Set(wseen, w, body)
           /\ wpc' = Set(wpc, w, then)
      [] control[Att(w)] = body ->
           \* Refused, and the worker reads the file: its own body, so an
           \* earlier try landed with its answer lost.
           /\ wseen' = Set(wseen, w, body)
           /\ wpc' = Set(wpc, w, then)
           /\ UNCHANGED control
      [] OTHER ->
           \* `ended`, missing, or another worker's claim: it stops.
           /\ wpc' = Set(wpc, w, "stopped")
           /\ UNCHANGED <<control, wseen>>

\* The body a worker at a swapping step writes.
Writes(w) ==
    CASE wpc[w] = "own" -> Owned(w)
      [] wpc[w] = "gate" -> Writing(w)
      [] wpc[w] = "seal" -> Sealed(w, wrote[w])

\* A swap that lands with its answer lost: the worker tries it again.
SwapUnheard(w) ==
    /\ LostAnswers
    /\ wpc[w] \in {"own", "seal"} \/ (wpc[w] = "gate" /\ TakeWriting)
    /\ control[Att(w)] = wseen[w]
    /\ control' = Set(control, Att(w), Writes(w))
    /\ UNCHANGED <<purged, fence, fenceBy, engine, worker, ghost>>

Own(w) ==
    /\ wpc[w] = "own"
    /\ Swap(w, Owned(w), "acquire")
    /\ UNCHANGED <<purged, fence, fenceBy, engine, wrote, ghost>>

\* Store.acquire (lifecycle.md §9.7): take generation Att(w) for the write
\* domain unless a newer one, or another worker of this one, holds it.
\* Refused, the worker writes nothing and seals a failed result.
Acquire(w) ==
    /\ wpc[w] = "acquire"
    /\ IF fence < Att(w) \/ (fence = Att(w) /\ fenceBy = w)
       THEN /\ fence' = Att(w) /\ fenceBy' = w
            /\ wpc' = Set(wpc, w, "gate")
       ELSE /\ wpc' = Set(wpc, w, "seal")
            /\ UNCHANGED <<fence, fenceBy>>
    /\ UNCHANGED <<control, purged, engine, wseen, wrote, ghost>>

\* Drain a requested cancel before writing anything: seal `canceled`,
\* nothing written. (Past `writing`, a drain completes its writes and seals
\* as usual.)
Drain(w) ==
    /\ wpc[w] \in {"acquire", "gate"} /\ Att(w) \in requested
    /\ wpc' = Set(wpc, w, "seal")
    /\ wrote' = Set(wrote, w, "none")
    /\ UNCHANGED <<control, purged, fence, fenceBy, engine, wseen, ghost>>

\* Mark the file `writing` before the first store mutation.
Gate(w) ==
    /\ wpc[w] = "gate"
    /\ IF TakeWriting
       THEN Swap(w, Writing(w), "write")
       ELSE /\ wpc' = Set(wpc, w, "write") /\ UNCHANGED <<control, wseen>>
    /\ UNCHANGED <<purged, fence, fenceBy, engine, wrote, ghost>>

\* The write transaction checks the fence (a fenced store). It lands and
\* returns (`complete`), lands and raises (`writing`: a client timeout
\* may hide a backend that completed), or is refused (`writing` too: any
\* store exception after the gate is).
Write(w) ==
    /\ wpc[w] = "write"
    /\ IF fence = Att(w) /\ fenceBy = w
       THEN /\ landed' = landed \cup {Att(w)}
            /\ last' = Att(w)
            /\ inOrder' = (inOrder /\ Att(w) >= last)
            /\ \E ev \in {"complete", "writing"} : wrote' = Set(wrote, w, ev)
       ELSE /\ wrote' = Set(wrote, w, "writing")
            /\ UNCHANGED ghost
    /\ wpc' = Set(wpc, w, "seal")
    /\ UNCHANGED <<control, purged, fence, fenceBy, engine, wseen>>

\* Seal the result into the file. Refused: the engine ended the attempt
\* first, and its result is not taken.
Seal(w) ==
    /\ wpc[w] = "seal"
    /\ Swap(w, Sealed(w, wrote[w]), "done")
    /\ UNCHANGED <<purged, fence, fenceBy, engine, wrote, ghost>>

\* The process dies between two requests.
Crash(w) ==
    /\ wpc[w] \notin {"idle"} \cup WorkerDone
    /\ wpc' = Set(wpc, w, "dead")
    /\ UNCHANGED <<control, purged, fence, fenceBy, engine, wseen, wrote, ghost>>

-----------------------------------------------------------------------------
\* The engine's steps are fair: it ends what it launched (a timeout, at
\* worst). Workers are not: one may pause forever.
EngineProgress(i) == Read(i) \/ End(i) \/ Decide(i)

Next ==
    \/ \E i \in Attempts : Launch(i) \/ Request(i) \/ EngineProgress(i) \/ ZombieEnd(i) \/ Purge(i)
    \/ Restart
    \/ \E w \in Workers :
          Boot(w) \/ ReadControl(w) \/ Own(w) \/ Acquire(w) \/ Drain(w) \/ Gate(w) \/ Write(w)
          \/ Seal(w) \/ SwapUnheard(w) \/ Crash(w)

Spec == Init /\ [][Next]_vars

FairSpec ==
    Spec /\ \A i \in Attempts : WF_vars(Read(i)) /\ WF_vars(End(i)) /\ WF_vars(Decide(i))

-----------------------------------------------------------------------------
(* Properties.                                                            *)

TypeOK ==
    /\ decided \in [Attempts -> Evidences]
    /\ fence \in 0..N
    /\ wrote \in [Workers -> {"none", "complete", "writing"}]

\* No write after an abort is decided: an attempt the journal says wrote
\* nothing has no store write that landed, before or after.
NoWriteAfterNone == \A i \in Attempts : decided[i] = "none" => i \notin landed

\* What a result calls complete did land.
CompleteLanded == \A i \in Attempts : decided[i] = "complete" => i \in landed

\* One attempt writes a partition at a time: writes land in generation
\* order, so an older attempt never writes over a newer one.
WritesInOrder == inOrder

\* Exactly one of the worker's seal and the engine's end wins: a final
\* file only goes, with its run. And a decision is made once.
OneOutcome ==
    [][\A i \in Attempts :
         /\ Final(control[i]) => control'[i] \in {control[i], Missing}
         /\ decided[i] # "undecided" => decided'[i] = decided[i]]_vars

\* Every attempt launched ends, durably.
EveryAttemptEnds == \A i \in Attempts : i \in launched ~> decided[i] # "undecided"

=============================================================================
