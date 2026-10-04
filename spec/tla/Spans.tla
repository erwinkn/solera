------------------------------- MODULE Spans --------------------------------
(***************************************************************************)
(* The span key index's lifecycle as built (key index step 2: b49bbef and  *)
(* later; python/solera_server/upkeep.py, model.py, engine.py): endpoint   *)
(* reservations, merges and their publication, pins and garbage, the      *)
(* orphan collector (D72), crashes, takeovers, resets.                    *)
(*                                                                         *)
(* What a span holds is the Lean proofs' (experiments/lean/KeyIndex),     *)
(* taken as given: a read at commit e from spans that keep e as a         *)
(* boundary (a span's first commit, or a segment start a merge kept       *)
(* because e was an endpoint when it was planned) agrees with the full    *)
(* history. So a span is its commits [a, b], its segment starts and the   *)
(* index's life; a read is exact iff its endpoints are boundaries.        *)
(*                                                                         *)
(* One index. Engines: the serving one, and after a takeover a zombie    *)
(* that runs on with the state it last knew until it halts; its journal  *)
(* writes are fenced, its deletes are not. The serving engine commits     *)
(* deltas, prepares attempts (Model.endpoints gets their reads, first and *)
(* the landing point end = head + 1, in the same step: engine._prepare is *)
(* synchronous; their pin is the claim's event counter), plans merges     *)
(* from the endpoints it knows, publishes them (IndexMerged: the index's  *)
(* life and inputs re-checked) or deletes a refused output, deletes       *)
(* garbage under the pin floor (Upkeep.collect), and collects orphans:    *)
(* merge outputs that no current span, garbage entry or merge running in  *)
(* the same engine names (Upkeep.collect_orphans, D72). A merge input set *)
(* fails at most R times, then that index merges no more (MERGE_ATTEMPTS). *)
(*                                                                         *)
(* FixLanding, FixBounds, FixLanes, FixInputs, FixLife, FixPinFloor,     *)
(* FixDurable, FixRetries and OrphansFenced select each guard (TRUE) or   *)
(* its absence (FALSE): the model must find what each one prevents.       *)
(* FixLanes and FixInputs back each other up: either alone keeps Tiling.  *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets

CONSTANTS
    Consumers,     \* consumers of the index, each with a position
    MaxCommits,    \* commits per life
    MaxFiles,      \* span files written, over the whole behaviour
    MaxResets,     \* resets and renames: a new life of the index
    MaxTakeovers,  \* takeovers: the old engine runs on as a zombie
    MaxClaims,     \* attempts claimed, over the whole behaviour
    MaxJobs,       \* merges in flight at once (the base and tail lanes)
    R,             \* merge attempts per input set (MERGE_ATTEMPTS)
    MergeFailures, \* a merge's work may fail (store errors)
    FixLanding,    \* a claim's landing point is an endpoint from its plan on
    FixBounds,     \* a merge keeps a segment start at every endpoint it was planned with
    FixLanes,      \* the merges in flight never share an input (maintain's busy lanes)
    FixInputs,     \* publication re-checks the inputs (IndexState.holds)
    FixLife,       \* publication and an attempt's commit re-check the index's life
    FixPinFloor,   \* garbage is deleted only once no reader pinned before it remains
    FixDurable,    \* inputs become garbage at publication, never before
    FixRetries,    \* after R failures of an input set, the index merges no more
    OrphansFenced  \* only the serving engine collects orphans (the fix to propose)

VARIABLES
    head, life, span, stored, state, pos, att, jobs, nfile, resets,
    ec,        \* the journal's event counter: pins and garbage entries are its values
    garbage,   \* {<<file, counter it was let go of at>>}
    serving,   \* the serving engine's number; older ones are zombies
    zombie,    \* [alive, view: the state it last knew, garbage it last knew]
    tries,     \* [input sets -> failed attempts]
    stopped,   \* the index merges no more (alarmed)
    claims     \* attempts claimed so far

vars == <<head, life, span, stored, state, pos, att, jobs, nfile, resets, ec, garbage, serving,
          zombie, tries, stopped, claims>>

None == [none |-> TRUE]
Active(c) == att[c] # None

-----------------------------------------------------------------------------
(* Endpoints and boundaries *)

\* Model.endpoints: every position, and the reads of every claim of this
\* life (first is a position's; end, its landing point, with FixLanding).
Endpoints ==
    {pos[c] : c \in Consumers}
    \cup {att[c].land : c \in {d \in Consumers : Active(d) /\ FixLanding /\ att[d].life = life}}

BoundaryIn(ids, e) == e = head + 1 \/ \E i \in ids : span[i].a = e \/ e \in span[i].bounds

Lo(ids) == CHOOSE x \in {span[i].a : i \in ids} : \A y \in {span[i].a : i \in ids} : x <= y
Hi(ids) == CHOOSE x \in {span[i].b : i \in ids} : \A y \in {span[i].b : i \in ids} : y <= x

Tiles(ids) ==
    /\ \A i \in ids : span[i].life = life /\ span[i].b <= head
    /\ \A n \in 1..head : Cardinality({i \in ids : span[i].a <= n /\ n <= span[i].b}) = 1

Contiguous(ids) ==
    \A n \in Lo(ids)..Hi(ids) : Cardinality({i \in ids : span[i].a <= n /\ n <= span[i].b}) = 1

GarbageFiles(g) == {x[1] : x \in g}

-----------------------------------------------------------------------------
Init ==
    /\ head = 0 /\ life = 0
    /\ span = [i \in {} |-> None]
    /\ stored = {} /\ state = {}
    /\ pos = [c \in Consumers |-> 1]
    /\ att = [c \in Consumers |-> None]
    /\ jobs = {} /\ nfile = 0 /\ resets = 0
    /\ ec = 0 /\ garbage = {}
    /\ serving = 0
    /\ zombie = [alive |-> FALSE, view |-> {}, garbage |-> {}]
    /\ tries = [x \in {} |-> 0] /\ stopped = FALSE /\ claims = 0

NewSpan(a, b, bounds, delta) ==
    /\ nfile < MaxFiles
    /\ nfile' = nfile + 1
    /\ span' = [i \in DOMAIN span \cup {nfile + 1} |->
                   IF i = nfile + 1 THEN [a |-> a, b |-> b, bounds |-> bounds, life |-> life, delta |-> delta]
                   ELSE span[i]]
    /\ stored' = stored \cup {nfile + 1}

\* An event of the serving engine's journal.
Tick == ec' = ec + 1

Commit ==
    /\ head < MaxCommits
    /\ NewSpan(head + 1, head + 1, {}, TRUE)
    /\ head' = head + 1
    /\ state' = state \cup {nfile + 1}
    /\ Tick
    /\ UNCHANGED <<life, pos, att, jobs, resets, garbage, serving, zombie, tries, stopped, claims>>

-----------------------------------------------------------------------------
(* Attempts *)

\* Prepare: the claim's reads are endpoints from now on (with FixLanding),
\* its manifest the state, its pin the claim's event counter.
Claim(c) ==
    /\ ~Active(c) /\ claims < MaxClaims
    /\ claims' = claims + 1
    /\ att' = [att EXCEPT ![c] = [land |-> head + 1, man |-> state, pin |-> ec, life |-> life]]
    /\ Tick
    /\ UNCHANGED <<head, life, span, stored, state, pos, jobs, nfile, resets, garbage, serving,
                   zombie, tries, stopped>>

\* Its result is part of the position; the claim, its reads and pin go. One
\* of an earlier life commits nothing (FixLife).
Settle(c) ==
    /\ Active(c)
    /\ pos' = IF ~FixLife \/ att[c].life = life THEN [pos EXCEPT ![c] = att[c].land] ELSE pos
    /\ att' = [att EXCEPT ![c] = None]
    /\ Tick
    /\ UNCHANGED <<head, life, span, stored, state, jobs, nfile, resets, garbage, serving, zombie,
                   tries, stopped, claims>>

Fail(c) ==
    /\ Active(c)
    /\ att' = [att EXCEPT ![c] = None]
    /\ Tick
    /\ UNCHANGED <<head, life, span, stored, state, pos, jobs, nfile, resets, garbage, serving,
                   zombie, tries, stopped, claims>>

-----------------------------------------------------------------------------
(* Merges (Upkeep.maintain, _merge) *)

InputsOf(ids) == {<<span[i].a, span[i].b>> : i \in ids}
Tries(x) == IF x \in DOMAIN tries THEN tries[x] ELSE 0
SetTries(x, n) == [y \in DOMAIN tries \cup {x} |-> IF y = x THEN n ELSE tries[y]]

\* Plan and upload: the endpoints known now are kept as segment starts
\* (FixBounds); the output's name is unique. Without FixDurable, the
\* inputs are let go of at once, before publication.
StartMerge(ids) ==
    /\ ~stopped
    /\ Cardinality({j \in jobs : j.st = "uploaded" /\ j.by = serving}) < MaxJobs
    /\ ids # {} /\ ids \subseteq state /\ Contiguous(ids)
    /\ FixLanes => ~\E j \in jobs : j.st = "uploaded" /\ j.by = serving /\ j.inputs \cap ids # {}
    /\ FixRetries => Tries(InputsOf(ids)) < R
    /\ LET inner == {span[i].a : i \in ids} \cup UNION {span[i].bounds : i \in ids} IN
       NewSpan(Lo(ids), Hi(ids), IF FixBounds THEN {e \in inner : e # Lo(ids) /\ e \in Endpoints} ELSE {},
               FALSE)
    /\ jobs' = jobs \cup {[inputs |-> ids, out |-> nfile + 1, life |-> life, by |-> serving, st |-> "uploaded"]}
    /\ IF FixDurable THEN UNCHANGED <<garbage, ec>>
       ELSE /\ garbage' = garbage \cup {<<i, ec>> : i \in ids} /\ Tick
    /\ UNCHANGED <<head, life, state, pos, att, resets, serving, zombie, tries, stopped, claims>>

\* The merge's work fails (a store error): no output; one more attempt of
\* this input set (with FixRetries: at R, the index stops merging).
MergeFails(ids) ==
    /\ MergeFailures /\ ~stopped /\ ids # {} /\ ids \subseteq state /\ Contiguous(ids)
    /\ FixRetries => Tries(InputsOf(ids)) < R
    /\ tries' = SetTries(InputsOf(ids), Tries(InputsOf(ids)) + 1)
    /\ stopped' = (FixRetries /\ Tries(InputsOf(ids)) + 1 >= R)
    /\ UNCHANGED <<head, life, span, stored, state, pos, att, jobs, nfile, resets, ec, garbage,
                   serving, zombie, claims>>

\* Publish (IndexMerged), by the serving engine that started it: the index
\* still of its life (FixLife) holding its inputs (FixInputs); the inputs
\* become garbage at this event. Refused, the output is deleted.
Publish(j) ==
    /\ j \in jobs /\ j.st = "uploaded" /\ j.by = serving
    /\ IF (FixInputs => j.inputs \subseteq state) /\ (FixLife => j.life = life)
       THEN /\ state' = (state \ j.inputs) \cup {j.out}
            /\ garbage' = IF FixDurable THEN garbage \cup {<<i, ec>> : i \in j.inputs} ELSE garbage
            /\ UNCHANGED stored
       ELSE /\ stored' = stored \ {j.out}
            /\ UNCHANGED <<state, garbage>>
    /\ jobs' = jobs \ {j}
    /\ Tick
    /\ UNCHANGED <<head, life, span, pos, att, nfile, resets, serving, zombie, tries, stopped, claims>>

\* The merge's process dies between upload and publication.
Crash(j) ==
    /\ j \in jobs /\ j.st = "uploaded"
    /\ jobs' = (jobs \ {j}) \cup {[j EXCEPT !.st = "abandoned"]}
    /\ UNCHANGED <<head, life, span, stored, state, pos, att, nfile, resets, ec, garbage, serving,
                   zombie, tries, stopped, claims>>

-----------------------------------------------------------------------------
(* Garbage and orphans *)

\* The oldest reader's pin (the claims' event counters).
PinFloor == IF \E c \in Consumers : Active(c)
            THEN CHOOSE p \in {att[c].pin : c \in {d \in Consumers : Active(d)}} :
                     \A c \in Consumers : Active(c) => p <= att[c].pin
            ELSE ec

\* Upkeep.collect: a garbage entry let go of at or before the pin floor.
CollectGarbage(g) ==
    /\ g \in garbage
    /\ FixPinFloor => g[2] <= PinFloor
    /\ stored' = stored \ {g[1]}
    /\ garbage' = garbage \ {g}
    /\ Tick
    /\ UNCHANGED <<head, life, span, state, pos, att, jobs, nfile, resets, serving, zombie, tries,
                   stopped, claims>>

\* Upkeep.collect_orphans, by an engine that knows `view` and `known`
\* garbage, running the merges `mine`: a merge output (not a delta) none of
\* them names. Without OrphansFenced, a zombie collects too.
Orphans(view, known, mine) ==
    {i \in stored : ~span[i].delta /\ i \notin view /\ i \notin GarbageFiles(known)
                    /\ ~\E j \in mine : j.out = i}
CollectOrphans(i) ==
    /\ i \in Orphans(state, garbage, {j \in jobs : j.st = "uploaded" /\ j.by = serving})
    /\ stored' = stored \ {i}
    /\ jobs' = {j \in jobs : ~(j.out = i /\ (j.st = "abandoned" \/ j.by # serving))}
    /\ UNCHANGED <<head, life, span, state, pos, att, nfile, resets, ec, garbage, serving, zombie,
                   tries, stopped, claims>>
ZombieCollects(i) ==
    /\ ~OrphansFenced /\ zombie.alive
    /\ i \in Orphans(zombie.view, zombie.garbage, {j \in jobs : j.st = "uploaded" /\ j.by # serving})
    /\ stored' = stored \ {i}
    /\ UNCHANGED <<head, life, span, state, pos, att, jobs, nfile, resets, ec, garbage, serving,
                   zombie, tries, stopped, claims>>

-----------------------------------------------------------------------------
(* Takeovers and resets *)

\* A new engine takes over: the old one runs on as a zombie, knowing the
\* state as of now; its merges can no longer publish.
Takeover ==
    /\ serving < MaxTakeovers
    /\ serving' = serving + 1
    /\ zombie' = [alive |-> TRUE, view |-> state, garbage |-> garbage]
    /\ UNCHANGED <<head, life, span, stored, state, pos, att, jobs, nfile, resets, ec, garbage,
                   tries, stopped, claims>>

\* The zombie learns it is fenced (its next journal write fails) and halts.
Halt ==
    /\ zombie.alive
    /\ zombie' = [zombie EXCEPT !.alive = FALSE]
    /\ UNCHANGED <<head, life, span, stored, state, pos, att, jobs, nfile, resets, ec, garbage,
                   serving, tries, stopped, claims>>

\* A reset, a store move or a rename to a new life: the index is new and
\* empty, every position goes; the old life's spans are garbage.
Reset ==
    /\ resets < MaxResets
    /\ resets' = resets + 1
    /\ life' = life + 1
    /\ head' = 0
    /\ state' = {}
    /\ garbage' = garbage \cup {<<i, ec>> : i \in state}
    /\ pos' = [c \in Consumers |-> 1]
    /\ Tick
    /\ UNCHANGED <<span, stored, att, jobs, nfile, serving, zombie, tries, stopped, claims>>

Next ==
    \/ Commit \/ Reset \/ Takeover \/ Halt
    \/ \E c \in Consumers : Claim(c) \/ Settle(c) \/ Fail(c)
    \/ \E ids \in SUBSET state : StartMerge(ids) \/ MergeFails(ids)
    \/ \E j \in jobs : Publish(j) \/ Crash(j)
    \/ \E g \in garbage : CollectGarbage(g)
    \/ \E i \in stored : CollectOrphans(i) \/ ZombieCollects(i)

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------
(* Properties *)

\* The published spans tile this life's commits.
Tiling == Tiles(state)
\* No reader's endpoint is merged away: every endpoint is a boundary.
ReadsExact == \A e \in Endpoints : BoundaryIn(state, e)
\* Nothing live, pinned or still publishing is deleted.
StateStored == state \subseteq stored
ReadersStored == \A c \in Consumers : Active(c) => att[c].man \subseteq stored
PublishingStored == \A j \in jobs : j.st = "uploaded" /\ j.by = serving => j.out \in stored
\* R bounds the attempts of an input set.
AttemptsBounded == \A x \in DOMAIN tries : tries[x] <= R

=============================================================================
