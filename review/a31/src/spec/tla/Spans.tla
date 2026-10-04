------------------------------- MODULE Spans --------------------------------
(***************************************************************************)
(* The span key index's lifecycle as built (key index step 2 and later,   *)
(* with c4eb4f7's epochs: python/solera_server/upkeep.py, model.py,       *)
(* engine.py, journal.py): endpoints, passes and claims, merges and their *)
(* publication, pins and garbage, the orphan collector, crashes,          *)
(* takeovers, resets.                                                      *)
(*                                                                         *)
(* What a span holds is the Lean proofs' (experiments/lean/KeyIndex),     *)
(* taken as given: a read at commit e from spans that keep e as a         *)
(* boundary (a span's first commit, or a segment start a merge kept       *)
(* because e was an endpoint when it was planned) agrees with the full    *)
(* history. So a span is its commits [a, b], its segment starts, the      *)
(* index's life, whether it has files, and its writer's epoch; a read is  *)
(* exact iff its endpoints are boundaries.                                 *)
(*                                                                         *)
(* One index. Engines: the serving one and, after a takeover, a zombie    *)
(* (the one before), running on with the model it last had until it      *)
(* halts. An engine's epoch is its number, written by its fencing swap.   *)
(* The serving engine applies a publication to its model (mem, memg)      *)
(* first and the journal makes it durable (state, garbage) later; a       *)
(* takeover loses what was not. The index's other events wait for that    *)
(* (pend): the journal orders them after it.                               *)
(*                                                                         *)
(* Fix* select each guard (TRUE) or its absence (FALSE): the model must   *)
(* find what each one prevents. FixLanes and FixInputs back each other    *)
(* up: either alone keeps Tiling.                                          *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets, Sequences

CONSTANTS
    Consumers,     \* consumers of the index, each with a position
    MaxCommits,    \* commits per life
    MaxFiles,      \* spans written, over the whole behaviour
    MaxResets,     \* resets and renames: a new life of the index
    MaxTakeovers,  \* takeovers: the engine before runs on as a zombie
    MaxClaims,     \* attempts claimed, over the whole behaviour
    MaxJobs,       \* merges in flight at once (the base and tail lanes)
    R,             \* merge attempts per input set (MERGE_ATTEMPTS)
    MergeFailures, \* a merge's work may fail (store errors)
    EmptySpans,    \* a commit may change no key: a span with no files
    Passes,        \* passes: batches land at the pass's end, which the position holds
    Collectors,    \* the orphan collectors run (a model without them is smaller)
    FixLanding,    \* a claim's landing point is an endpoint from its plan on
    FixBounds,     \* a merge keeps a segment start at every endpoint it was planned with
    FixLanes,      \* the merges in flight never share an input (maintain's busy lanes)
    FixInputs,     \* publication re-checks the inputs (IndexState.holds)
    FixLife,       \* publication re-checks the index's life
    FixSettleLife, \* an attempt's commit re-checks the index's life
    FixPinFloor,   \* garbage is deleted only once no reader pinned before it remains
    FixDurable,    \* inputs become garbage at publication, never before
    FixRetries,    \* after R failures of an input set, the index merges no more
    FixEpoch,      \* the collector deletes outputs of its epoch or earlier only
    FixJudgeAfter, \* the collector judges what is named after its listing
    FixGarbageNamed \* the collector counts the garbage its model knows as named

VARIABLES
    head, life, span, stored, nfile, resets,
    state, garbage, \* durable: the journal's spans, and {<<span, counter let go of at>>}
    mem, memg,      \* the serving engine's model of them
    pend,           \* a publication in its model, not yet durable
    pos,            \* each consumer's position
    pass,           \* each consumer's pass end (0: none), an endpoint while it runs
    att,            \* each consumer's claims
    claims,         \* claims so far: their numbers
    jobs,           \* merges uploaded, not yet published, refused or abandoned
    ec,             \* the journal's event counter: pins and garbage entries are its values
    serving,        \* the serving engine's epoch
    zombie,         \* [alive, epoch, view, garbage: the model it last had, col: its collector]
    col,            \* the serving engine's collector: [ph, named, upto: the last file listed]
    tries, stopped  \* failed attempts per input set; the index merges no more (alarmed)

vars == <<head, life, span, stored, nfile, resets, state, garbage, mem, memg, pend, pos, pass,
          att, claims, jobs, ec, serving, zombie, col, tries, stopped>>

Idle == [ph |-> "idle", named |-> {}, upto |-> 0]
Claims == UNION {att[c] : c \in Consumers}

-----------------------------------------------------------------------------
(* Endpoints and boundaries *)

\* Model.endpoints: every position, a pass's end, and the landing points of
\* the claims of this life (with FixLanding).
Endpoints ==
    {pos[c] : c \in Consumers} \cup {pass[c] : c \in {d \in Consumers : pass[d] # 0}}
    \cup {x.land : x \in {y \in Claims : FixLanding /\ y.life = life}}

BoundaryIn(ids, e) == e = head + 1 \/ \E i \in ids : span[i].a = e \/ e \in span[i].bounds

Lo(ids) == CHOOSE x \in {span[i].a : i \in ids} : \A y \in {span[i].a : i \in ids} : x <= y
Hi(ids) == CHOOSE x \in {span[i].b : i \in ids} : \A y \in {span[i].b : i \in ids} : y <= x

Tiles(ids) ==
    /\ \A i \in ids : span[i].life = life /\ span[i].b <= head
    /\ \A n \in 1..head : Cardinality({i \in ids : span[i].a <= n /\ n <= span[i].b}) = 1

Contiguous(ids) ==
    \A n \in Lo(ids)..Hi(ids) : Cardinality({i \in ids : span[i].a <= n /\ n <= span[i].b}) = 1

GarbageFiles(g) == {x[1] : x \in g}
\* A span with no files is never missing.
Stored(i) == span[i].empty \/ i \in stored

-----------------------------------------------------------------------------
Init ==
    /\ head = 0 /\ life = 0 /\ span = <<>> /\ stored = {} /\ nfile = 0 /\ resets = 0
    /\ state = {} /\ garbage = {} /\ mem = {} /\ memg = {} /\ pend = FALSE
    /\ pos = [c \in Consumers |-> 1] /\ pass = [c \in Consumers |-> 0]
    /\ att = [c \in Consumers |-> {}] /\ claims = 0
    /\ jobs = {} /\ ec = 0 /\ serving = 0
    /\ zombie = [alive |-> FALSE, epoch |-> 0, view |-> {}, garbage |-> {}, col |-> Idle]
    /\ col = Idle
    /\ tries = [x \in {} |-> 0] /\ stopped = FALSE

NewSpan(a, b, bounds, delta, empty) ==
    /\ nfile < MaxFiles
    /\ nfile' = nfile + 1
    /\ span' = Append(span, [a |-> a, b |-> b, bounds |-> bounds, life |-> life, delta |-> delta,
                             empty |-> empty, epoch |-> serving])
    /\ stored' = stored \cup {nfile + 1}

\* An event of the serving engine's journal.
Tick == ec' = ec + 1

\* The commits head + 1 .. b as one span: one commit, or (an index only
\* some commits write, a failure index) the ones since the last it took.
CommitTo(b, empty) ==
    /\ ~pend /\ head < b /\ b <= MaxCommits
    /\ empty => EmptySpans
    /\ NewSpan(head + 1, b, {}, TRUE, empty)
    /\ head' = b
    /\ state' = state \cup {nfile + 1} /\ mem' = mem \cup {nfile + 1}
    /\ Tick
    /\ UNCHANGED <<life, resets, garbage, memg, pend, pos, pass, att, claims, jobs, serving, zombie,
                   col, tries, stopped>>
Commit == \E empty \in BOOLEAN : CommitTo(head + 1, empty)

-----------------------------------------------------------------------------
(* Passes and attempts *)

\* Prepare: the claim's reads are endpoints from now on (with FixLanding),
\* its manifest the state, its pin the claim's event counter. It lands at
\* the head + 1 or, while a pass is under way, at the pass's end. A
\* consumer may hold two claims (a retry claimed before the one it
\* replaces goes).
ClaimAt(c, land, ispass) ==
    /\ ~pend /\ claims < MaxClaims /\ Cardinality(att[c]) < 2
    /\ claims' = claims + 1
    /\ att' = [att EXCEPT ![c] = @ \cup {[k |-> claims + 1, land |-> land, man |-> state, pin |-> ec,
                                           life |-> life, pass |-> ispass]}]
    /\ Tick
    /\ UNCHANGED <<head, life, span, stored, nfile, resets, state, garbage, mem, memg, pend, pos, pass,
                   jobs, serving, zombie, col, tries, stopped>>
Claim(c) ==
    \/ ClaimAt(c, head + 1, FALSE)
    \/ Passes /\ ClaimAt(c, head + 1, TRUE)
    \/ pass[c] # 0 /\ ClaimAt(c, pass[c], TRUE)

\* Its result is part of the position, which moves to its landing point
\* (a pass's last batch: the pass is done); the claim, its reads and pin
\* go. One of an earlier life commits nothing (FixSettleLife).
Settle(c, x) ==
    /\ ~pend /\ x \in att[c]
    /\ pos' = IF ~FixSettleLife \/ x.life = life THEN [pos EXCEPT ![c] = x.land] ELSE pos
    /\ pass' = IF x.pass THEN [pass EXCEPT ![c] = 0] ELSE pass
    /\ att' = [att EXCEPT ![c] = @ \ {x}]
    /\ Tick
    /\ UNCHANGED <<head, life, span, stored, nfile, resets, state, garbage, mem, memg, pend, jobs,
                   claims, serving, zombie, col, tries, stopped>>

\* A pass's batch that is not its last commits: the position stays and
\* holds the pass's end (positions.py: `pass`, `to`) until its last batch.
Batch(c, x) ==
    /\ ~pend /\ x \in att[c] /\ x.pass
    /\ pass' = IF ~FixSettleLife \/ x.life = life THEN [pass EXCEPT ![c] = x.land] ELSE pass
    /\ att' = [att EXCEPT ![c] = @ \ {x}]
    /\ Tick
    /\ UNCHANGED <<head, life, span, stored, nfile, resets, state, garbage, mem, memg, pend, pos,
                   jobs, claims, serving, zombie, col, tries, stopped>>

\* It fails: the position stays.
Fail(c, x) ==
    /\ ~pend /\ x \in att[c]
    /\ att' = [att EXCEPT ![c] = @ \ {x}]
    /\ Tick
    /\ UNCHANGED <<head, life, span, stored, nfile, resets, state, garbage, mem, memg, pend, pos, pass,
                   jobs, claims, serving, zombie, col, tries, stopped>>

-----------------------------------------------------------------------------
(* Merges (Upkeep.maintain, _merge) *)

InputsOf(ids) == {<<span[i].a, span[i].b>> : i \in ids}
Tries(x) == IF x \in DOMAIN tries THEN tries[x] ELSE 0
SetTries(x, n) == [y \in DOMAIN tries \cup {x} |-> IF y = x THEN n ELSE tries[y]]
Running(e) == {j \in jobs : j.st = "uploaded" /\ j.by = e}

\* Plan, from the serving engine's model, and upload: the endpoints known
\* now are kept as segment starts (FixBounds); the output's name carries
\* its epoch. Without FixDurable, the inputs are let go of at once.
Inner(ids) == ({span[i].a : i \in ids} \cup UNION {span[i].bounds : i \in ids}) \ {Lo(ids)}
Required(ids) == IF FixBounds THEN {e \in Inner(ids) : e \in Endpoints} ELSE {}
\* With `bounds` its segment starts: Required(ids) or, as the code may keep
\* more (endpoints this spec does not model, a pattern change's), any of
\* Inner(ids) beside them (SpansTrace.tla).
StartMergeWith(ids, bounds) ==
    /\ ~stopped
    /\ Cardinality(Running(serving)) < MaxJobs
    /\ ids # {} /\ ids \subseteq mem /\ Contiguous(ids)
    /\ FixLanes => ~\E j \in Running(serving) : j.inputs \cap ids # {}
    /\ FixRetries => Tries(InputsOf(ids)) < R
    /\ NewSpan(Lo(ids), Hi(ids), bounds, FALSE, \A i \in ids : span[i].empty)
    /\ jobs' = jobs \cup {[inputs |-> ids, out |-> nfile + 1, life |-> life, by |-> serving, st |-> "uploaded"]}
    /\ IF FixDurable THEN UNCHANGED <<garbage, memg, ec>>
       ELSE /\ ~pend
            /\ garbage' = garbage \cup {<<i, ec>> : i \in ids} /\ memg' = garbage'
            /\ Tick
    /\ UNCHANGED <<head, life, resets, state, mem, pend, pos, pass, att, claims, serving, zombie, col,
                   tries, stopped>>
StartMerge(ids) == StartMergeWith(ids, Required(ids))

\* The merge's work fails (a store error): no output; one more attempt of
\* this input set (with FixRetries: at R, the index stops merging).
MergeFails(ids) ==
    /\ MergeFailures /\ ~stopped /\ ids # {} /\ ids \subseteq mem /\ Contiguous(ids)
    /\ FixRetries => Tries(InputsOf(ids)) < R
    /\ tries' = SetTries(InputsOf(ids), Tries(InputsOf(ids)) + 1)
    /\ stopped' = (FixRetries /\ Tries(InputsOf(ids)) + 1 >= R)
    /\ UNCHANGED <<head, life, span, stored, nfile, resets, state, garbage, mem, memg, pend, pos, pass,
                   att, claims, jobs, ec, serving, zombie, col>>

\* IndexState.holds: a current span with the input's commits and files.
\* Spans with no files have the same (empty) name lists in any life.
Same(i, k) == span[k].a = span[i].a /\ span[k].b = span[i].b /\ (k = i \/ (span[i].empty /\ span[k].empty))
Holds(j, view) == \A i \in j.inputs : \E k \in view : Same(i, k)
Matched(j, view) == {k \in view : \E i \in j.inputs : Same(i, k)}

\* Publish (IndexMerged), in the model of the engine that started it: the
\* index still of its life (FixLife), holding its inputs (FixInputs); the
\* spans it replaces become garbage. Refused, the output is deleted.
Publishes(j, view) == (FixInputs => Holds(j, view)) /\ (FixLife => j.life = life)
Installed(j, view) == (view \ (j.inputs \cup Matched(j, view))) \cup {j.out}
Replaced(j, view) == {<<i, ec>> : i \in view \cap (j.inputs \cup Matched(j, view))}

PublishMem(j) ==
    /\ ~pend /\ j \in Running(serving)
    /\ IF Publishes(j, mem)
       THEN /\ mem' = Installed(j, mem)
            /\ memg' = IF FixDurable THEN memg \cup Replaced(j, mem) ELSE memg
            /\ pend' = TRUE
            /\ UNCHANGED stored
       ELSE /\ stored' = stored \ {j.out}
            /\ UNCHANGED <<mem, memg, pend>>
    /\ jobs' = jobs \ {j}
    /\ Tick
    /\ UNCHANGED <<head, life, span, nfile, resets, state, garbage, pos, pass, att, claims, serving,
                   zombie, col, tries, stopped>>

\* The journal makes it durable.
Flush ==
    /\ pend
    /\ state' = mem /\ garbage' = memg /\ pend' = FALSE
    /\ UNCHANGED <<head, life, span, stored, nfile, resets, mem, memg, pos, pass, att, claims, jobs,
                   ec, serving, zombie, col, tries, stopped>>

\* The merge's process dies between upload and publication.
Crash(j) ==
    /\ j \in jobs /\ j.st = "uploaded"
    /\ jobs' = (jobs \ {j}) \cup {[j EXCEPT !.st = "abandoned"]}
    /\ UNCHANGED <<head, life, span, stored, nfile, resets, state, garbage, mem, memg, pend, pos, pass,
                   att, claims, ec, serving, zombie, col, tries, stopped>>

\* A zombie publishes a merge it started, in its own model: never durable.
ZombiePublishes(j) ==
    /\ zombie.alive /\ j \in Running(zombie.epoch)
    /\ IF Publishes(j, zombie.view)
       THEN /\ zombie' = [zombie EXCEPT !.view = Installed(j, zombie.view),
                                        !.garbage = @ \cup Replaced(j, zombie.view)]
            /\ UNCHANGED stored
       ELSE /\ stored' = stored \ {j.out}
            /\ UNCHANGED zombie
    /\ jobs' = jobs \ {j}
    /\ UNCHANGED <<head, life, span, nfile, resets, state, garbage, mem, memg, pend, pos, pass, att,
                   claims, ec, serving, col, tries, stopped>>

-----------------------------------------------------------------------------
(* Garbage and orphans *)

\* The oldest reader's pin (the claims' event counters).
Pins == {x.pin : x \in Claims}
PinFloor == IF Pins = {} THEN ec ELSE CHOOSE p \in Pins : \A q \in Pins : p <= q

\* Upkeep.collect: a durable garbage entry let go of at or before the pin
\* floor (it awaits durable() first, which a fenced engine never passes).
CollectGarbage(g) ==
    /\ ~pend /\ g \in garbage
    /\ FixPinFloor => g[2] <= PinFloor
    /\ stored' = stored \ {g[1]}
    /\ garbage' = garbage \ {g} /\ memg' = memg \ {g}
    /\ Tick
    /\ UNCHANGED <<head, life, span, nfile, resets, state, mem, pend, pos, pass, att, claims, jobs,
                   serving, zombie, col, tries, stopped>>

\* A zombie's Upkeep.collect whose durable() passed before the fence: it
\* deletes garbage the journal let go of, which its successor replays as
\* garbage too, under the floor of the claims both know.
ZombieCollectsGarbage(g) ==
    /\ zombie.alive /\ g \in zombie.garbage /\ g \in garbage
    /\ FixPinFloor => g[2] <= PinFloor
    /\ stored' = stored \ {g[1]}
    /\ UNCHANGED <<head, life, span, nfile, resets, state, garbage, mem, memg, pend, pos, pass, att,
                   claims, jobs, ec, serving, zombie, col, tries, stopped>>

\* Upkeep.collect_orphans, by the engine of epoch `e` whose model is `view`
\* and `known` garbage: list the merge outputs (files that are not
\* deltas), then delete, one at a time, those its model does not name, of
\* its epoch or earlier (FixEpoch). What is named is judged after the
\* listing (FixJudgeAfter); without, as named before it. A listing is the
\* files written up to then (`upto`): files are never written again, so
\* those still stored are what it listed and nobody deleted since.
Named(view, known, e) ==
    view \cup (IF FixGarbageNamed THEN GarbageFiles(known) ELSE {}) \cup {j.out : j \in Running(e)}
Orphan(c, i, view, known, e) ==
    /\ c.ph = "listed" /\ i <= c.upto /\ i \in stored /\ ~span[i].delta /\ ~span[i].empty
    /\ i \notin (IF FixJudgeAfter THEN Named(view, known, e) ELSE c.named)
    /\ FixEpoch => span[i].epoch <= e
    /\ stored' = stored \ {i}

ServingLists ==
    /\ Collectors
    /\ \/ ~FixJudgeAfter /\ col' = [ph |-> "named", named |-> Named(mem, memg, serving), upto |-> 0]
       \/ (FixJudgeAfter \/ col.ph = "named") /\ col' = [col EXCEPT !.ph = "listed", !.upto = nfile]
    /\ UNCHANGED <<head, life, span, stored, nfile, resets, state, garbage, mem, memg, pend, pos, pass,
                   att, claims, jobs, ec, serving, zombie, tries, stopped>>
ServingDeletes(i) ==
    /\ Orphan(col, i, mem, memg, serving)
    /\ jobs' = {j \in jobs : ~(j.out = i /\ j.st = "abandoned")}
    /\ UNCHANGED <<head, life, span, nfile, resets, state, garbage, mem, memg, pend, pos, pass, att,
                   claims, ec, serving, zombie, col, tries, stopped>>
ZombieLists ==
    /\ Collectors /\ zombie.alive
    /\ \/ ~FixJudgeAfter
          /\ zombie' = [zombie EXCEPT !.col = [ph |-> "named", upto |-> 0,
                                               named |-> Named(zombie.view, zombie.garbage, zombie.epoch)]]
       \/ (FixJudgeAfter \/ zombie.col.ph = "named")
          /\ zombie' = [zombie EXCEPT !.col = [@ EXCEPT !.ph = "listed", !.upto = nfile]]
    /\ UNCHANGED <<head, life, span, stored, nfile, resets, state, garbage, mem, memg, pend, pos, pass,
                   att, claims, jobs, ec, serving, col, tries, stopped>>
ZombieDeletes(i) ==
    /\ zombie.alive
    /\ Orphan(zombie.col, i, zombie.view, zombie.garbage, zombie.epoch)
    /\ UNCHANGED <<head, life, span, nfile, resets, state, garbage, mem, memg, pend, pos, pass, att,
                   claims, jobs, ec, serving, zombie, col, tries, stopped>>

-----------------------------------------------------------------------------
(* Takeovers and resets *)

\* A new engine takes over: its fencing swap writes epoch + 1, and it
\* replays the durable journal. The one before runs on as a zombie with
\* its model, a publication not yet durable included, and its collector
\* wherever it was.
Takeover ==
    /\ serving < MaxTakeovers
    /\ serving' = serving + 1
    /\ zombie' = [alive |-> TRUE, epoch |-> serving, view |-> mem, garbage |-> memg, col |-> col]
    /\ mem' = state /\ memg' = garbage /\ pend' = FALSE
    /\ col' = Idle
    /\ UNCHANGED <<head, life, span, stored, nfile, resets, state, garbage, pos, pass, att, claims,
                   jobs, ec, tries, stopped>>

\* The zombie learns it is fenced (a journal write fails) and halts.
Halt ==
    /\ zombie.alive
    /\ zombie' = [zombie EXCEPT !.alive = FALSE]
    /\ UNCHANGED <<head, life, span, stored, nfile, resets, state, garbage, mem, memg, pend, pos, pass,
                   att, claims, jobs, ec, serving, col, tries, stopped>>

\* A reset, a store move or a rename to a new life: the index is new and
\* empty, every position and pass goes; the old life's spans are garbage.
Reset ==
    /\ ~pend /\ resets < MaxResets
    /\ resets' = resets + 1
    /\ life' = life + 1
    /\ head' = 0
    /\ state' = {} /\ mem' = {}
    /\ garbage' = garbage \cup {<<i, ec>> : i \in state} /\ memg' = garbage'
    /\ pos' = [c \in Consumers |-> 1] /\ pass' = [c \in Consumers |-> 0]
    /\ Tick
    /\ UNCHANGED <<span, stored, nfile, pend, att, claims, jobs, serving, zombie, col, tries, stopped>>

Next ==
    \/ Commit \/ Reset \/ Takeover \/ Halt \/ Flush \/ ServingLists \/ ZombieLists
    \/ \E c \in Consumers : Claim(c) \/ \E x \in att[c] : Settle(c, x) \/ Batch(c, x) \/ Fail(c, x)
    \/ \E ids \in SUBSET mem : StartMerge(ids) \/ MergeFails(ids)
    \/ \E j \in jobs : PublishMem(j) \/ Crash(j) \/ ZombiePublishes(j)
    \/ \E g \in garbage : CollectGarbage(g) \/ ZombieCollectsGarbage(g)
    \/ \E i \in stored : ServingDeletes(i) \/ ZombieDeletes(i)

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------
(* Properties *)

\* The published spans tile this life's commits, durable and in the model.
Tiling == Tiles(state) /\ Tiles(mem)
\* No reader's endpoint is merged away: every endpoint is a boundary.
ReadsExact == \A e \in Endpoints : BoundaryIn(state, e) /\ BoundaryIn(mem, e)
\* Nothing live, pinned or still publishing is deleted: what the journal
\* names (what a successor serves) and what the serving engine's model names.
StateStored == \A i \in state \cup mem : Stored(i)
ReadersStored == \A x \in Claims : \A i \in x.man : Stored(i)
PublishingStored == \A j \in Running(serving) : Stored(j.out)
\* R bounds the attempts of an input set.
AttemptsBounded == \A x \in DOMAIN tries : tries[x] <= R

\* Not a property: a state `takeover` must reach (check.sh's `unpublished`
\* finds it), the zombie's publication that never became durable: its
\* model let go of a merge output the journal still names.
NoUnpublished ==
    ~(zombie.alive /\ \E i \in GarbageFiles(zombie.garbage) \cap state : ~span[i].delta /\ ~span[i].empty)

=============================================================================
