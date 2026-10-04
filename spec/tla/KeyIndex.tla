------------------------------ MODULE KeyIndex ------------------------------
(***************************************************************************)
(* The span key index's lifecycle (docs/key-index-design.md, "Lifecycles  *)
(* the implementation must honour" and "What must be checked"): endpoint  *)
(* reservations, merge publication, pins, crashes, deletion, resets.      *)
(*                                                                         *)
(* What a span holds is the Lean proofs' (WriteBound, Segments, Tiling,   *)
(* Keys): given as proved, a read at commit e from spans that keep e as a *)
(* boundary (a span's first commit, or a segment start inside it, where   *)
(* a merge keeps the versions a live endpoint sees) agrees with the full  *)
(* history. So a span here is its commits [a, b], the segment starts it   *)
(* keeps, and the index's life it was written in; and a read is exact iff *)
(* its endpoints are boundaries of the spans it reads.                    *)
(*                                                                         *)
(* One index (one output partition). Commits append delta spans. Consumer *)
(* positions hold endpoints; an attempt reserves its landing point (the   *)
(* head + 1) at its claim, pins the manifest it reads, and moves its      *)
(* consumer's position there when it settles. Merge jobs upload an       *)
(* output, then publish it through the journal, or crash; inputs are      *)
(* deleted after publication; an orphan collector reclaims what nothing   *)
(* references. A reset starts a new life of the index.                    *)
(*                                                                         *)
(* FixLanding, FixBounds, FixInputs, FixLife, FixPinFloor, FixDurable and *)
(* FixAtomicPin select each rule as designed (TRUE) or without it (FALSE): *)
(* the model must find what each rule prevents.                           *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets

CONSTANTS
    Consumers,     \* consumers of the index, each with a position
    MaxCommits,    \* commits per life
    MaxFiles,      \* span files written, over the whole behaviour
    MaxResets,     \* resets (store moves, removals and re-adds)
    MaxJobs,       \* merges uploaded and not yet settled at once (the two upkeep lanes)
    FixLanding,    \* an attempt reserves its landing point at its claim (P1-1, A10)
    FixBounds,     \* a merge keeps a segment start at every live endpoint inside it
    FixInputs,     \* publication re-checks the merge's inputs are the current spans
    FixLife,       \* an attempt's commit re-checks the index's life
    FixPinFloor,   \* inputs are deleted only once no pin holds them
    FixDurable,    \* inputs are deleted only after the publication is durable
    FixAtomicPin   \* a reader acquires its manifest and its pin in one step

VARIABLES
    head,      \* commits in this life
    life,      \* the index's life
    span,      \* [file id -> [a, b, bounds, life]]: every span file ever written (ghost)
    stored,    \* the file ids the object store holds
    state,     \* the published index state: the current spans (the journal)
    pos,       \* [Consumers -> the commit its position reads from next]
    att,       \* [Consumers -> its attempt: none, or [land, man, pinned, life]]
    jobs,      \* merge jobs: [inputs, out, life, st]
    nfile,     \* file ids used
    resets

vars == <<head, life, span, stored, state, pos, att, jobs, nfile, resets>>

None == [none |-> TRUE]
Active(c) == att[c] # None

-----------------------------------------------------------------------------
(* Endpoints and boundaries *)

\* The endpoints reserved: every position, and every landing point of an
\* attempt of this life (with FixLanding; one of an earlier life is fenced).
Reserved == {pos[c] : c \in Consumers}
            \cup {att[c].land : c \in {d \in Consumers : Active(d) /\ FixLanding /\ att[d].life = life}}

\* Commit e is a boundary of the spans `ids`: past the head, a span's first
\* commit, or a segment start a span kept.
BoundaryIn(ids, e) == e = head + 1 \/ \E i \in ids : span[i].a = e \/ e \in span[i].bounds

\* The spans of `ids` tile commits 1..head of this life exactly.
Tiles(ids) ==
    /\ \A i \in ids : span[i].life = life
    /\ \A n \in 1..head : Cardinality({i \in ids : span[i].a <= n /\ n <= span[i].b}) = 1
    /\ \A i \in ids : span[i].b <= head

\* Adjacent spans of the state: a contiguous run of commits.
Contiguous(ids) ==
    LET lo == CHOOSE x \in {span[i].a : i \in ids} : \A y \in {span[i].a : i \in ids} : x <= y
        hi == CHOOSE x \in {span[i].b : i \in ids} : \A y \in {span[i].b : i \in ids} : y <= x
    IN \A n \in lo..hi : Cardinality({i \in ids : span[i].a <= n /\ n <= span[i].b}) = 1

-----------------------------------------------------------------------------
Init ==
    /\ head = 0
    /\ life = 0
    /\ span = [i \in {} |-> None]
    /\ stored = {}
    /\ state = {}
    /\ pos = [c \in Consumers |-> 1]
    /\ att = [c \in Consumers |-> None]
    /\ jobs = {}
    /\ nfile = 0
    /\ resets = 0

NewSpan(a, b, bounds) ==
    /\ nfile < MaxFiles
    /\ nfile' = nfile + 1
    /\ span' = [i \in DOMAIN span \cup {nfile + 1} |->
                   IF i = nfile + 1 THEN [a |-> a, b |-> b, bounds |-> bounds, life |-> life] ELSE span[i]]

\* A commit: its delta is the span [head + 1, head + 1], published.
Commit ==
    /\ head < MaxCommits
    /\ NewSpan(head + 1, head + 1, {})
    /\ stored' = stored \cup {nfile + 1}
    /\ head' = head + 1
    /\ state' = state \cup {nfile + 1}
    /\ UNCHANGED <<life, pos, att, jobs, resets>>

-----------------------------------------------------------------------------
(* Readers: a consumer's attempt *)

\* Claim: reserve the landing point, the head + 1 (FixLanding), and take
\* the manifest with its pin (FixAtomicPin: in one step; else the pin is
\* registered by a later step).
Claim(c) ==
    /\ ~Active(c)
    /\ att' = [att EXCEPT ![c] = [land |-> head + 1, man |-> state, pinned |-> FixAtomicPin, life |-> life]]
    /\ UNCHANGED <<head, life, span, stored, state, pos, jobs, nfile, resets>>

Pin(c) ==
    /\ Active(c) /\ ~att[c].pinned
    /\ att' = [att EXCEPT ![c].pinned = TRUE]
    /\ UNCHANGED <<head, life, span, stored, state, pos, jobs, nfile, resets>>

\* Settle: the result is durably part of the position, which moves to the
\* landing point; the reservation and the pin are released. An attempt of
\* an earlier life commits nothing (FixLife).
Settle(c) ==
    /\ Active(c) /\ att[c].pinned
    /\ pos' = IF ~FixLife \/ att[c].life = life THEN [pos EXCEPT ![c] = att[c].land] ELSE pos
    /\ att' = [att EXCEPT ![c] = None]
    /\ UNCHANGED <<head, life, span, stored, state, jobs, nfile, resets>>

\* A failed attempt releases its reservation and pin; the position stays.
Fail(c) ==
    /\ Active(c)
    /\ att' = [att EXCEPT ![c] = None]
    /\ UNCHANGED <<head, life, span, stored, state, pos, jobs, nfile, resets>>

-----------------------------------------------------------------------------
(* Merges *)

\* Upkeep merges adjacent spans of the state (or rewrites one alone):
\* uploads an output under a fresh name, keeping a segment start at every
\* live endpoint inside it (FixBounds). Without FixDurable, it deletes its
\* inputs now, before publishing.
StartMerge(ids) ==
    /\ Cardinality({j \in jobs : j.st = "uploaded"}) < MaxJobs
    /\ ids # {} /\ ids \subseteq state /\ Contiguous(ids)
    /\ LET lo == CHOOSE x \in {span[i].a : i \in ids} : \A y \in {span[i].a : i \in ids} : x <= y
           hi == CHOOSE x \in {span[i].b : i \in ids} : \A y \in {span[i].b : i \in ids} : y <= x
           inner == {span[i].a : i \in ids} \cup UNION {span[i].bounds : i \in ids}
       IN /\ NewSpan(lo, hi, IF FixBounds THEN {e \in inner : e # lo /\ e \in Reserved} ELSE {})
          /\ jobs' = jobs \cup {[inputs |-> ids, out |-> nfile + 1, life |-> life, st |-> "uploaded"]}
    /\ stored' = IF FixDurable THEN stored \cup {nfile + 1} ELSE (stored \cup {nfile + 1}) \ ids
    /\ UNCHANGED <<head, life, state, pos, att, resets>>

\* Publish through the journal: the merge's inputs must still be the
\* current spans (FixInputs) of this life; else it is refused, and its
\* output is an orphan. (The life check is redundant while file identities
\* never repeat: TLC finds nothing with it off. It guards against names
\* that could, such as names derived from commit numbers.)
Publish(j) ==
    /\ j \in jobs /\ j.st = "uploaded"
    /\ IF (FixInputs => j.inputs \subseteq state) /\ j.life = life
       THEN /\ state' = (state \ j.inputs) \cup {j.out}
            /\ jobs' = (jobs \ {j}) \cup {[j EXCEPT !.st = "published"]}
       ELSE /\ jobs' = (jobs \ {j}) \cup {[j EXCEPT !.st = "refused"]}
            /\ UNCHANGED state
    /\ UNCHANGED <<head, life, span, stored, pos, att, nfile, resets>>

\* The merge's process dies after the upload, before publishing.
Crash(j) ==
    /\ j \in jobs /\ j.st = "uploaded"
    /\ jobs' = (jobs \ {j}) \cup {[j EXCEPT !.st = "abandoned"]}
    /\ UNCHANGED <<head, life, span, stored, state, pos, att, nfile, resets>>

\* Delete a published merge's inputs, once no pin holds them (FixPinFloor).
Pinned == UNION {att[c].man : c \in {d \in Consumers : Active(d) /\ att[d].pinned}}
DeleteInputs(j) ==
    /\ j \in jobs /\ j.st = "published"
    /\ FixPinFloor => j.inputs \cap Pinned = {}
    /\ stored' = stored \ j.inputs
    /\ jobs' = jobs \ {j}
    /\ UNCHANGED <<head, life, span, state, pos, att, nfile, resets>>

\* The orphan collector: a file no current span, pin, or merge in progress
\* (an upload not yet published, refused or abandoned) references.
Collect(i) ==
    /\ i \in stored
    /\ i \notin state /\ i \notin Pinned
    /\ ~\E j \in jobs : j.st = "uploaded" /\ (i = j.out \/ i \in j.inputs)
    /\ ~\E j \in jobs : j.st = "published" /\ i \in j.inputs
    /\ stored' = stored \ {i}
    /\ jobs' = {j \in jobs : ~(j.st \in {"refused", "abandoned"} /\ j.out = i)}
    /\ UNCHANGED <<head, life, span, state, pos, att, nfile, resets>>

-----------------------------------------------------------------------------
\* A reset (a store move, a removal and re-add): a new life of the index,
\* empty; every position goes (its next run reads a full pass).
Reset ==
    /\ resets < MaxResets
    /\ resets' = resets + 1
    /\ life' = life + 1
    /\ head' = 0
    /\ state' = {}
    /\ pos' = [c \in Consumers |-> 1]
    /\ UNCHANGED <<span, stored, att, jobs, nfile>>

Next ==
    \/ Commit \/ Reset
    \/ \E c \in Consumers : Claim(c) \/ Pin(c) \/ Settle(c) \/ Fail(c)
    \/ \E ids \in SUBSET state : StartMerge(ids)
    \/ \E j \in jobs : Publish(j) \/ Crash(j) \/ DeleteInputs(j)
    \/ \E i \in stored : Collect(i)

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------
(* Properties *)

\* The published state tiles this life's commits exactly: no published
\* merge from an old life or over replaced inputs.
Tiling == Tiles(state)

\* Every reserved endpoint is a boundary of the state: reads there agree
\* with the full history (the Lean results).
ReadsExact == \A e \in Reserved : BoundaryIn(state, e)

\* Every file the state names, and every file a reader holds a manifest
\* of, is still stored.
StateStored == state \subseteq stored
ReadersStored == \A c \in Consumers : Active(c) => att[c].man \subseteq stored

=============================================================================
