------------------------------ MODULE Positions ------------------------------
(***************************************************************************)
(* Positions and staleness (docs/positions-from-reads.md: K43, K45 and    *)
(* its amendment, the full pass completed across runs, K46): what a run    *)
(* reads, what the partition record keeps of it, and whether the status   *)
(* derived from that record tells the truth.                               *)
(*                                                                         *)
(* A chain S -> A -> B of keyed outputs, one partition each. S is a       *)
(* source. A reads S incrementally and copies it; B reads A incrementally  *)
(* under its patterns, each=True or not (`Each`). Every commit has a      *)
(* fresh id, and a key's version is the id of the commit that last wrote  *)
(* it (0: absent), so "changed past a position" is a version comparison.  *)
(* A run plans one batch when it claims the partition and commits it      *)
(* later; the upstream may commit in between. No workers or faults: they  *)
(* are Execution.tla's.                                                    *)
(*                                                                         *)
(* The record of an incremental input (K45): a snapshot (the upstream     *)
(* commit read through), the keys keys= runs read past it, each with the  *)
(* commit it read at (at most MaxEntries of them), and the full pass due  *)
(* after a reset or an asset change, with the keys it delivered (the      *)
(* correction: a pass may be completed across runs, keys= or default).    *)
(* For each=True, the record is per key (K43): the version it read.       *)
(*                                                                         *)
(* FixNet, FixTransitive, FixSkip, FixContinue and FixCollapse select each *)
(* rule as designed (TRUE) or without it (FALSE): the model must find     *)
(* what each rule prevents.                                                *)
(***************************************************************************)
EXTENDS Integers, FiniteSets, Sequences, TLC

CONSTANTS
    NK,           \* keys are 1..NK
    MaxSrc,       \* commits to S
    MaxKeysRuns,  \* keys= runs, of one key each
    MaxChanges,   \* deploys: A moved (a reset), B's patterns changed or version bumped
    MaxEntries,   \* the read-ahead's cap, on entries (K45's amendment)
    Each,         \* B is each=True
    FixNet,       \* "behind" is the net delta: a key added and removed past the position does not count
    FixTransitive,\* stale if an upstream it depends on is stale (K46)
    FixSkip,      \* a default run skips a key a keys= run already read at its version
    FixContinue,  \* a default run continues a pass keys= runs began; else it starts over
    FixCollapse   \* a keys= commit collapses the record only once every key is covered

Keys == 1..NK
Assets == {"A", "B"}
Up(c) == IF c = "A" THEN "S" ELSE "A"
Down(c) == IF c = "S" THEN "A" ELSE "B"

VARIABLES
    log,      \* [{"S","A","B"} -> Seq([k |-> version])]: each output's commits in its life
    clock,    \* the last commit id
    life,     \* [{"S","A","B"} -> Nat]: a reset makes the output new under its name
    rec,      \* [Assets -> record of its input]
    from,     \* [Assets -> [Keys -> [v, d]]]: ghost: the input version and the definition
              \*   each held key was produced from
    chg,      \* [Assets -> Nat]: the deploy of its last asset change
    pat,      \* B's patterns
    ver,      \* B's version (A has one, never bumped)
    att,      \* [Assets -> the attempt in flight, or none]
    twice,    \* ghost: a key delivered again at a version, under a definition, it was produced from
    used      \* budgets spent: [src, keys, changes]

vars == <<log, clock, life, rec, from, chg, pat, ver, att, twice, used>>
Outs == {"S", "A", "B"}

-----------------------------------------------------------------------------
(* Versions *)

Max(S) == CHOOSE x \in S : \A y \in S : y <= x
\* The version of key k in output o after its first n commits (0: absent).
RECURSIVE VerAt(_, _, _)
VerAt(o, k, n) == IF n = 0 THEN 0
                  ELSE IF k \in DOMAIN log[o][n] THEN log[o][n][k] ELSE VerAt(o, k, n - 1)
Top(o) == Len(log[o])
Cur(o, k) == VerAt(o, k, Top(o))
Held(o) == {k \in Keys : Cur(o, k) # 0}
Pat(c) == IF c = "A" THEN Keys ELSE pat
EachOf(c) == Each /\ c = "B"
\* What c should hold: its input's keys, under c's patterns.
Want(c) == Held(Up(c)) \cap Pat(c)
\* A key c's patterns take that some commit of its input past n touched:
\* the plain rule "behind" was before the net delta (FixNet off).
Delta(c, n) == {k \in Pat(c) : \E i \in (n + 1)..Top(Up(c)) : k \in DOMAIN log[Up(c)][i]}

-----------------------------------------------------------------------------
(* The record of c's input, r:                                            *)
(*   snap, lf: the input commit read through, in the input's life `lf`    *)
(*     (-1: none, after a reset);                                          *)
(*   ahead: what keys= runs read past it, <<key, version read>> (K45);     *)
(*   pass: a full pass due (after a reset or an asset change), begun (its *)
(*     first batch started the output over), the keys it delivered;       *)
(*   per: each=True only, the version each key was read at (K43);         *)
(*   def: the definition (deploy) its last finished pass was under.       *)

NoPass == [due |-> FALSE, begun |-> FALSE, done |-> {}]
DuePass == [NoPass EXCEPT !.due = TRUE]
InPass(c, r) == r.pass.due \/ r.lf # life[Up(c)] \/ r.snap < 0

\* The version r says key k was last read at (-1: not read).
ReadAt(c, r, k) ==
    IF EachOf(c) THEN r.per[k]
    ELSE LET vs == {e[2] : e \in {e \in r.pass.done \cup r.ahead : e[1] = k}} IN
         IF vs # {} THEN CHOOSE v \in vs : TRUE   \* one entry per key
         ELSE IF InPass(c, r) THEN -1
         ELSE VerAt(Up(c), k, r.snap)

\* The keys r says are behind, c holding `held`: read at an older version or
\* not at all, or held though no longer wanted. A key's last read version
\* is the read-ahead's, else the pass's, else the snapshot's (K45): a key
\* read ahead and then removed upstream is behind, though the net delta
\* past the snapshot omits it. Without FixNet, a key some commit past the
\* snapshot touched is behind too, unless read ahead at its version.
BehindR(c, r, held) ==
    LET u == Up(c) IN
    {k \in Pat(c) : LET rv == ReadAt(c, r, k)  cv == Cur(u, k) IN rv # cv /\ ~(rv <= 0 /\ cv = 0)}
    \cup (held \ Want(c))
    \cup (IF FixNet \/ EachOf(c) \/ InPass(c, r) THEN {}
          ELSE {k \in Delta(c, r.snap) : ~\E e \in r.ahead : e[1] = k /\ e[2] = Cur(u, k)})

\* The status the record gives: directly, and with its upstream's (K46).
DirectStale(c) == BehindR(c, rec[c], Held(c)) # {} \/ rec[c].def < chg[c]
StoredStale(c) == DirectStale(c) \/ (FixTransitive /\ c = "B" /\ DirectStale("A"))

-----------------------------------------------------------------------------
(* The truth: what a rerun of the chain would change. c holds exactly what *)
(* it should, each key produced from its input's current version under    *)
(* c's current definition, and its last full pass was under it.           *)

Correct(c) ==
    /\ Held(c) = Want(c)
    /\ \A k \in Held(c) : from[c][k].v = Cur(Up(c), k) /\ from[c][k].d >= chg[c]
    /\ rec[c].def >= chg[c]
TrueStale(c) == ~Correct(c) \/ (c = "B" /\ ~Correct("A"))

-----------------------------------------------------------------------------
Init ==
    /\ log = [o \in Outs |-> <<>>]
    /\ clock = 0
    /\ life = [o \in Outs |-> 0]
    /\ chg = [c \in Assets |-> 0]
    /\ rec = [c \in Assets |-> [snap |-> -1, lf |-> 0, ahead |-> {}, pass |-> DuePass,
                                per |-> [k \in Keys |-> 0], def |-> 0]]
    /\ from = [c \in Assets |-> [k \in Keys |-> [v |-> 0, d |-> 0]]]
    /\ pat = Keys
    /\ ver = 1
    /\ att = [c \in Assets |-> [busy |-> FALSE]]
    /\ twice = FALSE
    /\ used = [src |-> 0, keys |-> 0, changes |-> 0]

\* A commit of o: each key of m written (a fresh version) or removed;
\* nothing, if m names no key.
Commit(o, m) ==
    IF DOMAIN m = {} THEN UNCHANGED <<log, clock>>
    ELSE /\ clock' = clock + 1
         /\ log' = [log EXCEPT ![o] = Append(@, [k \in DOMAIN m |-> IF m[k] THEN clock + 1 ELSE 0])]

-----------------------------------------------------------------------------
(* Planning a batch at the claim: the keys it delivers (with the version  *)
(* read) and whether it starts the output over.                          *)

\* A keys=(k) run: the first delivery of a pass starts over with k; later
\* ones continue the pass; outside a pass, k is read ahead if it is behind.
KeysPlan(c, k) ==
    LET r == rec[c]  first == InPass(c, r) /\ ~r.pass.begun IN
    [kind |-> "keys", over |-> first, pin |-> Top(Up(c)),
     keys |-> IF k \in Pat(c) /\ (first \/ k \in BehindR(c, r, Held(c))) THEN {k} ELSE {}]

\* A default run: in a pass, the keys it has not delivered at their current
\* version, and it finishes the pass (FixContinue; else it starts it over);
\* outside one, what is behind: the net delta past the snapshot, and the
\* read-ahead's keys that changed since (without FixSkip: every key read
\* ahead, read again).
DefaultPlan(c) ==
    LET r == rec[c]  goOn == FixContinue /\ r.pass.begun IN
    IF InPass(c, r)
    THEN [kind |-> "default", pin |-> Top(Up(c)), over |-> ~goOn,
          keys |-> IF goOn THEN BehindR(c, r, Held(c)) ELSE Want(c) \cup Held(c)]
    ELSE [kind |-> "default", pin |-> Top(Up(c)), over |-> FALSE,
          keys |-> IF FixSkip THEN BehindR(c, r, Held(c))
                   ELSE BehindR(c, r, Held(c)) \cup {e[1] : e \in r.ahead}]

\* A key delivered again at the input version, under the definition, it was
\* produced from: delivered twice.
Again(c, p) ==
    \E k \in p.keys \cap Pat(c) : Cur(c, k) # 0 /\ Cur(Up(c), k) # 0
                      /\ from[c][k].v = Cur(Up(c), k) /\ from[c][k].d >= chg[c]

Claim(c, p) ==
    /\ ~att[c].busy
    /\ att' = [att EXCEPT ![c] = [busy |-> TRUE, plan |-> p, lf |-> life[c], uplf |-> life[Up(c)],
                                   read |-> [k \in Keys |-> Cur(Up(c), k)], def |-> chg[c],
                                   rec |-> rec[c]]]
    /\ twice' = (twice \/ Again(c, p))
    /\ UNCHANGED <<log, clock, life, rec, from, chg, pat, ver>>

DefaultRun(c) == Claim(c, DefaultPlan(c)) /\ UNCHANGED used

\* A keys= run past the cap is refused: "run the partition first".
KeysRun(c, k) ==
    /\ used.keys < MaxKeysRuns
    /\ Cardinality(rec[c].ahead) < MaxEntries
    /\ Claim(c, KeysPlan(c, k))
    /\ used' = [used EXCEPT !.keys = @ + 1]

-----------------------------------------------------------------------------
(* Committing. Refused if a reset took the output or its input since the  *)
(* claim, its asset changed, or the record moved (another commit), as a   *)
(* stale head is: a batch planned under another definition finishes no    *)
(* pass due under this one.                                                *)

Settle(c) ==
    LET a == att[c]  p == a.plan  u == Up(c)  r == rec[c]  ks == p.keys
        valid == a.lf = life[c] /\ a.uplf = life[u] /\ a.rec = r /\ a.def = chg[c]
        held == (IF p.over THEN {} ELSE Held(c) \ ks) \cup {k \in ks \cap Pat(c) : a.read[k] # 0}
        got == {<<k, a.read[k]>> : k \in ks}
        \* A read of a key replaces the record's earlier one: the latest wins.
        Merge(es) == {e \in es : e[1] \notin ks} \cup got
        \* each=True: a key read holds its version; one a start-over dropped, none.
        per == [k \in Keys |-> IF k \in ks THEN a.read[k] ELSE IF p.over THEN 0 ELSE r.per[k]]
        r2 == IF p.kind = "default"
              THEN [snap |-> p.pin, lf |-> a.uplf, ahead |-> {}, pass |-> NoPass, per |-> per, def |-> a.def]
              ELSE IF InPass(c, r)
              THEN [r EXCEPT !.pass = [due |-> TRUE, begun |-> TRUE,
                                       done |-> IF p.over THEN got ELSE Merge(r.pass.done)],
                             !.per = per]
              ELSE [r EXCEPT !.ahead = Merge(@), !.per = per]
        \* A keys= commit that leaves nothing behind finishes the pass or
        \* collapses the read-ahead (without FixCollapse: any keys= commit).
        r3 == IF p.kind = "keys" /\ (FixCollapse => BehindR(c, r2, held) = {})
              THEN [r2 EXCEPT !.snap = Top(u), !.lf = life[u], !.ahead = {}, !.pass = NoPass,
                              !.def = IF InPass(c, r) THEN a.def ELSE @]
              ELSE r2
    IN
    /\ a.busy
    /\ att' = [att EXCEPT ![c] = [busy |-> FALSE]]
    /\ IF valid
       THEN /\ Commit(c, [k \in ks \cup (Held(c) \ held) |-> k \in held])
            /\ rec' = [rec EXCEPT ![c] = r3]
            /\ from' = [from EXCEPT ![c] = [k \in Keys |->
                          IF k \in ks THEN [v |-> a.read[k], d |-> a.def] ELSE @[k]]]
       ELSE UNCHANGED <<log, clock, rec, from>>
    /\ UNCHANGED <<life, chg, pat, ver, twice, used>>

-----------------------------------------------------------------------------
(* The environment *)

SrcCommit(k) ==
    /\ used.src < MaxSrc
    /\ \E present \in BOOLEAN : Commit("S", [j \in {k} |-> present])
    /\ used' = [used EXCEPT !.src = @ + 1]
    /\ UNCHANGED <<life, rec, from, chg, pat, ver, att, twice>>

\* A move of A resets it (K10): A is empty and new; its record and B's on
\* it go; a full pass is due for both. An asset change of A.
MoveA ==
    /\ used.changes < MaxChanges
    /\ log' = [log EXCEPT !["A"] = <<>>]
    /\ life' = [life EXCEPT !["A"] = @ + 1]
    /\ chg' = [chg EXCEPT !["A"] = used.changes + 1]
    /\ rec' = [c \in Assets |-> [rec[c] EXCEPT !.snap = -1, !.ahead = {}, !.pass = DuePass,
                                          !.per = [k \in Keys |-> 0]]]
    /\ from' = [from EXCEPT !["A"] = [k \in Keys |-> [v |-> 0, d |-> 0]]]
    /\ used' = [used EXCEPT !.changes = @ + 1]
    /\ UNCHANGED <<clock, pat, ver, att, twice>>

\* B's patterns change (they exclude key NK, or include it again), or its
\* version is bumped: an asset change of B, a full pass due.
ChangeB ==
    /\ used.changes < MaxChanges
    /\ \/ pat' = (IF pat = Keys THEN Keys \ {NK} ELSE Keys) /\ UNCHANGED ver
       \/ ver' = ver + 1 /\ UNCHANGED pat
    /\ chg' = [chg EXCEPT !["B"] = used.changes + 1]
    /\ rec' = [rec EXCEPT !["B"] = [@ EXCEPT !.pass = DuePass, !.ahead = {}]]
    /\ used' = [used EXCEPT !.changes = @ + 1]
    /\ UNCHANGED <<log, clock, life, from, att, twice>>

Next ==
    \/ \E c \in Assets : DefaultRun(c) \/ Settle(c) \/ \E k \in Keys : KeysRun(c, k)
    \/ \E k \in Keys : SrcCommit(k)
    \/ MoveA \/ ChangeB

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------
(* Properties *)

\* Where a status is reported (no attempt of it in flight), the record's
\* answer, with its upstream's (K46), is the truth.
StatusExact == \A c \in Assets : ~att[c].busy => (StoredStale(c) <=> TrueStale(c))

\* Nothing is delivered twice (K45): no key again at the version, under
\* the definition, it was produced from, but by a run that starts over.
DeliveredOnce == ~twice

=============================================================================
