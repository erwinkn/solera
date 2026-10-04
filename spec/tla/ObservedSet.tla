------------------------------ MODULE ObservedSet ------------------------------
(***************************************************************************)
(* The observed set (D126, D133; docs/observed-set.md, 389fcfb): what a   *)
(* consumer partition processed of one keyed incremental input, key by    *)
(* key, and its observation record E: a base (a commit, or a commit with  *)
(* a before-image), ranges and points. The design's claims, checked after *)
(* every commit:                                                           *)
(*                                                                         *)
(*   decode(E) = S, the observed set kept literally beside it;             *)
(*   a tally's count = the keys S holds present;                           *)
(*   the owed set the design computes (candidates, each classified once    *)
(*   from its decoded old state to upstream now) = S against upstream now; *)
(*   stale iff something is owed.                                          *)
(*                                                                         *)
(* Keys 1..N in key order, each in one prefix group: key k matches glob    *)
(* Group[k]; glob "d" matches none. Patterns are sets of globs, {} the     *)
(* universal include. Versions 1..MaxVer, 0 absent. Upstream: an index     *)
(* (its commits, `hist`, of one life) and a store's rows (`cur`), which a  *)
(* current-only store serves as they are now, ahead of the index or        *)
(* reverted, and a versioned store by the version a commit names (rows a   *)
(* cleanup may delete unless pinned). Context: the shared inputs'          *)
(* versions.                                                               *)
(*                                                                         *)
(* Runs: a default run is a pass at the head T, in batches over key        *)
(* ranges, each classifying what it owes at T from the decoded old state,  *)
(* loading it, reclassifying from what was served and committing its      *)
(* overwrite (a range at T, points where the store served something else) *)
(* with its outputs; the last batch rebases. A replan starts another pass. *)
(* A keys= run takes one key, if owed, and writes a point. A per-key batch *)
(* canceled part done commits its finished keys as points. A failed batch  *)
(* writes nothing (no step). An upstream reset or a definition change owes *)
(* a start-over, which the next pass's first batch makes. Retention cuts   *)
(* the index's history: a base or range it passes keeps a before-image.    *)
(*                                                                         *)
(* History = "free" explores every step; any other name replays that      *)
(* finding's history (docs/verification.md), which must run through with  *)
(* every rule on and break an invariant with its rule off. Fix* select     *)
(* each rule (TRUE) or its absence (FALSE).                                *)
(***************************************************************************)
EXTENDS Integers, FiniteSets, Sequences

CONSTANTS
    N, MaxVer, MaxCommits, MaxResets, Pats, Ctxs, Cap,
    CurrentOnly,       \* the upstream store serves current rows, not rows by version
    PerKey,            \* batches of a per-key consumer may be canceled part done
    History,           \* "free", or the name of a history to replay
    FixDecodeOld,      \* classes from the whole decoded old state, not the base alone (A19 R1, R3; A26 N2)
    FixFixedT,         \* a pass reads at its T, not the moving head (A19 R2)
    FixPointPatterns,  \* a point keeps the patterns it was read under (A19 R4; A27 R5)
    FixAbsentPoints,   \* an absent point is a live value, not a tombstone lookups skip (A27 R5)
    FixObserve,        \* classes and points follow the row served (A19 R5; A26 N4; A27 R2; F41)
    FixPin,            \* a pass pins its rows from its start (A26 N1; A27 R6)
    FixNoCap,          \* points spill; nothing is refused (A26 N5)
    FixFold,           \* an override folds only if decoding without it gives its value (A26 N3; A27 R8)
    FixSupersede,      \* a range write drops the points inside it (A27 R8)
    FixSplit,          \* a range write keeps the other ranges' parts (A27 R8)
    FixRebase,         \* a rebase drops only overrides the new base decodes to (A27 R8)
    FixContext,        \* layers carry their context; a moved context is a candidate (A27 R1)
    FixUniversal,      \* the universal include is a pattern: its change scans everything (A27 R3)
    FixClassOnce,      \* net changes are candidates, not classes (A27 R4)
    FixLives,          \* an upstream reset owes a rebuild (A27 R7)
    FixRangeCands,     \* each range's changes since its own T are candidates (A27 R9)
    FixPending,        \* staleness is the comparison, not a pattern label (A27 R10)
    FixImage,          \* a cut past a base or range leaves a before-image of what changed (D123)
    FixPassCut,        \* a cut past an active pass's T replans it (proposed: the design is silent)
    FixNoRegress,      \* a pass leaves a key observed after its T as it is (proposed)
    FixCommitCheck     \* a batch commits only if its pass, life and definition still hold

VARIABLES
    life, hist, cur, gone,      \* upstream: its life, index commits, store rows, rows deleted
    pat, ctx,                   \* the consumer's current patterns and context
    S, sLife, count,            \* the observed set (literal), the life it read, a tally of S
    base, rng, pts,             \* the observation record E
    rebuild, restart,           \* a start-over owed: an upstream reset; a definition change
    pass,                       \* the pass under way (its scan plan), or none
    fly,                        \* a batch dispatched, not yet committed: its plan, keys and life
    floor,                      \* the oldest commit retention keeps (0, the empty view, always)
    resets, broken, refused,    \* upstream resets so far; a load found its row gone; a run refused
    lost,                       \* a pass needed the key view at a T retention had cut
    step                        \* the history's next step

vars == <<life, hist, cur, gone, pat, ctx, S, sLife, count, base, rng, pts, rebuild, restart,
          pass, fly, floor, resets, broken, refused, lost, step>>

ASSUME N \in 1..3 /\ "w1" \in Ctxs
Keys == 1..N
Group == <<"a", "b", "c">>
Take(p, k) == p = {} \/ Group[k] \in p

Empty == [k \in Keys |-> 0]
head == Len(hist)
Index(T) == IF T >= 1 /\ T <= Len(hist) THEN hist[T] ELSE Empty

Absent == [pres |-> FALSE, ver |-> 0, ctx |-> "-", life |-> 0]
Obs(v, c) == IF v = 0 THEN Absent ELSE [pres |-> TRUE, ver |-> v, ctx |-> c, life |-> life]
NoImg == [k \in Keys |-> [has |-> FALSE, v |-> 0]]
Layer(T, p, c) == [has |-> TRUE, T |-> T, img |-> NoImg, pat |-> p, ctx |-> c, life |-> life]
NoLayer == [has |-> FALSE, T |-> 0, img |-> NoImg, pat |-> {}, ctx |-> "-", life |-> 0]
\* A point: the observation, the patterns it was read under, the head it was read at.
Point(o, p) == [has |-> TRUE, obs |-> o, pat |-> p, at |-> head]
NoPoint == [has |-> FALSE, obs |-> Absent, pat |-> {}, at |-> 0]
Bottom == Layer(0, {}, ctx)
NoRanges == [k \in Keys |-> NoLayer]
NoPoints == [k \in Keys |-> NoPoint]
NoPass == [on |-> FALSE, T |-> 0, cursor |-> 0, pat |-> {}, ctx |-> "-", pin |-> FALSE, over |-> FALSE]
NoFly == [on |-> FALSE, sel |-> FALSE, P |-> NoPass, c |-> 0, life |-> 0, pl |-> [k \in 1..N |-> Absent],
           sv |-> [k \in 1..N |-> Absent], gone |-> FALSE]

-----------------------------------------------------------------------------
(* Decode, the truth, and the design's comparison *)

\* What a layer (a base or a range) says of k: as at its endpoint, or as
\* its before-image keeps it, if its patterns take k.
LayerVer(L, k) == IF L.img[k].has THEN L.img[k].v ELSE Index(L.T)[k]
LayerObs(L, k) ==
    IF ~Take(L.pat, k) \/ LayerVer(L, k) = 0 THEN Absent
    ELSE [pres |-> TRUE, ver |-> LayerVer(L, k), ctx |-> L.ctx, life |-> L.life]
\* decode(E)(k): its point, else its range, else the base.
DecodeIn(b, r, p, k) ==
    IF p[k].has /\ (FixAbsentPoints \/ p[k].obs.pres)
    THEN IF FixPointPatterns \/ Take(b.pat, k) THEN p[k].obs ELSE Absent
    ELSE IF r[k].has THEN LayerObs(r[k], k) ELSE LayerObs(b, k)
Decode(k) == DecodeIn(base, rng, pts, k)
\* The old state a run classifies from.
Old(b, r, p, k) == IF FixDecodeOld THEN DecodeIn(b, r, p, k) ELSE LayerObs(b, k)

\* k as upstream has it at T under patterns q and context c; now.
At(T, q, c, k) == IF Take(q, k) THEN Obs(Index(T)[k], c) ELSE Absent
New(k) == At(head, pat, ctx, k)

Differs(o, n, withCtx) ==
    \/ o.pres # n.pres
    \/ o.pres /\ n.pres /\ (o.ver # n.ver \/ o.life # n.life \/ (withCtx /\ o.ctx # n.ctx))
Class(o, n, withCtx) ==
    IF ~Differs(o, n, withCtx) THEN "none"
    ELSE IF ~o.pres THEN "added" ELSE IF ~n.pres THEN "removed" ELSE "updated"
DClass(o, n) == Class(o, n, FixContext)

\* The truth: S against upstream now; a start-over owed.
TrueOwed == {k \in Keys : Differs(S[k], New(k), TRUE)}
TrueStale == restart \/ sLife # life \/ TrueOwed # {}

\* The design's candidates: net changes outside the overrides (each range
\* since its own T), every point, the keys a changed pattern can match in
\* each layer's view or now, and, its context moved, every key a layer
\* decodes present.
LayerOf(k) == IF rng[k].has THEN rng[k] ELSE base
Plain == {k \in Keys : ~pts[k].has}
Changed(L, k) == L.img[k].has \/ Index(L.T)[k] # Index(head)[k]
NetCands == {k \in Plain : Changed(IF rng[k].has /\ FixRangeCands THEN rng[k] ELSE base, k)}
PatScan(q, k) ==
    /\ q # pat
    /\ IF (q = {}) # (pat = {})
       THEN FixUniversal \/ Group[k] \in q \cup pat    \* without: the universal include as no glob
       ELSE Group[k] \in (q \ pat) \cup (pat \ q)
PatCands == {k \in Plain : PatScan(LayerOf(k).pat, k) /\ (LayerVer(LayerOf(k), k) # 0 \/ Index(head)[k] # 0)}
CtxCands == IF FixContext THEN {k \in Plain : LayerOf(k).ctx # ctx /\ LayerObs(LayerOf(k), k).pres} ELSE {}
Candidates == NetCands \cup {k \in Keys : pts[k].has} \cup PatCands \cup CtxCands
\* Each candidate classified once. Without FixClassOnce, a base key's net
\* change is classed under the base's own patterns.
BaseNet == {k \in NetCands : ~rng[k].has}
DesignOwed ==
    IF FixClassOnce THEN {k \in Candidates : DClass(Decode(k), New(k)) # "none"}
    ELSE {k \in Candidates \ BaseNet : DClass(Decode(k), New(k)) # "none"}
         \cup {k \in BaseNet : DClass(LayerObs(base, k), At(head, base.pat, base.ctx, k)) # "none"}
Stale == rebuild \/ restart \/ DesignOwed # {} \/ (~FixPending /\ pat # base.pat)

-----------------------------------------------------------------------------
Init ==
    /\ life = 0 /\ hist = <<>> /\ cur = Empty /\ gone = {}
    /\ pat = {} /\ ctx = "w1"
    /\ S = [k \in Keys |-> Absent] /\ sLife = 0 /\ count = 0
    /\ base = [has |-> TRUE, T |-> 0, img |-> NoImg, pat |-> {}, ctx |-> "w1", life |-> 0]
    /\ rng = NoRanges /\ pts = NoPoints
    /\ rebuild = FALSE /\ restart = FALSE /\ pass = NoPass /\ fly = NoFly
    /\ floor = 0 /\ resets = 0 /\ broken = FALSE /\ refused = FALSE /\ lost = FALSE /\ step = 1

Consumer == <<S, sLife, count, base, rng, pts, rebuild, restart, pass, fly, refused, lost>>

(* Upstream *)

\* A row written to the store: a current-only store serves it at once.
Write(k, v) ==
    /\ cur' = [cur EXCEPT ![k] = v]
    /\ UNCHANGED <<life, hist, gone, pat, ctx, floor, resets, broken, Consumer>>
\* The index commits the store's rows; a versioned store's rows exist again.
Commit ==
    /\ head < MaxCommits
    /\ hist' = Append(hist, cur)
    /\ gone' = gone \ {<<k, cur[k]>> : k \in Keys}
    /\ UNCHANGED <<life, cur, pat, ctx, floor, resets, broken, Consumer>>
\* A current-only store's row goes back to the index's (a revert).
Revert(k) ==
    /\ CurrentOnly /\ cur[k] # Index(head)[k]
    /\ cur' = [cur EXCEPT ![k] = Index(head)[k]]
    /\ UNCHANGED <<life, hist, gone, pat, ctx, floor, resets, broken, Consumer>>
\* A versioned store's cleanup deletes rows no commit at the head names,
\* but those a pass's pin holds.
Pinned(k, v) == pass.on /\ pass.pin /\ Index(pass.T)[k] = v
Cleanup ==
    /\ ~CurrentOnly
    /\ gone' = gone \cup {<<k, v>> \in Keys \X (1..MaxVer) : v # Index(head)[k] /\ ~Pinned(k, v)}
    /\ UNCHANGED <<life, hist, cur, pat, ctx, floor, resets, broken, Consumer>>
\* Removed and declared again, or moved: a new life, its old index gone.
UpReset ==
    /\ resets < MaxResets
    /\ resets' = resets + 1 /\ life' = life + 1
    /\ hist' = <<>> /\ cur' = Empty /\ gone' = {} /\ floor' = 0
    /\ rebuild' = (FixLives \/ rebuild)
    /\ pass' = NoPass
    /\ UNCHANGED <<pat, ctx, broken, S, sLife, count, base, rng, pts, restart, fly, refused, lost>>

\* Retention cuts the index's history at C: a base or a range whose
\* endpoint it passes first takes C as its endpoint, with a before-image
\* of the keys changed since, as they were (FixImage). An active pass
\* whose T it passes is replanned at the head (FixPassCut, proposed).
Image(L, C) ==
    IF L.T = 0 \/ L.T >= C THEN L
    ELSE [L EXCEPT !.T = C,
                   !.img = IF ~FixImage THEN NoImg
                           ELSE [k \in Keys |-> IF L.img[k].has \/ Index(L.T)[k] = Index(C)[k] THEN L.img[k]
                                                ELSE [has |-> TRUE, v |-> Index(L.T)[k]]]]
Cut(C) ==
    /\ floor < C /\ C <= head
    /\ floor' = C
    /\ base' = Image(base, C)
    /\ rng' = [k \in Keys |-> IF rng[k].has THEN Image(rng[k], C) ELSE rng[k]]
    /\ pass' = IF pass.on /\ pass.T >= 1 /\ pass.T < C /\ FixPassCut
               THEN [pass EXCEPT !.T = head, !.cursor = 0, !.pat = pat, !.ctx = ctx] ELSE pass
    /\ UNCHANGED <<life, hist, cur, gone, pat, ctx, resets, broken, S, sLife, count, pts, rebuild,
                   restart, fly, refused, lost>>

(* Deploys *)

SetPatterns(q) ==
    /\ q # pat /\ pat' = q
    /\ UNCHANGED <<life, hist, cur, gone, ctx, floor, resets, broken, Consumer>>
SetContext(c) ==
    /\ c # ctx /\ ctx' = c
    /\ UNCHANGED <<life, hist, cur, gone, pat, floor, resets, broken, Consumer>>
\* The definition changed: E resets to an empty base; a start-over is owed.
Redefine ==
    /\ restart' = TRUE
    /\ base' = Bottom /\ rng' = NoRanges /\ pts' = NoPoints /\ pass' = NoPass
    /\ UNCHANGED <<life, hist, cur, gone, pat, ctx, floor, resets, broken, S, sLife, count, rebuild, fly,
                   refused, lost>>

-----------------------------------------------------------------------------
(* Runs *)

Served(T, q, c, k) ==
    IF ~Take(q, k) THEN Absent ELSE IF CurrentOnly THEN Obs(cur[k], c) ELSE Obs(Index(T)[k], c)
RowGone(T, q, k) == ~CurrentOnly /\ Take(q, k) /\ Index(T)[k] # 0 /\ <<k, Index(T)[k]>> \in gone
Delta(cls) == Cardinality({k \in DOMAIN cls : cls[k] = "added"}) - Cardinality({k \in DOMAIN cls : cls[k] = "removed"})

\* A default run plans a pass at the head, under the current patterns and
\* context; one already under way is replanned (its committed ranges stay).
StartPass ==
    /\ ~fly.on
    /\ pass' = [on |-> TRUE, T |-> head, cursor |-> 0, pat |-> pat, ctx |-> ctx, pin |-> FixPin,
                over |-> rebuild \/ restart]
    /\ UNCHANGED <<life, hist, cur, gone, pat, ctx, floor, resets, broken, S, sLife, count, base, rng, pts,
                   rebuild, restart, fly, refused, lost>>

\* A batch over keys (cursor, c]: what is owed at T, classified from the
\* decoded old state; loaded; reclassified from what was served, the
\* producer given that; its overwrite committed with its outputs: a range
\* at T over (cursor, c], points where the store served what T does not
\* say. A start-over's first batch starts from nothing. The last batch
\* rebases at T. A row gone fails the batch: nothing is written.
\* What a batch of pass P over (cursor, c] reads: the index at its T, and
\* the rows the store serves; whether a row it needs is gone.
ReadT(P) == IF FixFixedT THEN P.T ELSE head
PlannedOf(P) == [k \in Keys |-> At(ReadT(P), P.pat, P.ctx, k)]
ServedOf(P) == [k \in Keys |-> Served(ReadT(P), P.pat, P.ctx, k)]
GoneOf(P, c) ==
    \E k \in Keys : /\ P.cursor < k /\ k <= c /\ RowGone(ReadT(P), P.pat, k)
                   /\ (P.over \/ DClass(Old(base, rng, pts, k), PlannedOf(P)[k]) # "none")

BatchOf(P, c, pl, sv, rowgone) ==
    /\ P.on /\ P.cursor < c /\ c <= N
    /\ LET b0 == IF P.over THEN Bottom ELSE base
           r0 == IF P.over THEN NoRanges ELSE rng
           p0 == IF P.over THEN NoPoints ELSE pts
           s0 == IF P.over THEN [k \in Keys |-> Absent] ELSE S
           ks == {k \in Keys : P.cursor < k /\ k <= c}
           T == ReadT(P)
           old(k) == Old(b0, r0, p0, k)
           planned(k) == pl[k]
           newer == {k \in ks : FixNoRegress /\ p0[k].has /\ p0[k].at > P.T}
           owed == {k \in ks \ newer : DClass(old(k), planned(k)) # "none"}
           served(k) == sv[k]
           cls == [k \in owed |-> DClass(old(k), IF FixObserve THEN served(k) ELSE planned(k))]
           span == Layer(P.T, P.pat, P.ctx)
           r1 == [k \in Keys |-> IF k \in ks THEN span ELSE IF FixSplit THEN r0[k] ELSE NoLayer]
           p1 == [k \in Keys |-> IF k \in owed /\ FixObserve /\ served(k) # planned(k)
                                 THEN Point(served(k), P.pat)
                                 ELSE IF k \in ks \ newer /\ FixSupersede THEN NoPoint ELSE p0[k]]
           nb == Layer(P.T, P.pat, P.ctx)
           \* The rebase: the base at T; an override stays only if the new
           \* base, without it, decodes to something else (without
           \* FixRebase: every override goes).
           r2 == [k \in Keys |-> IF FixRebase /\ r1[k].has /\ LayerObs(r1[k], k) # LayerObs(nb, k)
                                 THEN r1[k] ELSE NoLayer]
           p2 == [k \in Keys |-> IF FixRebase /\ p1[k].has /\ p1[k].obs # DecodeIn(nb, r2, NoPoints, k)
                                 THEN p1[k] ELSE NoPoint]
           last == c = N
       IN IF T >= 1 /\ T < floor
          THEN /\ lost' = TRUE
               /\ UNCHANGED <<S, sLife, count, base, rng, pts, rebuild, restart, pass, broken>>
          ELSE IF rowgone
          THEN /\ broken' = TRUE
               /\ UNCHANGED <<S, sLife, count, base, rng, pts, rebuild, restart, pass, lost>>
          ELSE /\ count' = (IF P.over THEN 0 ELSE count) + Delta(cls)
               /\ S' = [k \in Keys |-> IF k \in owed THEN served(k) ELSE s0[k]]
               /\ sLife' = life
               /\ base' = IF last THEN nb ELSE b0
               /\ rng' = IF last THEN r2 ELSE r1
               /\ pts' = IF last THEN p2 ELSE p1
               /\ rebuild' = FALSE /\ restart' = FALSE
               /\ pass' = IF last THEN NoPass ELSE [P EXCEPT !.cursor = c, !.over = FALSE]
               /\ UNCHANGED <<broken, lost>>
    /\ UNCHANGED <<life, hist, cur, gone, pat, ctx, floor, resets, refused>>

\* Dispatched and committed with nothing between (the histories' B).
Batch(c) == ~fly.on /\ BatchOf(pass, c, PlannedOf(pass), ServedOf(pass), GoneOf(pass, c)) /\ UNCHANGED fly

\* A batch is dispatched: its attempt holds the partition (one at a time).
Plan(c) ==
    /\ pass.on /\ ~fly.on /\ pass.cursor < c /\ c <= N
    /\ fly' = [on |-> TRUE, sel |-> FALSE, P |-> pass, c |-> c, life |-> life, pl |-> PlannedOf(pass),
               sv |-> ServedOf(pass), gone |-> GoneOf(pass, c)]
    /\ UNCHANGED <<life, hist, cur, gone, pat, ctx, floor, resets, broken, S, sLife, count, base, rng, pts,
                   rebuild, restart, pass, refused, lost>>

\* Its attempt ends: it commits, unless (FixCommitCheck) its pass was
\* replanned or ended, its life reset or a start-over is owed since; then
\* it writes nothing and its keys stay owed.
Holds == fly.life = life /\ pass = fly.P /\ ~rebuild /\ ~restart
CommitBatch ==
    /\ fly.on /\ ~fly.sel
    /\ fly' = NoFly
    /\ IF FixCommitCheck /\ ~Holds
       THEN UNCHANGED <<life, hist, cur, gone, pat, ctx, floor, resets, broken, S, sLife, count, base, rng,
                        pts, rebuild, restart, pass, refused, lost>>
       ELSE BatchOf(fly.P, fly.c, fly.pl, fly.sv, fly.gone)

\* A per-key batch canceled part done: its finished keys commit as points.
Cancel(c, done) ==
    /\ PerKey /\ ~fly.on /\ pass.on /\ ~pass.over /\ pass.cursor < c /\ c <= N /\ done # {}
    /\ LET P == pass
           T == IF FixFixedT THEN P.T ELSE head
           old(k) == Old(base, rng, pts, k)
           planned(k) == At(T, P.pat, P.ctx, k)
           served(k) == Served(T, P.pat, P.ctx, k)
       IN /\ ~(T >= 1 /\ T < floor)
          /\ done \subseteq {k \in Keys : P.cursor < k /\ k <= c /\ DClass(old(k), planned(k)) # "none"
                                         /\ ~(FixNoRegress /\ pts[k].has /\ pts[k].at > P.T)}
          /\ ~\E k \in done : RowGone(T, P.pat, k)
          /\ count' = count + Delta([k \in done |-> DClass(old(k), IF FixObserve THEN served(k) ELSE planned(k))])
          /\ S' = [k \in Keys |-> IF k \in done THEN served(k) ELSE S[k]]
          /\ pts' = [k \in Keys |-> IF k \in done
                                    THEN Point(IF FixObserve THEN served(k) ELSE planned(k), P.pat)
                                    ELSE pts[k]]
    /\ UNCHANGED <<life, hist, cur, gone, pat, ctx, floor, resets, broken, sLife, base, rng, rebuild,
                   restart, pass, fly, refused, lost>>

\* A keys= run of key k: if owed, loaded at the head, classified from what
\* was served, written as a point under the current patterns.
SelServed(k) == IF ~Take(pat, k) THEN Absent ELSE IF CurrentOnly THEN Obs(cur[k], ctx) ELSE New(k)
\* The keys= batch's commit, from what it read: k as upstream had it then
\* (`new`, under the patterns `q` it ran under) and as served.
SelectWith(k, new, served, q) ==
    /\ LET old == Old(base, rng, pts, k)
           used == IF FixObserve THEN served ELSE new
       IN IF DClass(old, new) = "none" THEN UNCHANGED <<S, sLife, count, base, rng, pts, rebuild, restart,
                                                        pass, refused, lost, broken>>
          ELSE IF ~FixNoCap /\ Cardinality({j \in Keys : pts[j].has}) >= Cap
          THEN /\ refused' = TRUE
               /\ UNCHANGED <<S, sLife, count, base, rng, pts, rebuild, restart, pass, fly, broken, lost>>
          ELSE /\ count' = count + Delta([j \in {k} |-> DClass(old, used)])
               /\ S' = [S EXCEPT ![k] = served]
               /\ pts' = [pts EXCEPT ![k] = Point(used, q)]
               /\ UNCHANGED <<sLife, base, rng, rebuild, restart, pass, refused, broken, lost>>
    /\ UNCHANGED <<life, hist, cur, gone, pat, ctx, floor, resets>>
Select(k) ==
    /\ ~rebuild /\ ~restart /\ ~fly.on
    /\ SelectWith(k, New(k), SelServed(k), pat) /\ UNCHANGED fly

\* A keys= batch dispatched, then committed: it starts no pass, so its
\* commit checks only that its life is current and no start-over is owed
\* (FixCommitCheck).
SelPlan(k) ==
    /\ ~rebuild /\ ~restart /\ ~fly.on
    /\ fly' = [on |-> TRUE, sel |-> TRUE, P |-> [NoPass EXCEPT !.pat = pat], c |-> k, life |-> life,
               pl |-> [j \in Keys |-> New(j)], sv |-> [j \in Keys |-> SelServed(j)], gone |-> FALSE]
    /\ UNCHANGED <<life, hist, cur, gone, pat, ctx, floor, resets, broken, S, sLife, count, base, rng, pts,
                   rebuild, restart, pass, refused, lost>>
SelCommit ==
    /\ fly.on /\ fly.sel
    /\ fly' = NoFly
    /\ IF FixCommitCheck /\ ~(fly.life = life /\ ~rebuild /\ ~restart)
       THEN UNCHANGED <<life, hist, cur, gone, pat, ctx, floor, resets, broken, S, sLife, count, base, rng,
                        pts, rebuild, restart, pass, refused, lost>>
       ELSE SelectWith(fly.c, fly.pl[fly.c], fly.sv[fly.c], fly.P.pat)

\* A point folds when decoding without it gives its value. Without
\* FixFold: when the base's raw version at P matches it.
Fold(k) ==
    /\ pts[k].has
    /\ IF FixFold THEN pts[k].obs = DecodeIn(base, rng, NoPoints, k)
       ELSE pts[k].obs.ver = LayerVer(base, k)
    /\ pts' = [pts EXCEPT ![k] = NoPoint]
    /\ UNCHANGED <<life, hist, cur, gone, pat, ctx, floor, resets, broken, S, sLife, count, base, rng,
                   rebuild, restart, pass, fly, refused, lost>>

-----------------------------------------------------------------------------
(* Free exploration *)

Free ==
    \/ \E k \in Keys, v \in 0..MaxVer : Write(k, v)
    \/ Commit \/ Cleanup \/ UpReset \/ Redefine
    \/ \E c \in 1..MaxCommits : Cut(c)
    \/ \E k \in Keys : Revert(k) \/ SelPlan(k) \/ Fold(k)
    \/ SelCommit
    \/ \E q \in Pats : SetPatterns(q)
    \/ \E c \in Ctxs : SetContext(c)
    \/ Stale /\ StartPass
    \/ CommitBatch
    \/ \E c \in Keys : Plan(c) \/ \E d \in SUBSET Keys : Cancel(c, d)

-----------------------------------------------------------------------------
(* Histories: each finding, in the observed set's terms *)

W(k, v) == [a |-> "write", k |-> k, v |-> v]
C == [a |-> "commit"]
Sel(k) == [a |-> "select", k |-> k]
Run == [a |-> "pass"]
B(c) == [a |-> "batch", c |-> c]
Pat(q) == [a |-> "patterns", q |-> q]
Ctx(c) == [a |-> "context", c |-> c]
Fo(k) == [a |-> "fold", k |-> k]
Rv(k) == [a |-> "revert", k |-> k]
Clean == [a |-> "cleanup"]
Cu(c) == [a |-> "cut", c |-> c]
Pl(c) == [a |-> "plan", c |-> c]
Co == [a |-> "commitbatch"]
SP(k) == [a |-> "selplan", k |-> k]
SC == [a |-> "selcommit"]
Def == [a |-> "redefine"]
Rst == [a |-> "reset"]
Full == <<Run, B(N)>>          \* a whole default run

Histories == [
    \* A19 R1: keys=[k1]; k1 updated; a default run: k1 updated once, k2 added.
    A19R1 |-> <<W(1, 1), W(2, 1), C, Sel(1), W(1, 2), C>> \o Full,
    \* A19 R2: a pass commits k1; k3 arrives; the pass ends at its T; the next adds k3 once.
    A19R2 |-> <<W(1, 1), W(2, 1), C, Run, B(1), W(3, 1), C, B(3)>> \o Full,
    \* A19 R3: keys=[k1]; k1 removed; keys=[k2]; a default run delivers k1's removal.
    A19R3 |-> <<W(1, 1), W(2, 1), C, Sel(1), W(1, 0), C, Sel(2)>> \o Full,
    \* A19 R4: built under include k1; widened; keys=[k2]; a default run adds nothing more.
    A19R4 |-> <<Pat({"a"}), W(1, 1), W(2, 1), C>> \o Full \o <<Pat({"a", "b"}), Sel(2)>> \o Full,
    \* A19 R5 (current-only): a delta under way; k2's removal lands before its batch reads it.
    A19R5 |-> <<W(1, 1), W(2, 1), C>> \o Full \o <<W(1, 2), W(2, 2), C, Run, B(1), W(2, 0), C, B(3)>> \o Full,
    \* F41 (current-only; tests/sim/test_replays.py, its end state): items
    \* holds k1, k3 (k3 is F41's k11); feed becomes {k3}, then {k1, k2};
    \* the store still serves k3 when items reads through that commit.
    F41 |-> <<W(1, 1), W(3, 1), C>> \o Full \o <<W(1, 0), C, W(3, 0), W(1, 2), W(2, 1), C, W(3, 1), Run, B(N),
              Rv(3)>> \o Full,
    \* A26 N1 (versioned): a pass commits k1; k2 changes; cleanup; the pass resumes at its T.
    A26N1 |-> <<W(1, 1), W(2, 1), C, Run, B(1), W(2, 2), C, Clean, B(3)>>,
    \* A26 N2: widened; keys=[k2] twice; a default run.
    A26N2 |-> <<Pat({"a"}), W(1, 1), W(2, 1), C>> \o Full \o <<Pat({"a", "b"}), Sel(2), Sel(2)>> \o Full,
    \* A26 N3: widened; k1 updated; keys=[k2]; its point folded; a default run.
    A26N3 |-> <<Pat({"a"}), W(1, 1), W(2, 1), C>> \o Full \o <<W(1, 2), C, Pat({"a", "b"}), Sel(2), Fo(2)>> \o Full,
    \* A26 N4 (current-only): k2 deleted before its batch reads it, then restored at the same version.
    A26N4 |-> <<W(1, 1), W(2, 1), C>> \o Full \o <<W(1, 2), W(2, 2), C, Run, B(1), W(2, 0), C, B(3), W(2, 2), C>> \o Full,
    \* A26 N5 (Cap 1): two keys= runs owed.
    A26N5 |-> <<W(1, 1), W(2, 1), C, Sel(1), Sel(2)>>,
    \* A27 R1: built under w1; the context moves; keys=[k1]; and back.
    A27R1 |-> <<W(1, 1), W(2, 1), C>> \o Full \o <<Ctx("w2"), Sel(1), Ctx("w1")>> \o Full,
    \* A27 R2 (current-only): a row served ahead of the index, then reverted.
    A27R2 |-> <<W(1, 1), W(2, 1), C>> \o Full \o <<W(2, 2), C, W(2, 3), Sel(2), Rv(2)>> \o Full,
    \* A27 R3: no include over k1, k2; include k1's group appears.
    A27R3 |-> <<W(1, 1), W(2, 1), C>> \o Full \o <<Pat({"a"})>> \o Full,
    \* A27 R4: built under groups a, b (b empty); narrowed to a; k2 (group b) added.
    A27R4 |-> <<Pat({"a", "b"}), W(1, 1), C>> \o Full \o <<Pat({"a"}), W(2, 1), C>> \o Full,
    \* A27 R5, absence: k1 removed; keys=[k1] writes it absent; the base still has it.
    A27R5a |-> <<W(1, 1), C>> \o Full \o <<W(1, 0), C, Sel(1)>> \o Full,
    \* A27 R5, the label: as A19 R4.
    A27R5b |-> <<Pat({"a"}), W(1, 1), W(2, 1), C>> \o Full \o <<Pat({"a", "b"}), Sel(2)>> \o Full,
    \* A27 R6: as A26 N1.
    A27R6 |-> <<W(1, 1), W(2, 1), C, Run, B(1), W(2, 2), C, Clean, B(3)>>,
    \* A27 R7: built; upstream reset; the start-over.
    A27R7 |-> <<W(1, 1), C>> \o Full \o <<Rst, W(2, 1), C>> \o Full,
    \* A27 R8, supersede: keys=[k1]; k1 updated; a default run.
    A27R8a |-> <<W(1, 1), C, Sel(1), W(1, 2), C>> \o Full,
    \* A27 R8, fold: base k1@1, a range k1@2, a newer selection k1@1, folded.
    A27R8b |-> <<W(1, 1), C>> \o Full \o <<W(1, 2), C, Run, B(1), W(1, 1), C, Sel(1), Fo(1)>> \o <<B(N)>> \o Full,
    \* A27 R8, a replan's tail: [k1, k2] at T1, replanned, [k1] at T2.
    A27R8c |-> <<W(1, 1), W(2, 1), C, Run, B(2), W(1, 2), C, Run, B(1)>> \o <<B(N)>> \o Full,
    \* A27 R9: base k1@1; a range removes it at T2; restored at the same version.
    A27R9 |-> <<W(1, 1), W(2, 1), C>> \o Full \o <<W(1, 0), C, Run, B(1), W(1, 1), C, B(N)>> \o Full,
    \* A27 R10: widened to a glob no key matches.
    A27R10 |-> <<Pat({"a"}), W(1, 1), C>> \o Full \o <<Pat({"a", "d"})>>,
    \* D123, a reader paused past the window: built at commit 1; k2 updated,
    \* k3 added; retention cuts at 3; the run.
    Paused |-> <<W(1, 1), W(2, 1), C>> \o Full \o <<W(2, 2), C, W(3, 1), C, Cu(3)>> \o Full,
    \* A pass under way when the cut passes its T: it commits k1; k2
    \* changes; the cut; its next batch.
    PassCut |-> <<W(1, 1), W(2, 1), C, Run, B(1), W(2, 2), C, Cu(2), B(N)>> \o Full,
    \* A pass at T1; k1 updated; keys=[k1] takes @2; the pass reaches k1.
    Regress |-> <<W(1, 1), W(2, 1), C, Run, W(1, 2), C, Sel(1), B(N)>> \o Full,
    \* A batch in flight when upstream resets; it commits after.
    ResetInFlight |-> <<W(1, 1), W(2, 1), C, Run, Pl(N), Rst, W(1, 1), C, Co>> \o Full,
    \* A batch in flight when the cut passes its T; it commits after.
    CutInFlight |-> <<W(1, 1), W(2, 1), C, Run, B(1), W(2, 2), C, Pl(N), Cu(2), Co>> \o Full,
    \* A keys= batch in flight across a cut and the replan it makes.
    SelCut |-> <<W(1, 1), W(2, 1), C, Run, B(1), W(2, 2), C, SP(2), Cu(2), SC, W(1, 2), C>> \o Full,
    \* A keys= batch in flight across a definition change: refused.
    SelDefine |-> <<W(1, 1), W(2, 1), C>> \o Full \o <<W(2, 2), C, SP(2), Def, SC>> \o Full,
    \* A keys= batch in flight across an upstream reset: refused.
    SelReset |-> <<W(1, 1), W(2, 1), C>> \o Full \o <<W(2, 2), C, SP(2), Rst, W(2, 3), C, SC>> \o Full
]

Script == IF History = "free" THEN <<>> ELSE Histories[History]

Do(s) ==
    CASE s.a = "write" -> Write(s.k, s.v)
      [] s.a = "commit" -> Commit
      [] s.a = "revert" -> Revert(s.k)
      [] s.a = "cleanup" -> Cleanup
      [] s.a = "cut" -> Cut(s.c)
      [] s.a = "reset" -> UpReset
      [] s.a = "patterns" -> SetPatterns(s.q)
      [] s.a = "context" -> SetContext(s.c)
      [] s.a = "select" -> Select(s.k)
      [] s.a = "pass" -> StartPass
      [] s.a = "batch" -> Batch(s.c)
      [] s.a = "plan" -> Plan(s.c)
      [] s.a = "commitbatch" -> CommitBatch
      [] s.a = "selplan" -> SelPlan(s.k)
      [] s.a = "selcommit" -> SelCommit
      [] s.a = "redefine" -> Redefine
      [] s.a = "fold" -> Fold(s.k) \/ (~ENABLED Fold(s.k) /\ UNCHANGED <<life, hist, cur, gone, pat, ctx, floor, resets, broken, Consumer>>)

Replay ==
    \/ step <= Len(Script) /\ Do(Script[step]) /\ step' = step + 1
    \/ step > Len(Script) /\ UNCHANGED vars        \* done

Next == IF History = "free" THEN Free /\ UNCHANGED step ELSE Replay

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------
(* Properties *)

DecodeExact == ~rebuild /\ ~restart => \A k \in Keys : Decode(k) = S[k]
CountExact == ~restart => count = Cardinality({k \in Keys : S[k].pres})
OwedExact == ~rebuild /\ ~restart => DesignOwed = TrueOwed
StaleExact == Stale = TrueStale
\* A tally holds what it would read now: once nothing is owed, its count
\* is the effective input's.
TallyExact == ~restart /\ sLife = life /\ TrueOwed = {} => count = Cardinality({k \in Keys : New(k).pres})
RowsKept == ~broken
EndpointsKept == ~lost
Admitted == ~refused
\* A history runs through: each step it names can be taken.
Through == step <= Len(Script) => ENABLED Do(Script[step])

\* No output goes back to an older version (histories whose versions only
\* grow: Regress).
NoRegress == [][\A k \in Keys : S[k].pres /\ S'[k].pres => S'[k].ver >= S[k].ver]_vars

\* The free model's bound (a tally's count is bounded where the rules hold).
Bounded == count \in -1..N + 1

=============================================================================
