------------------------------ MODULE ObservedSet ------------------------------
(***************************************************************************)
(* The observed set (D126, D133; docs/observed-set.md): what a consumer    *)
(* partition processed of one keyed incremental input, key by key, and    *)
(* its observation record E: a base (a commit, or a commit with a         *)
(* before-image), ranges and points. The design's claims, checked after   *)
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
(* cleanup may delete unless a claim pins them). Context: the shared       *)
(* inputs' versions.                                                       *)
(*                                                                         *)
(* Runs: a default run computes what is owed and processes it in key       *)
(* order, a batch at a time; its cursor is in its memory only, lost with  *)
(* it. A batch is dispatched at the head H (its claim pins H) and          *)
(* commits, if its life and definition still hold: it classifies its      *)
(* keys from the decoded old state to H, reclassifies from what was        *)
(* served, and records a range (prev, c] observed at H, points where the   *)
(* store served something else; older ranges fold to H where nothing      *)
(* changed in them, a changed key keeping what it decoded to as a point.   *)
(* A run's last batch rebases. A keys= run takes one key, if owed, and     *)
(* writes a point. A per-key batch canceled part done commits its          *)
(* finished keys as points. A failed batch writes nothing; a run dropped  *)
(* (canceled, failed, taken over) leaves what it committed. An upstream    *)
(* reset or a definition change owes a start-over, which the next run's   *)
(* first batch makes. Retention cuts the index's history: a base or range *)
(* it passes keeps a before-image.                                         *)
(*                                                                         *)
(* History = "free" explores every step; any other name replays that      *)
(* finding's history (docs/verification.md), which must run through with  *)
(* every rule on and break an invariant with its rule off. Fix* select     *)
(* each rule (TRUE) or its absence (FALSE).                                *)
(***************************************************************************)
EXTENDS Integers, FiniteSets, Sequences

CONSTANTS
    N, MaxVer, MaxCommits, MaxResets, MaxDefs, Pats, Ctxs, Cap,
    CurrentOnly,       \* the upstream store serves current rows, not rows by version
    PerKey,            \* batches of a per-key consumer may be canceled part done
    History,           \* "free", or the name of a history to replay
    FixDecodeOld,      \* classes from the whole decoded old state, not the base alone (A19 R1, R3; A26 N2)
    FixRecheck,        \* a batch classifies every key of its range at H, not just those owed at its run's start (A19 R2)
    FixPointPatterns,  \* a point keeps the patterns it was read under (A19 R4; A27 R5)
    FixAbsentPoints,   \* an absent point is a live value, not a tombstone lookups skip (A27 R5)
    FixObserve,        \* classes and points follow the row served (A19 R5; A26 N4; A27 R2; F41)
    FixNoCap,          \* points spill; nothing is refused (A26 N5)
    FixFold,           \* a point folds only if decoding without it gives its value (A26 N3; A27 R8)
    FixSupersede,      \* a range write drops the points inside it (A27 R8)
    FixSplit,          \* a range write keeps the other ranges' parts (A27 R8)
    FixRelabel,        \* an older range relabels to H only where nothing changed; a changed key becomes a point
    FixRebase,         \* a rebase drops only overrides the new base decodes to (A27 R8)
    FixContext,        \* layers carry their context; a moved context is a candidate (A27 R1)
    FixUniversal,      \* the universal include is a pattern: its change scans everything (A27 R3)
    FixClassOnce,      \* net changes are candidates, not classes (A27 R4)
    FixLives,          \* an upstream reset owes a rebuild (A27 R7)
    FixRangeCands,     \* each range's changes since its own endpoint are candidates (A27 R9)
    FixPending,        \* staleness is the comparison, not a pattern label (A27 R10)
    FixImage,          \* a cut past a base or range leaves a before-image of what changed (D123)
    FixClaimPin,       \* a batch's claim pins its H: no cleanup of its rows, no cut past it
    FixCommitCheck     \* a batch commits only if its life and definition still hold

VARIABLES
    life, hist, cur, gone,      \* upstream: its life, index commits, store rows, rows deleted
    pat, ctx, defs,             \* the consumer's patterns, context, definition changes so far
    S, sLife, count,            \* the observed set (literal), the life it read, a tally of S
    base, rng, pts,             \* the observation record E
    rebuild, restart,           \* a start-over owed: an upstream reset; a definition change
    run,                        \* a default run's memory: its cursor and what it owed; none
    fly,                        \* a batch dispatched, not yet committed
    floor,                      \* the oldest commit retention keeps (0, the empty view, always)
    resets, broken, refused,    \* upstream resets so far; a load found its row gone; a run refused
    lost,                       \* a batch needed the key view at an H retention had cut
    step                        \* the history's next step

vars == <<life, hist, cur, gone, pat, ctx, defs, S, sLife, count, base, rng, pts, rebuild, restart,
          run, fly, floor, resets, broken, refused, lost, step>>

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
\* A point: the observation and the patterns it was read under.
Point(o, p) == [has |-> TRUE, obs |-> o, pat |-> p]
NoPoint == [has |-> FALSE, obs |-> Absent, pat |-> {}]
Bottom == Layer(0, {}, ctx)
NoRanges == [k \in Keys |-> NoLayer]
NoPoints == [k \in Keys |-> NoPoint]
NoRun == [on |-> FALSE, cursor |-> 0, owed |-> {}, over |-> FALSE]
NoFly == [on |-> FALSE, sel |-> FALSE, prev |-> 0, c |-> 0, H |-> 0, pat |-> {}, ctx |-> "-",
          life |-> 0, def |-> 0, over |-> FALSE, owed |-> {},
          pl |-> [k \in 1..N |-> Absent], sv |-> [k \in 1..N |-> Absent]]

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
\* The old state a batch classifies from.
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
\* since its own endpoint), every point, the keys a changed pattern can
\* match in each layer's view or now, and, its context moved, every key a
\* layer decodes present.
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
    /\ pat = {} /\ ctx = "w1" /\ defs = 0
    /\ S = [k \in Keys |-> Absent] /\ sLife = 0 /\ count = 0
    /\ base = [has |-> TRUE, T |-> 0, img |-> NoImg, pat |-> {}, ctx |-> "w1", life |-> 0]
    /\ rng = NoRanges /\ pts = NoPoints
    /\ rebuild = FALSE /\ restart = FALSE /\ run = NoRun /\ fly = NoFly
    /\ floor = 0 /\ resets = 0 /\ broken = FALSE /\ refused = FALSE /\ lost = FALSE /\ step = 1

Consumer == <<S, sLife, count, base, rng, pts, rebuild, restart, run, fly, refused, lost>>
Record == <<S, sLife, count, base, rng, pts, rebuild, restart>>

(* Upstream *)

\* A row written to the store: a current-only store serves it at once.
Write(k, v) ==
    /\ cur' = [cur EXCEPT ![k] = v]
    /\ UNCHANGED <<life, hist, gone, pat, ctx, defs, floor, resets, broken, Consumer>>
\* The index commits the store's rows; a versioned store's rows exist again.
Commit ==
    /\ head < MaxCommits
    /\ hist' = Append(hist, cur)
    /\ gone' = gone \ {<<k, cur[k]>> : k \in Keys}
    /\ UNCHANGED <<life, cur, pat, ctx, defs, floor, resets, broken, Consumer>>
\* A current-only store's row goes back to the index's (a revert).
Revert(k) ==
    /\ CurrentOnly /\ cur[k] # Index(head)[k]
    /\ cur' = [cur EXCEPT ![k] = Index(head)[k]]
    /\ UNCHANGED <<life, hist, gone, pat, ctx, defs, floor, resets, broken, Consumer>>
\* A versioned store's cleanup deletes rows no commit at the head names,
\* but those a batch's claim pins (FixClaimPin).
InFlight == fly.on /\ ~fly.sel
Pinned(k, v) == FixClaimPin /\ InFlight /\ Index(fly.H)[k] = v
Cleanup ==
    /\ ~CurrentOnly
    /\ gone' = gone \cup {<<k, v>> \in Keys \X (1..MaxVer) : v # Index(head)[k] /\ ~Pinned(k, v)}
    /\ UNCHANGED <<life, hist, cur, pat, ctx, defs, floor, resets, broken, Consumer>>
\* Removed and declared again, or moved: a new life, its old index gone.
UpReset ==
    /\ resets < MaxResets
    /\ resets' = resets + 1 /\ life' = life + 1
    /\ hist' = <<>> /\ cur' = Empty /\ gone' = {} /\ floor' = 0
    /\ rebuild' = (FixLives \/ rebuild)
    /\ run' = NoRun
    /\ UNCHANGED <<pat, ctx, defs, broken, S, sLife, count, base, rng, pts, restart, fly, refused, lost>>

\* Retention cuts the index's history at C: a base whose endpoint it
\* passes first takes C as its endpoint, with a before-image of the keys
\* changed since, as they were (FixImage). It does not pass the H a
\* batch's claim pins (FixClaimPin).
Image(L, C) ==
    IF L.T = 0 \/ L.T >= C THEN L
    ELSE [L EXCEPT !.T = C,
                   !.img = IF ~FixImage THEN NoImg
                           ELSE [k \in Keys |-> IF L.img[k].has \/ Index(L.T)[k] = Index(C)[k] THEN L.img[k]
                                                ELSE [has |-> TRUE, v |-> Index(L.T)[k]]]]
\* A range older than the cut folds to C as at a batch commit: a key
\* changed in it becomes a point (without FixImage: none does).
Cut(C) ==
    /\ floor < C /\ C <= head
    /\ ~(FixClaimPin /\ InFlight /\ fly.H >= 1 /\ fly.H < C)
    /\ floor' = C
    /\ base' = Image(base, C)
    /\ LET older(k) == rng[k].has /\ rng[k].T >= 1 /\ rng[k].T < C
           moved(k) == older(k) /\ LayerVer(rng[k], k) # Index(C)[k]
       IN /\ pts' = [k \in Keys |-> IF FixImage /\ moved(k) /\ ~pts[k].has
                                     THEN Point(LayerObs(rng[k], k), rng[k].pat) ELSE pts[k]]
          /\ rng' = [k \in Keys |-> IF older(k) THEN [rng[k] EXCEPT !.T = C, !.img = NoImg] ELSE rng[k]]
    /\ UNCHANGED <<life, hist, cur, gone, pat, ctx, defs, resets, broken, S, sLife, count, rebuild,
                   restart, run, fly, refused, lost>>

(* Deploys *)

SetPatterns(q) ==
    /\ q # pat /\ pat' = q
    /\ UNCHANGED <<life, hist, cur, gone, ctx, defs, floor, resets, broken, Consumer>>
SetContext(c) ==
    /\ c # ctx /\ ctx' = c
    /\ UNCHANGED <<life, hist, cur, gone, pat, defs, floor, resets, broken, Consumer>>
\* The definition changed: E resets to an empty base; a start-over is owed.
Redefine ==
    /\ defs < MaxDefs /\ defs' = defs + 1
    /\ restart' = TRUE
    /\ base' = Bottom /\ rng' = NoRanges /\ pts' = NoPoints /\ run' = NoRun
    /\ UNCHANGED <<life, hist, cur, gone, pat, ctx, floor, resets, broken, S, sLife, count, rebuild, fly,
                   refused, lost>>

-----------------------------------------------------------------------------
(* Default runs *)

Served(k) == IF ~Take(pat, k) THEN Absent ELSE IF CurrentOnly THEN Obs(cur[k], ctx) ELSE New(k)
RowGone(T, q, k) == ~CurrentOnly /\ Take(q, k) /\ Index(T)[k] # 0 /\ <<k, Index(T)[k]>> \in gone
Delta(cls) == Cardinality({k \in DOMAIN cls : cls[k] = "added"}) - Cardinality({k \in DOMAIN cls : cls[k] = "removed"})

\* A default run starts: what is owed now, everything for a start-over.
StartRun ==
    /\ ~fly.on
    /\ run' = [on |-> TRUE, cursor |-> 0, over |-> rebuild \/ restart,
               owed |-> IF rebuild \/ restart THEN Keys ELSE DesignOwed]
    /\ UNCHANGED <<life, hist, cur, gone, pat, ctx, defs, floor, resets, broken, Record, fly, refused, lost>>
\* Its memory goes: canceled, failed, or its engine taken over.
DropRun ==
    /\ run.on /\ run' = NoRun
    /\ UNCHANGED <<life, hist, cur, gone, pat, ctx, defs, floor, resets, broken, Record, fly, refused, lost>>

\* A batch over (cursor, c], dispatched at the head H: what it reads, the
\* index at H and the rows served, under the current patterns and context.
Dispatch(c) ==
    [on |-> TRUE, sel |-> FALSE, prev |-> run.cursor, c |-> c, H |-> head, pat |-> pat, ctx |-> ctx,
     life |-> life, def |-> defs, over |-> run.over, owed |-> run.owed,
     pl |-> [k \in Keys |-> New(k)], sv |-> [k \in Keys |-> Served(k)]]

\* Its commit: classes from the decoded old state to H (every key of its
\* range, FixRecheck; else those owed at its run's start), reclassified
\* from what was served; the range (prev, c] at H, points where the store
\* served something else; older ranges fold to H where nothing changed
\* in them, a changed key keeping its decoded value as a point
\* (FixRelabel); a run's last batch rebases at H. A start-over's first
\* batch starts from nothing. A row gone, or H cut, fails it.
Apply(f) ==
    LET b0 == IF f.over THEN Bottom ELSE base
        r0 == IF f.over THEN NoRanges ELSE rng
        p0 == IF f.over THEN NoPoints ELSE pts
        s0 == IF f.over THEN [k \in Keys |-> Absent] ELSE S
        ks == {k \in Keys : f.prev < k /\ k <= f.c}
        H == f.H
        old(k) == Old(b0, r0, p0, k)
        owed == {k \in ks : DClass(old(k), f.pl[k]) # "none" /\ (FixRecheck \/ f.over \/ k \in f.owed)}
        cls == [k \in owed |-> DClass(old(k), IF FixObserve THEN f.sv[k] ELSE f.pl[k])]
        span == Layer(H, f.pat, f.ctx)
        r1 == [k \in Keys |-> IF k \in ks THEN span ELSE IF FixSplit THEN r0[k] ELSE NoLayer]
        p1 == [k \in Keys |-> IF k \in owed /\ FixObserve /\ f.sv[k] # f.pl[k] THEN Point(f.sv[k], f.pat)
                              ELSE IF k \in ks /\ FixSupersede THEN NoPoint ELSE p0[k]]
        older(k) == k \notin ks /\ r1[k].has /\ r1[k].T < H
        moved(k) == older(k) /\ LayerVer(r1[k], k) # Index(H)[k]
        p2 == [k \in Keys |-> IF FixRelabel /\ moved(k) /\ ~p1[k].has
                              THEN Point(DecodeIn(b0, r1, p1, k), r1[k].pat) ELSE p1[k]]
        r2 == [k \in Keys |-> IF older(k) THEN [r1[k] EXCEPT !.T = H, !.img = NoImg] ELSE r1[k]]
        last == f.c = N
        nb == Layer(H, f.pat, f.ctx)
        r3 == [k \in Keys |-> IF FixRebase /\ r2[k].has /\ LayerObs(r2[k], k) # LayerObs(nb, k)
                              THEN r2[k] ELSE NoLayer]
        p3 == [k \in Keys |-> IF FixRebase /\ p2[k].has /\ p2[k].obs # DecodeIn(nb, r3, NoPoints, k)
                              THEN p2[k] ELSE NoPoint]
    IN IF H >= 1 /\ H < floor
       THEN /\ lost' = TRUE
            /\ UNCHANGED <<Record, broken>>
       ELSE IF \E k \in owed : RowGone(H, f.pat, k)
       THEN /\ broken' = TRUE
            /\ UNCHANGED <<Record, lost>>
       ELSE /\ count' = (IF f.over THEN 0 ELSE count) + Delta(cls)
            /\ S' = [k \in Keys |-> IF k \in owed THEN f.sv[k] ELSE s0[k]]
            /\ sLife' = life
            /\ base' = IF last THEN nb ELSE b0
            /\ rng' = IF last THEN r3 ELSE r2
            /\ pts' = IF last THEN p3 ELSE p2
            /\ rebuild' = FALSE /\ restart' = FALSE
            /\ UNCHANGED <<broken, lost>>

\* Dispatched: its attempt holds the partition (one at a time); the run's
\* cursor moves on in its memory.
Plan(c) ==
    /\ run.on /\ ~fly.on /\ run.cursor < c /\ c <= N
    /\ fly' = Dispatch(c)
    /\ run' = [run EXCEPT !.cursor = c, !.over = FALSE]
    /\ UNCHANGED <<life, hist, cur, gone, pat, ctx, defs, floor, resets, broken, Record, refused, lost>>
\* Its attempt ends: it commits, unless (FixCommitCheck) its life was reset
\* or its definition changed since, or a start-over is owed that it is not.
Holds(f) == f.life = life /\ f.def = defs /\ (f.over \/ (~rebuild /\ ~restart))
CommitBatch ==
    /\ InFlight
    /\ fly' = NoFly
    /\ IF FixCommitCheck /\ ~Holds(fly)
       THEN UNCHANGED <<Record, broken, lost>>
       ELSE Apply(fly)
    /\ UNCHANGED <<life, hist, cur, gone, pat, ctx, defs, floor, resets, run, refused>>
\* Dispatched and committed with nothing between (the histories' B).
Batch(c) ==
    /\ run.on /\ ~fly.on /\ run.cursor < c /\ c <= N
    /\ Apply(Dispatch(c))
    /\ run' = [run EXCEPT !.cursor = c, !.over = FALSE]
    /\ UNCHANGED <<life, hist, cur, gone, pat, ctx, defs, floor, resets, fly, refused>>

\* A per-key batch canceled part done: its finished keys commit as points.
Cancel(c, done) ==
    /\ PerKey /\ run.on /\ ~run.over /\ ~fly.on /\ run.cursor < c /\ c <= N /\ done # {}
    /\ LET old(k) == Old(base, rng, pts, k)
       IN /\ done \subseteq {k \in Keys : run.cursor < k /\ k <= c /\ DClass(old(k), New(k)) # "none"}
          /\ count' = count + Delta([k \in done |-> DClass(old(k), IF FixObserve THEN Served(k) ELSE New(k))])
          /\ S' = [k \in Keys |-> IF k \in done THEN Served(k) ELSE S[k]]
          /\ pts' = [k \in Keys |-> IF k \in done THEN Point(IF FixObserve THEN Served(k) ELSE New(k), pat)
                                    ELSE pts[k]]
    /\ UNCHANGED <<life, hist, cur, gone, pat, ctx, defs, floor, resets, broken, sLife, base, rng, rebuild,
                   restart, run, fly, refused, lost>>

-----------------------------------------------------------------------------
(* keys= runs *)

\* The commit of a keys= batch of key k, from what it read: k as upstream
\* had it (`new`, under the patterns `q` it ran under) and as served.
SelectWith(k, new, served, q) ==
    LET old == Old(base, rng, pts, k)
        used == IF FixObserve THEN served ELSE new
    IN IF DClass(old, new) = "none" THEN UNCHANGED <<Record, refused>>
       ELSE IF ~FixNoCap /\ Cardinality({j \in Keys : pts[j].has}) >= Cap
       THEN /\ refused' = TRUE
            /\ UNCHANGED Record
       ELSE /\ count' = count + Delta([j \in {k} |-> DClass(old, used)])
            /\ S' = [S EXCEPT ![k] = served]
            /\ pts' = [pts EXCEPT ![k] = Point(used, q)]
            /\ UNCHANGED <<sLife, base, rng, rebuild, restart, refused>>
Select(k) ==
    /\ ~rebuild /\ ~restart /\ ~fly.on
    /\ SelectWith(k, New(k), Served(k), pat)
    /\ UNCHANGED <<life, hist, cur, gone, pat, ctx, defs, floor, resets, broken, run, fly, lost>>
\* Dispatched, then committed: it starts no run, so its commit checks only
\* that its life and definition hold and no start-over is owed.
SelPlan(k) ==
    /\ ~rebuild /\ ~restart /\ ~fly.on
    /\ fly' = [Dispatch(k) EXCEPT !.sel = TRUE, !.c = k]
    /\ UNCHANGED <<life, hist, cur, gone, pat, ctx, defs, floor, resets, broken, Record, run, refused, lost>>
SelCommit ==
    /\ fly.on /\ fly.sel
    /\ fly' = NoFly
    /\ IF FixCommitCheck /\ ~(fly.life = life /\ fly.def = defs /\ ~rebuild /\ ~restart)
       THEN UNCHANGED <<Record, refused>>
       ELSE SelectWith(fly.c, fly.pl[fly.c], fly.sv[fly.c], fly.pat)
    /\ UNCHANGED <<life, hist, cur, gone, pat, ctx, defs, floor, resets, broken, run, lost>>

\* Upkeep folds a point when decoding without it gives its value. Without
\* FixFold: when the base's raw version matches it.
Fold(k) ==
    /\ pts[k].has
    /\ IF FixFold THEN pts[k].obs = DecodeIn(base, rng, NoPoints, k)
       ELSE pts[k].obs.ver = LayerVer(base, k)
    /\ pts' = [pts EXCEPT ![k] = NoPoint]
    /\ UNCHANGED <<life, hist, cur, gone, pat, ctx, defs, floor, resets, broken, S, sLife, count, base, rng,
                   rebuild, restart, run, fly, refused, lost>>

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
    \/ Stale /\ StartRun
    \/ DropRun \/ CommitBatch
    \/ \E c \in Keys : Plan(c) \/ \E d \in SUBSET Keys : Cancel(c, d)

-----------------------------------------------------------------------------
(* Histories: each finding, in the observed set's terms *)

W(k, v) == [a |-> "write", k |-> k, v |-> v]
C == [a |-> "commit"]
Sel(k) == [a |-> "select", k |-> k]
Run == [a |-> "run"]
B(c) == [a |-> "batch", c |-> c]
Pat(q) == [a |-> "patterns", q |-> q]
Ctx(c) == [a |-> "context", c |-> c]
Fo(k) == [a |-> "fold", k |-> k]
Rv(k) == [a |-> "revert", k |-> k]
Clean == [a |-> "cleanup"]
Rst == [a |-> "reset"]
Cu(c) == [a |-> "cut", c |-> c]
Pl(c) == [a |-> "plan", c |-> c]
Co == [a |-> "commitbatch"]
SP(k) == [a |-> "selplan", k |-> k]
SC == [a |-> "selcommit"]
Def == [a |-> "redefine"]
Full == <<Run, B(N)>>          \* a whole default run

Histories == [
    \* A19 R1: keys=[k1]; k1 updated; a default run: k1 updated once, k2 added.
    A19R1 |-> <<W(1, 1), W(2, 1), C, Sel(1), W(1, 2), C>> \o Full,
    \* A19 R2: a run commits k1; k3 arrives; its next batch covers k2, k3.
    A19R2 |-> <<W(1, 1), W(2, 1), C, Run, B(1), W(3, 1), C, B(3)>> \o Full,
    \* A19 R3: keys=[k1]; k1 removed; keys=[k2]; a default run delivers k1's removal.
    A19R3 |-> <<W(1, 1), W(2, 1), C, Sel(1), W(1, 0), C, Sel(2)>> \o Full,
    \* A19 R4: built under include k1; widened; keys=[k2]; a default run adds nothing more.
    A19R4 |-> <<Pat({"a"}), W(1, 1), W(2, 1), C>> \o Full \o <<Pat({"a", "b"}), Sel(2)>> \o Full,
    \* A19 R5 (current-only): a run under way; the store drops k2 before its batch reads it.
    A19R5 |-> <<W(1, 1), W(2, 1), C>> \o Full \o <<W(1, 2), W(2, 2), C, Run, B(1), W(2, 0), B(3), C>> \o Full,
    \* F41 (current-only; tests/sim/test_replays.py, its end state): items
    \* holds k1, k3 (k3 is F41's k11); feed becomes {k3}, then {k1, k2};
    \* the store still serves k3 when items reads through that commit.
    F41 |-> <<W(1, 1), W(3, 1), C>> \o Full \o <<W(1, 0), C, W(3, 0), W(1, 2), W(2, 1), C, W(3, 1), Run, B(N),
              Rv(3)>> \o Full,
    \* A26 N1 (versioned): a run commits k1; k2 changes; cleanup; its next batch.
    A26N1 |-> <<W(1, 1), W(2, 1), C, Run, B(1), W(2, 2), C, Clean, B(3)>>,
    \* A26 N2: widened; keys=[k2] twice; a default run.
    A26N2 |-> <<Pat({"a"}), W(1, 1), W(2, 1), C>> \o Full \o <<Pat({"a", "b"}), Sel(2), Sel(2)>> \o Full,
    \* A26 N3: widened; k1 updated; keys=[k2]; its point folded; a default run.
    A26N3 |-> <<Pat({"a"}), W(1, 1), W(2, 1), C>> \o Full \o <<W(1, 2), C, Pat({"a", "b"}), Sel(2), Fo(2)>> \o Full,
    \* A26 N4 (current-only): the store drops k2 before its batch reads it; restored at the same version.
    A26N4 |-> <<W(1, 1), W(2, 1), C>> \o Full \o <<W(1, 2), W(2, 2), C, Run, B(1), W(2, 0), B(3), C, W(2, 2), C>> \o Full,
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
    A27R8b |-> <<W(1, 1), C>> \o Full \o <<W(1, 2), C, Run, B(1), W(1, 1), C, Sel(1), Fo(1), B(N)>> \o Full,
    \* A27 R8, a later run's tail: [k1, k2] at H1; a new run writes [k1] at H2.
    A27R8c |-> <<W(1, 1), W(2, 1), C, Run, B(2), W(1, 2), C, Run, B(1), B(N)>> \o Full,
    \* A27 R9: base k1@1; a range records its removal; restored at the same version.
    A27R9 |-> <<W(1, 1), W(2, 1), C>> \o Full \o <<W(1, 0), C, Run, B(1), W(1, 1), C>>,
    \* A27 R10: widened to a glob no key matches.
    A27R10 |-> <<Pat({"a"}), W(1, 1), C>> \o Full \o <<Pat({"a", "d"})>>,
    \* D123, a reader paused past the window: built at commit 1; k2 updated,
    \* k3 added; retention cuts at 3; the run.
    Paused |-> <<W(1, 1), W(2, 1), C>> \o Full \o <<W(2, 2), C, W(3, 1), C, Cu(3)>> \o Full,
    \* A run under way when the cut passes its first batch's H; its next batch.
    RunCut |-> <<W(1, 1), W(2, 1), C, Run, B(1), W(2, 2), C, Cu(2), B(N)>> \o Full,
    \* A run at H1; k1 updated; keys=[k1] takes @2; the run reaches k1.
    Regress |-> <<W(1, 1), W(2, 1), C, Run, W(1, 2), C, Sel(1), B(N)>> \o Full,
    \* A run commits k1 at H1; k1 updated; its next batch, at H2, folds [k1].
    Relabel |-> <<W(1, 1), W(2, 1), C, Run, B(1), W(1, 2), C, B(2)>> \o Full,
    \* A batch in flight when upstream resets; it commits after.
    ResetInFlight |-> <<W(1, 1), W(2, 1), C, Run, Pl(N), Rst, W(1, 1), C, Co>> \o Full,
    \* Built; k1 updated; a batch in flight when the definition changes; it commits after.
    DefineInFlight |-> <<W(1, 1), W(2, 1), C>> \o Full \o <<W(1, 2), C, Run, Pl(N), Def, Co>> \o Full,
    \* A batch in flight at H2; k1 updated; retention cuts at 3; it commits.
    CutInFlight |-> <<W(1, 1), W(2, 1), C, Run, B(1), W(2, 2), C, Pl(N), W(1, 2), C, Cu(3), Co>> \o Full,
    \* A keys= batch in flight across a cut.
    SelCut |-> <<W(1, 1), W(2, 1), C, Run, B(1), W(2, 2), C, SP(2), W(1, 2), C, Cu(3), SC>> \o Full,
    \* A keys= batch in flight across a definition change.
    SelDefine |-> <<W(1, 1), W(2, 1), C>> \o Full \o <<W(2, 2), C, SP(2), Def, SC>> \o Full,
    \* A keys= batch in flight across an upstream reset.
    SelReset |-> <<W(1, 1), W(2, 1), C>> \o Full \o <<W(2, 2), C, SP(2), Rst, W(2, 3), C, SC>> \o Full
]

Script == IF History = "free" THEN <<>> ELSE Histories[History]

Skip == UNCHANGED <<life, hist, cur, gone, pat, ctx, defs, floor, resets, broken, Consumer>>
Do(s) ==
    CASE s.a = "write" -> Write(s.k, s.v)
      [] s.a = "commit" -> Commit
      [] s.a = "revert" -> Revert(s.k)
      [] s.a = "cleanup" -> Cleanup
      [] s.a = "cut" -> Cut(s.c) \/ (~ENABLED Cut(s.c) /\ Skip)
      [] s.a = "reset" -> UpReset
      [] s.a = "redefine" -> Redefine
      [] s.a = "patterns" -> SetPatterns(s.q)
      [] s.a = "context" -> SetContext(s.c)
      [] s.a = "select" -> Select(s.k)
      [] s.a = "selplan" -> SelPlan(s.k)
      [] s.a = "selcommit" -> SelCommit
      [] s.a = "run" -> StartRun
      [] s.a = "batch" -> Batch(s.c)
      [] s.a = "plan" -> Plan(s.c)
      [] s.a = "commitbatch" -> CommitBatch
      [] s.a = "fold" -> Fold(s.k) \/ (~ENABLED Fold(s.k) /\ Skip)

Replay ==
    \/ step <= Len(Script) /\ Do(Script[step]) /\ step' = step + 1
    \/ step > Len(Script) /\ UNCHANGED vars        \* done

Next == IF History = "free" THEN Free /\ UNCHANGED step ELSE Replay

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------
(* Properties *)

DecodeExact == ~rebuild /\ ~restart => \A k \in Keys : Decode(k) = S[k]
CountExact == ~restart => count = Cardinality({k \in Keys : S[k].pres})
\* A tally holds what it would read now: once nothing is owed, its count
\* is the effective input's.
TallyExact == ~restart /\ sLife = life /\ TrueOwed = {} => count = Cardinality({k \in Keys : New(k).pres})
OwedExact == ~rebuild /\ ~restart => DesignOwed = TrueOwed
StaleExact == Stale = TrueStale
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
