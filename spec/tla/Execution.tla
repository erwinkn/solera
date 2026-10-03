----------------------------- MODULE Execution -----------------------------
(***************************************************************************)
(* Solera's execution semantics, as designed (docs/glossary.md), at small  *)
(* bounds: a source S, an asset A reading it incrementally, an asset B     *)
(* reading A incrementally; runs, claims, attempts, passes and batches,    *)
(* bookmarks; fenced stores with gates, fences and repairs; deploys (store *)
(* move, pattern change, version bump, removal and re-adding); engine      *)
(* crash, restart and takeover; worker crashes, timeouts, user cancels.    *)
(* docs/verification.md, "Formal model", says what is abstracted and why.  *)
(*                                                                         *)
(* FixF6, FixF9, FixF10, FixF13 select each rule as designed (TRUE) or as  *)
(* it was before its fix (FALSE): the model must find each known bug.      *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets, TLC

CONSTANTS
    NK,          \* keys are 1..NK
    MaxSrc,      \* commits to S (the last one is the final change)
    MaxDeploy,   \* deploys, all before the final change
    MaxFault,    \* faults (crashes, timeouts, cancels, takeovers), all before it
    MaxAtt,      \* attempts, over the whole behaviour
    MaxRuns,     \* runs, over the whole behaviour
    MaxTries,    \* attempts a run's task may fail before it fails
    MaxKeysRuns, \* manual runs of A with keys= (one key, read as given; no bookmark moves)
    Each,        \* B is an each=True asset (reconciled at the end of a full pass)
    WithB,       \* B is declared at the start (else the chain is S and A, unless B is added)
    Deploys,     \* the deploy kinds explored: subset of {"move","pattern","bump","remove"}
    Faults,      \* the fault kinds explored: subset of {"worker","crash","takeover","timeout","cancel","zombie"}
    FixF6, FixF9, FixF10, FixF13, FixF17

Keys == 1..NK
Assets == {"A", "B"}
Outputs == {"S", "A", "B"}
Stores == {"st1", "st2"}
Up(c) == IF c = "A" THEN "S" ELSE "A"

VARIABLES
    log,      \* [Outputs -> Seq([up, rm, reset])]: each output partition's commits (the journal)
    bm,       \* [Assets -> bookmark]: what each incremental input has read
    store,    \* [Assets -> [Stores -> SUBSET Keys]]: the rows each store holds
    fence,    \* [Assets -> [Stores -> Nat]]: the newest generation that acquired
    hst,      \* [Assets -> Stores]: the store the head is in
    owes,     \* [Assets -> SUBSET Keys]: repair intents owed (a writer died past its gate)
    att,      \* [1..MaxAtt -> attempt]; generation = index
    nAtt,
    runs,     \* [1..MaxRuns -> run]
    nRun,
    pending,  \* [Assets -> BOOLEAN]: an OnChange automation owes a firing
    man,      \* the manifest served
    eng,      \* [serving: BOOLEAN, zombie: BOOLEAN]
    used      \* environment budgets spent: [src, deploy, fault]

vars == <<log, bm, store, fence, hst, owes, att, nAtt, runs, nRun, pending, man, eng, used>>

-----------------------------------------------------------------------------
(* Content *)

Apply(c, e) == IF e.reset THEN e.up ELSE (c \ e.rm) \cup e.up
RECURSIVE Fold(_)
Fold(s) == IF s = <<>> THEN {} ELSE Apply(Fold(SubSeq(s, 1, Len(s) - 1)), s[Len(s)])
Content(o) == Fold(log[o])
UpTo(o, n) == Fold(SubSeq(log[o], 1, n))
\* The keys commits n+1..h of o touched (every key, past a reset).
Touched(o, n, h) ==
    IF \E i \in (n + 1)..h : log[o][i].reset THEN Keys
    ELSE UNION {log[o][i].up \cup log[o][i].rm : i \in (n + 1)..h}
Min(S) == CHOOSE x \in S : \A y \in S : x <= y

Pat(c) == IF c = "A" THEN Keys ELSE man.pat
Present(c) == c = "A" \/ man.hasB
Life(c) == IF c = "A" THEN 0 ELSE man.lifeB

NoPass == [kind |-> "none", at |-> 0, from |-> 0, first |-> FALSE]
\* The fingerprint is a digest of the declaration: the asset's version and,
\* once F13 is fixed, its output's store. Moving back restores the old one.
FP(c) == <<man.ver[c], IF c = "A" /\ FixF13 THEN man.storeA ELSE "">>
InitBm == [next |-> 0, fp |-> <<0, "never">>, pat |-> Keys, pass |-> NoPass]

-----------------------------------------------------------------------------
(* Planning one batch (the engine, at an attempt's claim) *)

NoWork == [act |-> "none"]
Batch(up, rm, reset, newbm, fullEnd) ==
    [act |-> "batch", up |-> up, rm |-> rm, reset |-> reset, newbm |-> newbm, fullEnd |-> fullEnd]

\* A full pass reads the current head a key at a time, past the last key it
\* delivered; deltas resume from `from`, the head when it began.
FullStep(c, ps, b0) ==
    LET u == Up(c)  P == b0.pat
        avail == {k \in Content(u) \cap P : k > ps.at}
        ended == [b0 EXCEPT !.next = ps.from, !.pass = NoPass]
    IN IF avail # {} THEN
          LET k == Min(avail)
              last == {j \in Content(u) \cap P : j > k} = {}
          IN IF last /\ ~Each
               THEN Batch({k}, {}, ps.first, ended, TRUE)
               ELSE Batch({k}, {}, ~Each /\ ps.first,
                          [b0 EXCEPT !.pass = [ps EXCEPT !.at = k, !.first = FALSE]], FALSE)
       ELSE IF ps.first /\ ~FixF10
          THEN Batch({}, {}, FALSE, ended, TRUE)          \* pre-fix: taken nothing, skipped: no write
       ELSE IF Each
          THEN Batch({}, Content(c) \ (Content(u) \cap P), FALSE, ended, TRUE)  \* reconcile
       ELSE Batch({}, {}, ps.first, ended, TRUE)          \* a first batch of nothing: the reset reaches it

DeltaStep(c, b, P) ==
    LET u == Up(c)  h == Len(log[u])  t == Touched(u, b.next, h) \cap P
    IN Batch(t \cap Content(u), t \ Content(u), FALSE, [b EXCEPT !.next = h], FALSE)

DiffStep(c, b, P) ==
    LET cur == Content(Up(c)) IN
    Batch(cur \cap (P \ b.pat), cur \cap (b.pat \ P), FALSE, [b EXCEPT !.pat = P], FALSE)

\* Where an asset writes, and whether that is a store its head is not in: its
\* write then starts the output over (a first write, or a moved output).
StoreOf(c) == IF c = "A" THEN man.storeA ELSE "st1"
Moved(c) == log[c] = <<>> \/ hst[c] # StoreOf(c)

\* A fingerprint change, (F17's fix) a write that starts the output over, or (F9's fix) a reset upstream commit past what the
\* input has read (or, during a full pass, past the head it began at),
\* starts a full pass: one under way starts over.
NeedsFull(c) ==
    LET b == bm[c]  seen == IF b.pass.kind = "full" THEN b.pass.from ELSE b.next IN
    \/ b.fp # FP(c)
    \/ FixF17 /\ Moved(c)
    \/ FixF9 /\ \E i \in (seen + 1)..Len(log[Up(c)]) : log[Up(c)][i].reset

\* A keys= run reads one key as given, writes it, and moves no bookmark.
KeysPlan(c, k) ==
    LET cur == Content(Up(c)) IN Batch({k} \cap cur, {k} \ cur, FALSE, bm[c], FALSE)

Plan(c) ==
    LET b == bm[c]  h == Len(log[Up(c)])  P == Pat(c) IN
    IF NeedsFull(c)
      THEN FullStep(c, [kind |-> "full", at |-> 0, from |-> h, first |-> TRUE],
                    [b EXCEPT !.fp = FP(c), !.pat = P])
    ELSE IF b.pass.kind = "full" THEN FullStep(c, b.pass, b)
    ELSE IF b.pat # P
      THEN IF b.next < h THEN DeltaStep(c, b, b.pat)   \* commits before the change, under the old patterns
           ELSE DiffStep(c, b, P)
    ELSE IF b.next < h THEN DeltaStep(c, b, P)
    ELSE NoWork

\* Whether work is left once a commit installed `nb` (a pass that ends
\* behind the head goes on to it; before F6's fix, the end of a full pass
\* ended the task).
MoreAfter(c, nb, fullEnd) ==
    IF fullEnd /\ ~FixF6 THEN FALSE
    ELSE \/ nb.pass.kind # "none"
         \/ nb.next < Len(log[Up(c)])
         \/ nb.pat # Pat(c)
         \/ nb.fp # FP(c)

-----------------------------------------------------------------------------
(* Init *)

Init ==
    /\ log = [o \in Outputs |-> IF o = "S" THEN <<[up |-> Keys, rm |-> {}, reset |-> TRUE]>> ELSE <<>>]
    /\ bm = [c \in Assets |-> InitBm]
    /\ store = [c \in Assets |-> [s \in Stores |-> {}]]
    /\ fence = [c \in Assets |-> [s \in Stores |-> 0]]
    /\ hst = [c \in Assets |-> "st1"]
    /\ owes = [c \in Assets |-> {}]
    /\ att = [i \in 1..MaxAtt |-> [status |-> "free"]]
    /\ nAtt = 0
    /\ runs = [r \in 1..MaxRuns |-> [st |-> "free"]]
    /\ nRun = 0
    /\ pending = [c \in Assets |-> c = "A"]
    /\ man = [storeA |-> "st1", ver |-> [c \in Assets |-> 1], pat |-> Keys, hasB |-> WithB, lifeB |-> 0]
    /\ eng = [serving |-> TRUE, zombie |-> FALSE]
    /\ used = [src |-> 0, deploy |-> 0, fault |-> 0, keys |-> 0]


\* What is left of a finished attempt (settled, its worker done; or dropped)
\* and of a finished run: only what other records refer to. Histories that
\* differ only in finished records are one state.
Gone(a) == \/ a.status = "dropped"
           \/ a.status = "settled" /\ a.w \in {"sealed", "drained", "dead", "quit"}
Tidy(a) == IF a.status \in {"prep", "launched", "settled", "dropped"} /\ Gone(a)
           THEN [status |-> "done", asset |-> a.asset, run |-> a.run, w |-> "gone", gate |-> "none"]
           ELSE a
TidyAtt(f) == [i \in DOMAIN f |-> Tidy(f[i])]
TidyRuns(f) == [r \in DOMAIN f |-> IF f[r].st \in {"succeeded", "failed", "canceled"}
                                   THEN [st |-> "ended", tgt |-> f[r].tgt, tries |-> 0, keys |-> 0] ELSE f[r]]

-----------------------------------------------------------------------------
(* The engine (the serving one; its decisions are journal events) *)

Claimed(c) == \E i \in 1..nAtt : att[i].asset = c /\ att[i].status \in {"prep", "launched"}
ActiveRun(c) == \E r \in 1..nRun : runs[r].st = "active" /\ runs[r].tgt = c

\* An OnChange automation fires once no run of its target is queued or running.
Fire(c) ==
    /\ eng.serving /\ pending[c] /\ Present(c) /\ ~ActiveRun(c) /\ nRun < MaxRuns
    /\ nRun' = nRun + 1
    /\ runs' = TidyRuns([runs EXCEPT ![nRun + 1] = [st |-> "active", tgt |-> c, tries |-> 0, keys |-> 0]])
    /\ pending' = [pending EXCEPT ![c] = FALSE]
    /\ UNCHANGED <<log, bm, store, fence, hst, owes, att, nAtt, man, eng, used>>

\* Claim the asset partition, pin, plan the batch; nothing to do ends the task.
Prepare(r) ==
    \* (A keys= run on an output that must start over reads a full pass instead: F17's fix.)
    LET c == runs[r].tgt
        p == IF runs[r].keys > 0 /\ ~(FixF17 /\ Moved(c)) THEN KeysPlan(c, runs[r].keys) ELSE Plan(c)
    IN
    /\ eng.serving /\ runs[r].st = "active" /\ Present(c) /\ ~Claimed(c)
    /\ IF p.act = "none"
         THEN /\ runs' = TidyRuns([runs EXCEPT ![r].st = "succeeded"])
              /\ UNCHANGED <<att, nAtt>>
         ELSE /\ nAtt < MaxAtt
              /\ LET st == StoreOf(c)  moved == Moved(c) IN
                 att' = TidyAtt([att EXCEPT ![nAtt + 1] =
                        [status |-> "prep", asset |-> c, run |-> r, life |-> Life(c), st |-> st,
                         up |-> p.up, rm |-> p.rm, reset |-> p.reset \/ moved, newbm |-> p.newbm,
                         fullEnd |-> p.fullEnd, owed |-> owes[c], w |-> "none", gate |-> "none",
                         rep |-> {}, cancel |-> FALSE]])
              /\ nAtt' = nAtt + 1
              /\ UNCHANGED runs
    /\ UNCHANGED <<log, bm, store, fence, hst, owes, nRun, pending, man, eng, used>>

Launch(i) ==
    /\ eng.serving /\ att[i].status = "prep"
    /\ att' = TidyAtt([att EXCEPT ![i].status = "launched"])
    /\ UNCHANGED <<log, bm, store, fence, hst, owes, nAtt, runs, nRun, pending, man, eng, used>>

\* Its result: the commit (heads, bookmark, repair) in one event.
Valid(i) == LET c == att[i].asset IN Present(c) /\ att[i].life = Life(c)

SettleOk(i) ==
    LET a == att[i]  c == a.asset
        touched == a.up \cup a.rm
        up == IF a.reset THEN a.up ELSE a.up \cup (a.rep \ touched)
        rm == IF a.reset THEN {} ELSE a.rm \cup ((a.owed \ a.rep) \ touched)
        entry == [up |-> up, rm |-> rm, reset |-> a.reset]
        change == a.reset \/ Apply(Content(c), entry) # Content(c)
        r == a.run
    IN
    /\ eng.serving /\ a.status = "launched" /\ a.w = "sealed"
    /\ att' = TidyAtt([att EXCEPT ![i].status = "settled"])
    /\ IF Valid(i)
         THEN /\ log' = [log EXCEPT ![c] = IF change THEN Append(@, entry) ELSE @]
              /\ bm' = [bm EXCEPT ![c] = a.newbm]
              /\ owes' = [owes EXCEPT ![c] = {}]
              /\ hst' = [hst EXCEPT ![c] = a.st]
              /\ pending' = IF c = "A" /\ change THEN [pending EXCEPT !["B"] = TRUE] ELSE pending
              /\ runs' = TidyRuns(IF runs[r].st # "active" THEN runs
                         ELSE IF runs[r].keys = 0 /\ MoreAfter(c, a.newbm, a.fullEnd) THEN runs
                         ELSE [runs EXCEPT ![r].st = "succeeded"])
         ELSE /\ runs' = TidyRuns(IF runs[r].st = "active" THEN [runs EXCEPT ![r].st = "canceled"] ELSE runs)
              /\ UNCHANGED <<log, bm, owes, hst, pending>>
    /\ UNCHANGED <<store, fence, nAtt, nRun, man, eng, used>>

\* A worker that drained a requested cancel before its gate: nothing written.
SettleDrained(i) ==
    /\ eng.serving /\ att[i].status = "launched" /\ att[i].w = "drained"
    /\ att' = TidyAtt([att EXCEPT ![i].status = "settled"])
    /\ UNCHANGED <<log, bm, store, fence, hst, owes, nAtt, runs, nRun, pending, man, eng, used>>

\* An attempt ended without a result: the engine takes the gate. Winning it,
\* nothing was written; finding `writing`, the writer's intents are owed a repair.
EndLost(i) ==
    LET a == att[i]  c == a.asset  r == a.run
        intents == IF a.reset THEN Keys ELSE a.up \cup a.rm
    IN
    /\ att' = TidyAtt([att EXCEPT ![i].status = "settled",
                          ![i].gate = IF @ = "none" THEN "aborted" ELSE @])
    /\ owes' = IF a.gate = "writing" /\ Valid(i)
               THEN [owes EXCEPT ![c] = @ \cup intents] ELSE owes
    /\ runs' = TidyRuns(IF runs[r].st # "active" THEN runs
               ELSE IF ~Valid(i) THEN [runs EXCEPT ![r].st = "canceled"]
               ELSE IF runs[r].tries + 1 >= MaxTries THEN [runs EXCEPT ![r].st = "failed", ![r].tries = @ + 1]
               ELSE [runs EXCEPT ![r].tries = @ + 1])

SettleLost(i) ==
    /\ eng.serving /\ att[i].status = "launched" /\ att[i].w \in {"dead", "quit"}
    /\ EndLost(i)
    /\ UNCHANGED <<log, bm, store, fence, hst, nAtt, nRun, pending, man, eng, used>>

-----------------------------------------------------------------------------
(* Workers: they run on whatever the engine decided, even after it settled *)
(* their attempt (a stale worker); only the gate and the fence stop them.  *)

Running(i) == att[i].status \in {"launched", "settled"}

\* The worker starts (until then it is provisioning), acquires the fence,
\* then reads back the keys a dead writer meant to write.
WAcquire(i) ==
    LET a == att[i] IN
    /\ Running(i) /\ a.w = "none"
    /\ IF fence[a.asset][a.st] > i
         THEN /\ att' = TidyAtt([att EXCEPT ![i].w = "quit"])
              /\ UNCHANGED fence
         ELSE /\ fence' = [fence EXCEPT ![a.asset][a.st] = i]
              /\ att' = TidyAtt([att EXCEPT ![i].w = "acq", ![i].rep = a.owed \cap store[a.asset][a.st]])
    /\ UNCHANGED <<log, bm, store, hst, owes, nAtt, runs, nRun, pending, man, eng, used>>

\* The gate: a requested cancel drains first; the engine's `aborted` stops it.
WGate(i) ==
    LET a == att[i] IN
    /\ Running(i) /\ a.w = "acq"
    /\ att' = TidyAtt([att EXCEPT ![i].w = IF a.cancel /\ a.gate = "none" THEN "drained"
                                    ELSE IF a.gate = "none" THEN "gated" ELSE "quit",
                          ![i].gate = IF ~a.cancel /\ a.gate = "none" THEN "writing" ELSE @])
    /\ UNCHANGED <<log, bm, store, fence, hst, owes, nAtt, runs, nRun, pending, man, eng, used>>

\* Every write transaction checks the fence (a fenced store).
WWrite(i) ==
    LET a == att[i]  rows == store[a.asset][a.st] IN
    /\ Running(i) /\ a.w = "gated"
    /\ IF fence[a.asset][a.st] = i
         THEN /\ store' = [store EXCEPT ![a.asset][a.st] = IF a.reset THEN a.up ELSE (rows \ a.rm) \cup a.up]
              /\ att' = TidyAtt([att EXCEPT ![i].w = "wrote"])
         ELSE /\ att' = TidyAtt([att EXCEPT ![i].w = "quit"])
              /\ UNCHANGED store
    /\ UNCHANGED <<log, bm, fence, hst, owes, nAtt, runs, nRun, pending, man, eng, used>>

WSeal(i) ==
    /\ Running(i) /\ att[i].w = "wrote"
    /\ att' = TidyAtt([att EXCEPT ![i].w = "sealed"])
    /\ UNCHANGED <<log, bm, store, fence, hst, owes, nAtt, runs, nRun, pending, man, eng, used>>

-----------------------------------------------------------------------------
(* The environment. Everything but commits to S happens before the final *)
(* one, so that the system then has to converge.                         *)

Early == used.src < MaxSrc   \* the final change to S is not made yet
Fault(kind) == kind \in Faults /\ used.fault < MaxFault /\ Early
Deploy(kind) == kind \in Deploys /\ used.deploy < MaxDeploy /\ Early
SpendFault == used' = [used EXCEPT !.fault = @ + 1]
SpendDeploy == used' = [used EXCEPT !.deploy = @ + 1]

\* A user runs A for one key with keys= (before the final change).
KeysRun(k) ==
    /\ used.keys < MaxKeysRuns /\ Early /\ eng.serving /\ nRun < MaxRuns
    /\ nRun' = nRun + 1
    /\ runs' = [runs EXCEPT ![nRun + 1] = [st |-> "active", tgt |-> "A", tries |-> 0, keys |-> k]]
    /\ used' = [used EXCEPT !.keys = @ + 1]
    /\ UNCHANGED <<log, bm, store, fence, hst, owes, att, nAtt, pending, man, eng>>

SrcCommit(k) ==
    /\ used.src < MaxSrc
    /\ LET e == IF k \in Content("S") THEN [up |-> {}, rm |-> {k}, reset |-> FALSE]
                ELSE [up |-> {k}, rm |-> {}, reset |-> FALSE]
       IN log' = [log EXCEPT !["S"] = Append(@, e)]
    /\ pending' = [pending EXCEPT !["A"] = TRUE]
    /\ used' = [used EXCEPT !.src = @ + 1]
    /\ UNCHANGED <<bm, store, fence, hst, owes, att, nAtt, runs, nRun, man, eng>>

\* Deploys: a new manifest. Moving a store changes the asset's fingerprint
\* (its declaration), so its inputs read a full pass (F13's fix).
MoveA ==
    /\ Deploy("move")
    /\ man' = [man EXCEPT !.storeA = IF @ = "st1" THEN "st2" ELSE "st1"]
    /\ SpendDeploy
    /\ UNCHANGED <<log, bm, store, fence, hst, owes, att, nAtt, runs, nRun, pending, eng>>

PatternB ==
    /\ Deploy("pattern") /\ man.hasB
    /\ man' = [man EXCEPT !.pat = IF @ = Keys THEN Keys \ {NK} ELSE Keys]
    /\ SpendDeploy
    /\ UNCHANGED <<log, bm, store, fence, hst, owes, att, nAtt, runs, nRun, pending, eng>>

BumpB ==
    /\ Deploy("bump") /\ man.hasB
    /\ man' = [man EXCEPT !.ver["B"] = @ + 1]
    /\ SpendDeploy
    /\ UNCHANGED <<log, bm, store, fence, hst, owes, att, nAtt, runs, nRun, pending, eng>>

\* Removing B cancels its queued work; a launched attempt settles, its commit
\* dropped. Re-adding B starts it fresh: a name no longer declared holds no
\* state (F12's fix), so its first write starts the store over.
RemoveB ==
    /\ Deploy("remove") /\ man.hasB
    /\ man' = [man EXCEPT !.hasB = FALSE]
    /\ att' = TidyAtt([i \in 1..MaxAtt |-> IF i <= nAtt /\ att[i].asset = "B" /\ att[i].status = "prep"
                                   THEN [att[i] EXCEPT !.status = "dropped"] ELSE att[i]])
    /\ runs' = TidyRuns([r \in 1..MaxRuns |->
                  IF r <= nRun /\ runs[r].st = "active" /\ runs[r].tgt = "B"
                     /\ ~\E i \in 1..nAtt : att[i].run = r /\ att[i].status = "launched"
                  THEN [runs[r] EXCEPT !.st = "canceled"] ELSE runs[r]])
    /\ SpendDeploy
    /\ UNCHANGED <<log, bm, store, fence, hst, owes, nAtt, nRun, pending, eng>>

AddB ==
    /\ Deploy("remove") /\ ~man.hasB
    /\ man' = [man EXCEPT !.hasB = TRUE, !.lifeB = @ + 1]
    /\ log' = [log EXCEPT !["B"] = <<>>]
    /\ bm' = [bm EXCEPT !["B"] = InitBm]
    /\ owes' = [owes EXCEPT !["B"] = {}]
    /\ pending' = [pending EXCEPT !["B"] = TRUE]
    /\ SpendDeploy
    /\ UNCHANGED <<store, fence, hst, att, nAtt, runs, nRun, eng>>

DropPrepared == att' = TidyAtt([i \in 1..MaxAtt |-> IF att[i].status = "prep"
                                            THEN [att[i] EXCEPT !.status = "dropped"] ELSE att[i]])

\* Faults.
WorkerCrash(i) ==
    /\ Fault("worker") /\ Running(i) /\ att[i].w \in {"none", "acq", "gated", "wrote"}
    /\ att' = TidyAtt([att EXCEPT ![i].w = "dead"])
    /\ SpendFault
    /\ UNCHANGED <<log, bm, store, fence, hst, owes, nAtt, runs, nRun, pending, man, eng>>

\* The serving engine dies: what it held in memory (prepared attempts) goes.
EngineCrash ==
    /\ Fault("crash") /\ eng.serving
    /\ eng' = [eng EXCEPT !.serving = FALSE]
    /\ DropPrepared
    /\ SpendFault
    /\ UNCHANGED <<log, bm, store, fence, hst, owes, nAtt, runs, nRun, pending, man>>

Restart ==
    /\ ~eng.serving
    /\ eng' = [eng EXCEPT !.serving = TRUE]
    /\ UNCHANGED <<log, bm, store, fence, hst, owes, att, nAtt, runs, nRun, pending, man, used>>

\* A new engine fences the serving one, which runs on as a zombie: its
\* journal writes fail, but it may still take gates.
Takeover ==
    /\ Fault("takeover") /\ eng.serving /\ ~eng.zombie
    /\ eng' = [eng EXCEPT !.zombie = TRUE]
    /\ DropPrepared
    /\ SpendFault
    /\ UNCHANGED <<log, bm, store, fence, hst, owes, nAtt, runs, nRun, pending, man>>

ZombieAbort(i) ==
    /\ eng.zombie /\ att[i].status = "launched" /\ att[i].gate = "none"
    /\ att' = TidyAtt([att EXCEPT ![i].gate = "aborted"])
    /\ UNCHANGED <<log, bm, store, fence, hst, owes, nAtt, runs, nRun, pending, man, eng, used>>

KillZombie ==
    /\ eng.zombie
    /\ eng' = [eng EXCEPT !.zombie = FALSE]
    /\ UNCHANGED <<log, bm, store, fence, hst, owes, att, nAtt, runs, nRun, pending, man, used>>

\* A timeout (or a forced cancel): the engine ends a running attempt; its
\* worker runs on, stale.
Timeout(i) ==
    /\ Fault("timeout") /\ eng.serving /\ att[i].status = "launched"
    /\ att[i].w \in {"none", "acq", "gated", "wrote"}
    /\ EndLost(i)
    /\ SpendFault
    /\ UNCHANGED <<log, bm, store, fence, hst, nAtt, nRun, pending, man, eng>>

\* A user cancels a run: queued work goes; a launched attempt is asked to
\* stop, and drains (before its gate) or completes (past it).
Cancel(r) ==
    /\ Fault("cancel") /\ eng.serving /\ runs[r].st = "active"
    /\ runs' = TidyRuns([runs EXCEPT ![r].st = "canceled"])
    /\ att' = TidyAtt([i \in 1..MaxAtt |->
                 IF i <= nAtt /\ att[i].run = r /\ att[i].status = "prep" THEN [att[i] EXCEPT !.status = "dropped"]
                 ELSE IF i <= nAtt /\ att[i].run = r /\ att[i].status = "launched" THEN [att[i] EXCEPT !.cancel = TRUE]
                 ELSE att[i]])
    /\ SpendFault
    /\ UNCHANGED <<log, bm, store, fence, hst, owes, nAtt, nRun, pending, man, eng>>

-----------------------------------------------------------------------------

Engine ==
    \/ \E c \in Assets : Fire(c)
    \/ \E r \in 1..MaxRuns : r <= nRun /\ Prepare(r)
    \/ \E i \in 1..MaxAtt : i <= nAtt /\ (Launch(i) \/ SettleOk(i) \/ SettleDrained(i) \/ SettleLost(i))

Worker == \E i \in 1..MaxAtt : i <= nAtt /\ (WAcquire(i) \/ WGate(i) \/ WWrite(i) \/ WSeal(i))

Env ==
    \/ \E k \in Keys : SrcCommit(k) \/ KeysRun(k)
    \/ MoveA \/ PatternB \/ BumpB \/ RemoveB \/ AddB
    \/ \E i \in 1..MaxAtt : i <= nAtt /\ (WorkerCrash(i) \/ Timeout(i) \/ ZombieAbort(i))
    \/ \E r \in 1..MaxRuns : r <= nRun /\ Cancel(r)
    \/ EngineCrash \/ Restart \/ Takeover \/ KillZombie

Next == Engine \/ Worker \/ Env

\* Every engine and worker step is bounded (attempts, runs, batches), so
\* each behaviour takes finitely many of them: weak fairness on all of them
\* together forces the system on until nothing is enabled. The environment
\* is not forced, but for the final change to S (and an engine restarting).
Fairness == WF_vars(Engine \/ Worker \/ Restart \/ \E k \in Keys : SrcCommit(k))

Spec == Init /\ [][Next]_vars /\ Fairness

-----------------------------------------------------------------------------
(* Properties *)

\* At most one attempt per asset partition is prepared or launched.
OneAttemptPerPartition ==
    \A c \in Assets : Cardinality({i \in 1..nAtt : att[i].asset = c /\ att[i].status \in {"prep", "launched"}}) <= 1

\* A bookmark never passes a change it did not deliver: with no pass under
\* way, every key no later commit touched is in the output exactly when it
\* was in the upstream at the bookmark, under the patterns it reads.
BookmarkHonest ==
    \A c \in Assets :
        (Present(c) /\ bm[c].pass.kind = "none") =>
            LET u == Up(c)  b == bm[c]  quiet == Keys \ Touched(u, b.next, Len(log[u])) IN
            \A k \in quiet : (k \in Content(c)) <=> (k \in UpTo(u, b.next) \cap b.pat)

\* With nothing in flight, nothing owed and no stale writer at its gate, a
\* store holds exactly what the journal says its output holds.
Busy(c) == \E i \in 1..nAtt : att[i].asset = c /\ (att[i].status = "launched" \/ att[i].w = "gated")
StoreMatchesJournal ==
    \A c \in Assets :
        (Present(c) /\ log[c] # <<>> /\ owes[c] = {} /\ ~Busy(c)) => store[c][hst[c]] = Content(c)

\* Budgets large enough that no behaviour is cut short by them.
WithinBudget == nAtt < MaxAtt /\ nRun < MaxRuns

\* Every behaviour takes finitely many system steps, so "every run ends" is
\* "eventually, no run is active, for good".
NoActiveRun == \A r \in 1..nRun : runs[r].st # "active"
EveryRunEnds == <>[]NoActiveRun

Converged ==
    /\ Content("A") = Content("S")
    /\ man.hasB => Content("B") = Content("A") \cap man.pat
Converges == <>[]Converged
\* Both, as one property (one tableau for TLC).
Quiesces == <>[](NoActiveRun /\ Converged)

=============================================================================
