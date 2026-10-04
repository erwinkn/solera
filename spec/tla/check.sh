#!/usr/bin/env bash
# Model-check the specs (docs/verification.md, "Formal model: ...").
#
#   spec/tla/check.sh [ci]             every spec's CI models and calibrations
#   spec/tla/check.sh SPEC [GROUP]     one spec's group (default ci), or one model by name
#
#   execution  ci: smoke and the calibrations; design: store, reset, shape, remove,
#              zombie (liveness included, ~30 min at 6 workers); big: deploys, faults,
#              each (too large to finish); safety: random behaviours (SAFETY_TRACES)
#   journal    ci: small, fixed, live and the calibrations; big: three engines, six writes
#   attempt    ci: small, dup, live and the calibrations; big: two attempts with a
#              duplicate worker each (too large to finish)
#   positions  ci: base, each and the calibrations; three: three keys
#   spans      ci: base, takeover (a zombie), orphans (the collector), retries (failing
#              merges), empty (spans with no files), passes, and the calibrations
#   every spec: calibrate (each rule switched off: TLC must find its bug), all
#
# A model is its spec's base config ({Spec}.cfg) with changes (`model` below):
#   CONSTANT=VALUE   a constant's value
#   KEYWORD=...      the whole SPECIFICATION, INVARIANT, PROPERTY, SYMMETRY or
#                    CONSTRAINT line (nothing after `=`: no such line)
#   -NAME            NAME left out of the invariants and properties
#
# TLC_WORKERS (2) and TLC_HEAP (4g) bound what a run takes of a shared machine;
# TLA_LOGS=DIR keeps TLC's output. Needs Java 11+ (else runs TLC in the
# eclipse-temurin:21-jre image). Downloads tla2tools.jar into spec/tla/.tools
# (gitignored).
set -euo pipefail
cd "$(dirname "$0")"

version=1.7.4
jar=.tools/tla2tools-$version.jar
if [ ! -f "$jar" ]; then
    mkdir -p .tools
    curl -fsSL -o "$jar" "https://github.com/tlaplus/tlaplus/releases/download/v$version/tla2tools.jar"
fi
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

# model SPEC NAME: set `changes`, what makes model NAME of SPEC from its base.
model() {
    local move='Deploys={"move"}' every_deploy='Deploys={"move", "pattern", "bump", "remove"}'
    case $1/$2 in
        # Execution.tla: the plain pipeline, keys 1 and 2, B declared.
        execution/smoke) changes=() ;;
        execution/store) changes=(MaxDeploy=2 "$move" WithB=FALSE) ;;
        execution/reset) changes=(MaxDeploy=1 "$move") ;;
        execution/shape) changes=(MaxDeploy=2 'Deploys={"pattern", "bump"}') ;;
        execution/remove) changes=(MaxDeploy=2 'Deploys={"remove"}') ;;
        execution/zombie) changes=(MaxFault=1 'Faults={"takeover", "zombie"}' WithB=FALSE) ;;
        execution/deploys) changes=(MaxDeploy=1 "$every_deploy") ;;
        execution/faults) changes=(MaxFault=2 'Faults={"worker", "crash", "takeover", "timeout", "cancel", "zombie"}') ;;
        execution/each)
            changes=(MaxDeploy=1 "$every_deploy" MaxFault=1 'Faults={"worker", "crash", "timeout", "cancel"}' Each=TRUE) ;;
        execution/safety)
            changes=(MaxDeploy=2 "$every_deploy" MaxFault=2 MaxAtt=14 MaxTries=4
                     'Faults={"worker", "crash", "takeover", "timeout", "cancel", "zombie"}' -Quiesces) ;;
        execution/F6) changes=(MaxFault=1 'Faults={"cancel"}' FixF6=FALSE -RunsEndCaughtUp) ;;
        execution/F10) changes=(MaxDeploy=2 'Deploys={"pattern", "bump"}' FixF10=FALSE -RunsEndCaughtUp) ;;
        execution/move) changes=(MaxDeploy=1 "$move" ResetOnMove=FALSE FixF17=FALSE -RunsEndCaughtUp) ;;
        execution/F22-move) changes=(MaxSrc=0 MaxDeploy=1 "$move" WithB=FALSE FixF22=FALSE -RunsEndCaughtUp) ;;
        execution/F22-shape) changes=(MaxSrc=0 MaxDeploy=1 'Deploys={"pattern", "bump"}' FixF22=FALSE -RunsEndCaughtUp) ;;
        execution/F22-add) changes=(MaxSrc=0 MaxDeploy=2 'Deploys={"remove"}' FixF22=FALSE -RunsEndCaughtUp) ;;
        # JournalObject.tla: two engines, seven head writes, every fault.
        journal/small) changes=() ;;
        journal/fixed) changes=('Engines={e1, e2, e3}' MaxWrites=5) ;;
        journal/big) changes=('Engines={e1, e2, e3}' MaxWrites=6) ;;
        journal/live)  # engines one at a time, each step fair; no 409s
            changes=('Engines={e1, e2, e3}' Overlap=FALSE Conflicts=FALSE SPECIFICATION=FairSpec SYMMETRY=
                     'PROPERTY=OpensAlone AppendsAlone') ;;
        journal/failures)  # a checkpoint or its cleanup may fail midway; fewer writes
            changes=(Failures=TRUE MaxWrites=5 CONSTRAINT=Bounded) ;;
        journal/engine-id) changes=(EngineId=FALSE) ;;
        journal/ask) model journal live; changes+=(AskJournal=FALSE -OpensAlone) ;;
        journal/re-get) changes=(ReGet=FALSE) ;;
        journal/verify) changes=(Verify=FALSE) ;;
        journal/list-first) changes=(ListFirst=FALSE) ;;
        # Attempt.tla: two attempts, one worker each, a zombie engine.
        attempt/small) changes=() ;;
        attempt/dup) changes=(N=1 'Copies={1, 2}') ;;
        attempt/live) changes=(SPECIFICATION=FairSpec PROPERTY=EveryAttemptEnds) ;;
        attempt/big) changes=('Copies={1, 2}') ;;
        attempt/pre-create) changes=(PreCreate=FALSE) ;;
        attempt/take-writing) changes=(TakeWriting=FALSE) ;;
        attempt/engine-swaps) changes=(EngineSwaps=FALSE -OneOutcome) ;;
        attempt/classify) changes=(Classify=FALSE) ;;
        attempt/offer) changes=(OfferDurable=FALSE) ;;
        # Positions.tla: keys 1 and 2, B not each=True, every rule as designed.
        positions/base) changes=() ;;
        positions/each) changes=(Each=TRUE) ;;
        positions/three) changes=(NK=3 MaxSrc=2 MaxKeysRuns=2) ;;
        positions/net) changes=(FixNet=FALSE) ;;
        positions/transitive) changes=(FixTransitive=FALSE) ;;
        positions/skip) changes=(FixSkip=FALSE) ;;
        positions/continue) changes=(FixContinue=FALSE) ;;
        positions/collapse) changes=(FixCollapse=FALSE) ;;
        positions/retry-collapse) changes=(Each=TRUE FixRetryCollapse=FALSE) ;;
        # Spans.tla: one consumer, three commits, one claim, one reset, one merge at a time.
        spans/base) changes=() ;;
        spans/takeover) changes=(MaxTakeovers=1 MaxResets=0 Collectors=TRUE MaxCommits=2 MaxFiles=5) ;;
        spans/unpublished) changes=(MaxTakeovers=1 MaxResets=0 Collectors=TRUE MaxCommits=2 MaxFiles=5 INVARIANT=NoUnpublished) ;;
        spans/orphans) changes=(Collectors=TRUE MaxResets=0) ;;
        spans/retries) changes=(MergeFailures=TRUE MaxResets=0 MaxCommits=2) ;;
        spans/empty) changes=(EmptySpans=TRUE MaxCommits=2) ;;
        spans/passes) changes=(Passes=TRUE MaxClaims=2 MaxResets=0) ;;
        spans/landing) changes=(FixLanding=FALSE) ;;
        spans/bounds) changes=(FixBounds=FALSE) ;;
        spans/inputs) changes=(MaxJobs=2 MaxCommits=2 MaxResets=0 FixLanes=FALSE FixInputs=FALSE) ;;
        spans/lanes) changes=(MaxJobs=2 MaxCommits=2 MaxResets=0 FixLanes=FALSE) ;;
        spans/inputs-alone) changes=(MaxJobs=2 MaxCommits=2 MaxResets=0 FixInputs=FALSE) ;;
        spans/life) changes=(EmptySpans=TRUE FixLife=FALSE) ;;
        spans/life-files) changes=(FixLife=FALSE) ;;
        spans/settle-life) changes=(FixSettleLife=FALSE) ;;
        spans/pin-floor) changes=(FixPinFloor=FALSE) ;;
        spans/durable) changes=(FixDurable=FALSE MaxResets=0) ;;
        spans/retries-cap) changes=(MergeFailures=TRUE MaxResets=0 MaxCommits=2 FixRetries=FALSE) ;;
        spans/epoch) changes=(MaxTakeovers=1 MaxResets=0 Collectors=TRUE FixEpoch=FALSE) ;;
        spans/epoch-state) changes=(MaxTakeovers=1 MaxResets=0 Collectors=TRUE FixEpoch=FALSE -PublishingStored) ;;
        spans/judge) changes=(Collectors=TRUE MaxResets=0 FixJudgeAfter=FALSE) ;;
        spans/garbage-named) changes=(Collectors=TRUE MaxResets=0 MaxClaims=0 MaxTakeovers=1 FixGarbageNamed=FALSE) ;;
        *) echo "no model $2 of $1" >&2; exit 2 ;;
    esac
}

# calibration SPEC: each rule switched off in turn, and the property TLC must
# find violated.
calibration() {
    echo "== calibrate"
    case $1 in
        execution)
            calibrate F6 Quiesces           # 1cad0bd: a pass that ends behind the head ends the task
            calibrate F10 PositionHonest    # a full pass its patterns take nothing from never reaches B
            calibrate move PositionHonest   # synthetic: a store move that changes neither the fingerprint nor the plan
            calibrate F22-move Quiesces     # d6585fb: a move resets A, and nothing fires A until S changes
            calibrate F22-shape Quiesces    # K34: B's patterns or version change, and nothing fires B
            calibrate F22-add Quiesces      # K34: B is removed and added back, and nothing fires B
            ;;
        journal)
            # Without the engine id in the journal, a fence can leave its bytes,
            # so its ETag, unchanged: the old engine's If-Match still holds.
            calibrate engine-id OneWriter
            # A refused write that does not GET the journal takes its own lost
            # answer for a newer engine's write: a lone engine stops.
            calibrate ask AppendsAlone
            # An opener whose checkpoint was cleaned up since it read the journal gives up.
            calibrate re-get OpensNeverFail
            # The journal moves to a checkpoint nobody can parse: the state is lost.
            calibrate verify NoAckedLoss
            # Cleanup that LISTs after its move deletes a newer engine's
            # checkpoint before that engine's move names it: the state is lost.
            calibrate list-first NoAckedLoss
            ;;
        positions)
            # K39 with the net rule: a key added and removed past the position
            # counted as behind, though nothing would be delivered.
            calibrate net StatusExact
            # K46: B looks fresh to its own check while A, its input, is stale.
            calibrate transitive StatusExact
            # K45: a default run delivers again a key a keys= run already read
            # at its version.
            calibrate skip DeliveredOnce
            # The correction: a default run starts over a pass keys= runs
            # began, delivering their keys again.
            calibrate continue DeliveredOnce
            # K45: a keys= commit collapses the record while a key is behind.
            calibrate collapse StatusExact
            # K47: a retry pass that leaves nothing behind keeps its entries.
            calibrate retry-collapse Collapsed
            ;;
        spans)
            # A claim whose landing point is no endpoint: a merge covers it
            # before the attempt settles (A10).
            calibrate landing ReadsExact
            # A merge that keeps no segment start at an endpoint it knew.
            calibrate bounds ReadsExact
            # Two merges over one span, and publication without re-checking the
            # inputs: the second publishes over replaced inputs. Either guard
            # alone suffices (models lanes and inputs-alone pass).
            calibrate inputs Tiling
            # Publication without the life check: a merge of spans with no files
            # planned before a reset matches the new life's, whose names are
            # as empty (W42). With files, the input check alone refuses it
            # (model life-files passes).
            calibrate life Tiling
            # An attempt of an earlier life lands its position in the new one.
            calibrate settle-life ReadsExact
            # Garbage deleted while a reader pinned before it still reads it.
            calibrate pin-floor ReadersStored
            # Inputs let go of at upload: a refused or crashed merge leaves the
            # state naming deleted files.
            calibrate durable StateStored
            # A failing input set merged again and again.
            calibrate retries-cap AttemptsBounded
            # F40, the code before c4eb4f7: a zombie collects orphans by the
            # model it last had, and deletes the output of a merge the serving
            # engine is publishing, or (epoch-state) has published since.
            calibrate epoch PublishingStored
            calibrate epoch-state StateStored
            # A collector that names before it lists: a merge of its own,
            # planned and uploaded in between, is listed and deleted.
            calibrate judge PublishingStored
            # A collector that does not count garbage as named: the inputs of a
            # publication not yet durable, which the journal still names, go.
            calibrate garbage-named StateStored
            # Not a rule: `takeover` reaches the zombie's publication that never
            # became durable (the coordinator's question), so its passing means
            # the zombie's collector spares what the journal still names.
            calibrate unpublished NoUnpublished
            ;;
        attempt)
            # A create-if-absent gate with nothing retained: a worker that read its
            # spec, paused, and resumes after its run was purged creates the file
            # again and writes, though the engine recorded that it wrote nothing.
            calibrate pre-create NoWriteAfterNone
            # A worker that writes without marking the file `writing` first: the
            # engine ends it as having written nothing.
            calibrate take-writing NoWriteAfterNone
            # The engine ends an attempt with a blind PUT over the `writing` it never read.
            calibrate engine-swaps NoWriteAfterNone
            # Ended from `writing`, the writes are taken for none.
            calibrate classify NoWriteAfterNone
            # F26: a worker offered an attempt before its AttemptLaunched is
            # durable runs it though its engine was fenced first: its rows
            # land under an attempt no engine knows, which nothing commits.
            calibrate offer NoOrphanWrite
            ;;
    esac
}

# configure NAME: write model NAME of $spec into $work/NAME.cfg.
configure() {
    local cfg="$work/$1.cfg" change key
    model "$spec" "$1"
    cp "$module.cfg" "$cfg"
    for change in ${changes[@]+"${changes[@]}"}; do
        key=${change%%=*}
        case $change in
            -*) edit "s/ ${change#-}( |$)/\1/" ;;
            SPECIFICATION=* | INVARIANT=* | PROPERTY=* | SYMMETRY=* | CONSTRAINT=*)
                edit "/^$key /d"
                if [ -n "${change#*=}" ]; then echo "$key ${change#*=}" >> "$cfg"; fi ;;
            *) edit "s/^( +$key) = .*/\1 = ${change#*=}/" ;;
        esac
    done
    edit '/^(INVARIANT|PROPERTY) *$/d'  # every name left out
}

# edit EXPRESSION: apply a sed -E expression to $cfg (no sed -i, whose syntax
# differs between GNU and BSD sed).
edit() { sed -E "$1" "$cfg" > "$cfg.tmp" && mv "$cfg.tmp" "$cfg"; }

tlc() {  # tlc NAME [TLC options]: run TLC on model NAME, its output in $work/NAME.log
    local name=$1
    shift
    local args=(-XX:+UseParallelGC "-Xmx${TLC_HEAP:-4g}" -cp "$jar" tlc2.TLC -workers "${TLC_WORKERS:-2}"
                -lncheck final -metadir "$work/states" "$@" -config "$work/$name.cfg" "$module.tla")
    if java -version >/dev/null 2>&1; then  # not just on PATH: macOS has a /usr/bin/java stub
        java "${args[@]}" > "$work/$name.log" 2>&1 || true
    else
        docker run --rm --user "$(id -u):$(id -g)" -v "$PWD":/w -v "$work":"$work" -w /w \
            eclipse-temurin:21-jre java "${args[@]}" > "$work/$name.log" 2>&1 || true
    fi
    rm -rf "$work/states"
    if [ -n "${TLA_LOGS:-}" ]; then mkdir -p "$TLA_LOGS"; cp "$work/$name.log" "$TLA_LOGS/$spec-$name.log"; fi
}

check() {  # check NAME [TLC options]: the model must pass
    local name=$1 log="$work/$1.log"
    echo "== $name"
    configure "$name"
    tlc "$@"
    grep -E "distinct states found|depth of the complete|^Finished in" "$log" | tail -3 | sed 's/^/   /'
    if grep -qE "^Error|violated" "$log" || ! grep -q "^Finished in" "$log"; then
        tail -60 "$log"
        echo "FAIL: $spec $name"
        exit 1
    fi
}

calibrate() {  # calibrate NAME EXPECTED: model NAME must violate EXPECTED
    local name=$1 expected=$2 log="$work/$1.log"
    configure "$name"
    tlc "$name"
    # TLC names a violated invariant or action property, not a temporal
    # property: then EXPECTED must be the only one checked.
    if grep -qE "(Invariant|Action property) $expected is violated" "$log" ||
        { grep -q "Temporal properties were violated" "$log" &&
          grep -qE "^PROPERTY +$expected *$" "$work/$name.cfg"; }; then
        printf '   %-13s %s violated in %s steps\n' "$name" "$expected" "$(grep -cE '^State [0-9]+:' "$log")"
    else
        tail -40 "$log"
        echo "FAIL: $spec $name: expected $expected violated"
        exit 1
    fi
}

run() {  # run SPEC GROUP
    spec=$1
    case $spec in
        execution) module=Execution ;;
        journal) module=JournalObject ;;
        attempt) module=Attempt ;;
        positions) module=Positions ;;
        spans) module=Spans ;;
        *) echo "usage: $0 [ci | execution|journal|attempt|positions|spans [GROUP|MODEL]]" >&2; exit 2 ;;
    esac
    case $spec/$2 in
        execution/ci) check smoke; calibration execution ;;
        execution/design) for m in store reset shape remove zombie; do check $m; done ;;
        execution/big) for m in deploys faults each; do check $m; done ;;
        execution/safety) check safety -simulate "num=${SAFETY_TRACES:-100000}" -depth 150 ;;
        execution/all) run execution ci; run execution design; run execution safety ;;
        journal/ci) check small; check fixed; check live; check failures; calibration journal ;;
        journal/all) run journal ci; check big ;;
        attempt/ci) check small; check dup; check live; calibration attempt ;;
        positions/ci) check base; check each; calibration positions ;;
        positions/all) run positions ci; check three ;;
        spans/ci) check base; check takeover; check orphans; check retries; check empty; check passes; calibration spans ;;
        attempt/all) run attempt ci; check big ;;
        */calibrate) calibration "$spec" ;;
        *) check "$2" ;;
    esac
}

case ${1:-ci} in
    ci) for s in execution journal attempt positions spans; do echo "# $s"; run $s ci; done ;;
    *) run "$1" "${2:-ci}" ;;
esac
