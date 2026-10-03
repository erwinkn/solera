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
        execution/store) changes=(MaxDeploy=2 "$move" MaxKeysRuns=1 WithB=FALSE) ;;
        execution/reset) changes=(MaxDeploy=1 "$move") ;;
        execution/shape) changes=(MaxDeploy=2 'Deploys={"pattern", "bump"}') ;;
        execution/remove) changes=(MaxDeploy=2 'Deploys={"remove"}') ;;
        execution/zombie) changes=(MaxFault=1 'Faults={"takeover", "zombie"}' WithB=FALSE) ;;
        execution/deploys) changes=(MaxDeploy=1 "$every_deploy" MaxKeysRuns=1) ;;
        execution/faults) changes=(MaxFault=2 'Faults={"worker", "crash", "takeover", "timeout", "cancel", "zombie"}') ;;
        execution/each)
            changes=(MaxDeploy=1 "$every_deploy" MaxFault=1 'Faults={"worker", "crash", "timeout", "cancel"}' Each=TRUE) ;;
        execution/safety)
            changes=(MaxDeploy=2 "$every_deploy" MaxFault=2 MaxKeysRuns=1 MaxAtt=14 MaxTries=4
                     'Faults={"worker", "crash", "takeover", "timeout", "cancel", "zombie"}' -Quiesces) ;;
        execution/F6) changes=(MaxFault=1 'Faults={"cancel"}' FixF6=FALSE -RunsEndCaughtUp) ;;
        execution/F10) changes=(MaxDeploy=2 'Deploys={"pattern", "bump"}' FixF10=FALSE -RunsEndCaughtUp) ;;
        execution/move) changes=(MaxDeploy=1 "$move" ResetOnMove=FALSE FixF17=FALSE -RunsEndCaughtUp) ;;
        execution/F17) changes=(MaxDeploy=2 "$move" MaxKeysRuns=1 FixF17=FALSE -Quiesces) ;;
        execution/selection) changes=(MaxDeploy=2 "$move" MaxKeysRuns=1 WithB=FALSE FixSelection=FALSE -Quiesces) ;;
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
            calibrate F17 RunsEndCaughtUp   # after a move resets A, a keys= run reads only its key into the new output, and succeeds
            calibrate selection RunsEndCaughtUp  # a keys= run made a full pass ends after its first batch
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
        *) echo "usage: $0 [ci | execution|journal|attempt [GROUP|MODEL]]" >&2; exit 2 ;;
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
        attempt/all) run attempt ci; check big ;;
        */calibrate) calibration "$spec" ;;
        *) check "$2" ;;
    esac
}

case ${1:-ci} in
    ci) for s in execution journal attempt; do echo "# $s"; run $s ci; done ;;
    *) run "$1" "${2:-ci}" ;;
esac
