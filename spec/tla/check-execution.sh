#!/usr/bin/env bash
# Model-check the execution spec (docs/verification.md, "Execution spec").
#
#   spec/tla/check-execution.sh             the smoke model and the calibrations (CI)
#   spec/tla/check-execution.sh design      the design with liveness: store moves, patterns and versions, removal (minutes)
#   spec/tla/check-execution.sh big         every deploy kind, every fault kind, each=True: too large to finish yet
#   spec/tla/check-execution.sh safety      every kind at once, two deploys and two faults: random behaviours (SAFETY_TRACES)
#   spec/tla/check-execution.sh calibrate   each fix put back out: TLC must find its bug
#   spec/tla/check-execution.sh all
#
# TLC_WORKERS (3) and TLC_HEAP (6g) bound what a run takes of a shared machine.
# Needs Java 11+ (else runs TLC in the eclipse-temurin:21-jre image). Downloads
# tla2tools.jar into spec/tla/.tools (gitignored).
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

tlc() {  # tlc CONFIG LOG [TLC options]
    local cfg=$1 log=$2
    shift 2
    local args=(-XX:+UseParallelGC "-Xmx${TLC_HEAP:-6g}" -cp "$jar" tlc2.TLC -workers "${TLC_WORKERS:-3}" -deadlock -lncheck final
                -metadir "$work/states" "$@" -config "$cfg" Execution.tla)
    if command -v java >/dev/null; then
        java "${args[@]}" > "$log" 2>&1 || true
    else
        docker run --rm --user "$(id -u):$(id -g)" -v "$PWD":/w -v "$work":"$work" -w /w \
            eclipse-temurin:21-jre java "${args[@]}" > "$log" 2>&1 || true
    fi
    rm -rf "$work/states"
    if [ -n "${TLA_LOGS:-}" ]; then mkdir -p "$TLA_LOGS"; cp "$log" "$TLA_LOGS/"; fi
}

check() {  # check NAME [TLC options]: the model must pass
    local name=$1 log="$work/$1.log"
    shift
    echo "== $name"
    tlc "Execution-$name.cfg" "$log" "$@"
    grep -E "distinct states found|states checked|^Finished in" "$log" | tail -2 | sed 's/^/   /'
    if grep -qE "^Error|violated" "$log" || ! grep -q "^Finished in" "$log"; then
        cat "$log"
        echo "FAIL: $name"
        exit 1
    fi
}

calibrate() {  # calibrate NAME EXPECTED: the model with its fix out must violate EXPECTED
    local log="$work/$1.log"
    tlc "Execution-$1.cfg" "$log"
    # TLC names a violated invariant, not a temporal property: Quiesces is the only one checked.
    if grep -qE "Invariant $2 is violated" "$log" ||
        { [ "$2" = Quiesces ] && grep -q "Temporal properties were violated" "$log"; }; then
        printf '   %-4s %s violated in %s steps\n' "$1" "$2" "$(grep -cE '^State [0-9]+:' "$log")"
    else
        tail -40 "$log"
        echo "FAIL: $1: expected $2 violated"
        exit 1
    fi
}

calibration() {
    echo "== calibrate"
    calibrate F6 Quiesces          # 1cad0bd: a pass that ends behind the head ends the task
    calibrate F10 BookmarkHonest   # a full pass its patterns take nothing from never reaches B
    calibrate F13 BookmarkHonest   # a store move leaves the fingerprint: the moved write starts over with one batch
    calibrate F17 BookmarkHonest   # moved and back with a keys= run between: the old fingerprint matches, the index started over
}

case "${1:-ci}" in
    ci) check smoke; calibration ;;
    design) for m in store shape remove; do check $m; done ;;
    big) for m in deploys faults each; do check $m; done ;;
    safety) check safety -simulate "num=${SAFETY_TRACES:-100000}" -depth 150 ;;
    calibrate) calibration ;;
    all) check smoke; calibration; for m in store shape remove; do check $m; done; check safety -simulate "num=${SAFETY_TRACES:-100000}" -depth 150 ;;
    *) echo "usage: $0 [ci|design|big|safety|calibrate|all]" >&2; exit 2 ;;
esac
