#!/usr/bin/env bash
# Model-check the execution spec (docs/verification.md, "Execution spec").
#
#   spec/tla/check-execution.sh             the smoke model and the calibrations (CI)
#   spec/tla/check-execution.sh design      the design with liveness: deploys, faults, each=True
#   spec/tla/check-execution.sh safety      safety only, two deploys and two faults
#   spec/tla/check-execution.sh calibrate   each fix put back out: TLC must find its bug
#   spec/tla/check-execution.sh all
#
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

tlc() {  # tlc CONFIG LOG
    local args=(-XX:+UseParallelGC -cp "$jar" tlc2.TLC -workers auto -deadlock -lncheck final
                -metadir "$work/states" -config "$1" Execution.tla)
    if command -v java >/dev/null; then
        java "${args[@]}" > "$2" 2>&1 || true
    else
        docker run --rm --user "$(id -u):$(id -g)" -v "$PWD":/w -v "$work":"$work" -w /w \
            eclipse-temurin:21-jre java "${args[@]}" > "$2" 2>&1 || true
    fi
    rm -rf "$work/states"
    if [ -n "${TLA_LOGS:-}" ]; then mkdir -p "$TLA_LOGS"; cp "$2" "$TLA_LOGS/"; fi
}

check() {  # check NAME: the model must pass
    local log="$work/$1.log"
    echo "== $1"
    tlc "Execution-$1.cfg" "$log"
    grep -E "distinct states found|^Finished in" "$log" | sed 's/^/   /'
    if ! grep -q "No error has been found" "$log"; then
        cat "$log"
        echo "FAIL: $1"
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
    design) check deploys; check faults; check each ;;
    safety) check safety ;;
    calibrate) calibration ;;
    all) check smoke; calibration; check deploys; check faults; check each; check safety ;;
    *) echo "usage: $0 [ci|design|safety|calibrate|all]" >&2; exit 2 ;;
esac
