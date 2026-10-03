#!/usr/bin/env bash
# Model-check the attempt control file (docs/verification.md, "Formal model:
# the attempt control file").
#
#   spec/tla/check-attempt.sh              small, dup, liveness and the calibrations (CI)
#   spec/tla/check-attempt.sh big          two attempts with a duplicate worker each (too large to finish)
#   spec/tla/check-attempt.sh all
#
# TLC_WORKERS (2) and TLC_HEAP (4g) bound what a run takes of a shared
# machine. Needs Java 11+. Downloads tla2tools.jar into spec/tla/.tools
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

tlc() {  # tlc CONFIG LOG
    java -XX:+UseParallelGC "-Xmx${TLC_HEAP:-4g}" -cp "$jar" tlc2.TLC -workers "${TLC_WORKERS:-2}" \
        -metadir "$work/states" -config "$1" Attempt.tla > "$2" 2>&1 || true
    rm -rf "$work/states"
    if [ -n "${TLA_LOGS:-}" ]; then mkdir -p "$TLA_LOGS"; cp "$2" "$TLA_LOGS/"; fi
}

# configure NAME BASE [CONSTANT=VALUE | -PROPERTY ...]: BASE's model, changed.
configure() {
    local cfg="$work/$1.cfg" change
    cp "Attempt-$2.cfg" "$cfg"
    shift 2
    for change in "$@"; do
        case $change in
            -*) sed -i -E "s/^PROPERTY ${change#-} *$//" "$cfg" ;;
            *) sed -i -E "s/^( +${change%%=*}) = .*/\1 = ${change#*=}/" "$cfg" ;;
        esac
    done
}

check() {  # check NAME BASE [changes]: the model must pass
    local log="$work/$1.log"
    configure "$@"
    echo "== $1"
    tlc "$work/$1.cfg" "$log"
    grep -E "distinct states found|depth of the complete|^Finished in" "$log" | sed 's/^/   /'
    if ! grep -q "No error has been found" "$log"; then
        tail -60 "$log"
        echo "FAIL: $1"
        exit 1
    fi
}

# calibrate NAME EXPECTED [changes]: the small model, a rule off, must violate EXPECTED.
calibrate() {
    local name=$1 expected=$2 log="$work/$1.log"
    shift 2
    configure "$name" small "$@"
    tlc "$work/$name.cfg" "$log"
    if grep -qE "(Invariant|Action property) $expected is violated" "$log"; then
        printf '   %-13s %s violated in %s steps\n' "$name" "$expected" "$(grep -cE '^State [0-9]+:' "$log")"
    else
        tail -40 "$log"
        echo "FAIL: $name: expected $expected violated"
        exit 1
    fi
}

calibration() {
    echo "== calibrate"
    # A create-if-absent gate with nothing retained: a worker that read its
    # spec, paused, and resumes after its run was purged creates the file
    # again and writes, though the engine recorded that it wrote nothing.
    calibrate pre-create NoWriteAfterNone PreCreate=FALSE
    # A worker that writes without marking the file `writing` first: the
    # engine ends it as having written nothing.
    calibrate take-writing NoWriteAfterNone TakeWriting=FALSE
    # The engine ends an attempt with a blind PUT over the `writing` it
    # never read.
    calibrate engine-swaps NoWriteAfterNone EngineSwaps=FALSE -OneOutcome
    # Ended from `writing`, the writes are taken for none.
    calibrate classify NoWriteAfterNone Classify=FALSE
}

case "${1:-ci}" in
    ci) check small small; check dup dup; check live live; calibration ;;
    big) check big dup N=2 ;;
    all) check small small; check dup dup; check live live; calibration; check big dup N=2 ;;
    *) echo "usage: $0 [ci|big|all]" >&2; exit 2 ;;
esac
