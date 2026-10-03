#!/usr/bin/env bash
# Model-check the journal spec (docs/verification.md, "Journal spec").
#
#   spec/tla/check-journal.sh              the small model, as built (CI)
#   spec/tla/check-journal.sh big          three engines, with the candidate fixes
#   spec/tla/check-journal.sh live         liveness, engines one at a time
#   spec/tla/check-journal.sh calibrate    each fix put back out: TLC must find its bug
#   spec/tla/check-journal.sh all
#
# Needs Java 11+. Downloads tla2tools.jar into spec/tla/.tools (gitignored).
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

tlc() {  # tlc CONFIG LOG: run TLC, keep its output
    java -XX:+UseParallelGC -cp "$jar" tlc2.TLC -workers auto -metadir "$work/states" \
        -config "$1" Journal.tla > "$2" 2>&1 || true
    rm -rf "$work/states"
    if [ -n "${TLA_LOGS:-}" ]; then mkdir -p "$TLA_LOGS"; cp "$2" "$TLA_LOGS/"; fi
}

check() {  # check NAME: the model must pass
    local log="$work/$1.log"
    echo "== $1"
    tlc "Journal-$1.cfg" "$log"
    grep -E "distinct states found|^Finished in" "$log" | sed 's/^/   /'
    if ! grep -q "No error has been found" "$log"; then
        cat "$log"
        echo "FAIL: $1"
        exit 1
    fi
}

# calibrate NAME BASE EXPECTED [CONSTANT=VALUE | -INVARIANT | +INVARIANT ...]:
# BASE's model with the constants changed and invariants left out or added
# must violate EXPECTED.
calibrate() {
    local name=$1 base=$2 expected=$3
    shift 3
    local cfg="$work/$name.cfg" log="$work/$name.log"
    cp "Journal-$base.cfg" "$cfg"
    for change in "$@"; do
        case $change in
            -*) sed -i -E "s/(^| )${change#-}( |$)/\1\2/" "$cfg" ;;
            +*) sed -i -E "s/^    TypeOK /    TypeOK ${change#+} /" "$cfg" ;;
            *) sed -i -E "s/^( +${change%%=*}) = .*/\1 = ${change#*=}/" "$cfg" ;;
        esac
    done
    tlc "$cfg" "$log"
    local found
    found=$(grep -oE "Invariant [A-Za-z]+ is violated|Temporal properties were violated" "$log" | head -1 || true)
    # TLC names a violated invariant, not a temporal property: that one must
    # be the only property checked.
    if [[ $found == *" $expected "* ]] ||
        { [[ $found == Temporal* ]] && grep -qE "^PROPERTY +$expected *$" "$cfg"; }; then
        printf '   %-12s %s violated in %s steps\n' "$name" "$expected" "$(grep -cE '^State [0-9]+:' "$log")"
    else
        tail -40 "$log"
        echo "FAIL: $name: expected $expected violated, got: ${found:-no violation}"
        exit 1
    fi
}

calibration() {
    echo "== calibrate"
    # 3c23397, F7: an opener whose fence lands in a hole serves under it,
    # without the events cleanup deleted; one that meets the gap fails.
    calibrate f7 small OneWriter FixF7=FALSE -OpensNeverFail
    calibrate f7-gap small OpensNeverFail FixF7=FALSE
    # 0b3e226: cleanup deletes a fence; the old engine appends into its slot.
    calibrate 0b3e226 small NoAckedLoss KeepFences=FALSE -FencesStay -OneWriter
    # f300500: two engines fence at one seq with the same bytes; both serve.
    calibrate f300500 small OneWriter FenceNonce=FALSE
    # A create that does not know its own bytes fences out a lone engine.
    calibrate own-bytes live AppendsAlone OwnBytes=FALSE -OpensAlone
    # The design does not keep a segment an opener listed until it reads it.
    calibrate listed-stay small ListedStayUntilRead +ListedStayUntilRead
    # F14, F15: as built, three engines.
    calibrate f14 big NoAckedLoss FixF14=FALSE FixF15=FALSE
    calibrate f15 big StatesArePrefixes FixF15=FALSE
    calibrate f15-writer big FencedSeesAcked FixF15=FALSE -StatesArePrefixes
}

case ${1:-small} in
    small) check small ;;
    big) check big ;;
    live) check live ;;
    calibrate) calibration ;;
    all) check small; check big; check live; calibration ;;
    *) echo "usage: $0 [small|big|live|calibrate|all]" >&2; exit 2 ;;
esac
