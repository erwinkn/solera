#!/usr/bin/env bash
# Model-check the journal spec (docs/verification.md, "Formal model: the journal").
#
#   spec/tla/check-journal.sh              the small model, as built
#   spec/tla/check-journal.sh fixed        three engines, four segments, with the F14 and F15 fix
#   spec/tla/check-journal.sh big          the same, five segments
#   spec/tla/check-journal.sh live         liveness, engines one at a time
#   spec/tla/check-journal.sh calibrate    each fix put back out: TLC must find its bug
#   spec/tla/check-journal.sh ci           small, fixed, live and calibrate
#   spec/tla/check-journal.sh all          ci and big
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
    java -XX:+UseParallelGC ${TLC_JAVA_OPTS:-} -cp "$jar" tlc2.TLC -workers auto -metadir "$work/states" \
        -config "$1" Journal.tla > "$2" 2>&1 || true
    rm -rf "$work/states"
    if [ -n "${TLA_LOGS:-}" ]; then mkdir -p "$TLA_LOGS"; cp "$2" "$TLA_LOGS/"; fi
}

# configure NAME BASE [CONSTANT=VALUE | -PROPERTY | +INVARIANT ...]: write
# BASE's model, with constants changed and properties left out or added.
configure() {
    local cfg="$work/$1.cfg" change
    cp "Journal-$2.cfg" "$cfg"
    shift 2
    for change in "$@"; do
        case $change in
            -*) sed -i -E "s/(^| )${change#-}( |$)/\1\2/" "$cfg" ;;
            +*) sed -i -E "s/^    TypeOK /    TypeOK ${change#+} /" "$cfg" ;;
            *) sed -i -E "s/^( +${change%%=*}) = .*/\1 = ${change#*=}/" "$cfg" ;;
        esac
    done
}

# check NAME BASE [changes]: the model must pass.
check() {
    local name=$1 log="$work/$1.log"
    configure "$@"
    echo "== $name"
    tlc "$work/$name.cfg" "$log"
    grep -E "distinct states found|depth of the complete|^Finished in" "$log" | sed 's/^/   /'
    if ! grep -q "No error has been found" "$log"; then
        tail -60 "$log"
        echo "FAIL: $name"
        exit 1
    fi
}

# calibrate NAME BASE EXPECTED [changes]: the model must violate EXPECTED.
calibrate() {
    local name=$1 base=$2 expected=$3
    shift 3
    local cfg="$work/$name.cfg" log="$work/$name.log"
    configure "$name" "$base" "$@"
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

# Every journal fix switched off in turn, on the rule with the F14 and F15
# fixes (Journal-big.cfg): TLC must find each bug again.
calibration() {
    echo "== calibrate"
    # 3c23397, F7: an opener whose fence lands in a hole serves under it,
    # without the events cleanup deleted; one whose GET finds the segment
    # gone fails.
    calibrate f7 big OneWriter FixF7=FALSE -OpensNeverFail
    calibrate f7-gap big OpensNeverFail FixF7=FALSE
    # 0b3e226: cleanup deletes a fence; the old engine appends into its slot.
    calibrate 0b3e226 big NoAckedLoss KeepFences=FALSE -FencesStay -OneWriter
    # f300500: two engines fence at one seq with the same bytes; both serve.
    calibrate f300500 big OneWriter FenceNonce=FALSE
    # F14, F15: each half of the hole test off, then both (as built). Without
    # FixF14 an opener deletes real fences, so HolesTwiceCovered, a lemma of
    # the fixed rule, fails first: it is left out to reach the lost event.
    calibrate f14 big NoAckedLoss FixF14=FALSE Unreadable=FALSE -HolesTwiceCovered
    calibrate f15 big StatesArePrefixes FixF15=FALSE
    calibrate f15-writer big FencedSeesAcked FixF15=FALSE -StatesArePrefixes
    calibrate as-built big NoAckedLoss FixF14=FALSE FixF15=FALSE Unreadable=FALSE -HolesTwiceCovered
    # As built, F14 needs only two engines once a checkpoint can be
    # unreadable: an opener deletes its fence after its successor read it.
    calibrate f14-two small CleanupCovered Unreadable=TRUE
    # A create that does not know its own bytes fences out a lone engine.
    calibrate own-bytes live AppendsAlone OwnBytes=FALSE -OpensAlone
    # The design does not keep a segment an opener listed until it reads it.
    calibrate listed-stay small ListedStayUntilRead +ListedStayUntilRead
}

live() {
    check live live
    check live-fixed live FixF14=TRUE FixF15=TRUE
}

case ${1:-small} in
    small) check small small ;;
    fixed) check fixed big MaxSeq=4 ;;
    big) check big big ;;
    live) live ;;
    calibrate) calibration ;;
    ci) check small small; check fixed big MaxSeq=4; live; calibration ;;
    all) check small small; check fixed big MaxSeq=4; live; calibration; check big big ;;
    *) echo "usage: $0 [small|fixed|big|live|calibrate|ci|all]" >&2; exit 2 ;;
esac
