#!/usr/bin/env bash
# Model-check the journal spec (docs/verification.md, "Formal model: the journal").
#
#   spec/tla/check-journal.sh              the small model: two engines, as built
#   spec/tla/check-journal.sh fixed        three engines, four segments
#   spec/tla/check-journal.sh big          the same, five segments
#   spec/tla/check-journal.sh live         liveness, engines one at a time
#   spec/tla/check-journal.sh calibrate    each fix put back out: TLC must find its bug
#   spec/tla/check-journal.sh ci           small, fixed, live and calibrate
#   spec/tla/check-journal.sh all          ci and big
#   spec/tla/check-journal.sh object       the journal as one object (JournalObject.tla):
#                                          two engines, three, liveness, calibration
#   spec/tla/check-journal.sh object-big   the same, three engines, six writes
#
# TLC_WORKERS (3) and TLC_HEAP (6g) bound what a run takes of a shared
# machine. Needs Java 11+. Downloads tla2tools.jar into spec/tla/.tools (gitignored).
set -euo pipefail
cd "$(dirname "$0")"

version=1.7.4
jar=.tools/tla2tools-$version.jar
if [ ! -f "$jar" ]; then
    mkdir -p .tools
    curl -fsSL -o "$jar" "https://github.com/tlaplus/tlaplus/releases/download/v$version/tla2tools.jar"
fi
module=Journal  # the spec checked: Journal, or JournalObject
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

tlc() {  # tlc CONFIG LOG: run TLC, keep its output
    java -XX:+UseParallelGC "-Xmx${TLC_HEAP:-6g}" -cp "$jar" tlc2.TLC -workers "${TLC_WORKERS:-3}" -metadir "$work/states" \
        -config "$1" "$module.tla" > "$2" 2>&1 || true
    rm -rf "$work/states"
    if [ -n "${TLA_LOGS:-}" ]; then mkdir -p "$TLA_LOGS"; cp "$2" "$TLA_LOGS/"; fi
}

# configure NAME BASE [CONSTANT=VALUE | -PROPERTY | +INVARIANT ...]: write
# BASE's model, with constants changed and properties left out or added.
configure() {
    local cfg="$work/$1.cfg" change
    cp "$module-$2.cfg" "$cfg"
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
        printf '   %-13s %s violated in %s steps\n' "$name" "$expected" "$(grep -cE '^State [0-9]+:' "$log")"
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
    # F14, F15: each half of the hole test off, then both (before 1367919). Without
    # FixF14 an opener deletes real fences, so HolesTwiceCovered, a lemma of
    # the fixed rule, fails first: it is left out to reach the lost event.
    calibrate f14 big NoAckedLoss FixF14=FALSE Unreadable=FALSE -HolesTwiceCovered
    calibrate f15 big StatesArePrefixes FixF15=FALSE
    calibrate f15-writer big FencedSeesAcked FixF15=FALSE -StatesArePrefixes
    calibrate pre-1367919 big NoAckedLoss FixF14=FALSE FixF15=FALSE Unreadable=FALSE -HolesTwiceCovered
    # Before 1367919, F14 needed only two engines once a checkpoint could be
    # unreadable: an opener deleted its fence after its successor read it.
    calibrate f14-two small CleanupCovered FixF14=FALSE FixF15=FALSE -HolesTwiceCovered
    # A create that does not know its own bytes fences out a lone engine.
    calibrate own-bytes live AppendsAlone OwnBytes=FALSE -OpensAlone
    # The design does not keep a segment an opener listed until it reads it.
    calibrate listed-stay small ListedStayUntilRead +ListedStayUntilRead
}

live() {
    check live live
}

# The journal as one object (docs/verification.md, "Formal model: the
# journal object"): its models, then each of its rules switched off in turn.
object() {
    module=JournalObject
    check object-small small
    check object-fixed big MaxWrites=5
    check object-live live
    echo "== calibrate the journal object"
    # Without the engine id in the journal, a fence can leave its bytes, so
    # its ETag, unchanged: the old engine's If-Match still holds.
    calibrate engine-id small OneWriter EngineId=FALSE
    # A refused write that does not GET the journal takes its own lost
    # answer for a newer engine's write: a lone engine stops.
    calibrate ask live AppendsAlone AskJournal=FALSE -OpensAlone
    # An opener whose checkpoint was cleaned up since it read the journal
    # gives up.
    calibrate re-get small OpensNeverFail ReGet=FALSE
    # The journal moves to a checkpoint nobody can parse: the state is lost.
    calibrate verify small NoAckedLoss Verify=FALSE
    # Cleanup that LISTs after its move deletes a newer engine's checkpoint
    # before that engine's move names it: the state is lost.
    calibrate list-first small NoAckedLoss ListFirst=FALSE
    module=Journal
}

case ${1:-small} in
    small) check small small ;;
    fixed) check fixed big MaxSeq=4 ;;
    big) check big big ;;
    live) live ;;
    calibrate) calibration ;;
    ci) check small small; check fixed big MaxSeq=4; live; calibration ;;
    all) check small small; check fixed big MaxSeq=4; live; calibration; check big big ;;
    object) object ;;
    object-big) module=JournalObject; check object-big big ;;
    *) echo "usage: $0 [small|fixed|big|live|calibrate|ci|all|object|object-big]" >&2; exit 2 ;;
esac
