#!/usr/bin/env bash
# Model-check the journal spec, the journal as one object swapped with
# If-Match (JournalObject.tla; docs/verification.md, "Formal model: the
# journal object").
#
#   spec/tla/check-journal.sh              the small model: two engines
#   spec/tla/check-journal.sh fixed        three engines, five writes
#   spec/tla/check-journal.sh big          three engines, six writes
#   spec/tla/check-journal.sh live         liveness, engines one at a time
#   spec/tla/check-journal.sh calibrate    each rule switched off: TLC must find its bug
#   spec/tla/check-journal.sh ci           small, fixed, live and calibrate
#   spec/tla/check-journal.sh all          ci and big
#
# TLC_WORKERS (2) and TLC_HEAP (4g) bound what a run takes of a shared
# machine. Needs Java 11+. Downloads tla2tools.jar into spec/tla/.tools (gitignored).
set -euo pipefail
cd "$(dirname "$0")"

version=1.7.4
jar=.tools/tla2tools-$version.jar
if [ ! -f "$jar" ]; then
    mkdir -p .tools
    curl -fsSL -o "$jar" "https://github.com/tlaplus/tlaplus/releases/download/v$version/tla2tools.jar"
fi
module=JournalObject
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

tlc() {  # tlc CONFIG LOG: run TLC, keep its output
    java -XX:+UseParallelGC "-Xmx${TLC_HEAP:-4g}" -cp "$jar" tlc2.TLC -workers "${TLC_WORKERS:-2}" -metadir "$work/states" \
        -config "$1" "$module.tla" > "$2" 2>&1 || true
    rm -rf "$work/states"
    if [ -n "${TLA_LOGS:-}" ]; then mkdir -p "$TLA_LOGS"; cp "$2" "$TLA_LOGS/"; fi
}

# configure NAME BASE [CONSTANT=VALUE | -PROPERTY | +INVARIANT ...]: write
# BASE's model, with constants changed and properties left out or added.
# edit EXPRESSION: apply a sed -E expression to $cfg in place (no sed -i,
# whose syntax differs between GNU and BSD sed).
edit() { sed -E "$1" "$cfg" > "$cfg.tmp" && mv "$cfg.tmp" "$cfg"; }

configure() {
    local cfg="$work/$1.cfg" change
    cp "$module-$2.cfg" "$cfg"
    shift 2
    for change in "$@"; do
        case $change in
            -*) edit "s/(^| )${change#-}( |$)/\1\2/" ;;
            +*) edit "s/^    TypeOK /    TypeOK ${change#+} /" ;;
            *) edit "s/^( +${change%%=*}) = .*/\1 = ${change#*=}/" ;;
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

# Each of the journal's rules switched off in turn: TLC must find its bug.
calibration() {
    echo "== calibrate"
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
}

case ${1:-small} in
    small) check small small ;;
    fixed) check fixed big MaxWrites=5 ;;
    big) check big big ;;
    live) check live live ;;
    calibrate) calibration ;;
    ci) check small small; check fixed big MaxWrites=5; check live live; calibration ;;
    all) check small small; check fixed big MaxWrites=5; check live live; calibration; check big big ;;
    *) echo "usage: $0 [small|fixed|big|live|calibrate|ci|all]" >&2; exit 2 ;;
esac
