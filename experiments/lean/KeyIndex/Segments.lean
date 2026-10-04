/-
# Segments in a physical span: the encoding round trip

Under the "versions" policy a physical span holds, per key, its kept
versions newest first and one predecessor (the span's). The model's
segment `i` for a key is its newest version in segment `i`, with the key's
state before segment `i` as predecessor. The encoding keeps each segment's
newest version (every version some live endpoint sees); decoding gives
segment `i` its newest version and, as predecessor, the newest kept
version of an earlier segment if live, else none if a tombstone, else the
span's predecessor.

`decode_encode`: under exact writes (each commit's entry names the key's
live generation before it), decoding the encoding gives exactly the
model's per-segment entries.
-/
import KeyIndex.Keys

namespace Segments

open Keys

/-- A version of a key: its generation and deleted flag. -/
structure Ver where
  gen : Nat
  del : Bool

/-- The live generation a version leaves, if any. -/
def live (v : Ver) : Option Nat := if v.del then none else some v.gen

/-- What a commit leaves: its version's, or the state before it. -/
def after (s : Option Nat) : Option Ver → Option Nat
  | none => s
  | some v => live v

/-- A commit's entry under exact writes: its version, naming the state before. -/
def ent (s : Option Nat) (c : Option Ver) : Option (Info Nat) :=
  c.map fun v => ⟨v.gen, v.del, s⟩

/-- The model: a segment's commits (oldest first) merged, from state `s`. -/
def segE (s : Option Nat) : List (Option Ver) → Option (Info Nat)
  | [] => none
  | c :: cs => over (segE (after s c) cs) (ent s c)

def segAfter (s : Option Nat) : List (Option Ver) → Option Nat
  | [] => s
  | c :: cs => segAfter (after s c) cs

/-- The model's entries for a span's segments, from the span's predecessor. -/
def model (s : Option Nat) : List (List (Option Ver)) → List (Option (Info Nat))
  | [] => []
  | seg :: rest => segE s seg :: model (segAfter s seg) rest

/-- A segment's newest version. -/
def newest : List (Option Ver) → Option Ver
  | [] => none
  | c :: cs => (newest cs).or c

/-- The encoding: each segment's newest version (none if it left the key alone). -/
def encode (segs : List (List (Option Ver))) : List (Option Ver) := segs.map newest

/-- Decoding, from the span's predecessor: each kept version, with the
newest earlier kept version (or the span's predecessor) as predecessor. -/
def decode (p : Option Nat) : List (Option Ver) → List (Option (Info Nat))
  | [] => []
  | e :: rest => ent p e :: decode (after p e) rest

theorem segE_newest (s : Option Nat) : ∀ cs, segE s cs = ent s (newest cs)
  | [] => rfl
  | c :: cs => by
    rw [segE, segE_newest (after s c) cs]
    cases hn : newest cs <;> cases c <;> simp [newest, hn, ent, over, comb, after]

theorem segAfter_newest (s : Option Nat) : ∀ cs, segAfter s cs = after s (newest cs)
  | [] => rfl
  | c :: cs => by
    rw [segAfter, segAfter_newest (after s c) cs]
    cases hn : newest cs <;> cases c <;> simp [newest, hn, after]

/-- **Round trip.** Decoding the per-segment newest versions, from the span's
predecessor, gives the model's segments. -/
theorem decode_encode (p : Option Nat) : ∀ segs, decode p (encode segs) = model p segs
  | [] => rfl
  | seg :: rest => by
    simp only [encode, List.map_cons, decode, model]
    rw [segE_newest, segAfter_newest]
    exact congrArg _ (decode_encode _ rest)

/-- The design's example: `k` changed in segments 1 and 3, not 2. Segment
3's predecessor is segment 1's version, kept because the endpoints starting
segments 2 and 3 both see it. -/
example :
    let segs : List (List (Option Ver)) := [[some ⟨10, false⟩], [none, none], [none, some ⟨30, false⟩]]
    decode none (encode segs) = [some ⟨10, false, none⟩, none, some ⟨30, false, some 10⟩] ∧
    model none segs = decode none (encode segs) := by
  exact ⟨rfl, (decode_encode none _).symm⟩

end Segments
