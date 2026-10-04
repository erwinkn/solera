/-
# The second review's reader rules (A12), with its counterexamples

A12-1: only the base's initial segment, before the oldest live endpoint,
drops tombstones and predecessors; later segments keep tombstones of keys
added then removed. A12-2: changes(P, N) is clipped to its pinned N: per
key, the newest version older than g(N + 1), and keys not touched in
[P, N] are left out.
-/
import KeyIndex.Keys

namespace Review

open Keys

/-! ## A12-1 -/

/-- Every live boundary lies past the base, so normalising the base (the
initial segment) never touches anything a catch-up reads. With
`lookup_head` (lookups don't notice it), this is why A12-1 is safe. -/
theorem boundaries_after_base {D : Type} (A : Tiling.Alg D) (c : Nat → D) {s : Tiling.St D}
    (h : Tiling.Reach A c s) : ∀ β ∈ s.bnds, s.baseN < β := by
  obtain ⟨_, htile, hbnd⟩ := Tiling.reach_inv A c h
  have hhi := Tiling.tiles_hi A c htile
  intro β hβ
  rcases hbnd β hβ with h1 | h1
  · have := Tiling.tiles_starts A c htile β h1; omega
  · omega

/-- The review's first case. A consumer sits at endpoint 1; `d` is added at
commit 1 (generation 20) and delivered live by a selection reading at 1;
commit 2 (generation 30) removes it. A base merge crosses endpoint 1, so
the segment after it, commits 1 to 2, holds `d`'s tombstone with no
predecessor. If the merge normalised that segment too, `d` would vanish
and the consumer's read-ahead would skip it, keeping `d` forever; kept, the
tombstone (generation 30 > 20) gives "removed", the truth. -/
theorem a12_1_counterexample :
    let commit1 : Option (Info Nat) := some ⟨20, false, none⟩
    let commit2 : Option (Info Nat) := some ⟨30, true, some 20⟩
    let seg := over commit2 commit1          -- the segment [1, 2]
    let readAhead (e : Option (Info Nat)) : Option Class :=
      match e with
      | none => none                          -- skip
      | some i => if i.gen ≤ 20 then none else some (classify true (!i.del))
    seg = some ⟨30, true, none⟩ ∧
    readAhead (normE seg) = none ∧            -- normalised: skipped, d kept forever
    readAhead seg = some .removed ∧           -- A12-1: removed
    classify true false = .removed := by      -- the truth: live at 1, absent at 2
  exact ⟨rfl, rfl, rfl, rfl⟩

/-! ## A12-2 -/

/-- The newest of a key's kept versions in `[lo, hi)`, by generation. -/
def newestIn (vs : List (Info Nat)) (lo hi : Nat) : Option (Info Nat) :=
  vs.foldl (fun acc v => if lo ≤ v.gen && v.gen < hi then
    match acc with
    | some a => if v.gen > a.gen then some v else acc
    | none => some v
    else acc) none

/-- The review's second case. `k` is added at commit 1 (generation 20) and
removed at commit 2 (generation 30); a pass pinned at N = 1 reserved its
landing point 2, so the span [1, 2] keeps one version per segment, [1] and
[2]. Clipped to [g(1), g(2)) = [20, 30), changes(1, 1) says "added", the
truth at N = 1. Unclipped, the span's newest version says "nothing". -/
theorem a12_2_counterexample :
    let kept : List (Info Nat) := [⟨30, true, some 20⟩, ⟨20, false, none⟩]  -- newest first
    let before := (newestIn kept 0 20).isSome                     -- not live before P
    let clipped := (newestIn kept 20 30).map fun v => classify before (!v.del)
    let unclipped := (newestIn kept 20 1000).map fun v => classify before (!v.del)
    clipped = some .added ∧                   -- A12-2
    unclipped = some .nothing ∧               -- the gap
    classify false true = .added := by        -- the truth: absent before 1, live at 1
  exact ⟨rfl, rfl, rfl⟩

end Review
