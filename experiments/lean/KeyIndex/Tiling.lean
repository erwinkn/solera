/-
# The tiling invariant

docs/key-index-design.md: the spans tile commit time from the base to the
head; a merge joins adjacent spans and never one across a boundary; every
boundary is born at the head + 1. Claim: after any sequence of commits,
boundary births and releases, and such merges, every live boundary starts a
span (or is the head + 1), and between any two boundaries P < Q the spans
tile [P, Q − 1] exactly and merge to the per-commit fold.

The model is generic in the span contents: any associative merge `mul`
(the delta merge is one: Delta.merge_assoc), and a normalisation `norm` for
the base (drop tombstones and predecessors) with `norm (norm x * y) =
norm (x * y)`. Commits are fixed in advance as `c : Nat → D`: commit `i`'s
delta, whatever happens to the spans around it.
-/

namespace Tiling

structure Alg (D : Type) where
  mul : D → D → D
  assoc : ∀ x y z, mul (mul x y) z = mul x (mul y z)
  norm : D → D
  norm_mul : ∀ x y, norm (mul (norm x) y) = norm (mul x y)

variable {D : Type} (A : Alg D) (c : Nat → D)

/-- The merge of `n + 1` commits from `a` on, oldest first. -/
def seg (a : Nat) : Nat → D
  | 0 => c a
  | n + 1 => A.mul (seg a n) (c (a + n + 1))

/-- Two adjacent ranges merge to the range they cover. -/
theorem seg_append (a n : Nat) : ∀ m,
    A.mul (seg A c a n) (seg A c (a + n + 1) m) = seg A c a (n + m + 1)
  | 0 => by simp [seg]
  | m + 1 => by
    rw [show n + (m + 1) + 1 = (n + m + 1) + 1 by omega]
    simp only [seg]
    rw [← A.assoc, seg_append a n m]
    congr 2; omega

/-- A span: commits `a .. a + n` (n + 1 of them), and its contents. -/
structure Span (D : Type) where
  a : Nat
  n : Nat
  x : D

/-- How many commits a run of spans covers. -/
def cover : List (Span D) → Nat
  | [] => 0
  | s :: r => s.n + 1 + cover r

theorem cover_append : ∀ (l1 l2 : List (Span D)), cover (l1 ++ l2) = cover l1 + cover l2
  | [], l2 => by simp [cover]
  | s :: r, l2 => by simp [cover, cover_append r l2]; omega

/-- `sp` tiles `[lo, hi)` exactly, each span holding its commits' merge. -/
def Tiles : Nat → List (Span D) → Nat → Prop
  | lo, [], hi => lo = hi
  | lo, s :: r, hi => s.a = lo ∧ s.x = seg A c s.a s.n ∧ Tiles (lo + s.n + 1) r hi

def starts (sp : List (Span D)) : List Nat := sp.map Span.a

/-- The merge of a run of spans, oldest first (none if empty). -/
def mergeAll : List (Span D) → Option D
  | [] => none
  | s :: r => match mergeAll r with
    | none => some s.x
    | some y => some (A.mul s.x y)

structure St (D : Type) where
  /-- The base covers commits `0 .. baseN`. -/
  baseN : Nat
  base : D
  /-- The spans after it, oldest first, up to `head`. -/
  spans : List (Span D)
  head : Nat
  bnds : List Nat

inductive Step : St D → St D → Prop
  /-- Commit `head + 1` writes its delta as a span of its own. -/
  | commit (s : St D) :
      Step s { s with spans := s.spans ++ [⟨s.head + 1, 0, c (s.head + 1)⟩], head := s.head + 1 }
  /-- An observer is born at the head + 1: a position, a pass's landing
  point, an attempt's reservation at its claim (selections included). -/
  | birth (s : St D) : Step s { s with bnds := (s.head + 1) :: s.bnds }
  /-- Observers go: any subset of the boundaries survives. -/
  | release (s : St D) (keep : List Nat) (h : ∀ β ∈ keep, β ∈ s.bnds) :
      Step s { s with bnds := keep }
  /-- A merge of two or more adjacent spans `f :: g :: rest`, with no
  boundary at the start of any but the first. -/
  | merge (s : St D) (pre post rest : List (Span D)) (f g : Span D) (y : D)
      (hsp : s.spans = pre ++ (f :: g :: rest) ++ post)
      (hb : ∀ β ∈ s.bnds, β ∉ starts (g :: rest))
      (hy : mergeAll A (f :: g :: rest) = some y) :
      Step s { s with spans := pre ++ ⟨f.a, cover (f :: g :: rest) - 1, y⟩ :: post }
  /-- The oldest spans merge into the base, none of them starting at a
  boundary; the base drops tombstones and predecessors. -/
  | base (s : St D) (ins post : List (Span D)) (y : D)
      (hsp : s.spans = ins ++ post)
      (hb : ∀ β ∈ s.bnds, β ∉ starts ins)
      (hy : mergeAll A ins = some y) :
      Step s { s with baseN := s.baseN + cover ins, base := A.norm (A.mul s.base y), spans := post }

/-- The index right after commit 0, which is the base. -/
def init : St D := ⟨0, A.norm (c 0), [], 0, []⟩

inductive Reach : St D → Prop
  | init : Reach (init A c)
  | step {s t : St D} : Reach s → Step A c s t → Reach t

/-- The invariant: the base holds its commits' merge, normalised; the spans
tile the rest up to the head; every boundary starts a span or is the head + 1. -/
def Inv (s : St D) : Prop :=
  s.base = A.norm (seg A c 0 s.baseN) ∧
  Tiles A c (s.baseN + 1) s.spans (s.head + 1) ∧
  ∀ β ∈ s.bnds, β ∈ starts s.spans ∨ β = s.head + 1

/-! ## Tilings -/

theorem tiles_hi : ∀ {lo hi : Nat} {l : List (Span D)}, Tiles A c lo l hi → hi = lo + cover l
  | _, _, [], h => by simp [Tiles, cover] at *; omega
  | lo, hi, s :: r, ⟨_, _, h⟩ => by have := tiles_hi h; simp [cover]; omega

theorem tiles_append : ∀ {lo hi : Nat} (l1 l2 : List (Span D)),
    Tiles A c lo (l1 ++ l2) hi ↔ Tiles A c lo l1 (lo + cover l1) ∧ Tiles A c (lo + cover l1) l2 hi
  | lo, hi, [], l2 => by simp [Tiles, cover]
  | lo, hi, s :: r, l2 => by
    simp only [List.cons_append, Tiles, cover]
    rw [tiles_append r l2]
    have e : lo + s.n + 1 + cover r = lo + (s.n + 1 + cover r) := by omega
    rw [e]; constructor
    · rintro ⟨h1, h2, h3, h4⟩; exact ⟨⟨h1, h2, h3⟩, h4⟩
    · rintro ⟨⟨h1, h2, h3⟩, h4⟩; exact ⟨h1, h2, h3, h4⟩

/-- A nonempty run tiling `[lo, hi)` merges to the merge of those commits. -/
theorem mergeAll_tiles : ∀ {lo hi : Nat} {l : List (Span D)}, l ≠ [] → Tiles A c lo l hi →
    mergeAll A l = some (seg A c lo (cover l - 1))
  | _, _, [], h, _ => absurd rfl h
  | lo, hi, [s], _, ⟨ha, hx, _⟩ => by simp [mergeAll, cover, hx, ha]
  | lo, hi, s :: t :: r, _, ⟨ha, hx, h⟩ => by
    have ih := mergeAll_tiles (List.cons_ne_nil t r) h
    simp only [mergeAll] at ih ⊢
    rw [ih]; simp only [hx, ha]
    have := seg_append A c lo s.n (cover (t :: r) - 1)
    have hc : cover (t :: r) ≥ 1 := by simp only [cover]; omega
    rw [this]; congr 2; simp only [cover]; omega

/-- A tiling's starts lie in `[lo, hi)`. -/
theorem tiles_starts : ∀ {lo hi : Nat} {l : List (Span D)}, Tiles A c lo l hi →
    ∀ x ∈ starts l, lo ≤ x ∧ x < hi
  | _, _, [], _, x, hx => by simp [starts] at hx
  | lo, hi, s :: r, ⟨ha, _, h⟩, x, hx => by
    have hhi := tiles_hi A c h
    simp only [starts, List.map_cons, List.mem_cons] at hx
    rcases hx with hx | hx
    · omega
    · have := tiles_starts h x hx; omega

/-- Cut a tiling at one of its starts (or its end). -/
theorem tiles_split : ∀ {lo hi p : Nat} {l : List (Span D)}, Tiles A c lo l hi →
    (p ∈ starts l ∨ p = hi) → ∃ l1 l2, l = l1 ++ l2 ∧ Tiles A c lo l1 p ∧ Tiles A c p l2 hi
  | lo, hi, p, [], h, hp => by
    simp [starts, Tiles] at hp h; subst hp; exact ⟨[], [], by simp, by simp [Tiles, h], by simp [Tiles]⟩
  | lo, hi, p, s :: r, ⟨ha, hx, h⟩, hp => by
    by_cases hps : p = s.a
    · exact ⟨[], s :: r, by simp, by simp [Tiles, hps, ha], by rw [hps, ha]; exact ⟨ha, hx, h⟩⟩
    · have hp' : p ∈ starts r ∨ p = hi := by
        simp [starts] at hp ⊢; rcases hp with (h1 | h1) | h1
        · exact absurd h1 hps
        · exact Or.inl h1
        · exact Or.inr h1
      obtain ⟨l1, l2, e, h1, h2⟩ := tiles_split h hp'
      exact ⟨s :: l1, l2, by simp [e], ⟨ha, hx, h1⟩, h2⟩

/-! ## The invariant holds -/

theorem init_inv : Inv A c (init A c) := by
  simp [Inv, init, seg, Tiles]

theorem step_inv {s t : St D} (h : Step A c s t) (hs : Inv A c s) : Inv A c t := by
  obtain ⟨hbase, htile, hbnd⟩ := hs
  cases h with
  | commit =>
    refine ⟨hbase, ?_, ?_⟩
    · rw [tiles_append]
      have := tiles_hi A c htile
      refine ⟨by rw [← this]; exact htile, ?_⟩
      simp [Tiles, seg, cover]; omega
    · intro β hβ
      rcases hbnd β hβ with h1 | h1
      · left; simp [starts] at h1 ⊢; exact Or.inl h1
      · left; simp [starts, h1]
  | birth =>
    refine ⟨hbase, htile, ?_⟩
    intro β hβ
    simp at hβ; rcases hβ with h1 | h1
    · right; exact h1
    · exact hbnd β h1
  | release keep hk =>
    exact ⟨hbase, htile, fun β hβ => hbnd β (hk β hβ)⟩
  | merge pre post rest f g y hsp hb hy =>
    rw [hsp] at htile hbnd
    rw [tiles_append, tiles_append] at htile
    obtain ⟨⟨hpre, hrun⟩, hpost⟩ := htile
    rw [cover_append] at hpost hrun
    have hy' := mergeAll_tiles A c (List.cons_ne_nil f (g :: rest)) hrun
    rw [hy] at hy'; injection hy' with hy'
    have hfa : f.a = s.baseN + 1 + cover pre := hrun.1
    refine ⟨hbase, ?_, ?_⟩
    · rw [tiles_append]
      refine ⟨hpre, ?_⟩
      have hc : cover (f :: g :: rest) ≥ 1 := by simp only [cover]; omega
      refine ⟨hfa, by rw [hy', hfa], ?_⟩
      have e : s.baseN + 1 + cover pre + (cover (f :: g :: rest) - 1) + 1 =
          s.baseN + 1 + cover pre + cover (f :: g :: rest) := by omega
      simp only [Nat.add_assoc] at e hpost ⊢; rw [e]; exact hpost
    · intro β hβ
      have hβ' := hb β hβ
      rcases hbnd β hβ with h1 | h1
      · left
        simp only [starts, List.map_append, List.map_cons, List.mem_append, List.mem_cons] at h1 hβ' ⊢
        rcases h1 with (h1 | h1 | h1 | h1) | h1
        · exact Or.inl h1
        · exact Or.inr (Or.inl h1)
        · exact absurd (Or.inl h1) hβ'
        · exact absurd (Or.inr h1) hβ'
        · exact Or.inr (Or.inr h1)
      · right; exact h1
  | base ins post y hsp hb hy =>
    rw [hsp] at htile hbnd
    rw [tiles_append] at htile
    obtain ⟨hins, hpost⟩ := htile
    have hne : ins ≠ [] := by intro e; subst e; simp [mergeAll] at hy
    have hy' := mergeAll_tiles A c hne hins
    rw [hy] at hy'; injection hy' with hy'
    have hc : cover ins ≥ 1 := by
      cases ins with
      | nil => exact absurd rfl hne
      | cons t r => simp only [cover]; omega
    refine ⟨?_, ?_, ?_⟩
    · simp only
      have hs := seg_append A c 0 s.baseN (cover ins - 1)
      rw [Nat.zero_add] at hs
      rw [hbase, A.norm_mul, hy', hs]
      congr 2; omega
    · simp only
      have e : s.baseN + cover ins + 1 = s.baseN + 1 + cover ins := by omega
      rw [e]; exact hpost
    · intro β hβ
      rcases hbnd β hβ with h1 | h1
      · left
        simp only [starts, List.map_append, List.mem_append] at h1 ⊢
        rcases h1 with h1 | h1
        · exact absurd h1 (hb β hβ)
        · exact h1
      · right; exact h1

theorem reach_inv {s : St D} (h : Reach A c s) : Inv A c s := by
  induction h with
  | init => exact init_inv A c
  | step _ hst ih => exact step_inv A c hst ih

/-! ## What the invariant gives readers -/

/-- **Tiling.** In every reachable state, between two boundaries `P < Q`
(or the head + 1 for `Q`), a run of consecutive spans tiles `[P, Q − 1]`
exactly, and merging it gives the merge of commits `P .. Q − 1`, as if
they had never been compacted. -/
theorem catch_up {s : St D} (h : Reach A c s) {P Q : Nat}
    (hP : P ∈ s.bnds) (hQ : Q ∈ s.bnds ∨ Q = s.head + 1) (hPQ : P < Q) :
    ∃ pre mid post, s.spans = pre ++ mid ++ post ∧
      mergeAll A mid = some (seg A c P (Q - P - 1)) := by
  obtain ⟨_, htile, hbnd⟩ := reach_inv A c h
  have hQ' : Q ∈ starts s.spans ∨ Q = s.head + 1 := by
    rcases hQ with hQ | hQ
    · exact hbnd Q hQ
    · exact Or.inr hQ
  obtain ⟨l1, l2, e1, h1, h2⟩ := tiles_split A c htile (hbnd P hP)
  have hQ2 : Q ∈ starts l2 ∨ Q = s.head + 1 := by
    rcases hQ' with hq | hq
    · left
      rw [e1] at hq
      simp only [starts, List.map_append, List.mem_append] at hq
      rcases hq with hq | hq
      · -- every start before P is below P, and Q > P
        have := tiles_starts A c h1 Q hq; omega
      · exact hq
    · exact Or.inr hq
  obtain ⟨m1, m2, e2, h3, _⟩ := tiles_split A c h2 hQ2
  have hne : m1 ≠ [] := by
    intro e; subst e; simp [Tiles] at h3; omega
  refine ⟨l1, m1, m2, by rw [e1, e2, List.append_assoc], ?_⟩
  rw [mergeAll_tiles A c hne h3]
  have := tiles_hi A c h3
  congr 2; omega

end Tiling
