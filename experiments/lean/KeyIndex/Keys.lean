/-
# Per key: what a span says of one key, lookups, and the read-ahead rule

A span's entry for a key: its newest generation and deleted flag, and the
key's predecessor before the span (none if the key was not live then). A
span's meaning is the function from keys to entries (`Sem`); the merge of
an older span with a newer one keeps, per key, the newer entry's state and
the older entry's predecessor. `Delta.lean` shows the sorted-list merge
has exactly this meaning, key by key.
-/
import KeyIndex.Tiling

namespace Keys

structure Info (G : Type) where
  gen : G
  del : Bool
  pred : Option G

/-- The older entry's predecessor, the newer entry's state. -/
def comb (o n : Info G) : Info G := { n with pred := o.pred }

/-- A key's newer entry `e` over its older one `m`. -/
def over : Option (Info G) → Option (Info G) → Option (Info G)
  | none, m => m
  | some i, none => some i
  | some i, some j => some (comb j i)

theorem over_assoc (a b c : Option (Info G)) : over a (over b c) = over (over a b) c := by
  cases a <;> cases b <;> cases c <;> rfl

/-- What the base keeps of an entry: live keys, no predecessor. -/
def normE : Option (Info G) → Option (Info G)
  | some i => if i.del then none else some { i with pred := none }
  | none => none

theorem normE_over (e m : Option (Info G)) : normE (over e (normE m)) = normE (over e m) := by
  cases e with
  | none => cases m with
    | none => rfl
    | some j => cases hj : j.del <;> simp [normE, over, hj]
  | some i => cases m with
    | none => rfl
    | some j =>
      cases hj : j.del <;> cases hi : i.del <;> simp [normE, over, comb, hi, hj]

/-- A span's meaning: each key's entry. -/
abbrev Sem (G : Type) := Nat → Option (Info G)

/-- Spans merge key by key, older first. -/
def alg (G : Type) : Tiling.Alg (Sem G) where
  mul x y := fun k => over (y k) (x k)
  assoc x y z := by funext k; exact over_assoc (z k) (y k) (x k)
  norm x := fun k => normE (x k)
  norm_mul x y := by funext k; exact normE_over (y k) (x k)

/-! ## Lookups at the head -/

/-- What a reader sees of an entry: the key's generation if it is live. -/
def live : Option (Info G) → Option G
  | some i => if i.del then none else some i.gen
  | none => none

/-- An entry's state, without its predecessor. -/
def st (e : Option (Info G)) : Option (G × Bool) := e.map fun i => (i.gen, i.del)

theorem st_over (e m : Option (Info G)) : st (over e m) = (st e).or (st m) := by
  cases e <;> cases m <;> rfl

theorem live_of_st {e f : Option (Info G)} (h : st e = st f) : live e = live f := by
  cases e <;> cases f <;> simp_all [st, live]

theorem live_normE (e : Option (Info G)) : live (normE e) = live e := by
  cases e with
  | none => rfl
  | some i => cases h : i.del <;> simp [normE, live, h]

/-- The newest spans' answer, else the base's: the same live view as the
merge, since the base only dropped what was not live. -/
theorem live_or_normE (f y m : Option (Info G)) (h : st f = st y) :
    live (f.or (normE m)) = live (over y m) := by
  cases f with
  | none =>
    cases y with
    | none => simpa [over] using live_normE m
    | some _ => simp [st] at h
  | some a =>
    cases y with
    | none => simp [st] at h
    | some b =>
      simp [st] at h
      cases m <;> simp [over, comb, live, h.1, h.2]

/-- The lookup at the head: the spans newest first, the first that holds
the key answers; else the base. -/
def lookupHead (s : Tiling.St (Sem G)) (k : Nat) : Option (Info G) :=
  (s.spans.reverse.findSome? (fun sp => sp.x k)).or (s.base k)

theorem mergeAll_some : ∀ (l : List (Tiling.Span (Sem G))), l ≠ [] →
    ∃ y, Tiling.mergeAll (alg G) l = some y
  | [], h => absurd rfl h
  | s :: r, _ => by
    rw [Tiling.mergeAll.eq_2]
    cases Tiling.mergeAll (alg G) r <;> simp

/-- Searching a run newest first finds the state its merge holds. -/
theorem findSome_mergeAll : ∀ (l : List (Tiling.Span (Sem G))) (y : Sem G) (k : Nat),
    Tiling.mergeAll (alg G) l = some y →
    st (l.reverse.findSome? (fun sp => sp.x k)) = st (y k)
  | [], _, _, h => by simp [Tiling.mergeAll] at h
  | [s], y, k, h => by
    have e : Tiling.mergeAll (alg G) [s] = some s.x := rfl
    rw [e] at h; injection h with h; subst h
    simp only [List.reverse_cons, List.reverse_nil, List.nil_append]
    cases hx : s.x k <;> simp [List.findSome?, hx]
  | s :: t :: r, y, k, h => by
    obtain ⟨y', hm⟩ := mergeAll_some (t :: r) (List.cons_ne_nil t r)
    have e : Tiling.mergeAll (alg G) (s :: t :: r) = some ((alg G).mul s.x y') := by
      rw [Tiling.mergeAll.eq_2, hm]
    rw [e] at h; injection h with h; subst h
    have ih := findSome_mergeAll (t :: r) y' k hm
    rw [List.reverse_cons, List.findSome?_append]
    show st ((List.findSome? _ (t :: r).reverse).or (List.findSome? _ [s])) = st (over (y' k) (s.x k))
    rw [st_over, ← ih]
    cases h1 : List.findSome? (fun sp => sp.x k) (t :: r).reverse <;>
      cases h2 : s.x k <;> simp [List.findSome?, h2, st]

/-- **Lookup.** In every reachable state, the newest-first lookup at the
head sees exactly what the merge of every commit since the first says:
the key's generation when live, nothing otherwise. -/
theorem lookup_head (c : Nat → Sem G) {s : Tiling.St (Sem G)} (h : Tiling.Reach (alg G) c s)
    (k : Nat) : live (lookupHead s k) = live (Tiling.seg (alg G) c 0 s.head k) := by
  obtain ⟨hbase, htile, _⟩ := Tiling.reach_inv (alg G) c h
  have hhi := Tiling.tiles_hi (alg G) c htile
  unfold lookupHead
  cases hsp : s.spans with
  | nil =>
    rw [hsp] at hhi; simp [Tiling.cover] at hhi
    rw [hbase]; simp only [List.reverse_nil, List.findSome?_nil, Option.none_or]
    rw [hhi]; exact live_normE _
  | cons t r =>
    rw [hsp] at htile hhi
    have hm := Tiling.mergeAll_tiles (alg G) c (List.cons_ne_nil t r) htile
    have hf := findSome_mergeAll (t :: r) _ k hm
    have hc : Tiling.cover (t :: r) ≥ 1 := by simp only [Tiling.cover]; omega
    have hseg := Tiling.seg_append (alg G) c 0 s.baseN (Tiling.cover (t :: r) - 1)
    rw [Nat.zero_add, show s.baseN + (Tiling.cover (t :: r) - 1) + 1 = s.head by omega] at hseg
    rw [← hseg, hbase]
    exact live_or_normE _ _ _ hf

/-! ## The read-ahead rule

One key, commits `P .. N` of a consumer's catch-up. Commit `j` writes the
key's entry `e j` (none if it does not touch the key), at generation
`g j`, and generations rise with commit numbers. `pr j` is the key's
presence after commit `j`. A keys= run read the key at commit `r`
(`P ≤ r ≤ N`) and delivered its presence then, `pr r` (the committed
attempt's sealed result). -/

inductive Class where
  | added | updated | removed | nothing
  deriving DecidableEq, Repr

/-- The change of a presence from `b` to `a`. -/
def classify : Bool → Bool → Class
  | false, true => .added
  | true, true => .updated
  | true, false => .removed
  | false, false => .nothing

section ReadAhead

variable (g : Nat → Nat) (e : Nat → Option (Info Nat)) (pr : Nat → Bool)

/-- The merge of commits `P .. P + n` at this key: the spans' answer,
however they were compacted (`Tiling.catch_up`). -/
def range (P : Nat) : Nat → Option (Info Nat)
  | 0 => e P
  | n + 1 => over (e (P + n + 1)) (range P n)

/-- The rule: no entry, or one no newer than what was read, is skipped;
otherwise the class runs from the delivered state to the newest entry. -/
def rule (P r N : Nat) : Option Class :=
  match range e P (N - P) with
  | none => none
  | some i => if i.gen ≤ g r then none else some (classify (pr r) (!i.del))

/-- The truth: changed after `r` exactly when some commit in `(r, N]`
touched the key, and then the class is its presence change from `r` to `N`. -/
def truth (r N : Nat) : Prop :=
  (∃ j, r < j ∧ j ≤ N ∧ (e j).isSome) 

/-- The range's entry is its last touching commit's state (or none). -/
theorem range_last (P : Nat) : ∀ n,
    (range e P n = none ∧ ∀ j, P ≤ j → j ≤ P + n → e j = none) ∨
    (∃ j i, P ≤ j ∧ j ≤ P + n ∧ e j = some i ∧
      (∃ i', range e P n = some i' ∧ i'.gen = i.gen ∧ i'.del = i.del) ∧
      ∀ j', j < j' → j' ≤ P + n → e j' = none)
  | 0 => by
    cases h : e P with
    | none => left; refine ⟨by simp [range, h], fun j h1 h2 => ?_⟩; rw [show j = P by omega]; exact h
    | some i => right; exact ⟨P, i, by omega, by omega, h, ⟨i, by simp [range, h]⟩, fun j' h1 h2 => by omega⟩
  | n + 1 => by
    cases h : e (P + n + 1) with
    | some i =>
      right
      refine ⟨P + n + 1, i, by omega, by omega, h, ?_, fun j' h1 h2 => by omega⟩
      cases hr : range e P n <;> simp [range, h, hr, over, comb]
    | none =>
      have hr : range e P (n + 1) = range e P n := by simp [range, h, over]
      rcases range_last P n with ⟨h1, h2⟩ | ⟨j, i, h1, h2, h3, h4, h5⟩
      · left
        refine ⟨by rw [hr, h1], fun j hj1 hj2 => ?_⟩
        by_cases hj : j = P + n + 1
        · rw [hj]; exact h
        · exact h2 j hj1 (by omega)
      · right
        refine ⟨j, i, h1, by omega, h3, by rw [hr]; exact h4, fun j' hj1 hj2 => ?_⟩
        by_cases hj : j' = P + n + 1
        · rw [hj]; exact h
        · exact h5 j' hj1 (by omega)

/-- Untouched commits keep the presence. -/
theorem pr_const (hpr : ∀ j, e (j + 1) = none → pr (j + 1) = pr j) (j : Nat) :
    ∀ n, (∀ j', j < j' → j' ≤ j + n → e j' = none) → pr (j + n) = pr j
  | 0, _ => rfl
  | n + 1, h => by
    have := pr_const hpr j n (fun j' h1 h2 => h j' h1 (by omega))
    rw [← Nat.add_assoc, hpr _ (h _ (by omega) (by omega)), this]

open Classical in
/-- **Read-ahead.** With generations strictly increasing and every commit
writing at its own generation, the rule skips the key exactly when no
commit after `r` touched it, and otherwise delivers the change of its
presence from `r` to `N`. -/
theorem read_ahead
    (hg : ∀ i j, i < j → g i < g j)
    (hgen : ∀ j i, e j = some i → i.gen = g j)
    (hlive : ∀ j i, e j = some i → pr j = !i.del)
    (hpr : ∀ j, e (j + 1) = none → pr (j + 1) = pr j)
    (P r N : Nat) (hPr : P ≤ r) (hrN : r ≤ N) :
    rule g e pr P r N =
      if truth e r N then some (classify (pr r) (pr N)) else none := by
  unfold rule truth
  rcases range_last e P (N - P) with ⟨h1, h2⟩ | ⟨j, i, h1, h2, h3, ⟨i', h4, h5, h6⟩, h7⟩
  · -- No entry: nothing touched the key since P.
    rw [h1]
    have : ¬ ∃ j, r < j ∧ j ≤ N ∧ (e j).isSome := by
      rintro ⟨j, hj1, hj2, hj3⟩
      rw [h2 j (by omega) (by omega)] at hj3; simp at hj3
    simp [this]
  · rw [h4]; simp only
    have hgj := hgen j i h3
    have hN : pr N = !i.del := by
      have := pr_const e pr hpr j (N - j) (fun j' a b => h7 j' a (by omega))
      rw [show j + (N - j) = N by omega] at this
      rw [this, hlive j i h3]
    by_cases hjr : j ≤ r
    · -- The last touch is no later than the read: skipped, and rightly.
      have hle : i'.gen ≤ g r := by
        rw [h5, hgj]
        rcases Nat.lt_or_eq_of_le hjr with h | h
        · exact Nat.le_of_lt (hg j r h)
        · rw [h]; exact Nat.le_refl _
      have : ¬ ∃ j', r < j' ∧ j' ≤ N ∧ (e j').isSome := by
        rintro ⟨j', hj1, hj2, hj3⟩
        rw [h7 j' (by omega) (by omega)] at hj3; simp at hj3
      simp [hle, this]
    · -- Touched after the read: delivered, from the delivered state to now.
      have hgt : ¬ i'.gen ≤ g r := by rw [h5, hgj]; have := hg r j (by omega); omega
      have : ∃ j', r < j' ∧ j' ≤ N ∧ (e j').isSome := ⟨j, by omega, by omega, by simp [h3]⟩
      simp [hgt, this, hN, h6]

end ReadAhead

/-- What the spans' merge says of a key is that key's `range`. -/
theorem seg_at (c : Nat → Sem Nat) (k P : Nat) : ∀ n,
    Tiling.seg (alg Nat) c P n k = range (fun j => c j k) P n
  | 0 => rfl
  | n + 1 => by
    show over (c (P + n + 1) k) (Tiling.seg (alg Nat) c P n k) = _
    rw [seg_at c k P n]; rfl

open Classical in
/-- **Read-ahead, on the spans.** In any reachable state, a consumer at
boundary `P` catching up to `N` (with `N + 1` a boundary or the head + 1)
merges a run of consecutive spans, and the rule applied to that run's
entry for a key read ahead at `r` gives the truth. -/
theorem read_ahead_spans (g : Nat → Nat) (c : Nat → Sem Nat) (pr : Nat → Bool) (k : Nat)
    (hg : ∀ i j, i < j → g i < g j)
    (hgen : ∀ j i, c j k = some i → i.gen = g j)
    (hlive : ∀ j i, c j k = some i → pr j = !i.del)
    (hpr : ∀ j, c (j + 1) k = none → pr (j + 1) = pr j)
    {s : Tiling.St (Sem Nat)} (h : Tiling.Reach (alg Nat) c s)
    {P r N : Nat} (hP : P ∈ s.bnds) (hN : N + 1 ∈ s.bnds ∨ N + 1 = s.head + 1)
    (hPr : P ≤ r) (hrN : r ≤ N) :
    ∃ pre mid post y, s.spans = pre ++ mid ++ post ∧ Tiling.mergeAll (alg Nat) mid = some y ∧
      (match y k with
       | none => none
       | some i => if i.gen ≤ g r then none else some (classify (pr r) (!i.del))) =
      if truth (fun j => c j k) r N then some (classify (pr r) (pr N)) else none := by
  obtain ⟨pre, mid, post, e, hm⟩ := Tiling.catch_up (alg Nat) c h hP hN (by omega)
  refine ⟨pre, mid, post, _, e, hm, ?_⟩
  have := read_ahead g (fun j => c j k) pr hg hgen hlive hpr P r N hPr hrN
  unfold rule at this
  rw [seg_at, show N + 1 - P - 1 = N - P by omega]
  exact this

/-! ### Why entries added and removed inside a span must stay

A merge that dropped them ("neither": no predecessor, deleted) breaks the
rule. The design's worked example: consumer Y at position 3; a keys= run
reads `d` at commit 4, live (added at generation 40); commit 5 removes it
(generation 50). Merged with the drop, span `[3, 5]` has no entry for `d`,
so Y skips it and keeps `d` forever; the truth is "removed". -/

/-- The merge if it dropped neither-entries. -/
def overDrop (a b : Option (Info Nat)) : Option (Info Nat) :=
  match over a b with
  | some i => if i.del && i.pred.isNone then none else some i
  | none => none

def exE : Nat → Option (Info Nat)
  | 4 => some ⟨40, false, none⟩
  | 5 => some ⟨50, true, some 40⟩
  | _ => none

def exPr : Nat → Bool
  | 4 => true
  | _ => false

theorem drop_breaks_read_ahead :
    -- The dropping merge leaves no entry for d in [3, 5], so the rule skips it,
    overDrop (exE 5) (overDrop (exE 4) (exE 3)) = none ∧
    -- while the kept tombstone (generation 50 > 40, deleted) says "removed",
    rule (fun j => 10 * j) exE exPr 3 4 5 = some .removed ∧
    -- which is the truth: commit 5 touched d after the read, live to absent.
    classify (exPr 4) (exPr 5) = .removed := by
  refine ⟨rfl, rfl, rfl⟩

end Keys
