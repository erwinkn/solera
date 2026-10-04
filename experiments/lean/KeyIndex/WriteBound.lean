/-
# The write bound of the balance-guarded merge policy, with dedup

docs/key-index-design.md (span design): spans tile commit time, and upkeep
merges adjacent spans. Every merge obeys the balance guard: its largest
input holds at most 4x the others combined (actual entries). A merge's
output has at least as many entries as its largest input (keys are
deduplicated, never dropped, outside the base) and at most as many as all
its inputs; a merge into the base may also drop tombstones.

The model keeps only sizes. A merge takes any run of adjacent spans and
writes any output size the dedup allows, so the theorem covers every
sequence of commits, boundary births and releases: boundaries only decide
which guarded merges happen, never what one costs.

Result: written entries (commits' own spans included) are at most
`6 C + 5 Σ n ℓ(n)` over the commits, `ℓ(n) ≈ 2 log₂(K / n)`, so at most
`(16 + 10 log₂ K) C` for spans of at most K entries (K: the distinct keys).
-/

namespace WriteBound

/-- A potential weight on span sizes up to `K`: it never rises with size,
and drops by one when a span grows by more than half. -/
structure Weight (K : Nat) where
  ℓ : Nat → Nat
  anti : ∀ {s u}, s ≤ u → ℓ u ≤ ℓ s
  grow : ∀ {s u}, 0 < s → u ≤ K → 3 * s < 2 * u → ℓ u + 1 ≤ ℓ s

variable {K : Nat} (w : Weight K)

/-- Each span's entries carry its size's weight. -/
def pot (l : List Nat) : Nat := (l.map fun s => s * w.ℓ s).sum

theorem pot_append (a b : List Nat) : pot w (a ++ b) = pot w a + pot w b := by
  simp [pot, List.sum_append]

theorem pot_cons (x : Nat) (l : List Nat) : pot w (x :: l) = x * w.ℓ x + pot w l := by
  simp [pot]

theorem le_sum_of_mem {x : Nat} : ∀ {l : List Nat}, x ∈ l → x ≤ l.sum
  | _ :: l, List.Mem.head _ => by simp
  | y :: l, List.Mem.tail _ h => by have := le_sum_of_mem h; simp; omega

/-- A lower bound on a list's potential, element by element. -/
theorem pot_ge (c : Nat) : ∀ (l : List Nat), (∀ x ∈ l, x * c ≤ x * w.ℓ x) → l.sum * c ≤ pot w l
  | [], _ => by simp [pot]
  | x :: l, h => by
    have hx := h x (by simp)
    have hl := pot_ge c l (fun y hy => h y (by simp [hy]))
    rw [pot_cons, List.sum_cons, Nat.add_mul]
    omega

/-- One merge pays for itself. Inputs `l1 ++ m :: l2`, `m` the largest,
`S` entries in all; output `u` entries, `m ≤ u ≤ S` (dedup). The cost `u`,
plus the potential the output carries, is covered by the inputs' potential
and 4 per entry the dedup removed. -/
theorem merge_pays (l1 l2 : List Nat) (m u : Nat)
    (hmax : ∀ x ∈ l1 ++ l2, x ≤ m)
    (hguard : 5 * m ≤ 4 * (l1 ++ m :: l2).sum)
    (hmu : m ≤ u) (huS : u ≤ (l1 ++ m :: l2).sum) (huK : u ≤ K) :
    u + 5 * (u * w.ℓ u) + 4 * u ≤ 4 * (l1 ++ m :: l2).sum + 5 * pot w (l1 ++ m :: l2) := by
  have hS : (l1 ++ m :: l2).sum = m + (l1 ++ l2).sum := by
    simp [List.sum_append]; omega
  have hpot : pot w (l1 ++ m :: l2) = m * w.ℓ m + pot w (l1 ++ l2) := by
    simp [pot_append, pot_cons]; omega
  rw [hpot]; rw [hS] at hguard huS ⊢
  -- The largest input's entries keep at least the output's weight.
  have hm : m * w.ℓ u ≤ m * w.ℓ m := Nat.mul_le_mul_left _ (w.anti hmu)
  have hU : u * w.ℓ u ≤ (m + (l1 ++ l2).sum) * w.ℓ u := Nat.mul_le_mul_right _ huS
  generalize hT : (l1 ++ l2).sum = T at *
  by_cases hB : 3 * (m + T) < 4 * u
  · -- Little dedup: every other input grows by more than half.
    have hrest : T * (w.ℓ u + 1) ≤ pot w (l1 ++ l2) := by
      rw [← hT]
      apply pot_ge
      intro x hx
      by_cases hx0 : x = 0
      · simp [hx0]
      · apply Nat.mul_le_mul_left
        apply w.grow (by omega) huK
        have := hmax x hx
        -- x and m are both inputs, so 2x ≤ S, and 4u > 3S.
        have : x + m ≤ m + T := by
          have : x ≤ (l1 ++ l2).sum := by
            rw [List.sum_append]; rw [List.mem_append] at hx
            rcases hx with hx | hx
            · have := le_sum_of_mem hx; omega
            · have := le_sum_of_mem hx; omega
          omega
        omega
    rw [Nat.mul_add, Nat.mul_one] at hrest
    rw [Nat.add_mul] at hU
    omega
  · -- Much dedup: the removed entries pay.
    have hrest : T * w.ℓ u ≤ pot w (l1 ++ l2) := by
      rw [← hT]
      apply pot_ge
      intro x hx
      exact Nat.mul_le_mul_left _ (w.anti (Nat.le_trans (hmax x hx) hmu))
    rw [Nat.add_mul] at hU
    omega

/-- The index's spans as sizes: the base, then the spans after it, oldest
first; and the running totals. -/
structure St where
  base : Nat
  spans : List Nat
  written : Nat
  committed : Nat
  injected : Nat

/-- What can happen. Merges join adjacent spans under the guard; which
ones (boundaries, size classes, cap) is the adversary's choice. -/
inductive Step : St → St → Prop
  /-- A commit writes its delta, a span of `n ≤ K` entries, at the head. -/
  | commit (b : Nat) (sp : List Nat) (W C I n : Nat) (hn : n ≤ K) :
      Step ⟨b, sp, W, C, I⟩ ⟨b, sp ++ [n], W + n, C + n, I + n * w.ℓ n⟩
  /-- A merge of adjacent spans, the largest input `m` at most 4x the
  others; the output keeps every key, deduplicated. -/
  | merge (b : Nat) (pre l1 l2 post : List Nat) (m u W C I : Nat)
      (hmax : ∀ x ∈ l1 ++ l2, x ≤ m)
      (hguard : 5 * m ≤ 4 * (l1 ++ m :: l2).sum)
      (hmu : m ≤ u) (huS : u ≤ (l1 ++ m :: l2).sum) (huK : u ≤ K) :
      Step ⟨b, pre ++ (l1 ++ m :: l2) ++ post, W, C, I⟩ ⟨b, pre ++ u :: post, W + u, C, I⟩
  /-- A merge of the oldest spans into the base, under the guard (only its
  bound on the base is used); it may drop tombstones, so the output is any
  size up to its inputs'. -/
  | base (b : Nat) (ins post : List Nat) (o W C I : Nat)
      (hguard : 5 * b ≤ 4 * (b + ins.sum)) (ho : o ≤ b + ins.sum) :
      Step ⟨b, ins ++ post, W, C, I⟩ ⟨o, post, W + o, C, I⟩

/-- Reachable from an index with only its base. -/
inductive Reach : St → Prop
  | init (b : Nat) : Reach ⟨b, [], 0, 0, 0⟩
  | step {s t : St} : Reach s → Step w s t → Reach t

/-- The invariant: what was written, plus 5x the potential and the entries
outside the base, never exceeds 6 per committed entry plus 5x the potential
commits brought in. -/
def Inv (s : St) : Prop :=
  s.written + 5 * pot w s.spans + 5 * s.spans.sum ≤ 6 * s.committed + 5 * s.injected

theorem step_inv {s t : St} (h : Step w s t) (hs : Inv w s) : Inv w t := by
  unfold Inv at *
  cases h with
  | commit b sp W C I n hn =>
    simp only [pot_append, List.sum_append] at *
    simp [pot] at *
    omega
  | merge b pre l1 l2 post m u W C I hmax hguard hmu huS huK =>
    have hp := merge_pays w l1 l2 m u hmax hguard hmu huS huK
    simp only [pot_append, pot_cons, List.sum_append, List.sum_cons] at *
    generalize (l1 ++ m :: l2).sum = S at *
    generalize pot w (l1 ++ m :: l2) = P at *
    omega
  | base b ins post o W C I hguard ho =>
    simp only [pot_append, List.sum_append] at *
    omega

theorem reach_inv {s : St} (h : Reach w s) : Inv w s := by
  induction h with
  | init b => simp [Inv, pot]
  | step _ hst ih => exact step_inv w hst ih

/-- Each commit brings in at most `ℓ(1)` potential per entry. -/
theorem reach_injected {s : St} (h : Reach w s) : s.injected ≤ s.committed * w.ℓ 1 := by
  induction h with
  | init b => simp
  | step _ hst ih =>
    cases hst with
    | commit b sp W C I n hn =>
      simp only at *
      by_cases h0 : n = 0
      · subst h0; simp; omega
      · have : n * w.ℓ n ≤ n * w.ℓ 1 := Nat.mul_le_mul_left _ (w.anti (by omega))
        rw [Nat.add_mul]; omega
    | merge => simpa using ih
    | base => simpa using ih

/-- **The write bound.** Every entry written (commits and merges, base
merges included) is paid for: `written ≤ 6 C + 5 Σ n ℓ(n) ≤ (6 + 5 ℓ(1)) C`. -/
theorem write_bound {s : St} (h : Reach w s) :
    s.written ≤ 6 * s.committed + 5 * s.injected ∧
    s.written ≤ (6 + 5 * w.ℓ 1) * s.committed := by
  have hi := reach_inv w h
  have hj := reach_injected w h
  unfold Inv at hi
  constructor
  · omega
  · rw [Nat.add_mul, Nat.mul_assoc, Nat.mul_comm (w.ℓ 1)]; omega

/-! ## A concrete weight: `ℓ(s) = 2 log₂ K + 2 − log₂ (s²)` -/

theorem log2_mono {a b : Nat} (ha : a ≠ 0) (hab : a ≤ b) : Nat.log2 a ≤ Nat.log2 b :=
  (Nat.le_log2 (by omega)).2 (Nat.le_trans (Nat.log2_self_le ha) hab)

/-- `log₂(s²)` for `s ≤ K` stays below `2 log₂ K + 2`. -/
theorem log2_sq_lt {s : Nat} (hs : s ≤ K) : Nat.log2 (s * s) < 2 * Nat.log2 K + 2 := by
  by_cases h0 : s = 0
  · subst h0; rw [Nat.zero_mul, Nat.log2_zero]; omega
  · have hK : K ≠ 0 := by omega
    rw [Nat.log2_lt (Nat.mul_ne_zero h0 h0)]
    have hk : K < 2 ^ (Nat.log2 K + 1) := (Nat.log2_lt hK).1 (by omega)
    have : s * s < 2 ^ (Nat.log2 K + 1) * 2 ^ (Nat.log2 K + 1) :=
      Nat.mul_lt_mul_of_lt_of_le (by omega) (by omega) (by omega)
    rw [← Nat.pow_add] at this
    have e : Nat.log2 K + 1 + (Nat.log2 K + 1) = 2 * Nat.log2 K + 2 := by omega
    rwa [e] at this

def log2Weight (K : Nat) : Weight K where
  ℓ s := 2 * Nat.log2 K + 2 - Nat.log2 (s * s)
  anti := by
    intro s u hsu
    by_cases h0 : s = 0
    · subst h0; rw [Nat.zero_mul, Nat.log2_zero]; omega
    · have : Nat.log2 (s * s) ≤ Nat.log2 (u * u) :=
        log2_mono (Nat.mul_ne_zero h0 h0) (Nat.mul_le_mul hsu hsu)
      omega
  grow := by
    intro s u hs huK h
    -- u > 1.5 s gives u² > 2 s², one more bit.
    have hsq : 2 * (s * s) ≤ u * u := by
      have : 9 * (s * s) < 4 * (u * u) := by
        have := Nat.mul_lt_mul_of_lt_of_le h (Nat.le_of_lt h) (by omega)
        have e1 : 3 * s * (3 * s) = 9 * (s * s) := Nat.mul_mul_mul_comm 3 s 3 s
        have e2 : 2 * u * (2 * u) = 4 * (u * u) := Nat.mul_mul_mul_comm 2 u 2 u
        omega
      omega
    have hs2 : s * s ≠ 0 := Nat.mul_ne_zero (by omega) (by omega)
    have h1 : Nat.log2 (2 * (s * s)) = Nat.log2 (s * s) + 1 := Nat.log2_two_mul hs2
    have h2 : Nat.log2 (2 * (s * s)) ≤ Nat.log2 (u * u) := log2_mono (by omega) hsq
    have h3 := log2_sq_lt huK
    show 2 * Nat.log2 K + 2 - Nat.log2 (u * u) + 1 ≤ 2 * Nat.log2 K + 2 - Nat.log2 (s * s)
    omega

/-- With spans of at most `K` entries: at most `16 + 10 log₂ K` entries
written per committed entry, whatever the order of commits, boundaries and
guarded merges. At K = 10⁸ (log₂ K = 26): 276. -/
theorem write_bound_log2 {s : St} (h : Reach (log2Weight K) s) :
    s.written ≤ (16 + 10 * Nat.log2 K) * s.committed := by
  have := (write_bound (log2Weight K) h).2
  have e : (log2Weight K).ℓ 1 = 2 * Nat.log2 K + 2 := by
    show 2 * Nat.log2 K + 2 - Nat.log2 (1 * 1) = _
    have : Nat.log2 (1 * 1) = 0 := by decide
    omega
  rw [e] at this
  rwa [show 6 + 5 * (2 * Nat.log2 K + 2) = 16 + 10 * Nat.log2 K by omega] at this

end WriteBound
