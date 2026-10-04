/-
# The write bound of the balance-guarded merge policy, with dedup

docs/key-index-design.md (span design, policy "versions" in
bench/keys/spans.py at 9e8183d): spans tile commit time, and upkeep merges
adjacent spans. Every merge obeys the balance guard: its largest input
holds at most 4x the others combined, counting the inputs' actual entries;
the one exception, a single span rewritten alone, must drop at least a
quarter of its entries, and so may any merge in place of the guard.
A merge's output has at most as many entries as its inputs. It can have
fewer than its largest input: versions superseded once an endpoint retires
are dropped, and the base drops tombstones and predecessors.

The model keeps only sizes. A merge takes any run of adjacent spans and
writes any output size up to its inputs', so the theorem covers every
sequence of commits, boundary births and releases. Boundaries decide which
guarded merges happen, never what one costs. A merge may be attempted up
to `R` times before it publishes, and every attempt writes its output.

Result: written entries, commits' own spans included, are at most
`(1 + 43 R + 20 R log₂ K) C`, where `C` is the entries committed and `K`
bounds a span's entries (the total committed will do).

Why. Either the merge drops at least a quarter of its inputs, and the
dropped entries pay (each is a copy of one committed entry and is dropped
once), or every input but the largest grows by more than half, and a
logarithmic weight on entries pays. The weight must also not jump when a
span shrinks; a harmonic sum has both properties.
-/

namespace WriteBound

/-- A weight on span sizes up to `K`, in units of `M`. It never rises with
size; it drops by `M` when a span grows by more than half; and shrinking a
span raises its total weight by at most `4M` per entry removed. -/
structure Weight (K : Nat) where
  ℓ : Nat → Nat
  M : Nat
  anti : ∀ {s u}, s ≤ u → ℓ u ≤ ℓ s
  grow : ∀ {s u}, 0 < s → u ≤ K → 3 * s < 2 * u → ℓ u + M ≤ ℓ s
  lip : ∀ {v w}, v ≤ w → w ≤ K → v * ℓ v + 4 * (M * v) ≤ w * ℓ w + 4 * (M * w)

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

/-- One merge pays for itself. Inputs `l1 ++ m :: l2`, with `m` the largest
and `S` entries in all; output `u ≤ S` entries. The merge obeys the guard,
or drops at least a quarter of its inputs (`4u ≤ 3S`): the single-span
rewrite that drops a retired endpoint's versions is the latter, with
`l1 = l2 = []`. The cost `M u`, plus the
weight the output carries, is covered by the inputs' weight and `23 M`
per input entry, less `23 M` per output entry. -/
theorem merge_pays (l1 l2 : List Nat) (m u : Nat)
    (hmax : ∀ x ∈ l1 ++ l2, x ≤ m) (hmK : m ≤ K)
    (hguard : 5 * m ≤ 4 * (l1 ++ m :: l2).sum ∨ 4 * u ≤ 3 * (l1 ++ m :: l2).sum)
    (huS : u ≤ (l1 ++ m :: l2).sum) (huK : u ≤ K) :
    w.M * u + 5 * (u * w.ℓ u) + 23 * (w.M * u) ≤
      23 * (w.M * (l1 ++ m :: l2).sum) + 5 * pot w (l1 ++ m :: l2) := by
  have hS : (l1 ++ m :: l2).sum = m + (l1 ++ l2).sum := by
    simp [List.sum_append]; omega
  have hpot : pot w (l1 ++ m :: l2) = m * w.ℓ m + pot w (l1 ++ l2) := by
    simp [pot_append, pot_cons]; omega
  -- Every other input holds at most half the entries.
  have hhalf : ∀ x ∈ l1 ++ l2, 2 * x ≤ m + (l1 ++ l2).sum := by
    intro x hx
    have := hmax x hx
    have : x ≤ (l1 ++ l2).sum := le_sum_of_mem hx
    omega
  rw [hpot]; rw [hS] at hguard huS ⊢
  generalize hT : (l1 ++ l2).sum = T at *
  generalize hP : pot w (l1 ++ l2) = P
  have hMS : w.M * (m + T) = w.M * m + w.M * T := Nat.mul_add _ _ _
  have hMu : w.M * u ≤ w.M * (m + T) := Nat.mul_le_mul_left _ huS
  have hQ : u * w.ℓ u ≤ m * w.ℓ u + T * w.ℓ u := by
    rw [← Nat.add_mul]; exact Nat.mul_le_mul_right _ huS
  -- In general: the largest input covers the output's weight, plus 4M per
  -- entry the output lacks.
  have hL : u * w.ℓ u + 4 * (w.M * u) ≤ m * w.ℓ m + P + 4 * (w.M * m) + 4 * (w.M * T) := by
    by_cases hmu : m ≤ u
    · have h1 : m * w.ℓ u ≤ m * w.ℓ m := Nat.mul_le_mul_left _ (w.anti hmu)
      have h2 : T * w.ℓ u ≤ P := by
        rw [← hT, ← hP]
        apply pot_ge
        intro x hx
        exact Nat.mul_le_mul_left _ (w.anti (Nat.le_trans (hmax x hx) hmu))
      omega
    · have := w.lip (Nat.le_of_lt (Nat.lt_of_not_le hmu)) hmK
      omega
  by_cases hB : 3 * (m + T) < 4 * u
  · -- Little was dropped: every other input grows by more than half.
    have hR : T * w.ℓ u + T * w.M ≤ P := by
      rw [← Nat.mul_add, ← hT, ← hP]
      apply pot_ge
      intro x hx
      by_cases hx0 : x = 0
      · simp [hx0]
      · apply Nat.mul_le_mul_left
        have := hhalf x hx
        exact w.grow (by omega) huK (by omega)
    rw [Nat.mul_comm T w.M] at hR
    -- Here the guard holds: a merge that keeps more than 3/4 needs it.
    have hg : m ≤ 4 * T := by rcases hguard with h | h <;> omega
    have hMm : w.M * m ≤ 4 * (w.M * T) := by
      have := Nat.mul_le_mul_left w.M hg
      rw [Nat.mul_left_comm] at this; exact this
    have hL' : u * w.ℓ u + w.M * T + 4 * (w.M * u) ≤
        m * w.ℓ m + P + 4 * (w.M * m) + 4 * (w.M * T) := by
      by_cases hmu : m ≤ u
      · have h1 : m * w.ℓ u ≤ m * w.ℓ m := Nat.mul_le_mul_left _ (w.anti hmu)
        omega
      · have := w.lip (Nat.le_of_lt (Nat.lt_of_not_le hmu)) hmK
        omega
    omega
  · -- At least a quarter was dropped: the dropped entries pay.
    have hA : 4 * (w.M * u) ≤ 3 * (w.M * m) + 3 * (w.M * T) := by
      have := Nat.mul_le_mul_left w.M (show 4 * u ≤ 3 * (m + T) by omega)
      rw [Nat.mul_left_comm, Nat.mul_left_comm w.M 3, hMS, Nat.mul_add] at this
      exact this
    omega

/-- The index's spans as sizes: the base, then the spans after it, oldest
first; and the running totals. -/
structure St where
  base : Nat
  spans : List Nat
  written : Nat
  committed : Nat
  injected : Nat

variable (R : Nat)

/-- What can happen. Merges join adjacent spans under the guard; which
ones (boundaries, size classes, the cap) is the adversary's choice. Each
merge is attempted `a ≤ R` times, and each attempt writes its output. -/
inductive Step : St → St → Prop
  /-- A commit writes its delta, a span of `n ≤ K` entries, at the head. -/
  | commit (b : Nat) (sp : List Nat) (W C I n : Nat) (hn : n ≤ K) :
      Step ⟨b, sp, W, C, I⟩ ⟨b, sp ++ [n], W + n, C + n, I + n * w.ℓ n⟩
  /-- A merge of adjacent spans: the largest input `m` at most 4x the
  others, or the output at most 3/4 of the inputs (a single span rewritten
  to drop a retired endpoint's versions is this, with one input). It may
  drop superseded versions, so its output is any size up to its inputs'. -/
  | merge (b : Nat) (pre l1 l2 post : List Nat) (m u a W C I : Nat)
      (hmax : ∀ x ∈ l1 ++ l2, x ≤ m) (hmK : m ≤ K)
      (hguard : 5 * m ≤ 4 * (l1 ++ m :: l2).sum ∨ 4 * u ≤ 3 * (l1 ++ m :: l2).sum)
      (huS : u ≤ (l1 ++ m :: l2).sum) (huK : u ≤ K) (ha : a ≤ R) :
      Step ⟨b, pre ++ (l1 ++ m :: l2) ++ post, W, C, I⟩ ⟨b, pre ++ u :: post, W + a * u, C, I⟩
  /-- A merge of the oldest spans into the base, under the guard (only its
  bound on the base is used); it may drop tombstones and versions. -/
  | base (b : Nat) (ins post : List Nat) (o a W C I : Nat)
      (hguard : 5 * b ≤ 4 * (b + ins.sum)) (ho : o ≤ b + ins.sum) (ha : a ≤ R) :
      Step ⟨b, ins ++ post, W, C, I⟩ ⟨o, post, W + a * o, C, I⟩

/-- Reachable from an index with only its base. -/
inductive Reach : St → Prop
  | init (b : Nat) : Reach ⟨b, [], 0, 0, 0⟩
  | step {s t : St} : Reach s → Step w R s t → Reach t

/-- The spans' weight and entries, in units of `M`. -/
def X (sp : List Nat) : Nat := 5 * pot w sp + 23 * (w.M * sp.sum)

/-- The invariant: `M` per entry written, plus `R` times the spans' weight
and entries, never exceeds `R` times what commits brought in, plus `M` per
entry committed. -/
def Inv (s : St) : Prop :=
  w.M * s.written + R * X w s.spans ≤ R * (23 * (w.M * s.committed) + 5 * s.injected) + w.M * s.committed

theorem step_inv {s t : St} (h : Step w R s t) (hs : Inv w R s) : Inv w R t := by
  unfold Inv at *
  cases h with
  | commit b sp W C I n hn =>
    simp only [X, pot_append, List.sum_append] at *
    have e1 : pot w [n] = n * w.ℓ n := by simp [pot]
    rw [e1] at *
    simp only [List.sum_cons, List.sum_nil, Nat.add_zero, Nat.mul_add] at *
    omega
  | merge b pre l1 l2 post m u a W C I hmax hmK hguard huS huK ha =>
    have hp := merge_pays w l1 l2 m u hmax hmK hguard huS huK
    -- The merged spans' X drops by at least M u.
    have hX : X w (pre ++ u :: post) + w.M * u ≤ X w (pre ++ (l1 ++ m :: l2) ++ post) := by
      simp only [X, pot_append, pot_cons, List.sum_append, List.sum_cons, Nat.mul_add] at *
      omega
    have h1 := Nat.mul_le_mul_left R hX
    rw [Nat.mul_add] at h1
    have h2 : w.M * (a * u) ≤ R * (w.M * u) := by
      rw [Nat.mul_left_comm]; exact Nat.mul_le_mul_right _ ha
    simp only [Nat.mul_add] at *
    omega
  | base b ins post o a W C I hguard ho ha =>
    have hX : X w post + w.M * o ≤ X w (ins ++ post) := by
      have h1 : w.M * o ≤ w.M * b + w.M * ins.sum := by rw [← Nat.mul_add]; exact Nat.mul_le_mul_left _ ho
      have h2 : w.M * b ≤ 4 * (w.M * ins.sum) := by
        have := Nat.mul_le_mul_left w.M (show b ≤ 4 * ins.sum by omega)
        rw [Nat.mul_left_comm] at this; exact this
      simp only [X, pot_append, List.sum_append, Nat.mul_add] at *
      omega
    have h1 := Nat.mul_le_mul_left R hX
    rw [Nat.mul_add] at h1
    have h2 : w.M * (a * o) ≤ R * (w.M * o) := by
      rw [Nat.mul_left_comm]; exact Nat.mul_le_mul_right _ ha
    simp only [Nat.mul_add] at *
    omega

theorem reach_inv {s : St} (h : Reach w R s) : Inv w R s := by
  induction h with
  | init b => simp [Inv, X, pot]
  | step _ hst ih => exact step_inv w R hst ih

/-- Each commit brings in at most `ℓ(1)` weight per entry. -/
theorem reach_injected {s : St} (h : Reach w R s) : s.injected ≤ s.committed * w.ℓ 1 := by
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

/-- **The write bound**, for any weight: `M` per entry written is at most
`(1 + 23 R) M + 5 R ℓ(1)` per entry committed. -/
theorem write_bound {s : St} (h : Reach w R s) :
    w.M * s.written ≤ (1 + 23 * R) * (w.M * s.committed) + 5 * R * (s.committed * w.ℓ 1) := by
  have hi := reach_inv w R h
  have hj := Nat.mul_le_mul_left (5 * R) (reach_injected w R h)
  unfold Inv at hi
  have : R * (23 * (w.M * s.committed) + 5 * s.injected) =
      23 * R * (w.M * s.committed) + 5 * R * s.injected := by
    grind
  rw [Nat.add_mul, Nat.one_mul]
  omega

/-! ## A weight that fits: harmonic sums

`ℓ(s) = Σ_{j = s+1}^{K} ⌊4K / j⌋`, in units of `M = K`: about `4K ln(K/s)`. -/

/-- `Σ_{i < n} ⌊4K / (a + i)⌋`. -/
def hsum (K : Nat) (a : Nat) : Nat → Nat
  | 0 => 0
  | n + 1 => 4 * K / a + hsum K (a + 1) n

def hl (K s : Nat) : Nat := hsum K (s + 1) (K - s)

theorem hsum_add (K a : Nat) : ∀ n m, hsum K a (n + m) = hsum K a n + hsum K (a + n) m
  | 0, m => by simp [hsum]
  | n + 1, m => by
    rw [show n + 1 + m = (n + m) + 1 by omega]
    simp only [hsum]
    rw [hsum_add K (a + 1) n m, show a + 1 + n = a + (n + 1) by omega]
    omega

/-- Between two sizes, the weight drops by the sum of the terms between. -/
theorem hl_split {K s u : Nat} (hsu : s ≤ u) (huK : u ≤ K) :
    hl K s = hsum K (s + 1) (u - s) + hl K u := by
  unfold hl
  rw [show K - s = (u - s) + (K - u) by omega, hsum_add, show s + 1 + (u - s) = u + 1 by omega]

theorem hsum_ge (K a u : Nat) : ∀ n, a + n ≤ u + 1 → 0 < a → n * (4 * K / u) ≤ hsum K a n
  | 0, _, _ => by simp [hsum]
  | n + 1, h, ha => by
    have ih := hsum_ge K (a + 1) u n (by omega) (by omega)
    have : 4 * K / u ≤ 4 * K / a := Nat.div_le_div_left (by omega) ha
    simp only [hsum]; rw [Nat.add_mul, Nat.one_mul]
    omega

theorem hsum_le (K a q : Nat) : ∀ n, q ≤ a → 0 < q → hsum K a n ≤ n * (4 * K / q)
  | 0, _, _ => by simp [hsum]
  | n + 1, h, hq => by
    have ih := hsum_le K (a + 1) q n (by omega) hq
    have : 4 * K / a ≤ 4 * K / q := Nat.div_le_div_left h hq
    simp only [hsum]; rw [Nat.add_mul, Nat.one_mul]
    omega

theorem hl_zero_of_ge {K u : Nat} (h : K ≤ u) : hl K u = 0 := by
  simp [hl, show K - u = 0 by omega, hsum]

def harmonic (K : Nat) : Weight K where
  ℓ := hl K
  M := K
  anti := by
    intro s u hsu
    show hl K u ≤ hl K s
    by_cases huK : u ≤ K
    · rw [hl_split hsu huK]; omega
    · rw [hl_zero_of_ge (by omega)]; omega
  grow := by
    intro s u hs huK h
    show hl K u + K ≤ hl K s
    rw [hl_split (s := s) (u := u) (by omega) huK]
    -- (u − s) terms of at least ⌊4K/u⌋ each, and that makes more than K.
    have h1 := hsum_ge K (s + 1) u (u - s) (by omega) (by omega)
    generalize hq : 4 * K / u = q at h1
    have hu0 : 0 < u := by omega
    have hdm := Nat.div_add_mod (4 * K) u
    have hmod := Nat.mod_lt (4 * K) hu0
    rw [hq] at hdm
    have h2 : (u + 1) * q ≤ (3 * (u - s)) * q := Nat.mul_le_mul_right _ (by omega)
    rw [Nat.add_mul, Nat.one_mul, Nat.mul_assoc] at h2
    omega
  lip := by
    intro v w hvw hwK
    show v * hl K v + 4 * (K * v) ≤ w * hl K w + 4 * (K * w)
    -- One entry at a time: shrinking t to t − 1 adds ⌊4K/t⌋ to each of
    -- t − 1 entries, at most 4K.
    have step : ∀ t, 0 < t → t ≤ K →
        (t - 1) * hl K (t - 1) + 4 * (K * (t - 1)) ≤ t * hl K t + 4 * (K * t) := by
      intro t ht htK
      have e := hl_split (show t - 1 ≤ t by omega) htK
      rw [show t - (t - 1) = 1 by omega, show t - 1 + 1 = t by omega] at e
      simp only [hsum, Nat.add_zero] at e
      rw [e, Nat.mul_add]
      have h1 : (t - 1) * (4 * K / t) ≤ 4 * K := by
        have := Nat.div_mul_le_self (4 * K) t
        have : (t - 1) * (4 * K / t) ≤ t * (4 * K / t) := Nat.mul_le_mul_right _ (by omega)
        rw [Nat.mul_comm t] at this; omega
      have h2 : (t - 1) * hl K t ≤ t * hl K t := Nat.mul_le_mul_right _ (by omega)
      have h3 : K * (t - 1) + K = K * t := by
        rw [← Nat.mul_succ]; congr 1; omega
      omega
    have : ∀ d v, v + d ≤ K → v * hl K v + 4 * (K * v) ≤ (v + d) * hl K (v + d) + 4 * (K * (v + d)) := by
      intro d
      induction d with
      | zero => intro v _; simp
      | succ d ih =>
        intro v hv
        have := step (v + d + 1) (by omega) hv
        rw [show v + d + 1 - 1 = v + d by omega] at this
        have := ih v (by omega)
        rw [show v + (d + 1) = v + d + 1 by omega]
        omega
    have := this (w - v) v (by omega)
    rwa [show v + (w - v) = w by omega] at this

/-- Each doubling of the size costs at most `4K` of weight. -/
theorem hl_double (K x : Nat) (hx : 0 < x) : hl K x ≤ 4 * K + hl K (2 * x) := by
  by_cases h2 : 2 * x ≤ K
  · rw [hl_split (show x ≤ 2 * x by omega) h2]
    have := hsum_le K (x + 1) (x + 1) (2 * x - x) (by omega) (by omega)
    have : (2 * x - x) * (4 * K / (x + 1)) ≤ 4 * K := by
      have := Nat.div_mul_le_self (4 * K) (x + 1)
      have : (2 * x - x) * (4 * K / (x + 1)) ≤ (x + 1) * (4 * K / (x + 1)) := Nat.mul_le_mul_right _ (by omega)
      rw [Nat.mul_comm (x + 1)] at this; omega
    omega
  · by_cases hxK : x ≤ K
    · unfold hl
      have := hsum_le K (x + 1) (x + 1) (K - x) (by omega) (by omega)
      have : (K - x) * (4 * K / (x + 1)) ≤ 4 * K := by
        have := Nat.div_mul_le_self (4 * K) (x + 1)
        have : (K - x) * (4 * K / (x + 1)) ≤ (x + 1) * (4 * K / (x + 1)) := Nat.mul_le_mul_right _ (by omega)
        rw [Nat.mul_comm (x + 1)] at this; omega
      omega
    · rw [hl_zero_of_ge (by omega)]; omega

theorem hl_one (K : Nat) : hl K 1 ≤ 4 * K * (Nat.log2 K + 1) := by
  have : ∀ j, hl K 1 ≤ 4 * K * j + hl K (2 ^ j) := by
    intro j
    induction j with
    | zero => simp
    | succ j ih =>
      have := hl_double K (2 ^ j) (Nat.two_pow_pos j)
      rw [Nat.pow_succ, Nat.mul_comm (2 ^ j) 2, Nat.mul_succ]
      omega
  have h := this (Nat.log2 K + 1)
  by_cases hK : K = 0
  · subst hK; simp [hl, hsum]
  · have : K < 2 ^ (Nat.log2 K + 1) := (Nat.log2_lt hK).1 (by omega)
    rw [hl_zero_of_ge (u := 2 ^ (Nat.log2 K + 1)) (by omega)] at h
    omega

/-- **The write bound, explicitly.** With spans of at most `K ≥ 1` entries
and each merge attempted at most `R` times: entries written, commits
included, are at most `(1 + 43 R + 20 R log₂ K)` per entry committed,
whatever the order of commits, boundaries and guarded merges. -/
theorem write_bound_log2 {s : St} (hK : 0 < K) (h : Reach (harmonic K) R s) :
    s.written ≤ (1 + 43 * R + 20 * R * Nat.log2 K) * s.committed := by
  have hb := write_bound (harmonic K) R h
  have h1 := hl_one K
  change K * s.written ≤ (1 + 23 * R) * (K * s.committed) + 5 * R * (s.committed * hl K 1) at hb
  have h2 : s.committed * hl K 1 ≤ s.committed * (4 * K * (Nat.log2 K + 1)) := Nat.mul_le_mul_left _ h1
  apply Nat.le_of_mul_le_mul_left (c := K) _ hK
  -- Everything is a multiple of K · committed.
  generalize hL : Nat.log2 K = L at *
  generalize hC : s.committed = C at *
  have e1 : C * (4 * K * (L + 1)) = 4 * (L + 1) * (K * C) := by grind
  have e2 : K * ((1 + 43 * R + 20 * R * L) * C) = (1 + 43 * R + 20 * R * L) * (K * C) := by grind
  rw [e1] at h2; rw [e2]
  have h3 : 5 * R * (C * hl K 1) ≤ 5 * R * (4 * (L + 1) * (K * C)) := Nat.mul_le_mul_left _ h2
  have e3 : 5 * R * (4 * (L + 1) * (K * C)) = (20 * R + 20 * R * L) * (K * C) := by grind
  rw [e3] at h3
  have e4 : (1 + 43 * R + 20 * R * L) * (K * C) =
      (1 + 23 * R) * (K * C) + (20 * R + 20 * R * L) * (K * C) := by
    grind
  rw [e4]; omega

end WriteBound
