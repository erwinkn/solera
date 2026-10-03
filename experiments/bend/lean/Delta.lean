-- The same delta algebra as ../delta/main.bend, in Lean 4 (core only, no
-- Mathlib), for the comparison in ../REPORT.md.

structure Info (G : Type) where
  gen : G
  del : Bool
  bef : Bool

/-- The older entry's "before", the newer entry's state. -/
def comb (o n : Info G) : Info G := { n with bef := o.bef }

inductive Delta (G : Type) where
  | nil
  | cons (k : Nat) (i : Info G) (r : Delta G)

open Delta

def merge : Delta G → Delta G → Delta G
  | nil, b => b
  | cons ka ia ra, nil => cons ka ia ra
  | cons ka ia ra, cons kb ib rb =>
    if ka < kb then cons ka ia (merge ra (cons kb ib rb))
    else if kb < ka then cons kb ib (merge (cons ka ia ra) rb)
    else cons ka (comb ia ib) (merge ra rb)

theorem merge_nil_r (a : Delta G) : merge a nil = a := by
  cases a <;> simp [merge]

theorem comb_assoc (a b c : Info G) : comb (comb a b) c = comb a (comb b c) := rfl

theorem merge_assoc : ∀ (a b c : Delta G), merge (merge a b) c = merge a (merge b c)
  | nil, b, c => by simp [merge]
  | cons kx ix xs, nil, c => by simp [merge]
  | cons kx ix xs, cons ky iy ys, nil => by simp [merge_nil_r]
  | cons kx ix xs, cons ky iy ys, cons kz iz zs => by
    have h1 := merge_assoc xs (cons ky iy ys) (cons kz iz zs)
    have h2 := merge_assoc xs (cons ky iy ys) zs
    have h3 := merge_assoc (cons kx ix xs) (cons ky iy ys) zs
    have h4 := merge_assoc xs ys (cons kz iz zs)
    have h5 := merge_assoc xs ys zs
    have h6 := merge_assoc (cons kx ix xs) ys (cons kz iz zs)
    have h7 := merge_assoc (cons kx ix xs) ys zs
    rcases Nat.lt_trichotomy kx ky with a | a | a <;>
    rcases Nat.lt_trichotomy kx kz with b | b | b <;>
    rcases Nat.lt_trichotomy ky kz with c | c | c <;>
    (try subst_vars) <;>
    first
    | omega
    | simp_all [merge, comb, Nat.lt_irrefl, Nat.lt_asymm]

theorem merge_nil_l (a : Delta G) : merge nil a = a := by simp [merge]

-- Classes
-- -------

inductive Class where
  | added | updated | removed | nothing
  deriving DecidableEq

def classify (bef del : Bool) : Class :=
  match bef, del with
  | false, false => .added
  | true, false => .updated
  | true, true => .removed
  | false, true => .nothing

def classes : Delta G → List (Nat × Class)
  | nil => []
  | cons k i r => (k, classify i.bef i.del) :: classes r

-- (b) grouped ranges
-- ------------------

def fold : List (Delta G) → Delta G → Delta G
  | [], acc => acc
  | d :: ds, acc => fold ds (merge acc d)

def summary (ds : List (Delta G)) : Delta G := fold ds nil

theorem fold_acc : ∀ (ds : List (Delta G)) (acc : Delta G), fold ds acc = merge acc (fold ds nil)
  | [], acc => by simp [fold, merge_nil_r]
  | d :: ds, acc => by
    simp only [fold, merge_nil_l]
    rw [fold_acc ds (merge acc d), fold_acc ds d, merge_assoc]

theorem fold_append : ∀ (xs ys : List (Delta G)) (acc : Delta G), fold (xs ++ ys) acc = fold ys (fold xs acc)
  | [], _, _ => rfl
  | x :: xs, ys, acc => fold_append xs ys (merge acc x)


theorem grouped_classes (gs : List (List (Delta G))) :
    classes (summary gs.flatten) = classes (summary (gs.map summary)) := by
  suffices h : ∀ acc, fold gs.flatten acc = fold (gs.map summary) acc by simp [summary, h]
  induction gs with
  | nil => intro acc; rfl
  | cons g gs ih =>
    intro acc
    simp only [List.flatten_cons, List.map_cons, fold, fold_append, summary]
    rw [ih, fold_acc g acc]

-- The parallel balanced reduction (sequential here).
def tree : Nat → List (Delta G) → Delta G
  | 0, ds => summary ds
  | _ + 1, [] => nil
  | _ + 1, [d] => d
  | f + 1, ds => merge (tree f (ds.take (ds.length / 2))) (tree f (ds.drop (ds.length / 2)))

theorem tree_summary : ∀ (fuel : Nat) (ds : List (Delta G)), tree fuel ds = summary ds
  | 0, ds => rfl
  | f + 1, [] => rfl
  | f + 1, [d] => by simp [tree, summary, fold, merge_nil_l]
  | f + 1, d :: e :: rest => by
    simp only [tree]
    rw [tree_summary f, tree_summary f]
    conv => rhs; rw [← List.take_append_drop ((d :: e :: rest).length / 2) (d :: e :: rest)]
    simp only [summary, fold_append]
    rw [fold_acc (List.drop _ _) (fold _ nil)]

-- (c) per key
-- -----------

def lookup (k : Nat) : Delta G → Option (Info G)
  | nil => none
  | cons kd i r => if k < kd then none else if k = kd then some i else lookup k r

def over : Option (Info G) → Option (Info G) → Option (Info G)
  | none, m => m
  | some i, none => some i
  | some i, some j => some (comb j i)

theorem lookup_merge (k : Nat) : ∀ (a b : Delta G), lookup k (merge a b) = over (lookup k b) (lookup k a)
  | nil, nil => by simp [merge, over, lookup]
  | nil, cons kb ib rb => by cases h : lookup k (cons kb ib rb) <;> simp [merge, over, lookup, h]
  | cons ka ia ra, nil => by simp [merge, over, lookup]
  | cons ka ia ra, cons kb ib rb => by
    have h1 := lookup_merge k ra (cons kb ib rb)
    have h2 := lookup_merge k (cons ka ia ra) rb
    have h3 := lookup_merge k ra rb
    rcases Nat.lt_trichotomy k ka with a | a | a <;>
    rcases Nat.lt_trichotomy k kb with b | b | b <;>
    rcases Nat.lt_trichotomy ka kb with c | c | c <;>
    (try subst_vars) <;>
    first
    | omega
    | simp_all [merge, lookup, over, Nat.lt_irrefl, Nat.lt_asymm, Nat.ne_of_gt]

def step (p : Bool) : Option (Info G) → Bool
  | none => p
  | some i => !i.del

def pres (k : Nat) : List (Delta G) → Bool → Bool
  | [], p => p
  | c :: cs, p => pres k cs (step p (lookup k c))

def Ok (k : Nat) : List (Delta G) → Bool → Prop
  | [], _ => True
  | c :: cs, p => (∀ i, lookup k c = some i → i.bef = p) ∧ Ok k cs (step p (lookup k c))

def Agrees (p q : Bool) : Option (Info G) → Prop
  | none => p = q
  | some i => i.bef = p ∧ (!i.del) = q

def foldO (k : Nat) : List (Delta G) → Option (Info G) → Option (Info G)
  | [], m => m
  | c :: cs, m => foldO k cs (over (lookup k c) m)

theorem lk_fold (k : Nat) : ∀ (cs : List (Delta G)) (acc : Delta G), lookup k (fold cs acc) = foldO k cs (lookup k acc)
  | [], acc => rfl
  | c :: cs, acc => by simp [fold, foldO, lk_fold k cs, lookup_merge]

theorem core (k : Nat) (p0 : Bool) : ∀ (cs : List (Delta G)) (p : Bool) (m : Option (Info G)),
    Agrees p0 p m → Ok k cs p → Agrees p0 (pres k cs p) (foldO k cs m)
  | [], p, m, a, _ => a
  | c :: cs, p, m, a, ⟨hb, hr⟩ => by
    apply core k p0 cs _ _ _ hr
    cases e : lookup k c with
    | none => simpa [step, over] using a
    | some i =>
      have := hb i e
      cases m <;> simp_all [Agrees, step, over, comb]

theorem range_agrees (k : Nat) (cs : List (Delta G)) (p : Bool) (h : Ok k cs p) :
    Agrees p (pres k cs p) (lookup k (summary cs)) := by
  simpa [summary, lk_fold, lookup] using core k p cs p none rfl h

def ClassAgrees (p q : Bool) : Option (Info G) → Prop
  | none => p = q
  | some i => classify i.bef i.del = classify p (!q)

theorem range_changes (k : Nat) (cs : List (Delta G)) (p : Bool) (h : Ok k cs p) :
    ClassAgrees p (pres k cs p) (lookup k (summary cs)) := by
  have := range_agrees k cs p h
  revert this
  cases lookup k (summary cs) with
  | none => exact id
  | some i => intro ⟨hb, hd⟩; subst hb; simp only [ClassAgrees]; rw [← hd]; cases i.del <;> rfl

-- (d) live count
-- --------------

inductive State (G : Type) where
  | snil
  | scon (k : Nat) (g : G) (r : State G)

open State

def size : State G → Nat
  | snil => 0
  | scon _ _ r => size r + 1

def put (k : Nat) (i : Info G) (s : State G) : State G := if i.del then s else scon k i.gen s

def lives : Delta G → State G
  | nil => snil
  | cons k i r => put k i (lives r)

def apply : State G → Delta G → State G
  | snil, d => lives d
  | scon ks gs rs, nil => scon ks gs rs
  | scon ks gs rs, cons kd i rd =>
    if ks < kd then scon ks gs (apply rs (cons kd i rd))
    else if kd < ks then put kd i (apply (scon ks gs rs) rd)
    else put kd i (apply rs rd)

def NoneBefore : Delta G → Prop
  | nil => True
  | cons _ i r => i.bef = false ∧ NoneBefore r

def Consistent : State G → Delta G → Prop
  | snil, d => NoneBefore d
  | scon _ _ _, nil => True
  | scon ks gs rs, cons kd i rd =>
    if ks < kd then Consistent rs (cons kd i rd)
    else if kd < ks then i.bef = false ∧ Consistent (scon ks gs rs) rd
    else i.bef = true ∧ Consistent rs rd

def added : Delta G → Nat
  | nil => 0
  | cons _ i r => (if classify i.bef i.del = .added then 1 else 0) + added r

def removed : Delta G → Nat
  | nil => 0
  | cons _ i r => (if classify i.bef i.del = .removed then 1 else 0) + removed r

theorem lives_count : ∀ (d : Delta G), NoneBefore d → size (lives d) + removed d = added d
  | nil, _ => rfl
  | cons k ⟨g, del, bef⟩ r, ⟨hb, hr⟩ => by
    have := lives_count r hr
    simp only at hb; subst hb
    cases del <;> simp [lives, put, size, removed, added, classify] <;> omega

theorem live_count : ∀ (s : State G) (d : Delta G), Consistent s d →
    size (apply s d) + removed d = size s + added d
  | snil, d, h => by
    have h' : NoneBefore d := by cases d <;> simpa [Consistent] using h
    simpa [apply, size] using lives_count d h'
  | scon ks gs rs, nil, _ => by simp [apply, removed, added]
  | scon ks gs rs, cons kd ⟨g, del, bef⟩ rd, h => by
    unfold Consistent at h
    by_cases a : ks < kd
    · have := live_count rs (cons kd ⟨g, del, bef⟩ rd) (by simpa [a] using h)
      simp only [apply, a, ite_true, size]; omega
    · by_cases b : kd < ks
      · simp only [a, b, ite_true, ite_false] at h
        obtain ⟨hb, hr⟩ := h
        have := live_count (scon ks gs rs) rd hr
        subst hb
        cases del <;> simp [apply, a, b, put, size, removed, added, classify] at * <;> omega
      · simp only [a, b, ite_false] at h
        obtain ⟨hb, hr⟩ := h
        have := live_count rs rd hr
        subst hb
        cases del <;> simp [apply, a, b, put, size, removed, added, classify] at * <;> omega
