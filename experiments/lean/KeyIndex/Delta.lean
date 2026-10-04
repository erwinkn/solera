/-
# The span merge on sorted lists, and its meaning per key

A span's entries as a key-sorted list, merged the way the native merge
walks two runs. `lookup_merge`: a key's entry in the merge is its newer
entry over its older one, so the list merge means `Keys.alg`'s merge, and
everything proven in Tiling and Keys holds of it. Lookups stop at the first
key past the one sought, as in a sorted run.
-/
import KeyIndex.Keys

namespace Delta

open Keys

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

def lookup (k : Nat) : Delta G → Option (Info G)
  | nil => none
  | cons kd i r => if k < kd then none else if k = kd then some i else lookup k r

/-- The list merge means the per-key merge. -/
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

theorem merge_assoc : ∀ (a b c : Delta G), merge (merge a b) c = merge a (merge b c)
  | nil, b, c => by simp [merge]
  | cons kx ix xs, nil, c => by simp [merge]
  | cons kx ix xs, cons ky iy ys, nil => by cases h : merge (cons kx ix xs) (cons ky iy ys) <;> simp_all [merge]
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

/-- A list delta's meaning: each key's entry. -/
def sem (d : Delta G) : Sem G := fun k => lookup k d

/-- The merge is a homomorphism into the per-key algebra. -/
theorem sem_merge (a b : Delta G) : sem (merge a b) = (alg G).mul (sem a) (sem b) := by
  funext k; exact lookup_merge k a b

end Delta
