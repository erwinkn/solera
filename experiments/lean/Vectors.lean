/-
Test vectors for the native span code (format v4), from the segment model.

    lake build vectors && .lake/build/bin/vectors HISTORIES SEED > vectors.jsonl

One history per line: commits with exact entries, endpoint births (at the
head + 1) and retirements, and merges of adjacent physical spans, which may
cross live endpoints ("versions"). After every step it prints the state's
encoded spans and the model's answers. Each answer is computed twice, from
the encoded spans as a reader would and from the per-commit history, and
the generator stops if the two disagree. The spans are maintained with the
proven model's own operations (`Keys.over` per key, `Keys.normE` for the
base's initial segment).

Rules (docs/key-index-design.md, review A12):
- A12-1: only the base's initial segment, before the oldest live endpoint
  inside it, drops tombstones and predecessors.
- A12-2: changes(P, N) clips each key to versions in [g(P), g(N+1)); the
  state at N is the newest version older than g(N+1), the state before P
  the newest older than g(P).
-/
import KeyIndex

open Keys

namespace Vectors

def nkeys : Nat := 4
def keyName (k : Nat) : String := (Char.ofNat (97 + k)).toString
def gen (c : Nat) : Nat := 10 * (c + 1)

/-- A segment: commits `start .. stop`, and each key's entry (one per key). -/
structure Seg where
  start : Nat
  stop : Nat
  ents : List (Option (Info Nat))

/-- A physical span: its segments, oldest first. -/
structure PSpan where
  segs : List Seg

def PSpan.a (s : PSpan) : Nat := (s.segs.head?.map (·.start)).getD 0
def PSpan.b (s : PSpan) : Nat := (s.segs.getLast?.map (·.stop)).getD 0

structure St where
  spans : List PSpan
  head : Nat
  live : List Nat
  hist : Array (List (Option (Info Nat)))

/-! ## The model's operations -/

def ent (e : List (Option (Info Nat))) (k : Nat) : Option (Info Nat) := e.getD k none

/-- Two adjacent segments merged, key by key: the newer state, the older predecessor. -/
def mergeSeg (o n : Seg) : Seg :=
  ⟨o.start, n.stop, (List.range nkeys).map fun k => over (ent n.ents k) (ent o.ents k)⟩

/-- A merge's output: its inputs' segments, coalesced across endpoints no
longer live; from commit 0, the initial segment drops tombstones and
predecessors (A12-1). -/
def mergeSpans (live : List Nat) (ins : List PSpan) : PSpan :=
  let segs := ins.flatMap (·.segs)
  let out := segs.foldl (fun acc s =>
    match acc.getLast? with
    | some p => if live.contains s.start then acc ++ [s] else acc.dropLast ++ [mergeSeg p s]
    | none => [s]) []
  match out with
  | s :: rest => if s.start == 0 then ⟨{ s with ents := s.ents.map normE } :: rest⟩ else ⟨s :: rest⟩
  | [] => ⟨[]⟩

/-- The truth: key `k`'s live generation after commits `0 .. c`. -/
def stateAfter (st : St) (k : Nat) (c : Nat) : Option Nat :=
  (List.range (c + 1)).foldl (fun s j =>
    match ent (st.hist.getD j []) k with
    | none => s
    | some i => if i.del then none else some i.gen) none

def stateBefore (st : St) (k e : Nat) : Option Nat := if e == 0 then none else stateAfter st k (e - 1)

def touched (st : St) (k lo hi : Nat) : Bool :=
  (List.range (hi + 1)).any fun j => lo ≤ j && (ent (st.hist.getD j []) k).isSome

/-- A span's encoding for a key: kept versions newest first, the predecessor of the oldest. -/
def versions (sp : PSpan) (k : Nat) : List (Info Nat) := sp.segs.reverse.filterMap fun s => ent s.ents k

def allVersions (st : St) (k : Nat) : List (Info Nat) := st.spans.flatMap (versions · k)

def newestIn (vs : List (Info Nat)) (lo hi : Nat) : Option (Info Nat) :=
  vs.foldl (fun acc v => if lo ≤ v.gen && v.gen < hi then
    match acc with
    | some a => if v.gen > a.gen then some v else acc
    | none => some v
    else acc) none

def liveOf : Option (Info Nat) → Option Nat
  | some i => if i.del then none else some i.gen
  | none => none

/-- Lookup at the head, as proven (`lookup_head`): segments newest first, first hit. -/
def lookupHead (st : St) (k : Nat) : Option Nat :=
  liveOf ((st.spans.reverse.flatMap fun sp => sp.segs.reverse).findSome? fun s => ent s.ents k)

def classStr : Class → String
  | .added => "added" | .updated => "updated" | .removed => "removed" | .nothing => "nothing"

/-! ## JSON -/

def jnum (n : Option Nat) : String := match n with | some x => toString x | none => "null"
def jbool (b : Bool) : String := if b then "true" else "false"
def jstr (s : String) : String := "\"" ++ s ++ "\""
def jarr (xs : List String) : String := "[" ++ ", ".intercalate xs ++ "]"
def jobj (kv : List (String × String)) : String :=
  "{" ++ ", ".intercalate (kv.map fun (k, v) => jstr k ++ ": " ++ v) ++ "}"

def jentry (k : Nat) (i : Info Nat) : String :=
  jobj [("key", jstr (keyName k)), ("gen", toString i.gen), ("del", jbool i.del), ("pred", jnum i.pred)]

def jspan (sp : PSpan) : String :=
  let keys := (List.range nkeys).filterMap fun k =>
    let vs := versions sp k
    match vs.getLast? with
    | none => none
    | some oldest => some (jstr (keyName k) ++ ": " ++ jobj [
        ("versions", jarr (vs.map fun v => jarr [toString v.gen, jbool v.del])),
        ("pred", jnum oldest.pred)])
  jobj [("a", toString sp.a), ("b", toString sp.b),
    ("segments", jarr (sp.segs.map fun s => toString s.start)),
    ("keys", "{" ++ ", ".intercalate keys ++ "}")]

/-- Every answer, from the spans, checked against the history. -/
def answers (st : St) : Except String String := do
  -- The invariant: every live endpoint starts a segment.
  for e in st.live do
    unless st.spans.any (fun sp => sp.segs.any (·.start == e)) || e == st.head + 1 do
      throw s!"endpoint {e} starts no segment"
  let keyObj (f : Nat → Option Nat) : String :=
    "{" ++ ", ".intercalate ((List.range nkeys).map fun k => jstr (keyName k) ++ ": " ++ jnum (f k)) ++ "}"
  -- Lookups.
  for k in List.range nkeys do
    unless lookupHead st k == stateAfter st k st.head do throw s!"lookup at head, key {k}"
  let mut atl := []
  for e in st.live do
    for k in List.range nkeys do
      unless liveOf (newestIn (allVersions st k) 0 (gen e)) == stateBefore st k e do
        throw s!"lookup at {e}, key {k}"
    atl := atl ++ [jobj [("endpoint", toString e), ("gen", toString (gen e)),
      ("keys", keyObj fun k => liveOf (newestIn (allVersions st k) 0 (gen e)))]]
  -- Changes and read-ahead, for every reserved P < N + 1.
  let ends := (st.live ++ [st.head + 1]).eraseDups
  let mut ch := []
  let mut ra := []
  for P in st.live do
    for Q in ends do
      if P < Q then
        let N := Q - 1
        let mut cls := []
        for k in List.range nkeys do
          let vs := allVersions st k
          let inRange := newestIn vs (gen P) (gen Q)
          let before := (liveOf (newestIn vs 0 (gen P))).isSome
          let model := inRange.map fun v => classify before (!v.del)
          let truth := if touched st k P N then
              some (classify (stateBefore st k P).isSome (stateAfter st k N).isSome) else none
          unless model == truth do throw s!"changes({P}, {N}), key {k}"
          if let some c := model then cls := cls ++ [jstr (keyName k) ++ ": " ++ jstr (classStr c)]
          for r in List.range (N + 1) do
            if P ≤ r then
              let delivered := (stateAfter st k r).isSome
              let rule := match inRange with
                | none => none
                | some v => if v.gen ≤ gen r then none else some (classify delivered (!v.del))
              let truthR := if touched st k (r + 1) N then
                  some (classify delivered (stateAfter st k N).isSome) else none
              unless rule == truthR do throw s!"read-ahead P={P} N={N} r={r} key {k}"
              ra := ra ++ [jobj [("P", toString P), ("N", toString N), ("key", jstr (keyName k)),
                ("r", toString r), ("r_gen", toString (gen r)),
                ("delivered", jstr (if delivered then "live" else "absent")),
                ("expected", jstr ((rule.map classStr).getD "skip"))]]
        ch := ch ++ [jobj [("P", toString P), ("N", toString N), ("from_gen", toString (gen P)),
          ("to_gen", toString (gen Q)), ("classes", "{" ++ ", ".intercalate cls ++ "}")]]
  return jobj [("head", toString st.head),
    ("live", jarr (st.live.map fun e => jobj [("endpoint", toString e), ("gen", toString (gen e))])),
    ("spans", jarr (st.spans.map jspan)),
    ("lookup_head", keyObj (lookupHead st)),
    ("lookup_at", jarr atl), ("changes", jarr ch), ("read_ahead", jarr ra)]

/-! ## Random histories -/

structure Rng where
  s : UInt64

def Rng.next (r : Rng) : Nat × Rng :=
  let x := r.s ^^^ (r.s <<< 13)
  let x := x ^^^ (x >>> 7)
  let x := x ^^^ (x <<< 17)
  (x.toNat, ⟨x⟩)

def Rng.below (r : Rng) (n : Nat) : Nat × Rng :=
  let (x, r) := r.next
  (x % (max n 1), r)

def history (id : Nat) (rng : Rng) : Except String (String × Rng) := do
  let mut rng := rng
  let (nc, r) := rng.below 5; rng := r
  let commits := 6 + nc
  let mut st : St := ⟨[], 0, [], #[]⟩
  let mut steps := []
  for c in List.range commits do
    -- A commit: exact entries, touching each key with probability 1/2.
    let mut ents := []
    for k in List.range nkeys do
      let (t, r) := rng.below 2; rng := r
      let now := if c == 0 then none else stateAfter st k (c - 1)
      let (d, r) := rng.below 3; rng := r
      let e : Option (Info Nat) :=
        if t == 0 then none
        else match now with
          | some g => some ⟨gen c, d == 0, some g⟩
          | none => some ⟨gen c, false, none⟩
      ents := ents ++ [e]
    let newSpan : PSpan := ⟨[⟨c, c, ents⟩]⟩
    st := { st with hist := st.hist.push ents, head := c, spans := st.spans ++ [newSpan] }
    let jents := (List.range nkeys).filterMap fun k => (ent ents k).map (jentry k)
    steps := steps ++ [jobj [("op", jstr "commit"), ("c", toString c), ("gen", toString (gen c)),
      ("entries", jarr jents), ("state", ← answers st)]]
    -- Maybe an endpoint born at the head + 1.
    let (b, r) := rng.below 2; rng := r
    if b == 0 && c + 1 < commits then
      st := { st with live := st.live ++ [c + 1] }
      steps := steps ++ [jobj [("op", jstr "birth"), ("endpoint", toString (c + 1)),
        ("gen", toString (gen (c + 1))), ("state", ← answers st)]]
    -- Maybe a retirement.
    let (x, r) := rng.below 3; rng := r
    if x == 0 && st.live.length > 0 then
      let (i, r) := rng.below st.live.length; rng := r
      let e := st.live.getD i 0
      st := { st with live := st.live.erase e }
      steps := steps ++ [jobj [("op", jstr "retire"), ("endpoint", toString e),
        ("gen", toString (gen e)), ("state", ← answers st)]]
    -- Maybe a merge of 1 to 3 adjacent spans (1: a rewrite coalescing retired endpoints).
    let (m, r) := rng.below 3; rng := r
    if m != 0 then
      let n := st.spans.length
      let (cnt, r) := rng.below 3; rng := r
      let cnt := min (cnt + 1) n
      let (i, r) := rng.below (n - cnt + 1); rng := r
      let ins := (st.spans.drop i).take cnt
      let out := mergeSpans st.live ins
      st := { st with spans := st.spans.take i ++ [out] ++ st.spans.drop (i + cnt) }
      steps := steps ++ [jobj [("op", jstr "merge"),
        ("inputs", jarr (ins.map fun sp => jobj [("a", toString sp.a), ("b", toString sp.b)])),
        ("live", jarr (st.live.map fun e => toString (gen e))),
        ("base", jbool (out.a == 0)), ("state", ← answers st)]]
  return (jobj [("history", toString id), ("gen_rule", jstr "gen(c) = 10 * (c + 1)"),
    ("keys", jarr ((List.range nkeys).map fun k => jstr (keyName k))),
    ("steps", jarr steps)], rng)

end Vectors

def main (args : List String) : IO UInt32 := do
  let n := (args.getD 0 "100").toNat!
  let seed := (args.getD 1 "1").toNat!
  let mut rng : Vectors.Rng := ⟨(seed * 2654435761 + 88172645463325252).toUInt64⟩
  for i in List.range n do
    match Vectors.history i rng with
    | .ok (line, r) => IO.println line; rng := r
    | .error e => IO.eprintln s!"history {i}: the model disagrees with itself: {e}"; return 1
  return 0
