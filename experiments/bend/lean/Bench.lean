-- Times Delta.lean's merge, compiled by Lean to C, on the harness's input
-- (../harness): `bench IN OUT fold|tree`, OUT as run.bend writes it.
import Delta

open Delta

def word (b : ByteArray) (i : Nat) : UInt32 :=
  b[4*i]!.toUInt32 ||| (b[4*i+1]!.toUInt32 <<< 8) ||| (b[4*i+2]!.toUInt32 <<< 16) ||| (b[4*i+3]!.toUInt32 <<< 24)

/-- n entries from word i on, consed onto acc (the file lists them backwards). -/
def readDelta (b : ByteArray) : Nat → Nat → Delta UInt64 → Delta UInt64
  | 0, _, acc => acc
  | n + 1, i, acc =>
    let k := (word b i).toNat * 4294967296 + (word b (i+1)).toNat
    let g := ((word b (i+2)).toUInt64 <<< 32) ||| (word b (i+3)).toUInt64
    let f := word b (i+4)
    readDelta b n (i + 5) (cons k ⟨g, f &&& 1 != 0, f &&& 2 != 0⟩ acc)

def readCommits (b : ByteArray) : Nat → Nat → List (Delta UInt64) → List (Delta UInt64)
  | 0, _, acc => acc.reverse
  | n + 1, i, acc =>
    let len := (word b i).toNat
    readCommits b n (i + 1 + 5 * len) (readDelta b len (i + 1) nil :: acc)

def check : Delta UInt64 → UInt32 → UInt32
  | nil, acc => acc
  | cons _ i r, acc => check r (acc * 31 + i.gen.toUInt32 + (if i.del then 1 else 0) + (if i.bef then 2 else 0))

def main (args : List String) : IO Unit := do
  let [inp, _out, mode] := args | throw (IO.userError "usage: bench IN OUT fold|tree")
  let b ← IO.FS.readBinFile inp
  let cs := readCommits b (word b 0).toNat 1 []
  -- Force the input before the clock starts.
  IO.eprintln s!"commits {cs.length} entries {cs.foldl (fun n d => n + (classes d).length) 0}"
  let t0 ← IO.monoNanosNow
  let s := if mode == "tree" then tree 32 cs else summary cs
  IO.eprintln s!"checksum {check s 0}"
  let t1 ← IO.monoNanosNow
  IO.println s!"merge us {(t1 - t0) / 1000}"
