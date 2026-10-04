-- The Lean model's merge, compiled, for the Rust differential harness
-- (../bend/harness): `run [--threads N] IN OUT fold|tree`, the same word
-- format as ../bend/delta/run.bend. Input entries carry a predecessor iff
-- their flag bit 1 is set; output bit 1 is "names a predecessor".
import KeyIndex

open Keys Delta

def word (b : ByteArray) (i : Nat) : Nat :=
  b[4*i]!.toNat ||| (b[4*i+1]!.toNat <<< 8) ||| (b[4*i+2]!.toNat <<< 16) ||| (b[4*i+3]!.toNat <<< 24)

def readDelta (b : ByteArray) : Nat → Nat → Delta.Delta Nat → Delta.Delta Nat
  | 0, _, acc => acc
  | n + 1, i, acc =>
    let k := word b i * 4294967296 + word b (i+1)
    let g := word b (i+2) * 4294967296 + word b (i+3)
    let f := word b (i+4)
    readDelta b n (i + 5) (.cons k ⟨g, f &&& 1 != 0, if f &&& 2 != 0 then some 0 else none⟩ acc)

def readCommits (b : ByteArray) : Nat → Nat → List (Delta.Delta Nat) → List (Delta.Delta Nat)
  | 0, _, acc => acc.reverse
  | n + 1, i, acc =>
    let len := word b i
    readCommits b n (i + 1 + 5 * len) (readDelta b len (i + 1) .nil :: acc)

def fold (cs : List (Delta.Delta Nat)) : Delta.Delta Nat := cs.foldl merge .nil

def tree : Nat → List (Delta.Delta Nat) → Delta.Delta Nat
  | 0, ds => fold ds
  | _ + 1, [] => .nil
  | _ + 1, [d] => d
  | f + 1, ds => merge (tree f (ds.take (ds.length / 2))) (tree f (ds.drop (ds.length / 2)))

def put (out : ByteArray) (x : Nat) : ByteArray :=
  out.push (x % 256).toUInt8 |>.push (x / 256 % 256).toUInt8
    |>.push (x / 65536 % 256).toUInt8 |>.push (x / 16777216 % 256).toUInt8

def emit : Delta.Delta Nat → ByteArray → ByteArray
  | .nil, out => out
  | .cons k i r, out =>
    let f := (if i.del then 1 else 0) + (if i.pred.isSome then 2 else 0)
    emit r (put (put (put (put (put out (k / 4294967296)) (k % 4294967296)) (i.gen / 4294967296)) (i.gen % 4294967296)) f)

def main (args : List String) : IO Unit := do
  let args := match args with
    | "--threads" :: _ :: rest => rest
    | a => a
  let [inp, out, mode] := args | throw (IO.userError "usage: run IN OUT fold|tree")
  let b ← IO.FS.readBinFile inp
  let cs := readCommits b (word b 0) 1 []
  let s := if mode == "tree" then tree 32 cs else fold cs
  IO.FS.writeBinFile out (emit s (put ByteArray.empty 0))
