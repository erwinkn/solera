# Runs the proven merge on commits Rust wrote, for the differential test and
# the throughput comparison (../harness).
#
#   run IN OUT fold   merge IN's commits one by one, oldest first, write the
#                     summary to OUT
#   run IN OUT tree   the same as a parallel balanced tree of merges
#
# IN, as u32 words: the number of commits; per commit its entry count, then
# its entries in descending key order, five words each: key's high and low
# halves, generation's high and low halves, flags (bit 0 deleted, bit 1
# live before). OUT: the summary's entries, ascending, in the same five
# words, after a header of the CPU time the merge took in microseconds.

import Base
import ./main.bend as M

def Bx.load(path: String) -> IO(U32):
  import "./io.c"
  import "./io.js"

def Bx.word(i: U32) -> IO(U32):
  import "./io.c"
  import "./io.js"

def Bx.put(x: U32) -> IO(Unit):
  import "./io.c"
  import "./io.js"

def Bx.save(path: String) -> IO(Unit):
  import "./io.c"
  import "./io.js"

def Bx.cpu() -> IO(U32):
  import "./io.c"
  import "./io.js"

def Bx.micros() -> IO(U32):
  import "./io.c"
  import "./io.js"

# A 64-bit generation, as two words.
type Gen is Data:
  Gen{hi: U32, lo: U32}

def two32() -> Nat:
  Nat.mul(65536n, 65536n)

def is_set(x: U32, bit: U32) -> Bool:
  Bool.not(U32.is_eq((x .&. bit : U32), 0))

# n entries from word i on, consed onto acc (the file lists them backwards).
def read_delta(n: Nat, +i: U32, acc: M.Delta<Gen>) -> IO(M.Delta<Gen>):
  match n:
    case 0n:
      IO.pure(M.Delta<Gen>, acc)
    case 1n+m:
      do IO<M.Delta<Gen>>:
        kh : U32 <- Bx.word(i)
        kl : U32 <- Bx.word((i + 1 : U32))
        gh : U32 <- Bx.word((i + 2 : U32))
        gl : U32 <- Bx.word((i + 3 : U32))
        +fl : U32 <- Bx.word((i + 4 : U32))
        k : Nat = Nat.add(Nat.mul(U32.to_nat(kh), two32()), U32.to_nat(kl))
        e : M.Info<Gen> = M.Info{Gen{gh, gl}, is_set(fl, 1), is_set(fl, 2)}
        read_delta(m, (i + 5 : U32), M.DCon{k, e, acc})

# n commits from word i on, consed onto acc (oldest last); answers the
# commits oldest first.
def read_commits(n: Nat, +i: U32, acc: List<&2, M.Delta<Gen>>) -> IO(List<&2, M.Delta<Gen>>):
  match n:
    case 0n:
      IO.pure(List<&2, M.Delta<Gen>>, List.reverse(&2, M.Delta<Gen>, acc))
    case 1n+m:
      do IO<List<&2, M.Delta<Gen>>>:
        +len : U32 <- Bx.word(i)
        d : M.Delta<Gen> <- read_delta(U32.to_nat(len), (i + 1 : U32), M.DNil{})
        read_commits(m, (i + 1 + len * 5 : U32), d <> acc)

def flags(d: Bool, b: Bool) -> U32:
  (Bool.to_u32(d) + Bool.to_u32(b) * 2 : U32)

def write_delta(d: M.Delta<Gen>) -> IO(Unit):
  match d:
    case M.DNil{}:
      IO.pure(Unit, Unit{})
    case M.DCon{k, M.Info{Gen{gh, gl}, del, bef}, rest}:
      do IO<Unit>:
        +kk : Nat = k
        Bx.put(U32.from_nat(Nat.div(kk, two32())))
        Bx.put(U32.from_nat(Nat.mod(kk, two32())))
        Bx.put(gh)
        Bx.put(gl)
        Bx.put(flags(del, bef))
        write_delta(rest)

# Entries and an order-sensitive checksum, so the merge is done before the
# clock stops.
def check(d: M.Delta<Gen>, acc: U32) -> U32:
  match d:
    case M.DNil{}:
      acc
    case M.DCon{k, M.Info{Gen{gh, gl}, del, bef}, rest}:
      check(rest, (acc * 31 + gl + flags(del, bef) : U32))

# The commits' summary: one by one ("fold"), or a parallel balanced tree.
def reduce(tree: Bool, +cs: List<&2, M.Delta<Gen>>) -> M.Delta<Gen>:
  match tree:
    case True{}:
      M.tree(Gen, 32n, cs)
    case False{}:
      M.summary(Gen, cs)

def go(args: List<String>) -> IO(Unit):
  match args:
    case Con{_, Con{inp, Con{out, Con{mode, _}}}}:
      do IO<Unit>:
        n : U32 <- Bx.load(inp)
        +count : U32 <- Bx.word(0)
        +cs : List<&2, M.Delta<Gen>> <- read_commits(U32.to_nat(count), 1, Nil{})
        +t0 : U32 <- Bx.micros()
        +c0 : U32 <- Bx.cpu()
        +s : M.Delta<Gen> = reduce(String.eq(mode, "tree"), cs)
        IO.print_err("checksum " ++ U32.show(check(s, 0)))
        +t1 : U32 <- Bx.micros()
        +c1 : U32 <- Bx.cpu()
        IO.print_err("merge wall us " ++ U32.show((t1 - t0 : U32)) ++ ", cpu us " ++ U32.show((c1 - c0 : U32)))
        Bx.put((c1 - c0 : U32))
        write_delta(s)
        Bx.save(out)
    case _:
      IO.die(Unit, 2, "usage: run IN OUT fold|tree")

def main() -> IO(Unit):
  do IO<Unit>:
    args : List<String> <- IO.args()
    go(args)
