# The key index's delta algebra (docs/key-index-format.md, K44's net delta).
#
# A delta holds, per key, what a run of commits did to it: the key's
# generation and deleted flag after the run, and whether it was live
# before. Two adjacent deltas merge into one: the older entry's "before"
# and the newer entry's state. A state is the set of live keys, with their
# generations. Keys are Nat (48 bits at runtime); the generation is any
# Data type G: the algebra never looks at it (the newer entry wins by
# position, not by comparing generations), so it is a parameter.

import Base

# Per key
# -------

# A delta's entry for one key, less the key.
type Info<-G: Data> is Data:
  Info{gen: G, del: Bool, bef: Bool}

# The older entry's "before", the newer entry's state.
def comb(-G: Data, o: Info<G>, n: Info<G>) -> Info<G>:
  match o n:
    case Info{go, dO, bo} Info{gn, dn, bn}:
      Info{gn, dn, bo}

type Class is Data:
  Added{}
  Updated{}
  Removed{}
  Nothing{}

# absent before, live after: added; live at both: updated; live before,
# absent after: removed; absent at both: nothing
def classify(bef: Bool, del: Bool) -> Class:
  match bef del:
    case False{} False{}:
      Added{}
    case True{} False{}:
      Updated{}
    case True{} True{}:
      Removed{}
    case False{} True{}:
      Nothing{}

def class(-G: Data, i: Info<G>) -> Class:
  match i:
    case Info{g, d, b}:
      classify(b, d)

# Deltas
# ------

# Entries by key, strictly increasing (Sorted, below; the merge itself
# does not need it).
type Delta<-G: Data> is Data:
  DNil{}
  DCon{key: Nat, info: Info<G>, rest: Delta<G>}

# How the heads' keys compare; EQ when either is empty (unused then).
def heads(-G: Data, +a: Delta<G>, +b: Delta<G>) -> Cmp:
  match a b:
    case DCon{ka, ia, ra} DCon{kb, ib, rb}:
      Nat.cmp(ka, kb)
    case _ _:
      EQ{}

# The merge of an older delta a and a newer b, given c = heads(a, b): the
# comparison comes in as an argument, as a match cannot inspect a call.
def mrg(-G: Data, +a: Delta<G>, +b: Delta<G>, c: Cmp) -> Delta<G>:
  match a b:
    case DNil{} _:
      b
    case _ DNil{}:
      a
    case DCon{ka, ia, ra} DCon{kb, ib, rb}:
      match c:
        case LT{}:
          DCon{ka, ia, mrg(G, ra, b, heads(G, ra, b))}
        case GT{}:
          DCon{kb, ib, mrg(G, a, rb, heads(G, a, rb))}
        case EQ{}:
          DCon{ka, comb(G, ia, ib), mrg(G, ra, rb, heads(G, ra, rb))}

def merge(-G: Data, +a: Delta<G>, +b: Delta<G>) -> Delta<G>:
  mrg(G, a, b, heads(G, a, b))

# Commits merged one by one, oldest first, onto acc.
def fold(-G: Data, ds: List<&2, Delta<G>>, +acc: Delta<G>) -> Delta<G>:
  match ds:
    case Nil{}:
      acc
    case Con{d, rest}:
      fold(G, rest, merge(G, acc, d))

def summary(-G: Data, ds: List<&2, Delta<G>>) -> Delta<G>:
  fold(G, ds, DNil{})

# Each group of commits summarised on its own.
def summaries(-G: Data, gs: List<&2, List<&2, Delta<G>>>) -> List<&2, Delta<G>>:
  match gs:
    case Nil{}:
      Nil{}
    case Con{g, rest}:
      summary(G, g) <> summaries(G, rest)

def concat(-G: Data, gs: List<&2, List<&2, Delta<G>>>) -> List<&2, Delta<G>>:
  List.concat(&2, Delta<G>, gs)

type Change is Data:
  Change{key: Nat, class: Class}

# Each entry's key and class.
def classes(-G: Data, d: Delta<G>) -> List<&2, Change>:
  match d:
    case DNil{}:
      Nil{}
    case DCon{k, i, rest}:
      Change{k, class(G, i)} <> classes(G, rest)

def bump(b: Bool, n: Nat) -> Nat:
  match b:
    case True{}:
      1n+n
    case False{}:
      n

def is_added(c: Class) -> Bool:
  match c:
    case Added{}:
      True{}
    case _:
      False{}

def is_removed(c: Class) -> Bool:
  match c:
    case Removed{}:
      True{}
    case _:
      False{}

def added(-G: Data, d: Delta<G>) -> Nat:
  match d:
    case DNil{}:
      0n
    case DCon{k, i, rest}:
      bump(is_added(class(G, i)), added(G, rest))

def removed(-G: Data, d: Delta<G>) -> Nat:
  match d:
    case DNil{}:
      0n
    case DCon{k, i, rest}:
      bump(is_removed(class(G, i)), removed(G, rest))

# States
# ------

# The live keys, with their generations, strictly increasing by key.
type State<-G: Data> is Data:
  StNil{}
  StCon{key: Nat, gen: G, rest: State<G>}

def size(-G: Data, s: State<G>) -> Nat:
  match s:
    case StNil{}:
      0n
    case StCon{k, g, rest}:
      1n+size(G, rest)

# The key in front of s, if the entry leaves it live.
def put(-G: Data, k: Nat, i: Info<G>, s: State<G>) -> State<G>:
  match i:
    case Info{g, d, b}:
      match d:
        case True{}:
          s
        case False{}:
          StCon{k, g, s}

def lives(-G: Data, d: Delta<G>) -> State<G>:
  match d:
    case DNil{}:
      StNil{}
    case DCon{k, i, rest}:
      put(G, k, i, lives(G, rest))

def sheads(-G: Data, +s: State<G>, +d: Delta<G>) -> Cmp:
  match s d:
    case StCon{ks, gs, rs} DCon{kd, id, rd}:
      Nat.cmp(ks, kd)
    case _ _:
      EQ{}

# The state after a delta, given c = sheads(s, d).
def app(-G: Data, +s: State<G>, +d: Delta<G>, c: Cmp) -> State<G>:
  match s d:
    case StNil{} _:
      lives(G, d)
    case _ DNil{}:
      s
    case StCon{ks, gs, rs} DCon{kd, id, rd}:
      match c:
        case LT{}:
          StCon{ks, gs, app(G, rs, d, sheads(G, rs, d))}
        case GT{}:
          put(G, kd, id, app(G, s, rd, sheads(G, s, rd)))
        case EQ{}:
          put(G, kd, id, app(G, rs, rd, sheads(G, rs, rd)))

def apply(-G: Data, +s: State<G>, +d: Delta<G>) -> State<G>:
  app(G, s, d, sheads(G, s, d))

# Proof that a boolean holds, or fails.
def Is(b: Bool) -> Type:
  match b:
    case True{}:
      Unit
    case False{}:
      Empty

def Bef(-G: Data, i: Info<G>, want: Bool) -> Type:
  match i:
    case Info{g, d, b}:
      Is(Bool.not(Bool.xor(b, want)))

def NoneBefore(-G: Data, d: Delta<G>) -> Type:
  match d:
    case DNil{}:
      Unit
    case DCon{k, i, rest}:
      (Bef(G, i, False{}) & NoneBefore(G, rest))

# A delta's "before" agrees with the state it applies to, walking both as
# app does: an entry of a key the state holds says it was live, any other
# that it was not.
def Cons(-G: Data, +s: State<G>, +d: Delta<G>, c: Cmp) -> Type:
  match s d:
    case StNil{} _:
      NoneBefore(G, d)
    case _ DNil{}:
      Unit
    case StCon{ks, gs, rs} DCon{kd, id, rd}:
      match c:
        case LT{}:
          Cons(G, rs, d, sheads(G, rs, d))
        case GT{}:
          (Bef(G, id, False{}) & Cons(G, s, rd, sheads(G, s, rd)))
        case EQ{}:
          (Bef(G, id, True{}) & Cons(G, rs, rd, sheads(G, rs, rd)))

def Consistent(-G: Data, +s: State<G>, +d: Delta<G>) -> Type:
  Cons(G, s, d, sheads(G, s, d))

# Keys
# ----

# How k compares with d's first key; EQ when d is empty (unused then).
def lhead(-G: Data, +k: Nat, +d: Delta<G>) -> Cmp:
  match d:
    case DNil{}:
      EQ{}
    case DCon{kd, i, r}:
      Nat.cmp(k, kd)

# k's entry in a sorted delta, given c = lhead(k, d): the walk stops at the
# first key past k.
def lk(-G: Data, +k: Nat, +d: Delta<G>, c: Cmp) -> Maybe<&2, Info<G>>:
  match d:
    case DNil{}:
      None{}
    case DCon{kd, i, r}:
      match c:
        case LT{}:
          None{}
        case EQ{}:
          Some{i}
        case GT{}:
          lk(G, k, r, lhead(G, k, r))

def lookup(-G: Data, +k: Nat, +d: Delta<G>) -> Maybe<&2, Info<G>>:
  lk(G, k, d, lhead(G, k, d))

# A key's newer entry e (if any) over its older one m: comb when both are there.
def over(-G: Data, e: Maybe<&2, Info<G>>, m: Maybe<&2, Info<G>>) -> Maybe<&2, Info<G>>:
  match e:
    case None{}:
      m
    case Some{i}:
      match m:
        case None{}:
          Some{i}
        case Some{j}:
          Some{comb(G, j, i)}

# A key's presence after a commit's entry for it (None: untouched).
def step(-G: Data, p: Bool, e: Maybe<&2, Info<G>>) -> Bool:
  match e:
    case None{}:
      p
    case Some{i}:
      match i:
        case Info{g, d, b}:
          Bool.not(d)

# Key k's presence after commits cs, from p.
def pres(-G: Data, cs: List<&2, Delta<G>>, p: Bool, +k: Nat) -> Bool:
  match cs:
    case Nil{}:
      p
    case Con{c, rest}:
      pres(G, rest, step(G, p, lookup(G, k, c)), k)

def BefM(-G: Data, p: Bool, e: Maybe<&2, Info<G>>) -> Type:
  match e:
    case None{}:
      Unit
    case Some{i}:
      Bef(G, i, p)

# Every commit's entry for k, if any, says what k's presence was before it.
def Ok(-G: Data, cs: List<&2, Delta<G>>, +p: Bool, +k: Nat) -> Type:
  match cs:
    case Nil{}:
      Unit
    case Con{c, rest}:
      (BefM(G, p, lookup(G, k, c)) & Ok(G, rest, step(G, p, lookup(G, k, c)), k))

# A range's entry for a key against its presence p at the start and q at
# the end: an entry's class is the change from p to q; no entry, no change.
def ClassAgrees(-G: Data, p: Bool, q: Bool, m: Maybe<&2, Info<G>>) -> Type:
  match m:
    case None{}:
      {p == q : Bool}
    case Some{i}:
      {class(G, i) == classify(p, Bool.not(q)) : Class}

# The same, on the entry's two bits: "before" is the presence at the start,
# the state the presence at the end.
def Agrees(-G: Data, p: Bool, q: Bool, m: Maybe<&2, Info<G>>) -> Type:
  match m:
    case None{}:
      {p == q : Bool}
    case Some{i}:
      match i:
        case Info{g, d, b}:
          ({b == p : Bool} & {Bool.not(d) == q : Bool})

# Ranges, in parallel
# -------------------

def take(-G: Data, n: Nat, ds: List<&2, Delta<G>>) -> List<&2, Delta<G>>:
  match n ds:
    case 0n _:
      Nil{}
    case 1n+m Nil{}:
      Nil{}
    case 1n+m Con{d, rest}:
      d <> take(G, m, rest)

def drop(-G: Data, n: Nat, ds: List<&2, Delta<G>>) -> List<&2, Delta<G>>:
  match n ds:
    case 0n _:
      ds
    case 1n+m Nil{}:
      Nil{}
    case 1n+m Con{d, rest}:
      drop(G, m, rest)

# The summary of commits ds as a balanced tree of merges, both halves at
# once; associativity is what makes it the one-by-one summary. Past `fuel`
# levels it summarises one by one.
def tree(-G: Data, +fuel: Nat, +ds: List<&2, Delta<G>>) -> Delta<G>:
  match fuel:
    case 0n:
      summary(G, ds)
    case 1n+f:
      match ds:
        case Nil{}:
          DNil{}
        case Con{d, Nil{}}:
          d
        case _:
          +h = Nat.div(List.length(&2, Delta<G>, ds), 2n)
          l r = tree(G, f, take(G, h, ds)) tree(G, f, drop(G, h, ds))
          merge(G, l, r)
