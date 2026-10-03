# The delta algebra's proofs.

import Base
import ./main.bend as M
import ./LAWS.bend as Laws

def Laws.merge_nil_l(G, a):
  match a:
    case M.DNil{}:
      {==}
    case M.DCon{k, i, r}:
      {==}

def Laws.merge_nil_r(G, a):
  match a:
    case M.DNil{}:
      {==}
    case M.DCon{k, i, r}:
      {==}

# Arithmetic
# ----------

def add_succ(a: Nat, -b: Nat) -> {1n+Nat.add(a, b) == Nat.add(a, 1n+b) : Nat}:
  match a:
    case 0n:
      {==}
    case 1n+p:
      %add_succ(p, b) : {2n+Nat.add(p, b) == 1n+_ : Nat}
      {==}

# l's successor, given l is that of m + n.
def succ_shift(-l: Nat, +m: Nat, -n: Nat, e: {l == 1n+Nat.add(m, n) : Nat}) -> {1n+l == 1n+Nat.add(m, 1n+n) : Nat}:
  %add_succ(m, n) : {1n+l == 1n+_ : Nat}
  %e : {1n+l == 1n+_ : Nat}
  {==}

def succ_eq(-a: Nat, -b: Nat, e: {a == b : Nat}) -> {1n+a == 1n+b : Nat}:
  %e : {1n+a == 1n+_ : Nat}
  {==}

# (d) live count
# --------------

# From the empty state, a delta whose entries were all absent before.
def lives_count(-G: Data, +d: M.Delta<G>, h: M.NoneBefore(G, d))
  -> {Nat.add(M.size(G, M.lives(G, d)), M.removed(G, d)) == M.added(G, d) : Nat}:
  match d:
    case M.DNil{}:
      {==}
    case M.DCon{k, i, r}:
      match i:
        case M.Info{g, dl, b}:
          match dl b:
            case _ True{}:
              (hb, hr) = h
              match hb:
            case True{} False{}:
              (hb, hr) = h
              lives_count(G, r, hr)
            case False{} False{}:
              (hb, hr) = h
              succ_eq(Nat.add(M.size(G, M.lives(G, r)), M.removed(G, r)), M.added(G, r), lives_count(G, r, hr))

# Any c: Cons walks as app does, whatever the comparisons say.
def count_go(-G: Data, +s: M.State<G>, +d: M.Delta<G>, c: Cmp, h: M.Cons(G, s, d, c))
  -> {Nat.add(M.size(G, M.app(G, s, d, c)), M.removed(G, d)) == Nat.add(M.size(G, s), M.added(G, d)) : Nat}:
  match s d:
    case M.StNil{} M.DNil{}:
      {==}
    case M.StNil{} M.DCon{kd, id, rd}:
      lives_count(G, M.DCon{kd, id, rd}, h)
    case M.StCon{ks, gs, rs} M.DNil{}:
      {==}
    case M.StCon{ks, gs, rs} M.DCon{kd, M.Info{g, dl, b}, rd}:
      match dl b c:
        case _ _ LT{}:
          +d = {M.DCon{kd, M.Info{g, dl, b}, rd} : M.Delta<G>}
          succ_eq(Nat.add(M.size(G, M.apply(G, rs, d)), M.removed(G, d)), Nat.add(M.size(G, rs), M.added(G, d)), count_go(G, rs, d, M.sheads(G, rs, d), h))
        case _ True{} GT{}:
          (hb, hr) = h
          match hb:
        case True{} False{} GT{}:
          (hb, hr) = h
          +s = {M.StCon{ks, gs, rs} : M.State<G>}
          count_go(G, s, rd, M.sheads(G, s, rd), hr)
        case False{} False{} GT{}:
          (hb, hr) = h
          +s = {M.StCon{ks, gs, rs} : M.State<G>}
          succ_shift(Nat.add(M.size(G, M.apply(G, s, rd)), M.removed(G, rd)), M.size(G, rs), M.added(G, rd), count_go(G, s, rd, M.sheads(G, s, rd), hr))
        case _ False{} EQ{}:
          (hb, hr) = h
          match hb:
        case True{} True{} EQ{}:
          (hb, hr) = h
          +x = M.size(G, M.apply(G, rs, rd))
          +r = M.removed(G, rd)
          %add_succ(x, r) : {_ == 1n+Nat.add(M.size(G, rs), M.added(G, rd)) : Nat}
          succ_eq(Nat.add(x, r), Nat.add(M.size(G, rs), M.added(G, rd)), count_go(G, rs, rd, M.sheads(G, rs, rd), hr))
        case False{} True{} EQ{}:
          (hb, hr) = h
          succ_eq(Nat.add(M.size(G, M.apply(G, rs, rd)), M.removed(G, rd)), Nat.add(M.size(G, rs), M.added(G, rd)), count_go(G, rs, rd, M.sheads(G, rs, rd), hr))

def Laws.live_count(G, s, d, h):
  count_go(G, s, d, M.sheads(G, s, d), h)

# (a) associativity
# -----------------

def Assoc(-G: Data, a: M.Delta<G>, b: M.Delta<G>, c: M.Delta<G>) -> Type:
  {M.merge(G, M.merge(G, a, b), c) == M.merge(G, a, M.merge(G, b, c)) : M.Delta<G>}

# Which (cmp(x, y), cmp(x, z), cmp(y, z)) can occur: Unit for the 13
# orderings of three keys, Empty for the 14 others.
def Imp(c1: Cmp, c2: Cmp, c3: Cmp) -> Type:
  match c1:
    case LT{}:
      match c2:
        case LT{}:
          Unit
        case EQ{}:
          match c3:
            case GT{}:
              Unit
            case _:
              Empty
        case GT{}:
          match c3:
            case GT{}:
              Unit
            case _:
              Empty
    case EQ{}:
      match c2 c3:
        case LT{} LT{}:
          Unit
        case EQ{} EQ{}:
          Unit
        case GT{} GT{}:
          Unit
        case _ _:
          Empty
    case GT{}:
      match c2:
        case GT{}:
          Unit
        case _:
          match c3:
            case LT{}:
              Unit
            case _:
              Empty

def imp_any_gt_gt(c: Cmp) -> Imp(c, GT{}, GT{}):
  match c:
    case LT{}:
      Unit{}
    case EQ{}:
      Unit{}
    case GT{}:
      Unit{}

def imp_gt_any_lt(c: Cmp) -> Imp(GT{}, c, LT{}):
  match c:
    case LT{}:
      Unit{}
    case EQ{}:
      Unit{}
    case GT{}:
      Unit{}

def tri(x: Nat, y: Nat, z: Nat) -> Imp(Nat.cmp(x, y), Nat.cmp(x, z), Nat.cmp(y, z)):
  match x y z:
    case 0n 0n 0n:
      Unit{}
    case 0n 0n 1n+zp:
      Unit{}
    case 0n 1n+yp 0n:
      Unit{}
    case 0n 1n+yp 1n+zp:
      Unit{}
    case 1n+xp 0n 0n:
      Unit{}
    case 1n+xp 0n 1n+zp:
      imp_gt_any_lt(Nat.cmp(xp, zp))
    case 1n+xp 1n+yp 0n:
      imp_any_gt_gt(Nat.cmp(xp, yp))
    case 1n+xp 1n+yp 1n+zp:
      tri(xp, yp, zp)

# One step of the merge of two non-empty deltas, given how their heads
# compare; _r lemmas read right to left (the merge on the right).
def lt_r(-G: Data, -kx: Nat, -ix: M.Info<G>, -xs: M.Delta<G>, -ky: Nat, -iy: M.Info<G>, -ys: M.Delta<G>, e: {LT{} == Nat.cmp(kx, ky) : Cmp})
  -> {M.DCon{kx, ix, M.merge(G, xs, M.DCon{ky, iy, ys})} == M.merge(G, M.DCon{kx, ix, xs}, M.DCon{ky, iy, ys}) : M.Delta<G>}:
  %e : {M.DCon{kx, ix, M.merge(G, xs, M.DCon{ky, iy, ys})} == M.mrg(G, M.DCon{kx, ix, xs}, M.DCon{ky, iy, ys}, _) : M.Delta<G>}
  {==}

def gt_r(-G: Data, -kx: Nat, -ix: M.Info<G>, -xs: M.Delta<G>, -ky: Nat, -iy: M.Info<G>, -ys: M.Delta<G>, e: {GT{} == Nat.cmp(kx, ky) : Cmp})
  -> {M.DCon{ky, iy, M.merge(G, M.DCon{kx, ix, xs}, ys)} == M.merge(G, M.DCon{kx, ix, xs}, M.DCon{ky, iy, ys}) : M.Delta<G>}:
  %e : {M.DCon{ky, iy, M.merge(G, M.DCon{kx, ix, xs}, ys)} == M.mrg(G, M.DCon{kx, ix, xs}, M.DCon{ky, iy, ys}, _) : M.Delta<G>}
  {==}

def eq_r(-G: Data, -kx: Nat, -ix: M.Info<G>, -xs: M.Delta<G>, -ky: Nat, -iy: M.Info<G>, -ys: M.Delta<G>, e: {EQ{} == Nat.cmp(kx, ky) : Cmp})
  -> {M.DCon{kx, M.comb(G, ix, iy), M.merge(G, xs, ys)} == M.merge(G, M.DCon{kx, ix, xs}, M.DCon{ky, iy, ys}) : M.Delta<G>}:
  %e : {M.DCon{kx, M.comb(G, ix, iy), M.merge(G, xs, ys)} == M.mrg(G, M.DCon{kx, ix, xs}, M.DCon{ky, iy, ys}, _) : M.Delta<G>}
  {==}

# The same, left to right.
def lt_l(-G: Data, -kx: Nat, -ix: M.Info<G>, -xs: M.Delta<G>, -ky: Nat, -iy: M.Info<G>, -ys: M.Delta<G>, e: {LT{} == Nat.cmp(kx, ky) : Cmp})
  -> {M.merge(G, M.DCon{kx, ix, xs}, M.DCon{ky, iy, ys}) == M.DCon{kx, ix, M.merge(G, xs, M.DCon{ky, iy, ys})} : M.Delta<G>}:
  %e : {M.mrg(G, M.DCon{kx, ix, xs}, M.DCon{ky, iy, ys}, _) == M.DCon{kx, ix, M.merge(G, xs, M.DCon{ky, iy, ys})} : M.Delta<G>}
  {==}

def gt_l(-G: Data, -kx: Nat, -ix: M.Info<G>, -xs: M.Delta<G>, -ky: Nat, -iy: M.Info<G>, -ys: M.Delta<G>, e: {GT{} == Nat.cmp(kx, ky) : Cmp})
  -> {M.merge(G, M.DCon{kx, ix, xs}, M.DCon{ky, iy, ys}) == M.DCon{ky, iy, M.merge(G, M.DCon{kx, ix, xs}, ys)} : M.Delta<G>}:
  %e : {M.mrg(G, M.DCon{kx, ix, xs}, M.DCon{ky, iy, ys}, _) == M.DCon{ky, iy, M.merge(G, M.DCon{kx, ix, xs}, ys)} : M.Delta<G>}
  {==}

def eq_l(-G: Data, -kx: Nat, -ix: M.Info<G>, -xs: M.Delta<G>, -ky: Nat, -iy: M.Info<G>, -ys: M.Delta<G>, e: {EQ{} == Nat.cmp(kx, ky) : Cmp})
  -> {M.merge(G, M.DCon{kx, ix, xs}, M.DCon{ky, iy, ys}) == M.DCon{kx, M.comb(G, ix, iy), M.merge(G, xs, ys)} : M.Delta<G>}:
  %e : {M.mrg(G, M.DCon{kx, ix, xs}, M.DCon{ky, iy, ys}, _) == M.DCon{kx, M.comb(G, ix, iy), M.merge(G, xs, ys)} : M.Delta<G>}
  {==}

def comb_assoc(-G: Data, a: M.Info<G>, b: M.Info<G>, c: M.Info<G>)
  -> {M.comb(G, M.comb(G, a, b), c) == M.comb(G, a, M.comb(G, b, c)) : M.Info<G>}:
  match a b c:
    case M.Info{ga, da, ba} M.Info{gb, db, bb} M.Info{gc, dc, bc}:
      {==}

# All three non-empty, given how their heads compare (t: consistently),
# and the induction hypotheses for every smaller triple.
def assoc3(-G: Data, +kx: Nat, +ix: M.Info<G>, +xs: M.Delta<G>, +ky: Nat, +iy: M.Info<G>, +ys: M.Delta<G>, +kz: Nat, +iz: M.Info<G>, +zs: M.Delta<G>,
  c1: Cmp, c13: Cmp, c23: Cmp,
  +e1: {c1 == Nat.cmp(kx, ky) : Cmp}, +e13: {c13 == Nat.cmp(kx, kz) : Cmp}, +e23: {c23 == Nat.cmp(ky, kz) : Cmp}, t: Imp(c1, c13, c23),
  h_xs_b_c: Assoc(G, xs, M.DCon{ky, iy, ys}, M.DCon{kz, iz, zs}),
  h_xs_b_zs: Assoc(G, xs, M.DCon{ky, iy, ys}, zs),
  h_a_b_zs: Assoc(G, M.DCon{kx, ix, xs}, M.DCon{ky, iy, ys}, zs),
  h_xs_ys_c: Assoc(G, xs, ys, M.DCon{kz, iz, zs}),
  h_xs_ys_zs: Assoc(G, xs, ys, zs),
  h_a_ys_c: Assoc(G, M.DCon{kx, ix, xs}, ys, M.DCon{kz, iz, zs}),
  h_a_ys_zs: Assoc(G, M.DCon{kx, ix, xs}, ys, zs))
  -> Assoc(G, M.DCon{kx, ix, xs}, M.DCon{ky, iy, ys}, M.DCon{kz, iz, zs}):
  match c1 c13 c23:
    # x < y < z
    case LT{} LT{} LT{}:
      +a = {M.DCon{kx, ix, xs} : M.Delta<G>}
      +b = {M.DCon{ky, iy, ys} : M.Delta<G>}
      +c = {M.DCon{kz, iz, zs} : M.Delta<G>}
      +rhs = M.merge(G, a, M.merge(G, b, c))
      %lt_r(G, kx, ix, xs, ky, iy, ys, e1) : {M.merge(G, _, c) == rhs : M.Delta<G>}
      %lt_r(G, kx, ix, M.merge(G, xs, b), kz, iz, zs, e13) : {_ == rhs : M.Delta<G>}
      +l = {M.DCon{kx, ix, M.merge(G, M.merge(G, xs, b), c)} : M.Delta<G>}
      %lt_r(G, ky, iy, ys, kz, iz, zs, e23) : {l == M.merge(G, a, _) : M.Delta<G>}
      %lt_r(G, kx, ix, xs, ky, iy, M.merge(G, ys, c), e1) : {l == _ : M.Delta<G>}
      %lt_l(G, ky, iy, ys, kz, iz, zs, e23) : {l == M.DCon{kx, ix, M.merge(G, xs, _)} : M.Delta<G>}
      %h_xs_b_c : {l == M.DCon{kx, ix, _} : M.Delta<G>}
      {==}
    # x < y = z
    case LT{} LT{} EQ{}:
      +a = {M.DCon{kx, ix, xs} : M.Delta<G>}
      +b = {M.DCon{ky, iy, ys} : M.Delta<G>}
      +c = {M.DCon{kz, iz, zs} : M.Delta<G>}
      +rhs = M.merge(G, a, M.merge(G, b, c))
      %lt_r(G, kx, ix, xs, ky, iy, ys, e1) : {M.merge(G, _, c) == rhs : M.Delta<G>}
      %lt_r(G, kx, ix, M.merge(G, xs, b), kz, iz, zs, e13) : {_ == rhs : M.Delta<G>}
      +l = {M.DCon{kx, ix, M.merge(G, M.merge(G, xs, b), c)} : M.Delta<G>}
      %eq_r(G, ky, iy, ys, kz, iz, zs, e23) : {l == M.merge(G, a, _) : M.Delta<G>}
      %lt_r(G, kx, ix, xs, ky, M.comb(G, iy, iz), M.merge(G, ys, zs), e1) : {l == _ : M.Delta<G>}
      %eq_l(G, ky, iy, ys, kz, iz, zs, e23) : {l == M.DCon{kx, ix, M.merge(G, xs, _)} : M.Delta<G>}
      %h_xs_b_c : {l == M.DCon{kx, ix, _} : M.Delta<G>}
      {==}
    # x < z < y
    case LT{} LT{} GT{}:
      +a = {M.DCon{kx, ix, xs} : M.Delta<G>}
      +b = {M.DCon{ky, iy, ys} : M.Delta<G>}
      +c = {M.DCon{kz, iz, zs} : M.Delta<G>}
      +rhs = M.merge(G, a, M.merge(G, b, c))
      %lt_r(G, kx, ix, xs, ky, iy, ys, e1) : {M.merge(G, _, c) == rhs : M.Delta<G>}
      %lt_r(G, kx, ix, M.merge(G, xs, b), kz, iz, zs, e13) : {_ == rhs : M.Delta<G>}
      +l = {M.DCon{kx, ix, M.merge(G, M.merge(G, xs, b), c)} : M.Delta<G>}
      %gt_r(G, ky, iy, ys, kz, iz, zs, e23) : {l == M.merge(G, a, _) : M.Delta<G>}
      %lt_r(G, kx, ix, xs, kz, iz, M.merge(G, b, zs), e13) : {l == _ : M.Delta<G>}
      %gt_l(G, ky, iy, ys, kz, iz, zs, e23) : {l == M.DCon{kx, ix, M.merge(G, xs, _)} : M.Delta<G>}
      %h_xs_b_c : {l == M.DCon{kx, ix, _} : M.Delta<G>}
      {==}
    # x = z < y
    case LT{} EQ{} GT{}:
      +a = {M.DCon{kx, ix, xs} : M.Delta<G>}
      +b = {M.DCon{ky, iy, ys} : M.Delta<G>}
      +c = {M.DCon{kz, iz, zs} : M.Delta<G>}
      +rhs = M.merge(G, a, M.merge(G, b, c))
      %lt_r(G, kx, ix, xs, ky, iy, ys, e1) : {M.merge(G, _, c) == rhs : M.Delta<G>}
      %eq_r(G, kx, ix, M.merge(G, xs, b), kz, iz, zs, e13) : {_ == rhs : M.Delta<G>}
      +l = {M.DCon{kx, M.comb(G, ix, iz), M.merge(G, M.merge(G, xs, b), zs)} : M.Delta<G>}
      %gt_r(G, ky, iy, ys, kz, iz, zs, e23) : {l == M.merge(G, a, _) : M.Delta<G>}
      %eq_r(G, kx, ix, xs, kz, iz, M.merge(G, b, zs), e13) : {l == _ : M.Delta<G>}
      %h_xs_b_zs : {l == M.DCon{kx, M.comb(G, ix, iz), _} : M.Delta<G>}
      {==}
    # z < x < y
    case LT{} GT{} GT{}:
      +a = {M.DCon{kx, ix, xs} : M.Delta<G>}
      +b = {M.DCon{ky, iy, ys} : M.Delta<G>}
      +c = {M.DCon{kz, iz, zs} : M.Delta<G>}
      +rhs = M.merge(G, a, M.merge(G, b, c))
      %lt_r(G, kx, ix, xs, ky, iy, ys, e1) : {M.merge(G, _, c) == rhs : M.Delta<G>}
      %gt_r(G, kx, ix, M.merge(G, xs, b), kz, iz, zs, e13) : {_ == rhs : M.Delta<G>}
      %lt_l(G, kx, ix, xs, ky, iy, ys, e1) : {M.DCon{kz, iz, M.merge(G, _, zs)} == rhs : M.Delta<G>}
      +l = {M.DCon{kz, iz, M.merge(G, M.merge(G, a, b), zs)} : M.Delta<G>}
      %gt_r(G, ky, iy, ys, kz, iz, zs, e23) : {l == M.merge(G, a, _) : M.Delta<G>}
      %gt_r(G, kx, ix, xs, kz, iz, M.merge(G, b, zs), e13) : {l == _ : M.Delta<G>}
      %h_a_b_zs : {l == M.DCon{kz, iz, _} : M.Delta<G>}
      {==}
    # x = y < z
    case EQ{} LT{} LT{}:
      +a = {M.DCon{kx, ix, xs} : M.Delta<G>}
      +b = {M.DCon{ky, iy, ys} : M.Delta<G>}
      +c = {M.DCon{kz, iz, zs} : M.Delta<G>}
      +rhs = M.merge(G, a, M.merge(G, b, c))
      %eq_r(G, kx, ix, xs, ky, iy, ys, e1) : {M.merge(G, _, c) == rhs : M.Delta<G>}
      %lt_r(G, kx, M.comb(G, ix, iy), M.merge(G, xs, ys), kz, iz, zs, e13) : {_ == rhs : M.Delta<G>}
      +l = {M.DCon{kx, M.comb(G, ix, iy), M.merge(G, M.merge(G, xs, ys), c)} : M.Delta<G>}
      %lt_r(G, ky, iy, ys, kz, iz, zs, e23) : {l == M.merge(G, a, _) : M.Delta<G>}
      %eq_r(G, kx, ix, xs, ky, iy, M.merge(G, ys, c), e1) : {l == _ : M.Delta<G>}
      %h_xs_ys_c : {l == M.DCon{kx, M.comb(G, ix, iy), _} : M.Delta<G>}
      {==}
    # x = y = z
    case EQ{} EQ{} EQ{}:
      +a = {M.DCon{kx, ix, xs} : M.Delta<G>}
      +b = {M.DCon{ky, iy, ys} : M.Delta<G>}
      +c = {M.DCon{kz, iz, zs} : M.Delta<G>}
      +rhs = M.merge(G, a, M.merge(G, b, c))
      %eq_r(G, kx, ix, xs, ky, iy, ys, e1) : {M.merge(G, _, c) == rhs : M.Delta<G>}
      %eq_r(G, kx, M.comb(G, ix, iy), M.merge(G, xs, ys), kz, iz, zs, e13) : {_ == rhs : M.Delta<G>}
      +l = {M.DCon{kx, M.comb(G, M.comb(G, ix, iy), iz), M.merge(G, M.merge(G, xs, ys), zs)} : M.Delta<G>}
      %eq_r(G, ky, iy, ys, kz, iz, zs, e23) : {l == M.merge(G, a, _) : M.Delta<G>}
      %eq_r(G, kx, ix, xs, ky, M.comb(G, iy, iz), M.merge(G, ys, zs), e1) : {l == _ : M.Delta<G>}
      %comb_assoc(G, ix, iy, iz) : {l == M.DCon{kx, _, M.merge(G, xs, M.merge(G, ys, zs))} : M.Delta<G>}
      %h_xs_ys_zs : {l == M.DCon{kx, M.comb(G, M.comb(G, ix, iy), iz), _} : M.Delta<G>}
      {==}
    # z < x = y
    case EQ{} GT{} GT{}:
      +a = {M.DCon{kx, ix, xs} : M.Delta<G>}
      +b = {M.DCon{ky, iy, ys} : M.Delta<G>}
      +c = {M.DCon{kz, iz, zs} : M.Delta<G>}
      +rhs = M.merge(G, a, M.merge(G, b, c))
      %eq_r(G, kx, ix, xs, ky, iy, ys, e1) : {M.merge(G, _, c) == rhs : M.Delta<G>}
      %gt_r(G, kx, M.comb(G, ix, iy), M.merge(G, xs, ys), kz, iz, zs, e13) : {_ == rhs : M.Delta<G>}
      %eq_l(G, kx, ix, xs, ky, iy, ys, e1) : {M.DCon{kz, iz, M.merge(G, _, zs)} == rhs : M.Delta<G>}
      +l = {M.DCon{kz, iz, M.merge(G, M.merge(G, a, b), zs)} : M.Delta<G>}
      %gt_r(G, ky, iy, ys, kz, iz, zs, e23) : {l == M.merge(G, a, _) : M.Delta<G>}
      %gt_r(G, kx, ix, xs, kz, iz, M.merge(G, b, zs), e13) : {l == _ : M.Delta<G>}
      %h_a_b_zs : {l == M.DCon{kz, iz, _} : M.Delta<G>}
      {==}
    # y < x, y < z
    case GT{} _ LT{}:
      +a = {M.DCon{kx, ix, xs} : M.Delta<G>}
      +b = {M.DCon{ky, iy, ys} : M.Delta<G>}
      +c = {M.DCon{kz, iz, zs} : M.Delta<G>}
      +rhs = M.merge(G, a, M.merge(G, b, c))
      %gt_r(G, kx, ix, xs, ky, iy, ys, e1) : {M.merge(G, _, c) == rhs : M.Delta<G>}
      %lt_r(G, ky, iy, M.merge(G, a, ys), kz, iz, zs, e23) : {_ == rhs : M.Delta<G>}
      +l = {M.DCon{ky, iy, M.merge(G, M.merge(G, a, ys), c)} : M.Delta<G>}
      %lt_r(G, ky, iy, ys, kz, iz, zs, e23) : {l == M.merge(G, a, _) : M.Delta<G>}
      %gt_r(G, kx, ix, xs, ky, iy, M.merge(G, ys, c), e1) : {l == _ : M.Delta<G>}
      %h_a_ys_c : {l == M.DCon{ky, iy, _} : M.Delta<G>}
      {==}
    # y = z < x
    case GT{} GT{} EQ{}:
      +a = {M.DCon{kx, ix, xs} : M.Delta<G>}
      +b = {M.DCon{ky, iy, ys} : M.Delta<G>}
      +c = {M.DCon{kz, iz, zs} : M.Delta<G>}
      +rhs = M.merge(G, a, M.merge(G, b, c))
      %gt_r(G, kx, ix, xs, ky, iy, ys, e1) : {M.merge(G, _, c) == rhs : M.Delta<G>}
      %eq_r(G, ky, iy, M.merge(G, a, ys), kz, iz, zs, e23) : {_ == rhs : M.Delta<G>}
      +l = {M.DCon{ky, M.comb(G, iy, iz), M.merge(G, M.merge(G, a, ys), zs)} : M.Delta<G>}
      %eq_r(G, ky, iy, ys, kz, iz, zs, e23) : {l == M.merge(G, a, _) : M.Delta<G>}
      %gt_r(G, kx, ix, xs, ky, M.comb(G, iy, iz), M.merge(G, ys, zs), e1) : {l == _ : M.Delta<G>}
      %h_a_ys_zs : {l == M.DCon{ky, M.comb(G, iy, iz), _} : M.Delta<G>}
      {==}
    # z < y < x
    case GT{} GT{} GT{}:
      +a = {M.DCon{kx, ix, xs} : M.Delta<G>}
      +b = {M.DCon{ky, iy, ys} : M.Delta<G>}
      +c = {M.DCon{kz, iz, zs} : M.Delta<G>}
      +rhs = M.merge(G, a, M.merge(G, b, c))
      %gt_r(G, kx, ix, xs, ky, iy, ys, e1) : {M.merge(G, _, c) == rhs : M.Delta<G>}
      %gt_r(G, ky, iy, M.merge(G, a, ys), kz, iz, zs, e23) : {_ == rhs : M.Delta<G>}
      %gt_l(G, kx, ix, xs, ky, iy, ys, e1) : {M.DCon{kz, iz, M.merge(G, _, zs)} == rhs : M.Delta<G>}
      +l = {M.DCon{kz, iz, M.merge(G, M.merge(G, a, b), zs)} : M.Delta<G>}
      %gt_r(G, ky, iy, ys, kz, iz, zs, e23) : {l == M.merge(G, a, _) : M.Delta<G>}
      %gt_r(G, kx, ix, xs, kz, iz, M.merge(G, b, zs), e13) : {l == _ : M.Delta<G>}
      %h_a_b_zs : {l == M.DCon{kz, iz, _} : M.Delta<G>}
      {==}
    # the 14 orderings that cannot occur
    case LT{} EQ{} LT{}:
      match t:
    case LT{} EQ{} EQ{}:
      match t:
    case LT{} GT{} LT{}:
      match t:
    case LT{} GT{} EQ{}:
      match t:
    case EQ{} LT{} EQ{}:
      match t:
    case EQ{} LT{} GT{}:
      match t:
    case EQ{} EQ{} LT{}:
      match t:
    case EQ{} EQ{} GT{}:
      match t:
    case EQ{} GT{} LT{}:
      match t:
    case EQ{} GT{} EQ{}:
      match t:
    case GT{} LT{} EQ{}:
      match t:
    case GT{} LT{} GT{}:
      match t:
    case GT{} EQ{} EQ{}:
      match t:
    case GT{} EQ{} GT{}:
      match t:

def Laws.merge_assoc(G, a, b, c):
  match a b c:
    case M.DNil{} M.DNil{} M.DNil{}:
      {==}
    case M.DNil{} M.DNil{} M.DCon{kz, iz, zs}:
      {==}
    case M.DNil{} M.DCon{ky, iy, ys} M.DNil{}:
      {==}
    case M.DNil{} M.DCon{ky, iy, ys} M.DCon{kz, iz, zs}:
      +m = M.merge(G, b, c)
      Equal.sym(M.Delta<G>, M.merge(G, M.DNil{}, m), m, Laws.merge_nil_l(G, m))
    case M.DCon{kx, ix, xs} M.DNil{} M.DNil{}:
      {==}
    case M.DCon{kx, ix, xs} M.DNil{} M.DCon{kz, iz, zs}:
      {==}
    case M.DCon{kx, ix, xs} M.DCon{ky, iy, ys} M.DNil{}:
      Laws.merge_nil_r(G, M.merge(G, a, b))
    case M.DCon{kx, ix, xs} M.DCon{ky, iy, ys} M.DCon{kz, iz, zs}:
      assoc3(G, kx, ix, xs, ky, iy, ys, kz, iz, zs, Nat.cmp(kx, ky), Nat.cmp(kx, kz), Nat.cmp(ky, kz), {==}, {==}, {==}, tri(kx, ky, kz),
        Laws.merge_assoc(G, xs, b, c), Laws.merge_assoc(G, xs, b, zs), Laws.merge_assoc(G, a, b, zs), Laws.merge_assoc(G, xs, ys, c),
        Laws.merge_assoc(G, xs, ys, zs), Laws.merge_assoc(G, a, ys, c), Laws.merge_assoc(G, a, ys, zs))

# (b) grouped ranges
# ------------------

def nil_l_back(-G: Data, +a: M.Delta<G>) -> {a == M.merge(G, M.DNil{}, a) : M.Delta<G>}:
  match a:
    case M.DNil{}:
      {==}
    case M.DCon{k, i, r}:
      {==}

# Folding onto acc is merging acc with the fold from nothing.
def fold_acc(-G: Data, +ds: List<&2, M.Delta<G>>, +acc: M.Delta<G>)
  -> {M.merge(G, acc, M.fold(G, ds, M.DNil{})) == M.fold(G, ds, acc) : M.Delta<G>}:
  match ds:
    case Nil{}:
      Laws.merge_nil_r(G, acc)
    case Con{d, rest}:
      +f = M.fold(G, rest, M.DNil{})
      +l = M.merge(G, M.merge(G, acc, d), f)
      %fold_acc(G, rest, M.merge(G, acc, d)) : {M.merge(G, acc, M.fold(G, rest, M.merge(G, M.DNil{}, d))) == _ : M.Delta<G>}
      %nil_l_back(G, d) : {M.merge(G, acc, M.fold(G, rest, _)) == l : M.Delta<G>}
      %fold_acc(G, rest, d) : {M.merge(G, acc, _) == l : M.Delta<G>}
      %Laws.merge_assoc(G, acc, d, f) : {_ == l : M.Delta<G>}
      {==}

def fold_app(-G: Data, +xs: List<&2, M.Delta<G>>, +ys: List<&2, M.Delta<G>>, +acc: M.Delta<G>)
  -> {M.fold(G, ys, M.fold(G, xs, acc)) == M.fold(G, List.append(&2, M.Delta<G>, xs, ys), acc) : M.Delta<G>}:
  match xs:
    case Nil{}:
      {==}
    case Con{x, rest}:
      fold_app(G, rest, ys, M.merge(G, acc, x))

def grp(-G: Data, +gs: List<&2, List<&2, M.Delta<G>>>, +acc: M.Delta<G>)
  -> {M.fold(G, M.concat(G, gs), acc) == M.fold(G, M.summaries(G, gs), acc) : M.Delta<G>}:
  match gs:
    case Nil{}:
      {==}
    case Con{g, rest}:
      +r = M.fold(G, M.summaries(G, rest), M.merge(G, acc, M.fold(G, g, M.DNil{})))
      %fold_app(G, g, M.concat(G, rest), acc) : {_ == r : M.Delta<G>}
      %fold_acc(G, g, acc) : {M.fold(G, M.concat(G, rest), _) == r : M.Delta<G>}
      grp(G, rest, M.merge(G, acc, M.fold(G, g, M.DNil{})))

def Laws.grouped_classes(G, gs):
  %grp(G, gs, M.DNil{}) : {M.classes(G, M.summary(G, M.concat(G, gs))) == M.classes(G, _) : List<&2, M.Change>}
  {==}

# (c) per key
# -----------

# One step of a lookup, given how k compares with the first key.
def lk_lt_r(-G: Data, -k: Nat, -kd: Nat, -i: M.Info<G>, -r: M.Delta<G>, e: {LT{} == Nat.cmp(k, kd) : Cmp})
  -> {None{} == M.lookup(G, k, M.DCon{kd, i, r}) : Maybe<&2, M.Info<G>>}:
  %e : {None{} == M.lk(G, k, M.DCon{kd, i, r}, _) : Maybe<&2, M.Info<G>>}
  {==}

def lk_eq_r(-G: Data, -k: Nat, -kd: Nat, -i: M.Info<G>, -r: M.Delta<G>, e: {EQ{} == Nat.cmp(k, kd) : Cmp})
  -> {Some{i} == M.lookup(G, k, M.DCon{kd, i, r}) : Maybe<&2, M.Info<G>>}:
  %e : {Some{i} == M.lk(G, k, M.DCon{kd, i, r}, _) : Maybe<&2, M.Info<G>>}
  {==}

def lk_gt_r(-G: Data, -k: Nat, -kd: Nat, -i: M.Info<G>, -r: M.Delta<G>, e: {GT{} == Nat.cmp(k, kd) : Cmp})
  -> {M.lookup(G, k, r) == M.lookup(G, k, M.DCon{kd, i, r}) : Maybe<&2, M.Info<G>>}:
  %e : {M.lookup(G, k, r) == M.lk(G, k, M.DCon{kd, i, r}, _) : Maybe<&2, M.Info<G>>}
  {==}

def over_none_back(-G: Data, e: Maybe<&2, M.Info<G>>) -> {e == M.over(G, e, None{}) : Maybe<&2, M.Info<G>>}:
  match e:
    case None{}:
      {==}
    case Some{i}:
      {==}

def LM(-G: Data, k: Nat, a: M.Delta<G>, b: M.Delta<G>) -> Type:
  {M.lookup(G, k, M.merge(G, a, b)) == M.over(G, M.lookup(G, k, b), M.lookup(G, k, a)) : Maybe<&2, M.Info<G>>}

# Both non-empty, given how k and the two first keys compare.
def lm3(-G: Data, +k: Nat, +ka: Nat, +ia: M.Info<G>, +ra: M.Delta<G>, +kb: Nat, +ib: M.Info<G>, +rb: M.Delta<G>,
  ca: Cmp, cb: Cmp, cab: Cmp,
  +ea: {ca == Nat.cmp(k, ka) : Cmp}, +eb: {cb == Nat.cmp(k, kb) : Cmp}, +eab: {cab == Nat.cmp(ka, kb) : Cmp}, t: Imp(ca, cb, cab),
  h_ra_b: LM(G, k, ra, M.DCon{kb, ib, rb}),
  h_a_rb: LM(G, k, M.DCon{ka, ia, ra}, rb),
  h_ra_rb: LM(G, k, ra, rb))
  -> LM(G, k, M.DCon{ka, ia, ra}, M.DCon{kb, ib, rb}):
  match ca cb cab:
    # k < ka < kb
    case LT{} LT{} LT{}:
      +a = {M.DCon{ka, ia, ra} : M.Delta<G>}
      +b = {M.DCon{kb, ib, rb} : M.Delta<G>}
      +r = M.over(G, M.lookup(G, k, b), M.lookup(G, k, a))
      %lt_r(G, ka, ia, ra, kb, ib, rb, eab) : {M.lookup(G, k, _) == r : Maybe<&2, M.Info<G>>}
      %lk_lt_r(G, k, ka, ia, M.merge(G, ra, b), ea) : {_ == r : Maybe<&2, M.Info<G>>}
      %lk_lt_r(G, k, kb, ib, rb, eb) : {None{} == M.over(G, _, M.lookup(G, k, a)) : Maybe<&2, M.Info<G>>}
      %lk_lt_r(G, k, ka, ia, ra, ea) : {None{} == _ : Maybe<&2, M.Info<G>>}
      {==}
    # k < ka = kb
    case LT{} LT{} EQ{}:
      +a = {M.DCon{ka, ia, ra} : M.Delta<G>}
      +b = {M.DCon{kb, ib, rb} : M.Delta<G>}
      +r = M.over(G, M.lookup(G, k, b), M.lookup(G, k, a))
      %eq_r(G, ka, ia, ra, kb, ib, rb, eab) : {M.lookup(G, k, _) == r : Maybe<&2, M.Info<G>>}
      %lk_lt_r(G, k, ka, M.comb(G, ia, ib), M.merge(G, ra, rb), ea) : {_ == r : Maybe<&2, M.Info<G>>}
      %lk_lt_r(G, k, kb, ib, rb, eb) : {None{} == M.over(G, _, M.lookup(G, k, a)) : Maybe<&2, M.Info<G>>}
      %lk_lt_r(G, k, ka, ia, ra, ea) : {None{} == _ : Maybe<&2, M.Info<G>>}
      {==}
    # k < kb < ka
    case LT{} LT{} GT{}:
      +a = {M.DCon{ka, ia, ra} : M.Delta<G>}
      +b = {M.DCon{kb, ib, rb} : M.Delta<G>}
      +r = M.over(G, M.lookup(G, k, b), M.lookup(G, k, a))
      %gt_r(G, ka, ia, ra, kb, ib, rb, eab) : {M.lookup(G, k, _) == r : Maybe<&2, M.Info<G>>}
      %lk_lt_r(G, k, kb, ib, M.merge(G, a, rb), eb) : {_ == r : Maybe<&2, M.Info<G>>}
      %lk_lt_r(G, k, kb, ib, rb, eb) : {None{} == M.over(G, _, M.lookup(G, k, a)) : Maybe<&2, M.Info<G>>}
      %lk_lt_r(G, k, ka, ia, ra, ea) : {None{} == _ : Maybe<&2, M.Info<G>>}
      {==}
    # k = kb < ka
    case LT{} EQ{} GT{}:
      +a = {M.DCon{ka, ia, ra} : M.Delta<G>}
      +b = {M.DCon{kb, ib, rb} : M.Delta<G>}
      +r = M.over(G, M.lookup(G, k, b), M.lookup(G, k, a))
      %gt_r(G, ka, ia, ra, kb, ib, rb, eab) : {M.lookup(G, k, _) == r : Maybe<&2, M.Info<G>>}
      %lk_eq_r(G, k, kb, ib, M.merge(G, a, rb), eb) : {_ == r : Maybe<&2, M.Info<G>>}
      %lk_eq_r(G, k, kb, ib, rb, eb) : {Some{ib} == M.over(G, _, M.lookup(G, k, a)) : Maybe<&2, M.Info<G>>}
      %lk_lt_r(G, k, ka, ia, ra, ea) : {Some{ib} == M.over(G, Some{ib}, _) : Maybe<&2, M.Info<G>>}
      {==}
    # kb < k < ka
    case LT{} GT{} GT{}:
      +a = {M.DCon{ka, ia, ra} : M.Delta<G>}
      +b = {M.DCon{kb, ib, rb} : M.Delta<G>}
      +r = M.over(G, M.lookup(G, k, b), M.lookup(G, k, a))
      %gt_r(G, ka, ia, ra, kb, ib, rb, eab) : {M.lookup(G, k, _) == r : Maybe<&2, M.Info<G>>}
      %lk_gt_r(G, k, kb, ib, M.merge(G, a, rb), eb) : {_ == r : Maybe<&2, M.Info<G>>}
      %lk_gt_r(G, k, kb, ib, rb, eb) : {M.lookup(G, k, M.merge(G, a, rb)) == M.over(G, _, M.lookup(G, k, a)) : Maybe<&2, M.Info<G>>}
      h_a_rb
    # k = ka < kb
    case EQ{} LT{} LT{}:
      +a = {M.DCon{ka, ia, ra} : M.Delta<G>}
      +b = {M.DCon{kb, ib, rb} : M.Delta<G>}
      +r = M.over(G, M.lookup(G, k, b), M.lookup(G, k, a))
      %lt_r(G, ka, ia, ra, kb, ib, rb, eab) : {M.lookup(G, k, _) == r : Maybe<&2, M.Info<G>>}
      %lk_eq_r(G, k, ka, ia, M.merge(G, ra, b), ea) : {_ == r : Maybe<&2, M.Info<G>>}
      %lk_lt_r(G, k, kb, ib, rb, eb) : {Some{ia} == M.over(G, _, M.lookup(G, k, a)) : Maybe<&2, M.Info<G>>}
      %lk_eq_r(G, k, ka, ia, ra, ea) : {Some{ia} == _ : Maybe<&2, M.Info<G>>}
      {==}
    # k = ka = kb
    case EQ{} EQ{} EQ{}:
      +a = {M.DCon{ka, ia, ra} : M.Delta<G>}
      +b = {M.DCon{kb, ib, rb} : M.Delta<G>}
      +r = M.over(G, M.lookup(G, k, b), M.lookup(G, k, a))
      %eq_r(G, ka, ia, ra, kb, ib, rb, eab) : {M.lookup(G, k, _) == r : Maybe<&2, M.Info<G>>}
      %lk_eq_r(G, k, ka, M.comb(G, ia, ib), M.merge(G, ra, rb), ea) : {_ == r : Maybe<&2, M.Info<G>>}
      %lk_eq_r(G, k, kb, ib, rb, eb) : {Some{M.comb(G, ia, ib)} == M.over(G, _, M.lookup(G, k, a)) : Maybe<&2, M.Info<G>>}
      %lk_eq_r(G, k, ka, ia, ra, ea) : {Some{M.comb(G, ia, ib)} == M.over(G, Some{ib}, _) : Maybe<&2, M.Info<G>>}
      {==}
    # kb < k = ka
    case EQ{} GT{} GT{}:
      +a = {M.DCon{ka, ia, ra} : M.Delta<G>}
      +b = {M.DCon{kb, ib, rb} : M.Delta<G>}
      +r = M.over(G, M.lookup(G, k, b), M.lookup(G, k, a))
      %gt_r(G, ka, ia, ra, kb, ib, rb, eab) : {M.lookup(G, k, _) == r : Maybe<&2, M.Info<G>>}
      %lk_gt_r(G, k, kb, ib, M.merge(G, a, rb), eb) : {_ == r : Maybe<&2, M.Info<G>>}
      %lk_gt_r(G, k, kb, ib, rb, eb) : {M.lookup(G, k, M.merge(G, a, rb)) == M.over(G, _, M.lookup(G, k, a)) : Maybe<&2, M.Info<G>>}
      h_a_rb
    # ka < k, ka < kb
    case GT{} _ LT{}:
      +a = {M.DCon{ka, ia, ra} : M.Delta<G>}
      +b = {M.DCon{kb, ib, rb} : M.Delta<G>}
      +r = M.over(G, M.lookup(G, k, b), M.lookup(G, k, a))
      %lt_r(G, ka, ia, ra, kb, ib, rb, eab) : {M.lookup(G, k, _) == r : Maybe<&2, M.Info<G>>}
      %lk_gt_r(G, k, ka, ia, M.merge(G, ra, b), ea) : {_ == r : Maybe<&2, M.Info<G>>}
      %lk_gt_r(G, k, ka, ia, ra, ea) : {M.lookup(G, k, M.merge(G, ra, b)) == M.over(G, M.lookup(G, k, b), _) : Maybe<&2, M.Info<G>>}
      h_ra_b
    # ka = kb < k
    case GT{} GT{} EQ{}:
      +a = {M.DCon{ka, ia, ra} : M.Delta<G>}
      +b = {M.DCon{kb, ib, rb} : M.Delta<G>}
      +r = M.over(G, M.lookup(G, k, b), M.lookup(G, k, a))
      %eq_r(G, ka, ia, ra, kb, ib, rb, eab) : {M.lookup(G, k, _) == r : Maybe<&2, M.Info<G>>}
      %lk_gt_r(G, k, ka, M.comb(G, ia, ib), M.merge(G, ra, rb), ea) : {_ == r : Maybe<&2, M.Info<G>>}
      %lk_gt_r(G, k, kb, ib, rb, eb) : {M.lookup(G, k, M.merge(G, ra, rb)) == M.over(G, _, M.lookup(G, k, a)) : Maybe<&2, M.Info<G>>}
      %lk_gt_r(G, k, ka, ia, ra, ea) : {M.lookup(G, k, M.merge(G, ra, rb)) == M.over(G, M.lookup(G, k, rb), _) : Maybe<&2, M.Info<G>>}
      h_ra_rb
    # kb < ka < k
    case GT{} GT{} GT{}:
      +a = {M.DCon{ka, ia, ra} : M.Delta<G>}
      +b = {M.DCon{kb, ib, rb} : M.Delta<G>}
      +r = M.over(G, M.lookup(G, k, b), M.lookup(G, k, a))
      %gt_r(G, ka, ia, ra, kb, ib, rb, eab) : {M.lookup(G, k, _) == r : Maybe<&2, M.Info<G>>}
      %lk_gt_r(G, k, kb, ib, M.merge(G, a, rb), eb) : {_ == r : Maybe<&2, M.Info<G>>}
      %lk_gt_r(G, k, kb, ib, rb, eb) : {M.lookup(G, k, M.merge(G, a, rb)) == M.over(G, _, M.lookup(G, k, a)) : Maybe<&2, M.Info<G>>}
      h_a_rb
    # the 14 orderings that cannot occur
    case LT{} EQ{} LT{}:
      match t:
    case LT{} EQ{} EQ{}:
      match t:
    case LT{} GT{} LT{}:
      match t:
    case LT{} GT{} EQ{}:
      match t:
    case EQ{} LT{} EQ{}:
      match t:
    case EQ{} LT{} GT{}:
      match t:
    case EQ{} EQ{} LT{}:
      match t:
    case EQ{} EQ{} GT{}:
      match t:
    case EQ{} GT{} LT{}:
      match t:
    case EQ{} GT{} EQ{}:
      match t:
    case GT{} LT{} EQ{}:
      match t:
    case GT{} LT{} GT{}:
      match t:
    case GT{} EQ{} EQ{}:
      match t:
    case GT{} EQ{} GT{}:
      match t:

def Laws.lookup_merge(G, k, a, b):
  match a b:
    case M.DNil{} M.DNil{}:
      {==}
    case M.DNil{} M.DCon{kb, ib, rb}:
      over_none_back(G, M.lookup(G, k, b))
    case M.DCon{ka, ia, ra} M.DNil{}:
      {==}
    case M.DCon{ka, ia, ra} M.DCon{kb, ib, rb}:
      lm3(G, k, ka, ia, ra, kb, ib, rb, Nat.cmp(k, ka), Nat.cmp(k, kb), Nat.cmp(ka, kb), {==}, {==}, {==}, tri(k, ka, kb),
        Laws.lookup_merge(G, k, ra, b), Laws.lookup_merge(G, k, a, rb), Laws.lookup_merge(G, k, ra, rb))

# A key's entry in a fold of commits, one commit at a time.
def foldO(-G: Data, cs: List<&2, M.Delta<G>>, +k: Nat, m: Maybe<&2, M.Info<G>>) -> Maybe<&2, M.Info<G>>:
  match cs:
    case Nil{}:
      m
    case Con{c, rest}:
      foldO(G, rest, k, M.over(G, M.lookup(G, k, c), m))

def lk_fold(-G: Data, +cs: List<&2, M.Delta<G>>, +k: Nat, +acc: M.Delta<G>)
  -> {foldO(G, cs, k, M.lookup(G, k, acc)) == M.lookup(G, k, M.fold(G, cs, acc)) : Maybe<&2, M.Info<G>>}:
  match cs:
    case Nil{}:
      {==}
    case Con{c, rest}:
      %lk_fold(G, rest, k, M.merge(G, acc, c)) : {foldO(G, rest, k, M.over(G, M.lookup(G, k, c), M.lookup(G, k, acc))) == _ : Maybe<&2, M.Info<G>>}
      %Laws.lookup_merge(G, k, acc, c) : {foldO(G, rest, k, _) == foldO(G, rest, k, M.lookup(G, k, M.merge(G, acc, c))) : Maybe<&2, M.Info<G>>}
      {==}

def bef_eq(b: Bool, p: Bool, h: M.Is(Bool.not(Bool.xor(b, p)))) -> {b == p : Bool}:
  match b p:
    case True{} True{}:
      {==}
    case True{} False{}:
      match h:
    case False{} True{}:
      match h:
    case False{} False{}:
      {==}

# One commit's entry e, agreeing with the presence p before it, keeps the
# range's entry m agreeing.
def agree_step(-G: Data, +p0: Bool, +p: Bool, e: Maybe<&2, M.Info<G>>, m: Maybe<&2, M.Info<G>>, a: M.Agrees(G, p0, p, m), hb: M.BefM(G, p, e))
  -> M.Agrees(G, p0, M.step(G, p, e), M.over(G, e, m)):
  match e:
    case None{}:
      a
    case Some{i}:
      match i:
        case M.Info{g, d, b}:
          match m:
            case None{}:
              (Equal.trans(Bool, b, p, p0, bef_eq(b, p, hb), Equal.sym(Bool, p0, p, a)), {==})
            case Some{j}:
              match j:
                case M.Info{gj, dj, bj}:
                  (ab, ad) = a
                  (ab, {==})

def core(-G: Data, +cs: List<&2, M.Delta<G>>, +p0: Bool, +p: Bool, +k: Nat, +m: Maybe<&2, M.Info<G>>, a: M.Agrees(G, p0, p, m), h: M.Ok(G, cs, p, k))
  -> M.Agrees(G, p0, M.pres(G, cs, p, k), foldO(G, cs, k, m)):
  match cs:
    case Nil{}:
      a
    case Con{c, rest}:
      (hb, hr) = h
      +e = M.lookup(G, k, c)
      core(G, rest, p0, M.step(G, p, e), k, M.over(G, e, m), agree_step(G, p0, p, e, m, a, hb), hr)

def Laws.range_agrees(G, cs, p, k, h):
  %lk_fold(G, cs, k, M.DNil{}) : M.Agrees(G, p, M.pres(G, cs, p, k), _)
  core(G, cs, p, p, k, None{}, {==}, h)

def not_not(b: Bool) -> {b == Bool.not(Bool.not(b)) : Bool}:
  match b:
    case True{}:
      {==}
    case False{}:
      {==}

def class_of(-G: Data, +p: Bool, +q: Bool, m: Maybe<&2, M.Info<G>>, a: M.Agrees(G, p, q, m)) -> M.ClassAgrees(G, p, q, m):
  match m:
    case None{}:
      a
    case Some{i}:
      match i:
        case M.Info{g, d, b}:
          (eb, ed) = a
          %eb : {M.classify(b, d) == M.classify(_, Bool.not(q)) : M.Class}
          %ed : {M.classify(b, d) == M.classify(b, Bool.not(_)) : M.Class}
          %not_not(d) : {M.classify(b, d) == M.classify(b, _) : M.Class}
          {==}

def Laws.range_changes(G, cs, p, k, h):
  +q = M.pres(G, cs, p, k)
  +m = M.lookup(G, k, M.summary(G, cs))
  class_of(G, p, q, m, Laws.range_agrees(G, cs, p, k, h))

# Parallel ranges
# ---------------

def take_drop(-G: Data, +n: Nat, +ds: List<&2, M.Delta<G>>)
  -> {List.append(&2, M.Delta<G>, M.take(G, n, ds), M.drop(G, n, ds)) == ds : List<&2, M.Delta<G>>}:
  match n ds:
    case 0n Nil{}:
      {==}
    case 0n Con{d, rest}:
      {==}
    case 1n+m Nil{}:
      {==}
    case 1n+m Con{d, rest}:
      %take_drop(G, m, rest) : {d <> List.append(&2, M.Delta<G>, M.take(G, m, rest), M.drop(G, m, rest)) == d <> _ : List<&2, M.Delta<G>>}
      {==}

def tree_ok(-G: Data, +fuel: Nat, +ds: List<&2, M.Delta<G>>) -> {M.summary(G, ds) == M.tree(G, fuel, ds) : M.Delta<G>}:
  match fuel ds:
    case 0n _:
      {==}
    case 1n+f Nil{}:
      {==}
    case 1n+f Con{d, Nil{}}:
      Laws.merge_nil_l(G, d)
    case 1n+f Con{d, Con{e, rest}}:
      +ds = {d <> e <> rest : List<&2, M.Delta<G>>}
      +h = Nat.div(List.length(&2, M.Delta<G>, ds), 2n)
      +t = M.take(G, h, ds)
      +u = M.drop(G, h, ds)
      +r = M.merge(G, M.summary(G, t), M.summary(G, u))
      %tree_ok(G, f, t) : {M.fold(G, ds, M.DNil{}) == M.merge(G, _, M.tree(G, f, u)) : M.Delta<G>}
      %tree_ok(G, f, u) : {M.fold(G, ds, M.DNil{}) == M.merge(G, M.summary(G, t), _) : M.Delta<G>}
      %take_drop(G, h, ds) : {M.fold(G, _, M.DNil{}) == r : M.Delta<G>}
      %fold_app(G, t, u, M.DNil{}) : {_ == r : M.Delta<G>}
      %fold_acc(G, u, M.fold(G, t, M.DNil{})) : {_ == r : M.Delta<G>}
      {==}

def Laws.tree_summary(G, fuel, ds):
  Equal.sym(M.Delta<G>, M.summary(G, ds), M.tree(G, fuel, ds), tree_ok(G, fuel, ds))
