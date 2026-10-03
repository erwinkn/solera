// run.bend's word effects are native only: build with `bend run.bend -o run`.
function bx_none() {
  throw new Error("run.bend: build it native (bend run.bend -o run)");
}
io_eff(CID(Bx.load), bx_none);
io_eff(CID(Bx.word), bx_none);
io_eff(CID(Bx.put), bx_none);
io_eff(CID(Bx.save), bx_none);
io_eff(CID(Bx.micros), bx_none);
io_eff(CID(Bx.cpu), bx_none);
