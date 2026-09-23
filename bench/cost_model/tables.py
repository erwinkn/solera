from model import *
SIZES = [1_000, 10_000, 100_000, 1_000_000, 10_000_000, 100_000_000]
def h(n): return {1_000:"1K",10_000:"10K",100_000:"100K",1_000_000:"1M",10_000_000:"10M",100_000_000:"100M"}[n]
def usd(x):
    if x == 0: return "$0"
    if x < 0.0001: return f"${x:.7f}"
    if x < 0.01: return f"${x:.5f}"
    if x < 10: return f"${x:.3f}"
    return f"${x:,.0f}"
def sec(x): return f"{x*1000:.0f} ms" if x < 1 else (f"{x:.1f} s" if x < 120 else f"{x/60:.0f} min")
def num(x): return f"{x:,.0f}"
hdr = "| | " + " | ".join(h(n) for n in SIZES) + " |\n|---|" + "---|" * len(SIZES)
def row(label, f): print(f"| {label} | " + " | ".join(f(n) for n in SIZES) + " |")

print("### Index size and storage\n"); print(hdr)
row("index size (incl. filters)", lambda n: (lambda b: f"{b/1e6:,.2f} MB" if b < 1e9 else f"{b/1e9:.1f} GB")(size(n)*1.1 + n*FILTER_B))
row("storage / month", lambda n: usd(storage(n)))

print("\n### Initial load (first commit, empty index)\n"); print(hdr)
row("requests", lambda n: f"{initial_load(n)['puts']} PUT")
row("cost", lambda n: usd(initial_load(n)['usd']))
row("upload time", lambda n: sec(initial_load(n)['io_s']))
row("sort, native", lambda n: sec(initial_load(n)['sort_native_s']))
row("sort, pure Python", lambda n: sec(initial_load(n)['sort_python_s']))

for k, cl, un, label in [(100, False, 0.0, "100 random keys changed"),
                         (1000, False, 0.0, "1K random keys changed"),
                         (1000, False, 0.5, "1K random keys written, half of them unchanged"),
                         (1000, True, 0.0, "1K clustered keys changed"),
                         (100_000, False, 0.0, "100K random keys changed")]:
    print(f"\n### Incremental commit: {label}\n"); print(hdr)
    row("GETs, cold, exact lookups only", lambda n: num(commit(n, min(k, n), cl, filters=False, unchanged=un)['gets']))
    row("GETs, cold, with filters", lambda n: num(commit(n, min(k, n), cl, unchanged=un)['gets']))
    row("GETs, warm cache", lambda n: num(commit(n, min(k, n), cl, warm=True, unchanged=un)['gets']))
    row("cost, cold", lambda n: usd(commit(n, min(k, n), cl, unchanged=un)['usd']))
    row("lookup wall time, cold", lambda n: sec(commit(n, min(k, n), cl, unchanged=un)['wall']))
    row("compaction I/O (amortized)", lambda n: f"{commit(n, min(k, n), cl, unchanged=un)['compaction_mb']:.2f} MB")

print("\n### Full replacement (bare return of every row, 1% changed)\n"); print(hdr)
row("GETs", lambda n: num(full_replace(n, n//100)['gets']))
row("cost", lambda n: usd(full_replace(n, n//100)['usd']))
row("read time", lambda n: sec(full_replace(n, n//100)['io_s']))
row("compare, native", lambda n: sec(full_replace(n, n//100)['cpu_native_s']))
row("compare, pure Python", lambda n: sec(full_replace(n, n//100)['cpu_python_s']))

for pg in (10_000, 100_000):
  print(f"\n### Full delivery to a consumer (pages of {pg//1000}K keys, one attempt per page)\n"); print(hdr)
  row("pages / attempts", lambda n: num(full_delivery(n, pg)['pages']))
  row("GETs", lambda n: num(full_delivery(n, pg)['gets']))
  row("cost (index + attempt overhead)", lambda n: usd(full_delivery(n, pg)['usd']))
