from model import *
OVERHEAD = ATTEMPT_PUT * PUT + ATTEMPT_GET * GET + JOURNAL_PUT * PUT
MONTH = 30 * 24 * 3600
def usd(x): return f"${x:,.2f}" if x >= 0.01 else (f"${x:.4f}" if x >= 0.0001 else "<$0.0001")
def sec(x): return f"{x*1000:.0f} ms" if x < 1 else f"{x:.1f} s"

rows = []
def add(name, n, per_month, op, extra=None, note=""):
    c = op()
    total_ops = c["usd"] * per_month
    overhead = OVERHEAD * per_month
    index = total_ops - overhead
    extra_usd, extra_label = (extra() if extra else (0.0, ""))
    stor = storage(n)
    lat = c.get("wall", c.get("io_s", 0))
    rows.append((name, f"{per_month:,.0f}", usd(index + extra_usd), usd(overhead), usd(stor),
                 usd(index + extra_usd + overhead + stor), sec(lat), note or extra_label))

add("A. Reference table, 1K keys, full replace hourly (1% changed)", 1_000, 24 * 30,
    lambda: full_replace(1_000, 10))
add("B. SharePoint inventory, 100K keys, 100 random changes every 10 s", 100_000, MONTH / 10,
    lambda: commit(100_000, 100))
add("C. Event table, 10M keys, 10K clustered changes every minute", 10_000_000, MONTH / 60,
    lambda: commit(10_000_000, 10_000, clustered=True))
add("D. Large dimension, 100M keys, 1M random changes daily", 100_000_000, 30,
    lambda: commit(100_000_000, 1_000_000),
    extra=lambda: (full_delivery(100_000_000, 100_000)["usd"], "+ one full delivery to a new consumer (100K pages)"))
add("E. Worst case, 100M keys, 1K random changes every 10 s", 100_000_000, MONTH / 10,
    lambda: commit(100_000_000, 1_000))
add("F. Big full replacement, 100M keys daily (1% changed)", 100_000_000, 30,
    lambda: full_replace(100_000_000, 1_000_000))

print("| Workload | Commits / month | Key index | Attempt + journal overhead | Index storage | **Total / month** | Time per commit | |")
print("|---|---|---|---|---|---|---|---|")
for r in rows: print("| " + " | ".join(r) + " |")

print("\n**Scenario E variants**\n")
print("| Variant | Commits / month | Key index | Attempt + journal overhead | **Total / month** | Time per commit |")
print("|---|---|---|---|---|---|")
def variant(name, per_month, c):
    over = OVERHEAD * per_month
    idx = c["usd"] * per_month - over
    print(f"| {name} | {per_month:,.0f} | {usd(idx)} | {usd(over)} | {usd(idx + over + storage(100_000_000))} | {sec(c['wall'])} |")
MONTH = 30 * 24 * 3600
variant("E0. exact lookups only, cold worker", MONTH / 10, commit(100_000_000, 1_000, filters=False))
base = commit(100_000_000, 1_000)
variant("E. with filters, cold worker", MONTH / 10, base)
disk = dict(base); disk["usd"] = base["usd"] - (base["gets"] - ATTEMPT_GET) * GET; disk["wall"] = 0.05
variant("E1. with filters + index files cached on local disk", MONTH / 10, disk)
variant("E2. same changes committed every 10 min (60K per commit), cold", MONTH / 600, commit(100_000_000, 60_000))
