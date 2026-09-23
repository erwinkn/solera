# K0 benchmark results

Local MinIO with injected S3-like latency; reproduce with `uv run python bench/keys/bench.py --sizes 1e6,1e7,1e8 --latency 0.03` (a MinIO at 127.0.0.1:9100, see the script). Machine: 8 cores, native extension, 2026-09-23.
### Key index benchmark (native, 30 ms per request, 80 MB/s per connection, 64 in parallel)

| Keys | Build | Index size | Per entry (incl. filters) | Filters per entry | Files | Levels |
|---|---|---|---|---|---|---|
| 1,000,000 | 1.3 s | 29.0 MB | 29.0 B | 3.51 B | 1 | 1 |
| 10,000,000 | 11.6 s | 281.4 MB | 28.1 B | 3.51 B | 3 | 2 |
| 100,000,000 | 113.9 s | 2,719.6 MB | 27.2 B | 3.51 B | 29 | 3 |

| Operation | 1,000,000 keys | 10,000,000 keys | 100,000,000 keys |
|---|---|---|---|
| 100 random keys changed | 342 ms · 2 GET 0 PUT · 29.0 MB | 239 ms · 3 GET 0 PUT · 35.1 MB | 434 ms · 28 GET 0 PUT · 338.5 MB |
| 1K random keys changed | 469 ms · 2 GET 0 PUT · 29.0 MB | 280 ms · 5 GET 0 PUT · 35.2 MB | 474 ms · 34 GET 0 PUT · 351.2 MB |
| 1K random keys, half unchanged | 461 ms · 2 GET 0 PUT · 29.0 MB | 773 ms · 453 GET 0 PUT · 66.0 MB | 843 ms · 542 GET 0 PUT · 383.3 MB |
| 1K clustered keys changed | 307 ms · 2 GET 0 PUT · 29.0 MB | 259 ms · 2 GET 0 PUT · 12.5 MB | 219 ms · 1 GET 0 PUT · 12.4 MB |
| 1K new keys inserted | 475 ms · 2 GET 0 PUT · 29.0 MB | 247 ms · 3 GET 0 PUT · 35.1 MB | 495 ms · 29 GET 0 PUT · 350.9 MB |
| 100K random keys changed | 868 ms · 2 GET 0 PUT · 29.0 MB | 2.6 s · 17 GET 0 PUT · 281.4 MB | 1.3 s · 376 GET 0 PUT · 372.7 MB |
| 1K random keys changed, disk cache warm | 163 ms · 0 GET 0 PUT · 0.0 MB | 28 ms · 0 GET 0 PUT · 0.0 MB | 227 ms · 0 GET 0 PUT · 0.0 MB |
| full-delivery page of 10K keys | 80 ms · 2 GET 0 PUT · 0.3 MB | 81 ms · 2 GET 0 PUT · 0.3 MB | 83 ms · 2 GET 0 PUT · 0.3 MB |
| commit: 1K random changes + delta write | 485 ms · 2 GET 1 PUT · 29.0 MB | 321 ms · 6 GET 1 PUT · 35.3 MB | 733 ms · 36 GET 1 PUT · 351.3 MB |
| compaction: 8 delta files into level 1 | 1.4 s · 10 GET 1 PUT · 29.3 MB | 92 ms · 8 GET 1 PUT · 0.3 MB | 91 ms · 8 GET 1 PUT · 0.3 MB |
| full replacement, 1% changed | 805 ms · 2 GET 0 PUT · 29.0 MB | 5.9 s · 17 GET 0 PUT · 281.4 MB | — |
