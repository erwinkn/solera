# Replayed and modelled numbers (model.py)

Output of `python3 bench/keys/twoviews/model.py --sizes N --readers R` for N in 1e6, 1e8 and R in 10, 100, each under `systemd-run --user --scope -p MemoryMax=8G -p CPUQuota=400%` (wall times: 13 s, 57 s, 46 s, 2 min 18 s). Estimates on the model in the script's docstring; see docs/key-index-two-views.md § Numbers for the calibration.

### Time view at 1,000,000 keys: fanout and top level

| b | top level (commits per node) | writes per entry committed | hourly: nodes, read × changed | daily: nodes, read × changed | 10,000 behind: nodes, read × changed | a week: nodes, read × changed | a month: nodes, read × changed |
|---|---|---|---|---|---|---|---|
| 2 | 13 (8,192) | 10.1 | 8.4, 1.11× | 13.2, 2.97× | 13.3, 3.03× | 19.7, 4.52× | 43.9, 5.41× |
| 2 | 18 (262,144) | 10.3 | 8.4, 1.11× | 13.2, 2.99× | 13.3, 3.01× | 15.9, 3.12× | 17.7, 1.97× |
| 4 | 6 (4,096) | 4.8 | 12.3, 1.12× | 19.1, 3.37× | 19.8, 3.71× | 32.2, 7.11× | 80.1, 9.40× |
| 4 | 9 (262,144) | 4.9 | 12.3, 1.13× | 19.0, 3.39× | 19.0, 3.48× | 23.7, 3.95× | 27.2, 2.43× |
| 8 | 4 (4,096) | 3.0 | 19.5, 1.15× | 29.0, 3.66× | 28.3, 3.74× | 41.8, 7.27× | 89.3, 9.47× |
| 8 | 6 (262,144) | 3.1 | 18.7, 1.15× | 28.8, 3.73× | 31.2, 4.04× | 35.9, 4.94× | 42.2, 3.20× |
| 16 | 3 (4,096) | 2.1 | 29.6, 1.13× | 45.1, 3.96× | 46.0, 4.08× | 57.6, 7.42× | 106.5, 9.49× |
| 16 | 5 (1,048,576) | 2.2 | 28.8, 1.13× | 45.9, 4.09× | 47.0, 4.21× | 57.1, 7.36× | 61.0, 3.73× |

### 1,000,000 keys, 10 daily readers + 1 hourly (T: fanout 4, top level 9)

| | spans, as built | spans + levers | two views (K + T) |
|---|---|---|---|
| runs at the head, mean (max) | 12.6 (17) | 5.9 (7) | 5.1 (9) |
| entries stored per live key (K for two views) | 7.62 | 9.20 | 1.12 |
| background entry writes per entry committed | 7.5 | 15.5 | 8.0 (K) + 4.9 (T) = 12.9 |
| background PUTs · GETs per commit | 0.67 · 1.34 | 1.90 · 1.95 | 0.99 · 2.66 |
| 1K exact head lookups, cold | 1 RT · 13 GETs · 76 MB · 0.55 s | 1 RT · 11 GETs · 92 MB · 0.43 s | 1 RT · 5 GETs · 11 MB · 0.11 s |
| 100K-key page at the head or a pinned snapshot, cold | 2 RT · 21 GETs · 9.2 MB · 0.13 s | 2 RT · 7 GETs · 10.3 MB · 0.14 s | 2 RT · 6 GETs · 2.3 MB · 0.08 s |
| changes(P, head), daily reader | 13 runs · first page 0.11 s · full 182 GETs, 76 MB, 1.1 s · reads 6.16× what changed | 6 runs · first page 0.11 s · full 79 GETs, 82 MB, 1.1 s · reads 6.77× what changed | 19 runs · first page 0.09 s · full 266 GETs, 41 MB, 0.8 s · reads 3.40× what changed |
| changes(P, head), hourly reader | 6 runs · first page 0.08 s · full 6 GETs, 7 MB, 0.2 s · reads 2.34× what changed | 5 runs · first page 0.11 s · full 21 GETs, 20 MB, 0.3 s · reads 6.52× what changed | 13 runs · first page 0.06 s · full 13 GETs, 4 MB, 0.1 s · reads 1.13× what changed |
| bytes stored, mean | 0.08 GB | 0.09 GB | 0.01 GB (K) + 0.14 GB (T) + 0.09 GB (raw deltas, a day) |
| $/month, S3: append: the delta PUT | $1.30 | $1.30 | $1.30 |
| $/month, S3: writers' head lookups, if cold every commit | $1.31 | $1.13 | $0.53 |
| $/month, S3: background merges and node builds | $1.01 | $2.66 | $1.56 |
| $/month, S3: changes(): every catch-up | $0.024 | $0.016 | $0.036 |
| $/month, S3: one full pass (pinned-snapshot pages) | $0.000 | $0.000 | $0.000 |
| $/month, S3: bytes stored (lagging readers included) | $0.002 | $0.002 | $0.005 |
| **$/month, S3, total, warm writer** | $2.33 | $3.97 | $2.90 |
| **$/month, S3, total, cold writer** | $3.64 | $5.11 | $3.43 |
| **$/month, Railway, total (storage only)** | $0.001 | $0.001 | $0.003 |

### 1,000,000 keys, 100 daily readers + 1 hourly (T: fanout 4, top level 9)

| | spans, as built | spans + levers | two views (K + T) |
|---|---|---|---|
| runs at the head, mean (max) | 17.9 (22) | 5.9 (7) | 5.1 (9) |
| entries stored per live key (K for two views) | 9.58 | 12.15 | 1.12 |
| background entry writes per entry committed | 7.7 | 17.7 | 8.0 (K) + 4.9 (T) = 12.9 |
| background PUTs · GETs per commit | 0.69 · 1.34 | 1.90 · 1.95 | 0.99 · 2.66 |
| 1K exact head lookups, cold | 1 RT · 18 GETs · 96 MB · 0.71 s | 1 RT · 12 GETs · 121 MB · 0.50 s | 1 RT · 5 GETs · 11 MB · 0.11 s |
| 100K-key page at the head or a pinned snapshot, cold | 2 RT · 31 GETs · 11.2 MB · 0.14 s | 2 RT · 7 GETs · 13.2 MB · 0.16 s | 2 RT · 6 GETs · 2.3 MB · 0.08 s |
| changes(P, head), daily reader | 18 runs · first page 0.12 s · full 252 GETs, 89 MB, 1.2 s · reads 7.15× what changed | 6 runs · first page 0.13 s · full 79 GETs, 111 MB, 1.3 s · reads 9.03× what changed | 19 runs · first page 0.09 s · full 266 GETs, 41 MB, 0.8 s · reads 3.36× what changed |
| changes(P, head), hourly reader | 6 runs · first page 0.08 s · full 6 GETs, 6 MB, 0.2 s · reads 1.96× what changed | 5 runs · first page 0.12 s · full 21 GETs, 22 MB, 0.3 s · reads 7.34× what changed | 12 runs · first page 0.06 s · full 12 GETs, 3 MB, 0.1 s · reads 1.12× what changed |
| bytes stored, mean | 0.10 GB | 0.12 GB | 0.01 GB (K) + 0.26 GB (T) + 0.09 GB (raw deltas, a day) |
| $/month, S3: append: the delta PUT | $1.30 | $1.30 | $1.30 |
| $/month, S3: writers' head lookups, if cold every commit | $1.86 | $1.26 | $0.53 |
| $/month, S3: background merges and node builds | $1.03 | $2.66 | $1.56 |
| $/month, S3: changes(): every catch-up | $0.30 | $0.10 | $0.32 |
| $/month, S3: one full pass (pinned-snapshot pages) | $0.000 | $0.000 | $0.000 |
| $/month, S3: bytes stored (lagging readers included) | $0.002 | $0.003 | $0.008 |
| **$/month, S3, total, warm writer** | $2.63 | $4.06 | $3.19 |
| **$/month, S3, total, cold writer** | $4.49 | $5.33 | $3.72 |
| **$/month, Railway, total (storage only)** | $0.001 | $0.002 | $0.005 |

### Time view at 100,000,000 keys: fanout and top level

| b | top level (commits per node) | writes per entry committed | hourly: nodes, read × changed | daily: nodes, read × changed | 10,000 behind: nodes, read × changed | a week: nodes, read × changed | a month: nodes, read × changed |
|---|---|---|---|---|---|---|---|
| 2 | 13 (8,192) | 12.9 | 8.4, 1.00× | 13.2, 1.03× | 13.3, 1.03× | 19.7, 1.27× | 43.9, 2.53× |
| 2 | 18 (262,144) | 16.4 | 8.4, 1.00× | 13.2, 1.03× | 13.3, 1.03× | 15.9, 1.20× | 17.7, 1.86× |
| 4 | 6 (4,096) | 6.0 | 12.3, 1.00× | 19.1, 1.03× | 19.8, 1.04× | 32.2, 1.29× | 80.1, 2.58× |
| 4 | 9 (262,144) | 8.0 | 12.3, 1.00× | 19.0, 1.03× | 19.0, 1.03× | 23.7, 1.24× | 27.2, 2.09× |
| 8 | 4 (4,096) | 4.0 | 19.5, 1.00× | 29.0, 1.03× | 28.3, 1.03× | 41.8, 1.29× | 89.3, 2.58× |
| 8 | 6 (262,144) | 5.2 | 18.7, 1.00× | 28.8, 1.03× | 31.2, 1.04× | 35.9, 1.22× | 42.2, 2.31× |
| 16 | 3 (4,096) | 3.0 | 29.6, 1.00× | 45.1, 1.03× | 46.0, 1.04× | 57.6, 1.29× | 106.5, 2.58× |
| 16 | 5 (1,048,576) | 3.8 | 28.8, 1.00× | 45.9, 1.03× | 47.0, 1.04× | 57.1, 1.29× | 61.0, 2.12× |

### 100,000,000 keys, 10 daily readers + 1 hourly (T: fanout 4, top level 9)

| | spans, as built | spans + levers | two views (K + T) |
|---|---|---|---|
| runs at the head, mean (max) | 17.8 (22) | 6.0 (7) | 8.4 (14) |
| entries stored per live key (K for two views) | 1.20 | 1.22 | 1.13 |
| background entry writes per entry committed | 14.1 | 19.8 | 13.3 (K) + 8.0 (T) = 21.3 |
| background PUTs · GETs per commit | 0.68 · 1.34 | 1.90 · 1.95 | 1.01 · 2.67 |
| 1K exact head lookups, cold | 2 RT · 1,100 GETs · 318 MB · 1.37 s | 2 RT · 1,178 GETs · 249 MB · 1.26 s | 2 RT · 1,091 GETs · 242 MB · 1.21 s |
| 100K-key page at the head or a pinned snapshot, cold | 2 RT · 46 GETs · 4.5 MB · 0.09 s | 2 RT · 7 GETs · 3.6 MB · 0.09 s | 2 RT · 27 GETs · 4.4 MB · 0.09 s |
| changes(P, head), daily reader | 16 runs · first page 0.07 s · full 1,344 GETs, 90 MB, 3.8 s · reads 1.10× what changed | 6 runs · first page 0.09 s · full 499 GETs, 261 MB, 4.7 s · reads 3.18× what changed | 19 runs · first page 0.07 s · full 1,596 GETs, 84 MB, 3.9 s · reads 1.03× what changed |
| changes(P, head), hourly reader | 7 runs · first page 0.09 s · full 7 GETs, 8 MB, 0.2 s · reads 2.02× what changed | 5 runs · first page 0.12 s · full 21 GETs, 30 MB, 0.4 s · reads 8.01× what changed | 13 runs · first page 0.06 s · full 13 GETs, 4 MB, 0.1 s · reads 1.00× what changed |
| bytes stored, mean | 1.20 GB | 1.22 GB | 1.13 GB (K) + 0.20 GB (T) + 0.09 GB (raw deltas, a day) |
| $/month, S3: append: the delta PUT | $1.30 | $1.30 | $1.30 |
| $/month, S3: writers' head lookups, if cold every commit | $114.04 | $122.14 | $113.06 |
| $/month, S3: background merges and node builds | $1.02 | $2.66 | $1.58 |
| $/month, S3: changes(): every catch-up | $0.16 | $0.066 | $0.20 |
| $/month, S3: one full pass (pinned-snapshot pages) | $0.005 | $0.002 | $0.001 |
| $/month, S3: bytes stored (lagging readers included) | $0.028 | $0.028 | $0.032 |
| **$/month, S3, total, warm writer** | $2.51 | $4.05 | $3.11 |
| **$/month, S3, total, cold writer** | $116.55 | $126.19 | $116.17 |
| **$/month, Railway, total (storage only)** | $0.018 | $0.018 | $0.021 |

### 100,000,000 keys, 100 daily readers + 1 hourly (T: fanout 4, top level 9)

| | spans, as built | spans + levers | two views (K + T) |
|---|---|---|---|
| runs at the head, mean (max) | 20.6 (26) | 6.0 (7) | 8.4 (14) |
| entries stored per live key (K for two views) | 1.22 | 1.23 | 1.13 |
| background entry writes per entry committed | 14.2 | 19.7 | 13.3 (K) + 8.0 (T) = 21.3 |
| background PUTs · GETs per commit | 0.69 · 1.35 | 1.90 · 1.95 | 1.01 · 2.67 |
| 1K exact head lookups, cold | 2 RT · 1,115 GETs · 326 MB · 1.39 s | 2 RT · 1,186 GETs · 250 MB · 1.26 s | 2 RT · 1,091 GETs · 242 MB · 1.21 s |
| 100K-key page at the head or a pinned snapshot, cold | 2 RT · 52 GETs · 4.5 MB · 0.09 s | 2 RT · 7 GETs · 3.7 MB · 0.09 s | 2 RT · 27 GETs · 4.4 MB · 0.09 s |
| changes(P, head), daily reader | 18 runs · first page 0.07 s · full 1,512 GETs, 90 MB, 3.9 s · reads 1.08× what changed | 6 runs · first page 0.09 s · full 499 GETs, 266 MB, 4.7 s · reads 3.20× what changed | 19 runs · first page 0.07 s · full 1,596 GETs, 85 MB, 3.9 s · reads 1.03× what changed |
| changes(P, head), hourly reader | 6 runs · first page 0.08 s · full 6 GETs, 6 MB, 0.2 s · reads 1.89× what changed | 5 runs · first page 0.13 s · full 21 GETs, 31 MB, 0.4 s · reads 8.32× what changed | 12 runs · first page 0.06 s · full 12 GETs, 3 MB, 0.1 s · reads 1.00× what changed |
| bytes stored, mean | 1.22 GB | 1.23 GB | 1.13 GB (K) + 0.33 GB (T) + 0.09 GB (raw deltas, a day) |
| $/month, S3: append: the delta PUT | $1.30 | $1.30 | $1.30 |
| $/month, S3: writers' head lookups, if cold every commit | $115.60 | $122.95 | $113.06 |
| $/month, S3: background merges and node builds | $1.03 | $2.66 | $1.58 |
| $/month, S3: changes(): every catch-up | $1.82 | $0.60 | $1.92 |
| $/month, S3: one full pass (pinned-snapshot pages) | $0.006 | $0.002 | $0.001 |
| $/month, S3: bytes stored (lagging readers included) | $0.028 | $0.028 | $0.035 |
| **$/month, S3, total, warm writer** | $4.18 | $4.59 | $4.83 |
| **$/month, S3, total, cold writer** | $119.77 | $127.54 | $117.90 |
| **$/month, Railway, total (storage only)** | $0.018 | $0.018 | $0.023 |

