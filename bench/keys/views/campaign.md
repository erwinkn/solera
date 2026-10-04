# Campaign, 1M keys (T31 phase 2)

Each cell: first page · full (catch-ups) or wall time, GETs, MB read, the reader process's peak memory above its
baseline, and the 1-minute load average when the read started (⚠: the 45-minute wait for a load below 6 was
used up, timed anyway). Every read checked key by key against the per-commit fold.

## Base trace

| | layers | spans, zlib64 | spans, re-encoded zstd16 | spans, zstd16 | two views, window | two views, cover |
|---|---|---|---|---|---|---|
| 100 behind (first page · full) | 0.09 · 0.11 s · 7 GETs · 0.9 MB · 39 MB peak · load 4.4 | 0.51 · 0.52 s · 9 GETs · 6.2 MB · 107 MB peak · load 3.6 | 0.49 · 0.51 s · 9 GETs · 5.2 MB · 75 MB peak · load 4.4 | 0.31 · 0.32 s · 6 GETs · 2.6 MB · 71 MB peak · load 3.3 | 0.23 · 0.24 s · 5 GETs · 1.2 MB · 48 MB peak · load 3.6 | 0.21 · 0.22 s · 5 GETs · 1.2 MB · 58 MB peak · load 3.9 |
| 360 behind | 0.26 · 0.46 s · 15 GETs · 5.2 MB · 73 MB peak · load 4.4 | 0.38 · 0.60 s · 10 GETs · 6.2 MB · 156 MB peak · load 3.4 | 0.37 · 0.58 s · 10 GETs · 5.2 MB · 113 MB peak · load 4.4 | 0.38 · 0.62 s · 11 GETs · 5.4 MB · 146 MB peak · load 3.3 | 0.48 · 0.61 s · 10 GETs · 4.3 MB · 143 MB peak · load 3.8 | 0.42 · 0.56 s · 10 GETs · 4.3 MB · 131 MB peak · load 3.9 |
| 8,640 behind | 0.40 · 1.82 s · 33 GETs · 21.0 MB · 164 MB peak · load 4.4 | 0.87 · 6.00 s · 109 GETs · 62.2 MB · 417 MB peak · load 3.4 | 0.82 · 5.51 s · 109 GETs · 51.2 MB · 326 MB peak · load 4.4 | 0.71 · 4.31 s · 78 GETs · 42.7 MB · 322 MB peak · load 3.3 | 0.32 · 1.48 s · 16 GETs · 13.9 MB · 218 MB peak · load 3.8 | 1.22 · 5.89 s · 111 GETs · 47.2 MB · 434 MB peak · load 3.9 |
| 10,000 behind | 0.41 · 1.80 s · 33 GETs · 21.0 MB · 159 MB peak · load 4.4 | 1.04 · 7.55 s · 141 GETs · 75.1 MB · 489 MB peak · load 3.4 | 0.97 · 6.78 s · 141 GETs · 61.9 MB · 374 MB peak · load 4.1 | 0.79 · 5.16 s · 97 GETs · 51.1 MB · 341 MB peak · load 3.0 | 1.03 · 4.14 s · 71 GETs · 27.0 MB · 345 MB peak · load 3.8 | 1.20 · 6.55 s · 125 GETs · 54.1 MB · 418 MB peak · load 3.8 |
| 1K cold lookups | 0.50 s · 17 GETs · 17.0 MB · 47 MB peak · load 4.1 | 0.54 s · 15 GETs · 98.0 MB · 206 MB peak · load 3.2 | 0.31 s · 15 GETs · 82.1 MB · 182 MB peak · load 3.9 | 0.31 s · 13 GETs · 71.3 MB · 154 MB peak · load 3.0 | 0.17 s · 7 GETs · 9.1 MB · 28 MB peak · load 3.6 | 0.16 s · 7 GETs · 9.1 MB · 28 MB peak · load 3.6 |
| 1K-key cold write | 0.53 s · 17 GETs · 17.0 MB · 55 MB peak · load 4.1 | 0.41 s · 15 GETs · 98.0 MB · 205 MB peak · load 3.2 | 0.28 s · 15 GETs · 82.1 MB · 180 MB peak · load 3.9 | 0.29 s · 13 GETs · 71.3 MB · 159 MB peak · load 3.0 | 0.18 s · 7 GETs · 9.1 MB · 35 MB peak · load 3.6 | 0.17 s · 7 GETs · 9.1 MB · 36 MB peak · load 3.6 |
| 100K-key page | 0.35 s · 16 GETs · 4.5 MB · 61 MB peak · load 4.1 | 1.23 s · 26 GETs · 14.9 MB · 250 MB peak · load 3.2 | 1.12 s · 26 GETs · 12.0 MB · 203 MB peak · load 3.9 | 1.05 s · 23 GETs · 13.6 MB · 180 MB peak · load 3.0 | 0.32 s · 8 GETs · 1.8 MB · 76 MB peak · load 3.6 | 0.33 s · 8 GETs · 1.8 MB · 86 MB peak · load 3.6 |
| scan cust-00042* (prefix) | 0.22 s · 16 GETs · 0.2 MB · 3 MB peak · load 4.1 | 1.30 s · 26 GETs · 14.9 MB · 258 MB peak · load 3.3 | 1.22 s · 26 GETs · 12.0 MB · 207 MB peak · load 3.6 | 1.15 s · 23 GETs · 13.6 MB · 191 MB peak · load 3.2 | 0.38 s · 8 GETs · 1.8 MB · 81 MB peak · load 3.5 | 0.42 s · 8 GETs · 1.8 MB · 94 MB peak · load 3.6 |
| scan *4242* (infix, matches) | 0.54 s · 16 GETs · 17.0 MB · 33 MB peak · load 4.1 | 6.44 s · 112 GETs · 85.0 MB · 498 MB peak · load 3.2 | 5.69 s · 111 GETs · 69.1 MB · 352 MB peak · load 3.8 | 4.77 s · 82 GETs · 59.1 MB · 360 MB peak · load 2.9 | 0.95 s · 14 GETs · 9.1 MB · 212 MB peak · load 3.6 | 0.95 s · 14 GETs · 9.1 MB · 223 MB peak · load 3.6 |
| scan *template* (infix, none) | 0.53 s · 16 GETs · 17.0 MB · 33 MB peak · load 4.1 | 6.43 s · 112 GETs · 85.0 MB · 484 MB peak · load 3.2 | 5.74 s · 111 GETs · 69.1 MB · 354 MB peak · load 3.6 | 4.74 s · 82 GETs · 59.1 MB · 353 MB peak · load 3.1 | 0.95 s · 14 GETs · 9.1 MB · 213 MB peak · load 3.6 | 0.93 s · 14 GETs · 9.1 MB · 223 MB peak · load 3.6 |
| stored, mean · peak | 16 · 22 MB | 57 · 98 MB | 48 · 82 MB | 42 · 74 MB | 293 · 510 MB | 55 · 88 MB |
| background entry writes per entry committed | 6.18 | 6.34 | 6.34 | 6.73 | 8.54 | 8.31 |
| PUTs per commit | 1.39 | 1.45 | 1.45 | 1.45 | 1.34 | 1.34 |
| $ a month, S3, warm writer · cold writer | $1.91 · $3.67 | $1.99 · $3.55 | $1.99 · $3.55 | $1.99 · $3.34 | $1.85 · $2.57 | $1.84 · $2.57 |
| $ a month, Railway | $0.67 | $1.35 | $1.14 | $1.15 | $1.28 | $1.24 |

## daily100

| | layers | spans, zlib64 | two views, window | two views, cover |
|---|---|---|---|---|
| 8,640 behind | 0.40 · 1.80 s · 33 GETs · 21.0 MB · 163 MB peak · load 5.0 | 1.59 · 10.89 s · 216 GETs · 94.2 MB · 630 MB peak · load 4.5 | 0.33 · 1.51 s · 16 GETs · 13.9 MB · 217 MB peak · load 4.0 | 1.28 · 6.02 s · 111 GETs · 47.2 MB · 438 MB peak · load 3.7 |
| 10,000 behind | 0.40 · 1.81 s · 33 GETs · 21.0 MB · 158 MB peak · load 5.0 | 1.80 · 13.23 s · 268 GETs · 111.0 MB · 680 MB peak · load 4.3 | 1.02 · 4.17 s · 71 GETs · 27.0 MB · 348 MB peak · load 4.0 | 1.22 · 6.77 s · 125 GETs · 54.1 MB · 411 MB peak · load 4.5 |
| 1K cold lookups | 0.51 s · 17 GETs · 17.0 MB · 47 MB peak · load 4.7 | 0.64 s · 22 GETs · 140.2 MB · 258 MB peak · load 4.0 | 0.17 s · 7 GETs · 9.1 MB · 27 MB peak · load 3.8 | 0.17 s · 7 GETs · 9.1 MB · 27 MB peak · load 4.0 |
| stored, mean · peak | 16 · 22 MB | 79 · 140 MB | 294 · 526 MB | 160 · 275 MB |
| background entry writes per entry committed | 6.20 | 6.08 | 12.93 | 8.31 |
| PUTs per commit | 1.39 | 1.46 | 1.34 | 1.34 |
| $ a month, S3, warm writer · cold writer | $1.91 · $3.67 | $2.00 · $4.28 | $1.85 · $2.58 | $1.84 · $2.57 |
| $ a month, Railway | $0.67 | $1.30 | $1.92 | $1.24 |

## stall

| | layers | spans, zlib64 | two views, window |
|---|---|---|---|
| 8,640 behind | 0.40 · 1.79 s · 33 GETs · 21.0 MB · 163 MB peak · load 4.6 | 0.89 · 6.05 s · 109 GETs · 62.2 MB · 416 MB peak · load 3.6 | 0.33 · 1.49 s · 16 GETs · 13.9 MB · 217 MB peak · load 3.7 |
| 10,000 behind | 0.40 · 1.79 s · 33 GETs · 21.0 MB · 158 MB peak · load 4.6 | 1.05 · 7.58 s · 141 GETs · 75.1 MB · 479 MB peak · load 4.1 | 1.03 · 4.17 s · 71 GETs · 27.0 MB · 352 MB peak · load 3.9 |
| 1K cold lookups | 0.51 s · 17 GETs · 17.0 MB · 47 MB peak · load 4.6 | 0.54 s · 15 GETs · 98.0 MB · 212 MB peak · load 3.7 | 0.17 s · 7 GETs · 9.1 MB · 27 MB peak · load 4.1 |
| stalled pass: catch-up | 0.39 · 1.78 s · 33 GETs · 21.0 MB · 159 MB peak · load 4.5 | 1.04 · 7.57 s · 141 GETs · 75.1 MB · 481 MB peak · load 3.9 | 1.02 · 4.06 s · 71 GETs · 27.0 MB · 357 MB peak · load 3.6 |
| stored, mean · peak | 16 · 22 MB | 57 · 98 MB | 300 · 519 MB |
| background entry writes per entry committed | 6.18 | 6.34 | 8.54 |
| PUTs per commit | 1.39 | 1.45 | 1.34 |
| $ a month, S3, warm writer · cold writer | $1.91 · $3.67 | $1.99 · $3.55 | $1.85 · $2.57 |
| $ a month, Railway | $0.67 | $1.35 | $1.28 |

## churn

| | layers | spans, zlib64 | two views, window |
|---|---|---|---|
| 8,640 behind | 0.56 · 3.14 s · 49 GETs · 35.1 MB · 124 MB peak · load 4.3 | 0.81 · 10.62 s · 198 GETs · 65.9 MB · 689 MB peak · load 4.8 | 0.33 · 1.50 s · 19 GETs · 12.6 MB · 216 MB peak · load 3.7 |
| 10,000 behind | 0.55 · 3.11 s · 49 GETs · 35.1 MB · 121 MB peak · load 5.4 | 0.87 · 12.74 s · 252 GETs · 74.6 MB · 857 MB peak · load 4.4 | 0.98 · 2.81 s · 34 GETs · 23.5 MB · 372 MB peak · load 3.7 |
| 1K cold lookups | 0.57 s · 63 GETs · 20.0 MB · 59 MB peak · load 5.1 | 0.63 s · 14 GETs · 102.8 MB · 259 MB peak · load 4.4 | 0.17 s · 5 GETs · 8.9 MB · 27 MB peak · load 3.8 |
| stored, mean · peak | 25 · 39 MB | 56 · 103 MB | 239 · 420 MB |
| background entry writes per entry committed | 6.23 | 6.46 | 7.99 |
| PUTs per commit | 1.37 | 1.40 | 1.34 |
| $ a month, S3, warm writer · cold writer | $1.88 · $8.62 | $1.92 · $3.37 | $1.85 · $2.36 |
| $ a month, Railway | $0.65 | $1.26 | $1.09 |

## large

| | layers | spans, zlib64 | two views, window |
|---|---|---|---|
| 8,640 behind | 0.26 · 1.48 s · 28 GETs · 18.2 MB · 157 MB peak · load 4.1 | 0.92 · 6.37 s · 118 GETs · 64.2 MB · 464 MB peak · load 3.1 | 0.37 · 2.04 s · 31 GETs · 19.1 MB · 242 MB peak · load 3.5 |
| 10,000 behind | 0.25 · 1.46 s · 28 GETs · 18.2 MB · 156 MB peak · load 4.1 | 1.32 · 8.98 s · 169 GETs · 88.7 MB · 698 MB peak · load 3.1 | 1.06 · 4.73 s · 83 GETs · 32.6 MB · 361 MB peak · load 3.6 |
| 1K cold lookups | 0.36 s · 13 GETs · 13.7 MB · 44 MB peak · load 4.3 | 0.64 s · 18 GETs · 113.3 MB · 262 MB peak · load 4.7 | 0.18 s · 8 GETs · 16.7 MB · 45 MB peak · load 4.1 |
| stored, mean · peak | 15 · 22 MB | 62 · 113 MB | 324 · 567 MB |
| background entry writes per entry committed | 5.51 | 5.49 | 7.66 |
| PUTs per commit | 1.39 | 1.45 | 1.34 |
| $ a month, S3, warm writer · cold writer | $2.23 · $3.47 | $2.62 · $4.48 | $2.37 · $3.20 |
| $ a month, Railway | $0.74 | $1.41 | $1.36 |

## daily100-h1000

| | spans, zlib64 | two views, floor + horizon 1,000 |
|---|---|---|
| 8,640 behind | 0.46 · 2.01 s · 33 GETs · 23.1 MB · 283 MB peak · load 3.9 | 0.34 · 0.97 s · 14 GETs · 9.1 MB · 196 MB peak · load 3.8 |
| 10,000 behind | 0.45 · 1.98 s · 33 GETs · 23.1 MB · 285 MB peak · load 3.7 | 0.33 · 0.97 s · 14 GETs · 9.1 MB · 197 MB peak · load 3.8 |
| 1K cold lookups | 0.32 s · 7 GETs · 26.8 MB · 81 MB peak · load 3.7 | 0.17 s · 7 GETs · 9.1 MB · 27 MB peak · load 3.8 |
| stored, mean · peak | 27 · 34 MB | 48 · 53 MB |
| background entry writes per entry committed | 7.70 | 7.65 |
| PUTs per commit | 1.46 | 1.34 |
| $ a month, S3, warm writer · cold writer | $2.00 · $2.72 | $1.84 · $2.56 |
| $ a month, Railway | $1.55 | $1.13 |

Reads: 236, mismatches against the fold: 0. Load at the start of each read: 2.9–5.7, 0 flagged.
