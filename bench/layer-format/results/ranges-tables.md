
### daily-clustered, 10M keys

| variant | stored | B/entry | vs ours | index + records | range records | entries they stand for |
|---|---|---|---|---|---|---|
| ours | 150.1 MB | 11.31 | 1.00 | 0.17 MB | 0 | 0 |
| ours+ranges | 121.7 MB | 9.17 | 0.81 | 0.17 MB | 1,440 | 3,123,900 |
| ours+rle | 155.7 MB | 11.73 | 1.04 | 0.17 MB | 0 | 0 |
| ours+ranges+rle | 126.6 MB | 9.53 | 0.84 | 0.17 MB | 1,440 | 3,123,900 |

Cold: `time · GETs · MB read`; warm: CPU s. ✓: matches the fold.

| query | ours, cold | ours+ranges, cold | ours+rle, cold | ours+ranges+rle, cold | warm CPU s, ours · ours+ranges · ours+rle · ours+ranges+rle | ✓ |
|---|---|---|---|---|---|---|
| diff-1c | 0.15 s · 4 · 0.8 MB | 0.10 s · 10 · 0.3 MB | 0.15 s · 4 · 0.9 MB | 0.10 s · 10 · 0.3 MB | 0.01 · 0.00 · 0.02 · 0.00 | ✓ |
| diff-1d-first10k | 0.07 s · 5 · 1.1 MB | 0.14 s · 23 · 1.0 MB | 0.07 s · 4 · 0.5 MB | 0.14 s · 23 · 1.0 MB | 0.00 · 0.02 · 0.00 · 0.01 | ✓ |
| diff-1d | 0.19 s · 8 · 1.7 MB | 0.21 s · 36 · 2.0 MB | 0.19 s · 8 · 1.8 MB | 0.21 s · 36 · 2.0 MB | 0.02 · 0.03 · 0.02 · 0.03 | ✓ |
| diff-1d-range1pct | 0.06 s · 4 · 0.0 MB | 0.03 s · 2 · 0.0 MB | 0.06 s · 4 · 0.0 MB | 0.03 s · 2 · 0.0 MB | 0.00 · 0.00 · 0.00 · 0.00 | ✓ |
| diff-paused-first10k | 0.08 s · 12 · 1.6 MB | 0.12 s · 14 · 1.0 MB | 0.08 s · 12 · 1.6 MB | 0.12 s · 14 · 1.0 MB | 0.01 · 0.02 · 0.01 · 0.02 | ✓ |
| diff-paused | 1.06 s · 28 · 29.4 MB | 1.17 s · 354 · 44.1 MB | 1.04 s · 28 · 30.1 MB | 1.20 s · 354 · 45.8 MB | 0.44 · 0.59 · 0.37 · 0.56 | ✓ |
| diff-paused-range1pct | 0.19 s · 15 · 1.6 MB | 0.20 s · 10 · 1.4 MB | 0.17 s · 15 · 1.6 MB | 0.20 s · 10 · 1.4 MB | 0.04 · 0.03 · 0.02 · 0.02 | ✓ |
| scan-head | 3.08 s · 45 · 150.1 MB | 2.95 s · 30 · 121.7 MB | 3.00 s · 45 · 155.7 MB | 3.02 s · 30 · 126.6 MB | 1.53 · 1.42 · 1.86 · 1.79 | ✓ |
| scan-head-range1pct | 0.19 s · 19 · 3.0 MB | 0.17 s · 10 · 1.4 MB | 0.20 s · 19 · 3.0 MB | 0.16 s · 10 · 1.4 MB | 0.04 · 0.02 · 0.04 · 0.02 | ✓ |
| scan-1d-range1pct | 0.18 s · 17 · 3.0 MB | 0.17 s · 9 · 1.4 MB | 0.18 s · 17 · 3.0 MB | 0.16 s · 9 · 1.4 MB | 0.04 · 0.02 · 0.04 · 0.02 | ✓ |
| get-1k | 0.69 s · 705 · 32.4 MB | 0.55 s · 576 · 23.6 MB | 0.65 s · 707 · 33.2 MB | 0.56 s · 578 · 24.4 MB | 0.28 · 0.21 · 0.25 · 0.22 | ✓ |
| merge-newest4 | 0.29 s · 14 · 3.2 MB → 3.2 MB | 0.07 s · 8 · 0.1 MB → 0.1 MB | 0.26 s · 14 · 3.3 MB → 3.3 MB | 0.07 s · 8 · 0.1 MB → 0.1 MB | 0.13 · 0.01 · 0.09 · 0.00 | ✓ |
| fold-into-base | 4.13 s · 45 · 150.1 MB → 105.7 MB | 4.16 s · 30 · 121.7 MB → 105.7 MB | 3.98 s · 45 · 155.7 MB → 109.2 MB | 4.00 s · 30 · 126.6 MB → 109.2 MB | 2.98 · 2.78 · 2.84 · 2.61 | ✓ |

### daily-scattered, 10M keys

| variant | stored | B/entry | vs ours | index + records | range records | entries they stand for |
|---|---|---|---|---|---|---|
| ours | 171.6 MB | 12.95 | 1.00 | 0.18 MB | 0 | 0 |
| ours+ranges | 171.6 MB | 12.95 | 1.00 | 0.18 MB | 0 | 0 |
| ours+rle | 182.4 MB | 13.77 | 1.06 | 0.18 MB | 0 | 0 |
| ours+ranges+rle | 182.4 MB | 13.77 | 1.06 | 0.18 MB | 0 | 0 |

Cold: `time · GETs · MB read`; warm: CPU s. ✓: matches the fold.

| query | ours, cold | ours+ranges, cold | ours+rle, cold | ours+ranges+rle, cold | warm CPU s, ours · ours+ranges · ours+rle · ours+ranges+rle | ✓ |
|---|---|---|---|---|---|---|
| diff-1c | 0.06 s · 2 · 0.1 MB | 0.06 s · 2 · 0.1 MB | 0.06 s · 2 · 0.1 MB | 0.06 s · 2 · 0.1 MB | 0.00 · 0.00 · 0.00 · 0.00 | ✓ |
| diff-1d-first10k | 0.07 s · 6 · 0.6 MB | 0.07 s · 6 · 0.6 MB | 0.07 s · 7 · 1.1 MB | 0.07 s · 6 · 0.6 MB | 0.01 · 0.01 · 0.01 · 0.01 | ✓ |
| diff-1d | 0.18 s · 10 · 2.5 MB | 0.19 s · 10 · 2.5 MB | 0.19 s · 10 · 2.6 MB | 0.18 s · 10 · 2.6 MB | 0.04 · 0.03 · 0.03 · 0.03 | ✓ |
| diff-1d-range1pct | 0.06 s · 6 · 0.1 MB | 0.06 s · 6 · 0.1 MB | 0.06 s · 6 · 0.1 MB | 0.07 s · 6 · 0.1 MB | 0.00 · 0.00 · 0.00 · 0.00 | ✓ |
| diff-paused-first10k | 0.09 s · 12 · 1.5 MB | 0.08 s · 12 · 1.5 MB | 0.09 s · 16 · 3.6 MB | 0.07 s · 12 · 1.5 MB | 0.02 · 0.02 · 0.02 · 0.01 | ✓ |
| diff-paused | 1.41 s · 31 · 50.7 MB | 1.40 s · 31 · 50.7 MB | 1.45 s · 31 · 55.9 MB | 1.40 s · 31 · 55.9 MB | 0.44 · 0.45 · 0.45 · 0.46 | ✓ |
| diff-paused-range1pct | 0.11 s · 13 · 0.7 MB | 0.11 s · 13 · 0.7 MB | 0.11 s · 13 · 0.8 MB | 0.11 s · 13 · 0.8 MB | 0.01 · 0.01 · 0.02 · 0.01 | ✓ |
| scan-head | 3.34 s · 48 · 171.6 MB | 3.21 s · 48 · 171.6 MB | 3.22 s · 48 · 182.4 MB | 3.21 s · 48 · 182.4 MB | 1.84 · 1.86 · 1.83 · 1.78 | ✓ |
| scan-head-range1pct | 0.17 s · 17 · 2.0 MB | 0.19 s · 17 · 2.0 MB | 0.18 s · 17 · 2.1 MB | 0.18 s · 17 · 2.1 MB | 0.03 · 0.04 · 0.02 · 0.03 | ✓ |
| scan-1d-range1pct | 0.17 s · 13 · 2.0 MB | 0.17 s · 13 · 2.0 MB | 0.17 s · 13 · 2.1 MB | 0.19 s · 13 · 2.1 MB | 0.03 · 0.02 · 0.02 · 0.03 | ✓ |
| get-1k | 0.81 s · 670 · 64.8 MB | 0.82 s · 670 · 64.8 MB | 0.82 s · 670 · 70.2 MB | 0.83 s · 670 · 70.2 MB | 0.46 · 0.54 · 0.47 · 0.49 | ✓ |
| merge-newest4 | 0.25 s · 14 · 3.8 MB → 3.8 MB | 0.24 s · 14 · 3.8 MB → 3.8 MB | 0.23 s · 14 · 4.0 MB → 4.3 MB | 0.24 s · 14 · 4.0 MB → 4.3 MB | 0.11 · 0.10 · 0.09 · 0.10 | ✓ |
| fold-into-base | 4.44 s · 48 · 171.6 MB → 128.2 MB | 4.20 s · 48 · 171.6 MB → 128.2 MB | 4.04 s · 48 · 182.4 MB → 127.9 MB | 4.20 s · 48 · 182.4 MB → 127.9 MB | 3.29 · 3.05 · 2.90 · 3.06 | ✓ |

### hot, 10M keys

| variant | stored | B/entry | vs ours | index + records | range records | entries they stand for |
|---|---|---|---|---|---|---|
| ours | 128.7 MB | 10.69 | 1.00 | 0.15 MB | 0 | 0 |
| ours+ranges | 128.7 MB | 10.69 | 1.00 | 0.15 MB | 0 | 0 |
| ours+rle | 131.2 MB | 10.90 | 1.02 | 0.15 MB | 0 | 0 |
| ours+ranges+rle | 131.2 MB | 10.90 | 1.02 | 0.15 MB | 0 | 0 |

Cold: `time · GETs · MB read`; warm: CPU s. ✓: matches the fold.

| query | ours, cold | ours+ranges, cold | ours+rle, cold | ours+ranges+rle, cold | warm CPU s, ours · ours+ranges · ours+rle · ours+ranges+rle | ✓ |
|---|---|---|---|---|---|---|
| diff-1c | 0.06 s · 2 · 0.0 MB | 0.06 s · 2 · 0.0 MB | 0.06 s · 2 · 0.0 MB | 0.06 s · 2 · 0.0 MB | 0.00 · 0.00 · 0.00 · 0.00 | ✓ |
| diff-1d-first10k | 0.11 s · 7 · 0.6 MB | 0.11 s · 7 · 0.6 MB | 0.08 s · 6 · 0.5 MB | 0.08 s · 6 · 0.5 MB | 0.02 · 0.01 · 0.02 · 0.02 | ✓ |
| diff-1d | 0.11 s · 7 · 0.6 MB | 0.11 s · 7 · 0.6 MB | 0.08 s · 6 · 0.5 MB | 0.08 s · 6 · 0.5 MB | 0.02 · 0.00 · 0.01 · 0.02 | ✓ |
| diff-1d-range1pct | 0.06 s · 6 · 0.0 MB | 0.06 s · 6 · 0.0 MB | 0.07 s · 6 · 0.0 MB | 0.06 s · 6 · 0.0 MB | 0.00 · 0.00 · 0.00 · 0.00 | ✓ |
| diff-paused-first10k | 0.35 s · 31 · 8.0 MB | 0.33 s · 31 · 8.0 MB | 0.26 s · 24 · 5.5 MB | 0.27 s · 24 · 5.5 MB | 0.10 · 0.10 · 0.09 · 0.10 | ✓ |
| diff-paused | 0.30 s · 31 · 8.0 MB | 0.33 s · 31 · 8.0 MB | 0.25 s · 24 · 5.5 MB | 0.27 s · 24 · 5.5 MB | 0.10 · 0.10 · 0.09 · 0.10 | ✓ |
| diff-paused-range1pct | 0.07 s · 18 · 0.2 MB | 0.07 s · 18 · 0.2 MB | 0.07 s · 18 · 0.2 MB | 0.07 s · 18 · 0.2 MB | 0.00 · 0.00 · 0.00 · 0.01 | ✓ |
| scan-head | 3.24 s · 48 · 128.7 MB | 3.12 s · 48 · 128.7 MB | 3.19 s · 41 · 131.2 MB | 3.17 s · 41 · 131.2 MB | 1.69 · 1.60 · 1.57 · 1.62 | ✓ |
| scan-head-range1pct | 0.20 s · 22 · 1.6 MB | 0.17 s · 22 · 1.6 MB | 0.17 s · 22 · 1.6 MB | 0.17 s · 22 · 1.6 MB | 0.02 · 0.03 · 0.04 · 0.03 | ✓ |
| scan-1d-range1pct | 0.16 s · 18 · 1.5 MB | 0.16 s · 18 · 1.5 MB | 0.18 s · 18 · 1.5 MB | 0.18 s · 18 · 1.5 MB | 0.02 · 0.02 · 0.04 · 0.02 | ✓ |
| get-1k | 0.72 s · 587 · 31.5 MB | 0.66 s · 587 · 31.5 MB | 0.64 s · 589 · 29.9 MB | 0.68 s · 589 · 29.9 MB | 0.32 · 0.32 · 0.32 · 0.34 | ✓ |
| merge-newest4 | 0.14 s · 10 · 0.9 MB → 0.8 MB | 0.14 s · 10 · 0.9 MB → 0.8 MB | 0.12 s · 8 · 0.7 MB → 0.6 MB | 0.09 s · 8 · 0.7 MB → 0.6 MB | 0.05 · 0.05 · 0.05 · 0.03 | ✓ |
| fold-into-base | 4.16 s · 48 · 128.7 MB → 120.7 MB | 4.38 s · 48 · 128.7 MB → 120.7 MB | 4.13 s · 41 · 131.2 MB → 125.6 MB | 4.36 s · 41 · 131.2 MB → 125.6 MB | 2.78 · 2.99 · 2.74 · 2.98 | ✓ |

### rewrite, 10M keys

| variant | stored | B/entry | vs ours | index + records | range records | entries they stand for |
|---|---|---|---|---|---|---|
| ours | 233.5 MB | 11.26 | 1.00 | 0.27 MB | 0 | 0 |
| ours+ranges | 132.3 MB | 6.38 | 0.57 | 0.14 MB | 101 | 9,983,709 |
| ours+rle | 246.6 MB | 11.89 | 1.06 | 0.27 MB | 0 | 0 |
| ours+ranges+rle | 139.8 MB | 6.74 | 0.60 | 0.14 MB | 101 | 9,983,709 |

Cold: `time · GETs · MB read`; warm: CPU s. ✓: matches the fold.

| query | ours, cold | ours+ranges, cold | ours+rle, cold | ours+ranges+rle, cold | warm CPU s, ours · ours+ranges · ours+rle · ours+ranges+rle | ✓ |
|---|---|---|---|---|---|---|
| diff-1c | 0.16 s · 4 · 1.0 MB | 0.20 s · 7 · 1.5 MB | 0.16 s · 4 · 1.1 MB | 0.20 s · 7 · 1.5 MB | 0.02 · 0.03 · 0.02 · 0.02 | ✓ |
| diff-1d-first10k | 0.28 s · 17 · 7.2 MB | 0.11 s · 9 · 0.7 MB | 0.26 s · 17 · 7.2 MB | 0.11 s · 9 · 0.7 MB | 0.05 · 0.01 · 0.04 · 0.01 | ✓ |
| diff-1d | 1.16 s · 27 · 26.1 MB | 1.12 s · 18 · 31.6 MB | 1.31 s · 28 · 27.4 MB | 1.14 s · 18 · 33.3 MB | 0.33 · 0.38 · 0.35 · 0.36 | ✓ |
| diff-1d-range1pct | 0.00 s · 0 · 0.0 MB | 0.00 s · 0 · 0.0 MB | 0.00 s · 0 · 0.0 MB | 0.00 s · 0 · 0.0 MB | 0.00 · 0.00 · 0.00 · 0.00 | ✓ |
| diff-paused-first10k | 0.08 s · 16 · 2.3 MB | 0.11 s · 11 · 0.7 MB | 0.10 s · 19 · 3.9 MB | 0.10 s · 11 · 0.7 MB | 0.01 · 0.01 · 0.02 · 0.01 | ✓ |
| diff-paused | 4.21 s · 51 · 112.7 MB | 3.07 s · 31 · 132.3 MB | 4.25 s · 54 · 120.1 MB | 3.06 s · 31 · 139.8 MB | 1.43 · 1.39 · 1.46 · 1.38 | ✓ |
| diff-paused-range1pct | 0.16 s · 6 · 1.2 MB | 0.24 s · 7 · 1.5 MB | 0.16 s · 6 · 1.3 MB | 0.19 s · 7 · 1.6 MB | 0.01 · 0.01 · 0.02 · 0.02 | ✓ |
| scan-head | 5.23 s · 68 · 233.5 MB | 3.02 s · 31 · 132.3 MB | 5.61 s · 71 · 246.6 MB | 3.11 s · 31 · 139.8 MB | 2.74 · 1.36 · 2.80 · 1.48 | ✓ |
| scan-head-range1pct | 0.20 s · 10 · 2.6 MB | 0.18 s · 7 · 1.5 MB | 0.19 s · 10 · 2.7 MB | 0.18 s · 7 · 1.6 MB | 0.03 · 0.03 · 0.03 · 0.03 | ✓ |
| scan-1d-range1pct | 0.19 s · 10 · 2.6 MB | 0.18 s · 7 · 1.5 MB | 0.19 s · 10 · 2.7 MB | 0.17 s · 7 · 1.6 MB | 0.04 · 0.03 · 0.03 · 0.04 | ✓ |
| get-1k | 1.07 s · 1070 · 56.5 MB | 0.66 s · 577 · 35.2 MB | 1.03 s · 1082 · 59.6 MB | 0.64 s · 577 · 38.0 MB | 0.44 · 0.30 · 0.47 · 0.30 | ✓ |
| diff-across-rewrite | 4.10 s · 51 · 112.7 MB | 3.03 s · 31 · 132.3 MB | 4.16 s · 54 · 120.1 MB | 3.03 s · 31 · 139.8 MB | 1.49 · 1.36 · 1.36 · 1.48 | ✓ |
| diff-across-rewrite-first10k | 0.11 s · 16 · 2.3 MB | 0.11 s · 11 · 0.7 MB | 0.10 s · 16 · 2.3 MB | 0.11 s · 11 · 0.7 MB | 0.02 · 0.01 · 0.02 · 0.02 | ✓ |
| merge-newest4 | 0.73 s · 20 · 10.0 MB → 10.0 MB | 0.03 s · 4 · 0.0 MB → 0.0 MB | 0.72 s · 20 · 10.5 MB → 10.5 MB | 0.03 s · 4 · 0.0 MB → 0.0 MB | 0.29 · 0.00 · 0.28 · 0.00 | ✓ |
| fold-into-base | 6.04 s · 68 · 233.5 MB → 61.3 MB | 3.82 s · 31 · 132.3 MB → 61.3 MB | 6.34 s · 71 · 246.6 MB → 61.2 MB | 3.91 s · 31 · 139.8 MB → 61.2 MB | 4.01 · 2.43 · 4.12 · 2.52 | ✓ |

### daily-scattered, 100M keys

| variant | stored | B/entry | vs ours | index + records | range records | entries they stand for |
|---|---|---|---|---|---|---|
| ours | 1,714.9 MB | 12.95 | 1.00 | 1.81 MB | 0 | 0 |
| ours+ranges | 1,714.9 MB | 12.95 | 1.00 | 1.81 MB | 0 | 0 |
| ours+rle | 1,827.6 MB | 13.80 | 1.07 | 1.83 MB | 0 | 0 |
| ours+ranges+rle | 1,827.6 MB | 13.80 | 1.07 | 1.83 MB | 0 | 0 |

Cold: `time · GETs · MB read`; warm: CPU s. ✓: matches the fold.

| query | ours, cold | ours+ranges, cold | ours+rle, cold | ours+ranges+rle, cold | warm CPU s, ours · ours+ranges · ours+rle · ours+ranges+rle | ✓ |
|---|---|---|---|---|---|---|
| diff-1c | 0.11 s · 3 · 0.7 MB | 0.11 s · 3 · 0.7 MB | 0.11 s · 3 · 0.7 MB | 0.11 s · 3 · 0.7 MB | 0.01 · 0.01 · 0.01 · 0.00 | ✓ |
| diff-1d-first10k | 0.08 s · 12 · 1.7 MB | 0.09 s · 12 · 1.7 MB | 0.13 s · 18 · 4.6 MB | 0.12 s · 18 · 4.6 MB | 0.01 · 0.02 · 0.03 · 0.02 | ✓ |
| diff-1d | 1.19 s · 31 · 45.9 MB | 1.13 s · 31 · 45.9 MB | 1.27 s · 31 · 49.5 MB | 1.20 s · 31 · 49.5 MB | 0.44 · 0.47 · 0.45 · 0.48 | ✓ |
| diff-1d-range1pct | 0.11 s · 13 · 0.6 MB | 0.10 s · 13 · 0.6 MB | 0.11 s · 13 · 0.6 MB | 0.11 s · 13 · 0.6 MB | 0.01 · 0.00 · 0.01 · 0.02 | ✓ |
| diff-paused-first10k | 0.11 s · 22 · 4.9 MB | 0.09 s · 18 · 3.0 MB | 0.10 s · 18 · 3.0 MB | 0.09 s · 18 · 3.0 MB | 0.02 · 0.02 · 0.02 · 0.02 | ✓ |
| diff-paused | 8.43 s · 88 · 505.8 MB | 9.01 s · 88 · 505.8 MB | 8.83 s · 93 · 562.7 MB | 8.97 s · 93 · 562.7 MB | 5.40 · 6.04 · 5.74 · 5.76 | ✓ |
| diff-paused-range1pct | 0.28 s · 25 · 5.7 MB | 0.33 s · 25 · 5.7 MB | 0.33 s · 25 · 6.3 MB | 0.30 s · 25 · 6.3 MB | 0.06 · 0.06 · 0.06 · 0.06 | ✓ |
| scan-head | 29.72 s · 202 · 1715.0 MB | 30.19 s · 202 · 1715.0 MB | 31.06 s · 213 · 1827.7 MB | 32.02 s · 213 · 1827.7 MB | 20.20 · 20.67 · 20.00 · 23.39 | ✓ |
| scan-head-range1pct | 0.65 s · 33 · 17.9 MB | 0.68 s · 33 · 17.9 MB | 0.67 s · 33 · 19.0 MB | 0.70 s · 33 · 19.0 MB | 0.21 · 0.20 · 0.20 · 0.26 | ✓ |
| scan-1d-range1pct | 0.65 s · 23 · 17.6 MB | 0.66 s · 23 · 17.6 MB | 0.67 s · 23 · 18.7 MB | 0.62 s · 23 · 18.7 MB | 0.18 · 0.25 · 0.17 · 0.18 | ✓ |
| get-1k | 2.29 s · 2346 · 134.1 MB | 2.40 s · 2346 · 134.1 MB | 2.24 s · 2346 · 147.2 MB | 2.27 s · 2346 · 147.2 MB | 1.09 · 1.12 · 1.03 · 1.39 | ✓ |
| merge-newest4 | 0.34 s · 16 · 6.9 MB → 7.0 MB | 0.38 s · 16 · 6.9 MB → 7.0 MB | 0.37 s · 16 · 6.9 MB → 7.1 MB | 0.37 s · 16 · 6.9 MB → 7.1 MB | 0.16 · 0.18 · 0.18 · 0.18 | ✓ |
| fold-into-base | 42.82 s · 202 · 1715.0 MB → 1282.1 MB | 42.94 s · 202 · 1715.0 MB → 1282.1 MB | 41.46 s · 213 · 1827.7 MB → 1279.0 MB | 43.15 s · 213 · 1827.7 MB → 1279.0 MB | 33.94 · 34.03 · 32.13 · 33.81 | ✓ |

### rewrite, 100M keys

| variant | stored | B/entry | vs ours | index + records | range records | entries they stand for |
|---|---|---|---|---|---|---|
| ours | 2,336.3 MB | 11.27 | 1.00 | 2.68 MB | 0 | 0 |
| ours+ranges | 1,323.9 MB | 6.38 | 0.57 | 1.39 MB | 101 | 99,839,001 |
| ours+rle | 2,463.9 MB | 11.88 | 1.05 | 2.69 MB | 0 | 0 |
| ours+ranges+rle | 1,397.8 MB | 6.74 | 0.60 | 1.40 MB | 101 | 99,839,001 |

Cold: `time · GETs · MB read`; warm: CPU s. ✓: matches the fold.

| query | ours, cold | ours+ranges, cold | ours+rle, cold | ours+ranges+rle, cold | warm CPU s, ours · ours+ranges · ours+rle · ours+ranges+rle | ✓ |
|---|---|---|---|---|---|---|
| diff-1c | 0.47 s · 7 · 10.0 MB | 0.67 s · 13 · 13.3 MB | 0.49 s · 7 · 10.5 MB | 0.61 s · 13 · 14.0 MB | 0.08 · 0.13 · 0.09 · 0.14 | ✓ |
| diff-1d-first10k | 3.07 s · 30 · 137.7 MB | 0.14 s · 10 · 1.0 MB | 3.23 s · 30 · 137.7 MB | 0.14 s · 10 · 1.0 MB | 1.52 · 0.01 · 1.42 · 0.01 | ✓ |
| diff-1d | 8.59 s · 62 · 351.1 MB | 6.55 s · 46 · 314.7 MB | 8.70 s · 63 · 368.8 MB | 6.49 s · 46 · 332.3 MB | 4.20 · 3.30 · 4.05 · 3.30 | ✓ |
| diff-1d-range1pct | 0.00 s · 0 · 0.0 MB | 0.00 s · 0 · 0.0 MB | 0.00 s · 0 · 0.0 MB | 0.00 s · 0 · 0.0 MB | 0.00 · 0.00 · 0.00 · 0.00 | ✓ |
| diff-paused-first10k | 0.11 s · 16 · 3.6 MB | 0.20 s · 12 · 1.9 MB | 0.10 s · 16 · 3.6 MB | 0.20 s · 12 · 1.9 MB | 0.03 · 0.04 · 0.03 · 0.04 | ✓ |
| diff-paused | 25.91 s · 149 · 1127.2 MB | 25.12 s · 137 · 1323.9 MB | 25.69 s · 155 · 1199.0 MB | 25.65 s · 144 · 1397.8 MB | 14.80 · 14.27 · 14.29 · 14.97 | ✓ |
| diff-paused-range1pct | 0.54 s · 11 · 11.9 MB | 0.65 s · 13 · 13.4 MB | 0.52 s · 11 · 12.6 MB | 0.63 s · 13 · 14.2 MB | 0.11 · 0.18 · 0.14 · 0.18 | ✓ |
| scan-head | 35.77 s · 263 · 2336.3 MB | 24.37 s · 137 · 1323.9 MB | 34.98 s · 275 · 2463.9 MB | 25.20 s · 144 · 1397.8 MB | 27.57 · 13.87 · 26.72 · 14.23 | ✓ |
| scan-head-range1pct | 0.65 s · 19 · 24.0 MB | 0.67 s · 13 · 13.4 MB | 0.69 s · 19 · 25.3 MB | 0.61 s · 13 · 14.2 MB | 0.22 · 0.16 · 0.21 · 0.18 | ✓ |
| scan-1d-range1pct | 0.70 s · 19 · 24.0 MB | 0.61 s · 13 · 13.4 MB | 0.68 s · 19 · 25.3 MB | 0.59 s · 13 · 14.2 MB | 0.20 · 0.15 · 0.22 · 0.14 | ✓ |
| get-1k | 1.78 s · 2360 · 58.8 MB | 1.26 s · 1478 · 45.2 MB | 1.74 s · 2361 · 64.2 MB | 1.20 s · 1478 · 50.2 MB | 0.48 · 0.39 · 0.50 · 0.35 | ✓ |
| diff-across-rewrite | 25.52 s · 149 · 1127.2 MB | 24.47 s · 137 · 1323.9 MB | 25.53 s · 155 · 1199.0 MB | 24.61 s · 144 · 1397.8 MB | 14.78 · 15.11 · 14.08 · 14.26 | ✓ |
| diff-across-rewrite-first10k | 0.10 s · 16 · 3.6 MB | 0.17 s · 12 · 1.9 MB | 0.10 s · 16 · 3.6 MB | 0.17 s · 12 · 1.9 MB | 0.03 · 0.04 · 0.04 · 0.02 | ✓ |
| merge-newest4 | 3.33 s · 31 · 70.2 MB → 70.2 MB | 0.03 s · 4 · 0.0 MB → 0.0 MB | 3.29 s · 31 · 73.8 MB → 73.8 MB | 0.03 s · 4 · 0.0 MB → 0.0 MB | 1.83 · 0.00 · 1.75 · 0.00 | ✓ |
| fold-into-base | 44.05 s · 263 · 2336.3 MB → 612.3 MB | 32.64 s · 137 · 1323.9 MB → 612.3 MB | 43.67 s · 275 · 2463.9 MB → 612.1 MB | 33.43 s · 144 · 1397.8 MB → 612.1 MB | 36.41 · 23.51 · 36.39 · 23.83 | ✓ |
