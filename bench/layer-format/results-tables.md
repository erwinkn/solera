
### daily-clustered, 10,000,000 keys

| query | mode | ours | parquet | results | fold |
|---|---|---|---|---|---|
| diff-1c | cold | 0.143 s · 4 GETs · 0.85 MB · 5 MB peak | 0.108 s · 5 GETs · 0.77 MB · 7 MB peak | 4,375 | ✓ |
| diff-1c | warm | 0.012 s · 0.012 s CPU · 5 MB peak | 0.005 s · 0.005 s CPU · 7 MB peak | 4,375 | ✓ |
| diff-1d-first10k | cold | 0.070 s · 4 GETs · 0.54 MB · 4 MB peak | 0.069 s · 6 GETs · 0.57 MB · 6 MB peak | 10,000 | ✓ |
| diff-1d-first10k | warm | 0.005 s · 0.004 s CPU · 6 MB peak | 0.004 s · 0.004 s CPU · 8 MB peak | 10,000 | ✓ |
| diff-1d | cold | 0.181 s · 8 GETs · 1.71 MB · 4 MB peak | 0.113 s · 10 GETs · 1.55 MB · 9 MB peak | 107,630 | ✓ |
| diff-1d | warm | 0.014 s · 0.014 s CPU · 6 MB peak | 0.012 s · 0.013 s CPU · 9 MB peak | 107,630 | ✓ |
| diff-1d-range1pct | cold | 0.063 s · 4 GETs · 0.03 MB · 0 MB peak | 0.063 s · 6 GETs · 0.08 MB · 1 MB peak | 0 | ✓ |
| diff-1d-range1pct | warm | 0.000 s · 0.000 s CPU · 0 MB peak | 0.000 s · 0.000 s CPU · 1 MB peak | 0 | ✓ |
| diff-paused-first10k | cold | 0.076 s · 12 GETs · 1.65 MB · 17 MB peak | 0.077 s · 24 GETs · 3.52 MB · 23 MB peak | 10,000 | ✓ |
| diff-paused-first10k | warm | 0.011 s · 0.011 s CPU · 20 MB peak | 0.009 s · 0.010 s CPU · 26 MB peak | 10,000 | ✓ |
| diff-paused | cold | 0.923 s · 28 GETs · 29.35 MB · 84 MB peak | 0.890 s · 45 GETs · 27.49 MB · 124 MB peak | 2,671,940 | ✓ |
| diff-paused | warm | 0.356 s · 0.357 s CPU · 87 MB peak | 0.286 s · 0.286 s CPU · 124 MB peak | 2,671,940 | ✓ |
| diff-paused-range1pct | cold | 0.164 s · 15 GETs · 1.60 MB · 8 MB peak | 0.130 s · 23 GETs · 1.90 MB · 14 MB peak | 85,580 | ✓ |
| diff-paused-range1pct | warm | 0.035 s · 0.035 s CPU · 8 MB peak | 0.027 s · 0.027 s CPU · 14 MB peak | 85,580 | ✓ |
| scan-head | cold | 2.800 s · 45 GETs · 150.06 MB · 285 MB peak | 2.485 s · 93 GETs · 131.99 MB · 401 MB peak | 10,001,124 | ✓ |
| scan-head | warm | 1.436 s · 1.435 s CPU · 284 MB peak | 1.245 s · 1.244 s CPU · 401 MB peak | 10,001,124 | ✓ |
| scan-head-range1pct | cold | 0.182 s · 19 GETs · 2.96 MB · 11 MB peak | 0.170 s · 37 GETs · 3.77 MB · 20 MB peak | 99,001 | ✓ |
| scan-head-range1pct | warm | 0.028 s · 0.028 s CPU · 11 MB peak | 0.036 s · 0.036 s CPU · 20 MB peak | 99,001 | ✓ |
| scan-1d-range1pct | cold | 0.202 s · 17 GETs · 2.95 MB · 11 MB peak | 0.165 s · 34 GETs · 3.73 MB · 20 MB peak | 99,001 | ✓ |
| scan-1d-range1pct | warm | 0.040 s · 0.041 s CPU · 11 MB peak | 0.036 s · 0.036 s CPU · 20 MB peak | 99,001 | ✓ |
| get-1k | cold | 0.587 s · 705 GETs · 32.37 MB · 156 MB peak | 0.568 s · 393 GETs · 85.31 MB · 387 MB peak | 991 | ✓ |
| get-1k | warm | 0.189 s · 0.189 s CPU · 156 MB peak | 0.298 s · 0.298 s CPU · 387 MB peak | 991 | ✓ |
| merge-newest4 | cold | 0.23 s · 0.07 s CPU · read 3.2 MB in 14 GETs · wrote 3.2 MB · 12 MB peak | 0.18 s · 0.08 s CPU · read 3.0 MB in 18 GETs · wrote 2.9 MB · 22 MB peak | 342,790 | ✓ |
| fold-into-base | cold | 3.52 s · 2.34 s CPU · read 150.1 MB in 45 GETs · wrote 105.7 MB · 285 MB peak | 3.25 s · 2.35 s CPU · read 132.0 MB in 93 GETs · wrote 93.9 MB · 402 MB peak | 10,001,124 | ✓ |

Stored: ours 150.1 MB (11.31 B/entry, metadata 0.17 MB), parquet 131.2 MB (9.88 B/entry, metadata 0.98 MB); base: ours 120.7 MB, parquet 104.1 MB. 7 layers (at most 12), 720 commits, merges wrote 5.66 entries per entry committed. Layers: 0-1000:10000000, 1001-1511:2329185, 1512-1644:601685, 1645-1663:85506, 1664-1682:86432, 1683-1701:85929, 1702-1720:84923.

### daily-scattered, 10,000,000 keys

| query | mode | ours | parquet | results | fold |
|---|---|---|---|---|---|
| diff-1c | cold | 0.064 s · 2 GETs · 0.07 MB · 0 MB peak | 0.064 s · 2 GETs · 0.06 MB · 1 MB peak | 4,535 | ✓ |
| diff-1c | warm | 0.000 s · 0.000 s CPU · 0 MB peak | 0.001 s · 0.001 s CPU · 1 MB peak | 4,535 | ✓ |
| diff-1d-first10k | cold | 0.073 s · 6 GETs · 0.61 MB · 3 MB peak | 0.074 s · 10 GETs · 0.88 MB · 4 MB peak | 10,000 | ✓ |
| diff-1d-first10k | warm | 0.008 s · 0.008 s CPU · 4 MB peak | 0.007 s · 0.007 s CPU · 6 MB peak | 10,000 | ✓ |
| diff-1d | cold | 0.159 s · 10 GETs · 2.50 MB · 5 MB peak | 0.156 s · 22 GETs · 2.69 MB · 8 MB peak | 108,169 | ✓ |
| diff-1d | warm | 0.019 s · 0.018 s CPU · 6 MB peak | 0.020 s · 0.020 s CPU · 8 MB peak | 108,169 | ✓ |
| diff-1d-range1pct | cold | 0.063 s · 6 GETs · 0.09 MB · 0 MB peak | 0.063 s · 12 GETs · 0.18 MB · 1 MB peak | 997 | ✓ |
| diff-1d-range1pct | warm | 0.001 s · 0.001 s CPU · 0 MB peak | 0.002 s · 0.002 s CPU · 1 MB peak | 997 | ✓ |
| diff-paused-first10k | cold | 0.073 s · 12 GETs · 1.48 MB · 8 MB peak | 0.073 s · 28 GETs · 2.77 MB · 9 MB peak | 10,000 | ✓ |
| diff-paused-first10k | warm | 0.006 s · 0.006 s CPU · 10 MB peak | 0.008 s · 0.008 s CPU · 14 MB peak | 10,000 | ✓ |
| diff-paused | cold | 1.272 s · 31 GETs · 50.71 MB · 123 MB peak | 1.180 s · 92 GETs · 47.86 MB · 145 MB peak | 2,760,327 | ✓ |
| diff-paused | warm | 0.472 s · 0.472 s CPU · 126 MB peak | 0.369 s · 0.369 s CPU · 145 MB peak | 2,760,327 | ✓ |
| diff-paused-range1pct | cold | 0.103 s · 13 GETs · 0.69 MB · 3 MB peak | 0.109 s · 31 GETs · 1.03 MB · 5 MB peak | 27,525 | ✓ |
| diff-paused-range1pct | warm | 0.005 s · 0.006 s CPU · 3 MB peak | 0.013 s · 0.012 s CPU · 5 MB peak | 27,525 | ✓ |
| scan-head | cold | 3.009 s · 48 GETs · 171.57 MB · 333 MB peak | 2.796 s · 143 GETs · 153.93 MB · 447 MB peak | 9,946,366 | ✓ |
| scan-head | warm | 1.726 s · 1.726 s CPU · 333 MB peak | 1.414 s · 1.413 s CPU · 447 MB peak | 9,946,366 | ✓ |
| scan-head-range1pct | cold | 0.174 s · 17 GETs · 2.03 MB · 6 MB peak | 0.169 s · 43 GETs · 2.91 MB · 13 MB peak | 99,483 | ✓ |
| scan-head-range1pct | warm | 0.031 s · 0.031 s CPU · 6 MB peak | 0.038 s · 0.038 s CPU · 13 MB peak | 99,483 | ✓ |
| scan-1d-range1pct | cold | 0.157 s · 13 GETs · 1.98 MB · 6 MB peak | 0.169 s · 36 GETs · 2.77 MB · 12 MB peak | 99,512 | ✓ |
| scan-1d-range1pct | warm | 0.017 s · 0.017 s CPU · 6 MB peak | 0.037 s · 0.037 s CPU · 12 MB peak | 99,512 | ✓ |
| get-1k | cold | 0.789 s · 670 GETs · 64.81 MB · 257 MB peak | 0.863 s · 368 GETs · 112.10 MB · 533 MB peak | 992 | ✓ |
| get-1k | warm | 0.416 s · 0.416 s CPU · 257 MB peak | 0.552 s · 0.551 s CPU · 533 MB peak | 992 | ✓ |
| merge-newest4 | cold | 0.21 s · 0.07 s CPU · read 3.8 MB in 14 GETs · wrote 3.8 MB · 8 MB peak | 0.20 s · 0.07 s CPU · read 4.1 MB in 32 GETs · wrote 3.7 MB · 16 MB peak | 249,089 | ✓ |
| fold-into-base | cold | 3.83 s · 2.68 s CPU · read 171.6 MB in 48 GETs · wrote 128.2 MB · 333 MB peak | 3.74 s · 2.50 s CPU · read 153.9 MB in 143 GETs · wrote 107.9 MB · 448 MB peak | 9,946,366 | ✓ |

Stored: ours 171.6 MB (12.95 B/entry, metadata 0.18 MB), parquet 152.6 MB (11.52 B/entry, metadata 1.00 MB); base: ours 120.9 MB, parquet 105.8 MB. 7 layers (at most 12), 720 commits, merges wrote 5.80 entries per entry committed. Layers: 0-1000:9999809, 1001-1532:2394657, 1533-1665:601858, 1666-1684:86022, 1685-1703:86017, 1704-1719:72515, 1720-1720:4535.

### hot, 10,000,000 keys

| query | mode | ours | parquet | results | fold |
|---|---|---|---|---|---|
| diff-1c | cold | 0.062 s · 2 GETs · 0.04 MB · 0 MB peak | 0.063 s · 2 GETs · 0.04 MB · 0 MB peak | 2,844 | ✓ |
| diff-1c | warm | 0.000 s · 0.000 s CPU · 0 MB peak | 0.001 s · 0.001 s CPU · 0 MB peak | 2,844 | ✓ |
| diff-1d-first10k | cold | 0.105 s · 7 GETs · 0.57 MB · 10 MB peak | 0.113 s · 12 GETs · 0.97 MB · 8 MB peak | 4,994 | ✓ |
| diff-1d-first10k | warm | 0.015 s · 0.015 s CPU · 10 MB peak | 0.012 s · 0.012 s CPU · 8 MB peak | 4,994 | ✓ |
| diff-1d | cold | 0.103 s · 7 GETs · 0.57 MB · 10 MB peak | 0.110 s · 12 GETs · 0.97 MB · 8 MB peak | 4,994 | ✓ |
| diff-1d | warm | 0.015 s · 0.014 s CPU · 10 MB peak | 0.012 s · 0.012 s CPU · 8 MB peak | 4,994 | ✓ |
| diff-1d-range1pct | cold | 0.063 s · 6 GETs · 0.04 MB · 1 MB peak | 0.065 s · 9 GETs · 0.29 MB · 1 MB peak | 54 | ✓ |
| diff-1d-range1pct | warm | 0.001 s · 0.000 s CPU · 1 MB peak | 0.002 s · 0.001 s CPU · 1 MB peak | 54 | ✓ |
| diff-paused-first10k | cold | 0.314 s · 31 GETs · 7.95 MB · 79 MB peak | 0.272 s · 61 GETs · 12.11 MB · 62 MB peak | 5,000 | ✓ |
| diff-paused-first10k | warm | 0.105 s · 0.106 s CPU · 79 MB peak | 0.075 s · 0.074 s CPU · 61 MB peak | 5,000 | ✓ |
| diff-paused | cold | 0.278 s · 31 GETs · 7.95 MB · 80 MB peak | 0.289 s · 61 GETs · 12.11 MB · 61 MB peak | 5,000 | ✓ |
| diff-paused | warm | 0.112 s · 0.112 s CPU · 79 MB peak | 0.088 s · 0.088 s CPU · 61 MB peak | 5,000 | ✓ |
| diff-paused-range1pct | cold | 0.065 s · 18 GETs · 0.22 MB · 4 MB peak | 0.067 s · 33 GETs · 0.95 MB · 6 MB peak | 54 | ✓ |
| diff-paused-range1pct | warm | 0.002 s · 0.002 s CPU · 4 MB peak | 0.004 s · 0.004 s CPU · 6 MB peak | 54 | ✓ |
| scan-head | cold | 2.922 s · 48 GETs · 128.66 MB · 296 MB peak | 2.700 s · 109 GETs · 116.61 MB · 380 MB peak | 10,000,000 | ✓ |
| scan-head | warm | 1.512 s · 1.513 s CPU · 296 MB peak | 1.241 s · 1.239 s CPU · 380 MB peak | 10,000,000 | ✓ |
| scan-head-range1pct | cold | 0.163 s · 22 GETs · 1.56 MB · 8 MB peak | 0.204 s · 45 GETs · 2.82 MB · 14 MB peak | 100,000 | ✓ |
| scan-head-range1pct | warm | 0.025 s · 0.025 s CPU · 8 MB peak | 0.019 s · 0.019 s CPU · 14 MB peak | 100,000 | ✓ |
| scan-1d-range1pct | cold | 0.174 s · 18 GETs · 1.54 MB · 7 MB peak | 0.175 s · 40 GETs · 2.64 MB · 13 MB peak | 100,000 | ✓ |
| scan-1d-range1pct | warm | 0.034 s · 0.034 s CPU · 7 MB peak | 0.034 s · 0.034 s CPU · 13 MB peak | 100,000 | ✓ |
| get-1k | cold | 0.708 s · 587 GETs · 31.49 MB · 275 MB peak | 0.591 s · 312 GETs · 79.84 MB · 457 MB peak | 1,000 | ✓ |
| get-1k | warm | 0.307 s · 0.307 s CPU · 275 MB peak | 0.345 s · 0.344 s CPU · 457 MB peak | 1,000 | ✓ |
| merge-newest4 | cold | 0.12 s · 0.03 s CPU · read 0.9 MB in 10 GETs · wrote 0.8 MB · 15 MB peak | 0.13 s · 0.03 s CPU · read 1.5 MB in 17 GETs · wrote 1.2 MB · 14 MB peak | 214,893 | ✓ |
| fold-into-base | cold | 3.68 s · 2.29 s CPU · read 128.7 MB in 48 GETs · wrote 120.7 MB · 296 MB peak | 3.48 s · 2.27 s CPU · read 116.6 MB in 109 GETs · wrote 104.3 MB · 377 MB peak | 10,000,000 | ✓ |

Stored: ours 128.7 MB (10.69 B/entry, metadata 0.15 MB), parquet 115.0 MB (9.56 B/entry, metadata 0.86 MB); base: ours 120.7 MB, parquet 104.1 MB. 10 layers (at most 11), 720 commits, merges wrote 6.38 entries per entry committed. Layers: 0-1000:10000000, 1001-1196:554190, 1197-1392:554294, 1393-1588:554011, 1589-1616:79140, 1617-1644:79208, 1645-1672:79209, 1673-1700:79066, 1701-1719:53774, 1720-1720:2844.

### rewrite, 10,000,000 keys

| query | mode | ours | parquet | results | fold |
|---|---|---|---|---|---|
| diff-1c | cold | 0.145 s · 4 GETs · 1.00 MB · 5 MB peak | 0.152 s · 7 GETs · 0.98 MB · 8 MB peak | 98,833 | ✓ |
| diff-1c | warm | 0.014 s · 0.014 s CPU · 5 MB peak | 0.017 s · 0.016 s CPU · 8 MB peak | 98,833 | ✓ |
| diff-1d-first10k | cold | 0.287 s · 17 GETs · 7.20 MB · 29 MB peak | 0.271 s · 34 GETs · 7.66 MB · 38 MB peak | 10,000 | ✓ |
| diff-1d-first10k | warm | 0.045 s · 0.045 s CPU · 33 MB peak | 0.048 s · 0.047 s CPU · 46 MB peak | 10,000 | ✓ |
| diff-1d | cold | 1.145 s · 27 GETs · 26.09 MB · 74 MB peak | 0.992 s · 53 GETs · 23.97 MB · 97 MB peak | 2,372,284 | ✓ |
| diff-1d | warm | 0.318 s · 0.318 s CPU · 78 MB peak | 0.291 s · 0.290 s CPU · 97 MB peak | 2,372,284 | ✓ |
| diff-1d-range1pct | cold | 0.000 s · 0 GETs · 0.00 MB · 0 MB peak | 0.000 s · 0 GETs · 0.00 MB · 0 MB peak | 0 | ✓ |
| diff-1d-range1pct | warm | 0.000 s · 0.000 s CPU · 0 MB peak | 0.000 s · 0.000 s CPU · 0 MB peak | 0 | ✓ |
| diff-paused-first10k | cold | 0.080 s · 19 GETs · 3.90 MB · 19 MB peak | 0.091 s · 43 GETs · 7.39 MB · 28 MB peak | 10,000 | ✓ |
| diff-paused-first10k | warm | 0.013 s · 0.013 s CPU · 20 MB peak | 0.016 s · 0.016 s CPU · 29 MB peak | 10,000 | ✓ |
| diff-paused | cold | 3.695 s · 51 GETs · 112.68 MB · 148 MB peak | 3.354 s · 115 GETs · 106.11 MB · 178 MB peak | 10,015,776 | ✓ |
| diff-paused | warm | 1.290 s · 1.290 s CPU · 152 MB peak | 1.142 s · 1.141 s CPU · 178 MB peak | 10,015,776 | ✓ |
| diff-paused-range1pct | cold | 0.148 s · 6 GETs · 1.23 MB · 5 MB peak | 0.156 s · 15 GETs · 1.56 MB · 9 MB peak | 100,132 | ✓ |
| diff-paused-range1pct | warm | 0.011 s · 0.011 s CPU · 5 MB peak | 0.024 s · 0.025 s CPU · 9 MB peak | 100,132 | ✓ |
| scan-head | cold | 4.727 s · 68 GETs · 233.54 MB · 364 MB peak | 4.215 s · 166 GETs · 212.18 MB · 475 MB peak | 9,983,709 | ✓ |
| scan-head | warm | 2.535 s · 2.534 s CPU · 364 MB peak | 2.202 s · 2.200 s CPU · 475 MB peak | 9,983,709 | ✓ |
| scan-head-range1pct | cold | 0.168 s · 10 GETs · 2.57 MB · 8 MB peak | 0.178 s · 27 GETs · 3.43 MB · 16 MB peak | 99,807 | ✓ |
| scan-head-range1pct | warm | 0.019 s · 0.018 s CPU · 8 MB peak | 0.038 s · 0.037 s CPU · 16 MB peak | 99,807 | ✓ |
| scan-1d-range1pct | cold | 0.181 s · 10 GETs · 2.57 MB · 8 MB peak | 0.162 s · 27 GETs · 3.43 MB · 16 MB peak | 99,807 | ✓ |
| scan-1d-range1pct | warm | 0.038 s · 0.037 s CPU · 8 MB peak | 0.017 s · 0.017 s CPU · 16 MB peak | 99,807 | ✓ |
| get-1k | cold | 0.989 s · 1070 GETs · 56.53 MB · 246 MB peak | 1.091 s · 738 GETs · 128.22 MB · 576 MB peak | 991 | ✓ |
| get-1k | warm | 0.369 s · 0.370 s CPU · 246 MB peak | 0.611 s · 0.612 s CPU · 576 MB peak | 991 | ✓ |
| diff-across-rewrite | cold | 3.599 s · 51 GETs · 112.68 MB · 152 MB peak | 3.243 s · 115 GETs · 106.11 MB · 178 MB peak | 9,983,709 | ✓ |
| diff-across-rewrite | warm | 1.267 s · 1.268 s CPU · 152 MB peak | 1.143 s · 1.142 s CPU · 178 MB peak | 9,983,709 | ✓ |
| diff-across-rewrite-first10k | cold | 0.077 s · 20 GETs · 4.43 MB · 19 MB peak | 0.086 s · 43 GETs · 7.39 MB · 28 MB peak | 10,000 | ✓ |
| diff-across-rewrite-first10k | warm | 0.011 s · 0.012 s CPU · 19 MB peak | 0.018 s · 0.019 s CPU · 29 MB peak | 10,000 | ✓ |
| merge-newest4 | cold | 0.71 s · 0.27 s CPU · read 10.0 MB in 20 GETs · wrote 10.0 MB · 21 MB peak | 0.55 s · 0.21 s CPU · read 9.3 MB in 36 GETs · wrote 9.1 MB · 36 MB peak | 988,457 | ✓ |
| fold-into-base | cold | 5.18 s · 3.08 s CPU · read 233.5 MB in 68 GETs · wrote 61.3 MB · 364 MB peak | 5.13 s · 3.19 s CPU · read 212.2 MB in 166 GETs · wrote 65.2 MB · 478 MB peak | 9,983,709 | ✓ |

Stored: ours 233.5 MB (11.26 B/entry, metadata 0.27 MB), parquet 210.8 MB (10.16 B/entry, metadata 1.57 MB); base: ours 120.9 MB, parquet 105.8 MB. 9 layers (at most 11), 269 commits, merges wrote 2.57 entries per entry committed. Layers: 0-1000:9999809, 1001-1187:2631912, 1188-1215:2767743, 1216-1243:2767776, 1244-1259:1581521, 1260-1263:395435, 1264-1267:395374, 1268-1268:98815, 1269-1269:98833.

### daily-clustered, 100,000,000 keys

| query | mode | ours | parquet | results | fold |
|---|---|---|---|---|---|
| diff-1c | cold | 0.103 s · 3 GETs · 0.45 MB · 2 MB peak | 0.104 s · 5 GETs · 0.42 MB · 4 MB peak | 45,833 | ✓ |
| diff-1c | warm | 0.004 s · 0.003 s CPU · 2 MB peak | 0.008 s · 0.008 s CPU · 4 MB peak | 45,833 | ✓ |
| diff-1d-first10k | cold | 0.077 s · 12 GETs · 1.63 MB · 14 MB peak | 0.082 s · 27 GETs · 2.80 MB · 21 MB peak | 10,000 | ✓ |
| diff-1d-first10k | warm | 0.009 s · 0.010 s CPU · 16 MB peak | 0.016 s · 0.016 s CPU · 23 MB peak | 10,000 | ✓ |
| diff-1d | cold | 0.701 s · 26 GETs · 15.35 MB · 29 MB peak | 0.614 s · 47 GETs · 14.23 MB · 48 MB peak | 1,088,832 | ✓ |
| diff-1d | warm | 0.161 s · 0.161 s CPU · 31 MB peak | 0.145 s · 0.145 s CPU · 48 MB peak | 1,088,832 | ✓ |
| diff-1d-range1pct | cold | 0.063 s · 8 GETs · 0.08 MB · 1 MB peak | 0.064 s · 12 GETs · 0.30 MB · 2 MB peak | 2,083 | ✓ |
| diff-1d-range1pct | warm | 0.001 s · 0.001 s CPU · 1 MB peak | 0.002 s · 0.002 s CPU · 2 MB peak | 2,083 | ✓ |
| diff-paused-first10k | cold | 0.112 s · 24 GETs · 3.63 MB · 30 MB peak | 0.156 s · 59 GETs · 9.65 MB · 46 MB peak | 10,000 | ✓ |
| diff-paused-first10k | warm | 0.025 s · 0.025 s CPU · 35 MB peak | 0.028 s · 0.027 s CPU · 49 MB peak | 10,000 | ✓ |
| diff-paused | cold | 6.498 s · 85 GETs · 290.24 MB · 589 MB peak | 5.949 s · 166 GETs · 271.56 MB · 690 MB peak | 28,104,616 | ✓ |
| diff-paused | warm | 3.827 s · 3.823 s CPU · 595 MB peak | 3.399 s · 3.396 s CPU · 690 MB peak | 28,104,616 | ✓ |
| diff-paused-range1pct | cold | 0.460 s · 25 GETs · 9.24 MB · 38 MB peak | 0.432 s · 45 GETs · 9.39 MB · 54 MB peak | 858,796 | ✓ |
| diff-paused-range1pct | warm | 0.113 s · 0.113 s CPU · 38 MB peak | 0.106 s · 0.106 s CPU · 54 MB peak | 858,796 | ✓ |
| scan-head | cold | 26.040 s · 199 GETs · 1497.81 MB · 841 MB peak | 23.793 s · 388 GETs · 1314.60 MB · 1009 MB peak | 100,011,829 | ✓ |
| scan-head | warm | 16.795 s · 16.783 s CPU · 841 MB peak | 14.927 s · 14.914 s CPU · 1009 MB peak | 100,011,829 | ✓ |
| scan-head-range1pct | cold | 0.728 s · 33 GETs · 21.52 MB · 55 MB peak | 0.708 s · 69 GETs · 20.61 MB · 77 MB peak | 985,835 | ✓ |
| scan-head-range1pct | warm | 0.239 s · 0.239 s CPU · 55 MB peak | 0.217 s · 0.217 s CPU · 77 MB peak | 985,835 | ✓ |
| scan-1d-range1pct | cold | 0.725 s · 27 GETs · 21.45 MB · 54 MB peak | 0.654 s · 60 GETs · 20.40 MB · 76 MB peak | 987,918 | ✓ |
| scan-1d-range1pct | warm | 0.228 s · 0.228 s CPU · 54 MB peak | 0.205 s · 0.206 s CPU · 76 MB peak | 987,918 | ✓ |
| get-1k | cold | 1.035 s · 1257 GETs · 36.24 MB · 203 MB peak | 2.249 s · 3179 GETs · 100.43 MB · 524 MB peak | 993 | ✓ |
| get-1k | warm | 0.318 s · 0.318 s CPU · 203 MB peak | 0.556 s · 0.556 s CPU · 525 MB peak | 993 | ✓ |
| merge-newest4 | cold | 0.36 s · 0.12 s CPU · read 5.8 MB in 15 GETs · wrote 5.8 MB · 25 MB peak | 0.36 s · 0.14 s CPU · read 5.3 MB in 28 GETs · wrote 5.2 MB · 41 MB peak | 589,580 | ✓ |
| fold-into-base | cold | 34.40 s · 25.36 s CPU · read 1497.8 MB in 199 GETs · wrote 1047.4 MB · 842 MB peak | 33.30 s · 25.08 s CPU · read 1314.6 MB in 388 GETs · wrote 934.0 MB · 1020 MB peak | 100,011,829 | ✓ |

Stored: ours 1497.8 MB (11.28 B/entry, metadata 1.67 MB), parquet 1312.2 MB (9.88 B/entry, metadata 9.82 MB); base: ours 1207.6 MB, parquet 1041.4 MB. 13 layers (at most 15), 720 commits, merges wrote 3.95 entries per entry committed. Layers: 0-1000:100000000, 1001-1427:19495223, 1428-1500:3318948, 1501-1579:3571700, 1580-1646:3045815, 1647-1674:1256034, 1675-1684:448915, 1685-1697:593746, 1698-1707:453419, 1708-1717:452081, 1718-1718:45833, 1719-1719:45833, 1720-1720:45833.

### daily-scattered, 100,000,000 keys

| query | mode | ours | parquet | results | fold |
|---|---|---|---|---|---|
| diff-1c | cold | 0.107 s · 3 GETs · 0.67 MB · 3 MB peak | 0.110 s · 4 GETs · 0.69 MB · 3 MB peak | 45,335 | ✓ |
| diff-1c | warm | 0.004 s · 0.004 s CPU · 3 MB peak | 0.009 s · 0.009 s CPU · 3 MB peak | 45,335 | ✓ |
| diff-1d-first10k | cold | 0.088 s · 12 GETs · 1.71 MB · 9 MB peak | 0.078 s · 38 GETs · 4.90 MB · 12 MB peak | 10,000 | ✓ |
| diff-1d-first10k | warm | 0.021 s · 0.021 s CPU · 12 MB peak | 0.009 s · 0.009 s CPU · 13 MB peak | 10,000 | ✓ |
| diff-1d | cold | 1.121 s · 31 GETs · 45.92 MB · 103 MB peak | 1.003 s · 90 GETs · 45.18 MB · 104 MB peak | 1,082,072 | ✓ |
| diff-1d | warm | 0.393 s · 0.393 s CPU · 106 MB peak | 0.347 s · 0.347 s CPU · 104 MB peak | 1,082,072 | ✓ |
| diff-1d-range1pct | cold | 0.102 s · 13 GETs · 0.60 MB · 3 MB peak | 0.077 s · 26 GETs · 1.02 MB · 4 MB peak | 10,736 | ✓ |
| diff-1d-range1pct | warm | 0.005 s · 0.005 s CPU · 3 MB peak | 0.012 s · 0.013 s CPU · 4 MB peak | 10,736 | ✓ |
| diff-paused-first10k | cold | 0.086 s · 18 GETs · 3.02 MB · 14 MB peak | 0.148 s · 44 GETs · 6.45 MB · 19 MB peak | 10,000 | ✓ |
| diff-paused-first10k | warm | 0.014 s · 0.014 s CPU · 19 MB peak | 0.029 s · 0.029 s CPU · 27 MB peak | 10,000 | ✓ |
| diff-paused | cold | 7.954 s · 88 GETs · 505.84 MB · 450 MB peak | 7.546 s · 253 GETs · 474.16 MB · 538 MB peak | 27,608,014 | ✓ |
| diff-paused | warm | 5.146 s · 5.142 s CPU · 455 MB peak | 4.432 s · 4.430 s CPU · 538 MB peak | 27,608,014 | ✓ |
| diff-paused-range1pct | cold | 0.261 s · 25 GETs · 5.72 MB · 12 MB peak | 0.290 s · 73 GETs · 6.41 MB · 19 MB peak | 274,921 | ✓ |
| diff-paused-range1pct | warm | 0.051 s · 0.051 s CPU · 12 MB peak | 0.052 s · 0.052 s CPU · 19 MB peak | 274,921 | ✓ |
| scan-head | cold | 27.281 s · 202 GETs · 1714.95 MB · 680 MB peak | 25.490 s · 475 GETs · 1533.36 MB · 867 MB peak | 99,465,848 | ✓ |
| scan-head | warm | 18.158 s · 18.145 s CPU · 680 MB peak | 16.189 s · 16.170 s CPU · 867 MB peak | 99,465,848 | ✓ |
| scan-head-range1pct | cold | 0.661 s · 33 GETs · 17.85 MB · 42 MB peak | 0.563 s · 103 GETs · 17.71 MB · 62 MB peak | 994,835 | ✓ |
| scan-head-range1pct | warm | 0.186 s · 0.185 s CPU · 42 MB peak | 0.170 s · 0.170 s CPU · 62 MB peak | 994,835 | ✓ |
| scan-1d-range1pct | cold | 0.638 s · 23 GETs · 17.60 MB · 41 MB peak | 0.592 s · 82 GETs · 17.17 MB · 60 MB peak | 994,949 | ✓ |
| scan-1d-range1pct | warm | 0.163 s · 0.163 s CPU · 41 MB peak | 0.147 s · 0.146 s CPU · 60 MB peak | 994,949 | ✓ |
| get-1k | cold | 2.164 s · 2346 GETs · 134.14 MB · 504 MB peak | 3.689 s · 4070 GETs · 319.41 MB · 989 MB peak | 988 | ✓ |
| get-1k | warm | 0.950 s · 0.947 s CPU · 504 MB peak | 1.458 s · 1.456 s CPU · 989 MB peak | 988 | ✓ |
| merge-newest4 | cold | 0.29 s · 0.11 s CPU · read 6.9 MB in 16 GETs · wrote 7.0 MB · 14 MB peak | 0.30 s · 0.12 s CPU · read 7.0 MB in 32 GETs · wrote 6.7 MB · 21 MB peak | 453,302 | ✓ |
| fold-into-base | cold | 37.33 s · 28.39 s CPU · read 1715.0 MB in 202 GETs · wrote 1282.1 MB · 680 MB peak | 36.00 s · 27.41 s CPU · read 1533.4 MB in 475 GETs · wrote 1079.0 MB · 870 MB peak | 99,465,848 | ✓ |

Stored: ours 1714.9 MB (12.95 B/entry, metadata 1.81 MB), parquet 1529.4 MB (11.55 B/entry, metadata 10.08 MB); base: ours 1209.1 MB, parquet 1057.6 MB. 10 layers (at most 15), 720 commits, merges wrote 4.16 entries per entry committed. Layers: 0-1000:100000741, 1001-1469:21100159, 1470-1611:6420877, 1612-1654:1946698, 1655-1697:1947456, 1698-1710:589256, 1711-1714:181290, 1715-1718:181312, 1719-1719:45365, 1720-1720:45335.

### hot, 100,000,000 keys

| query | mode | ours | parquet | results | fold |
|---|---|---|---|---|---|
| diff-1c | cold | 0.100 s · 3 GETs · 0.35 MB · 2 MB peak | 0.102 s · 4 GETs · 0.48 MB · 2 MB peak | 28,252 | ✓ |
| diff-1c | warm | 0.002 s · 0.003 s CPU · 2 MB peak | 0.006 s · 0.006 s CPU · 2 MB peak | 28,252 | ✓ |
| diff-1d-first10k | cold | 0.084 s · 12 GETs · 1.63 MB · 18 MB peak | 0.085 s · 22 GETs · 2.15 MB · 15 MB peak | 10,000 | ✓ |
| diff-1d-first10k | warm | 0.015 s · 0.014 s CPU · 20 MB peak | 0.019 s · 0.019 s CPU · 21 MB peak | 10,000 | ✓ |
| diff-1d | cold | 0.199 s · 22 GETs · 4.85 MB · 23 MB peak | 0.189 s · 42 GETs · 6.98 MB · 28 MB peak | 49,937 | ✓ |
| diff-1d | warm | 0.057 s · 0.057 s CPU · 26 MB peak | 0.047 s · 0.047 s CPU · 28 MB peak | 49,937 | ✓ |
| diff-1d-range1pct | cold | 0.063 s · 12 GETs · 0.12 MB · 1 MB peak | 0.064 s · 26 GETs · 0.37 MB · 2 MB peak | 461 | ✓ |
| diff-1d-range1pct | warm | 0.001 s · 0.001 s CPU · 1 MB peak | 0.002 s · 0.002 s CPU · 2 MB peak | 461 | ✓ |
| diff-paused-first10k | cold | 0.604 s · 49 GETs · 41.44 MB · 182 MB peak | 0.679 s · 126 GETs · 55.23 MB · 239 MB peak | 10,000 | ✓ |
| diff-paused-first10k | warm | 0.309 s · 0.308 s CPU · 170 MB peak | 0.246 s · 0.246 s CPU · 264 MB peak | 10,000 | ✓ |
| diff-paused | cold | 1.799 s · 62 GETs · 83.39 MB · 611 MB peak | 1.616 s · 153 GETs · 115.94 MB · 582 MB peak | 49,981 | ✓ |
| diff-paused | warm | 1.076 s · 1.075 s CPU · 628 MB peak | 0.724 s · 0.723 s CPU · 582 MB peak | 49,981 | ✓ |
| diff-paused-range1pct | cold | 0.106 s · 25 GETs · 1.05 MB · 14 MB peak | 0.119 s · 57 GETs · 2.87 MB · 16 MB peak | 461 | ✓ |
| diff-paused-range1pct | warm | 0.009 s · 0.009 s CPU · 14 MB peak | 0.018 s · 0.018 s CPU · 16 MB peak | 461 | ✓ |
| scan-head | cold | 23.770 s · 176 GETs · 1290.97 MB · 863 MB peak | 21.682 s · 375 GETs · 1158.98 MB · 916 MB peak | 100,000,000 | ✓ |
| scan-head | warm | 14.477 s · 14.468 s CPU · 863 MB peak | 12.585 s · 12.584 s CPU · 916 MB peak | 100,000,000 | ✓ |
| scan-head-range1pct | cold | 0.615 s · 33 GETs · 13.16 MB · 40 MB peak | 0.586 s · 88 GETs · 13.98 MB · 59 MB peak | 1,000,000 | ✓ |
| scan-head-range1pct | warm | 0.148 s · 0.148 s CPU · 40 MB peak | 0.133 s · 0.133 s CPU · 59 MB peak | 1,000,000 | ✓ |
| scan-1d-range1pct | cold | 0.583 s · 23 GETs · 13.07 MB · 40 MB peak | 0.545 s · 67 GETs · 13.66 MB · 58 MB peak | 1,000,000 | ✓ |
| scan-1d-range1pct | warm | 0.140 s · 0.140 s CPU · 40 MB peak | 0.128 s · 0.128 s CPU · 58 MB peak | 1,000,000 | ✓ |
| get-1k | cold | 1.255 s · 1134 GETs · 85.41 MB · 787 MB peak | 2.306 s · 2672 GETs · 179.87 MB · 1192 MB peak | 1,000 | ✓ |
| get-1k | warm | 0.754 s · 0.751 s CPU · 787 MB peak | 0.720 s · 0.720 s CPU · 1192 MB peak | 1,000 | ✓ |
| merge-newest4 | cold | 0.17 s · 0.06 s CPU · read 2.6 MB in 14 GETs · wrote 1.7 MB · 15 MB peak | 0.20 s · 0.07 s CPU · read 3.8 MB in 26 GETs · wrote 2.4 MB · 20 MB peak | 367,435 | ✓ |
| fold-into-base | cold | 34.06 s · 24.93 s CPU · read 1291.0 MB in 176 GETs · wrote 1207.9 MB · 863 MB peak | 31.25 s · 22.81 s CPU · read 1159.0 MB in 375 GETs · wrote 1043.3 MB · 927 MB peak | 100,000,000 | ✓ |

Stored: ours 1291.0 MB (10.73 B/entry, metadata 1.45 MB), parquet 1154.4 MB (9.59 B/entry, metadata 8.61 MB); base: ours 1207.6 MB, parquet 1041.4 MB. 13 layers (at most 14), 720 commits, merges wrote 4.08 entries per entry committed. Layers: 0-1000:100000000, 1001-1343:9696065, 1344-1539:5540437, 1540-1588:1385506, 1589-1637:1385481, 1638-1686:1385903, 1687-1693:197947, 1694-1700:197872, 1701-1707:197952, 1708-1714:197796, 1715-1718:113177, 1719-1719:28210, 1720-1720:28252.

### rewrite, 100,000,000 keys

| query | mode | ours | parquet | results | fold |
|---|---|---|---|---|---|
| diff-1c | cold | 0.462 s · 7 GETs · 10.03 MB · 36 MB peak | 0.440 s · 17 GETs · 9.17 MB · 51 MB peak | 988,420 | ✓ |
| diff-1c | warm | 0.077 s · 0.076 s CPU · 36 MB peak | 0.075 s · 0.075 s CPU · 51 MB peak | 988,420 | ✓ |
| diff-1d-first10k | cold | 2.787 s · 30 GETs · 137.68 MB · 300 MB peak | 2.697 s · 58 GETs · 133.65 MB · 413 MB peak | 10,000 | ✓ |
| diff-1d-first10k | warm | 1.412 s · 1.411 s CPU · 333 MB peak | 1.095 s · 1.094 s CPU · 445 MB peak | 10,000 | ✓ |
| diff-1d | cold | 7.720 s · 62 GETs · 351.06 MB · 300 MB peak | 7.123 s · 121 GETs · 319.35 MB · 413 MB peak | 23,724,628 | ✓ |
| diff-1d | warm | 3.676 s · 3.673 s CPU · 300 MB peak | 3.231 s · 3.227 s CPU · 413 MB peak | 23,724,628 | ✓ |
| diff-1d-range1pct | cold | 0.000 s · 0 GETs · 0.00 MB · 0 MB peak | 0.000 s · 0 GETs · 0.00 MB · 0 MB peak | 0 | ✓ |
| diff-1d-range1pct | warm | 0.000 s · 0.000 s CPU · 0 MB peak | 0.000 s · 0.000 s CPU · 0 MB peak | 0 | ✓ |
| diff-paused-first10k | cold | 0.090 s · 16 GETs · 3.59 MB · 22 MB peak | 0.148 s · 28 GETs · 10.83 MB · 38 MB peak | 10,000 | ✓ |
| diff-paused-first10k | warm | 0.023 s · 0.022 s CPU · 26 MB peak | 0.025 s · 0.026 s CPU · 46 MB peak | 10,000 | ✓ |
| diff-paused | cold | 22.624 s · 149 GETs · 1127.20 MB · 491 MB peak | 20.933 s · 270 GETs · 1043.24 MB · 621 MB peak | 100,160,208 | ✓ |
| diff-paused | warm | 12.441 s · 12.430 s CPU · 495 MB peak | 11.045 s · 11.035 s CPU · 621 MB peak | 100,160,208 | ✓ |
| diff-paused-range1pct | cold | 0.507 s · 11 GETs · 11.91 MB · 38 MB peak | 0.516 s · 36 GETs · 10.70 MB · 54 MB peak | 1,001,565 | ✓ |
| diff-paused-range1pct | warm | 0.101 s · 0.100 s CPU · 38 MB peak | 0.095 s · 0.094 s CPU · 54 MB peak | 1,001,565 | ✓ |
| scan-head | cold | 31.739 s · 263 GETs · 2336.31 MB · 706 MB peak | 28.284 s · 492 GETs · 2102.44 MB · 927 MB peak | 99,839,001 | ✓ |
| scan-head | warm | 24.089 s · 24.084 s CPU · 706 MB peak | 21.053 s · 21.035 s CPU · 895 MB peak | 99,839,001 | ✓ |
| scan-head-range1pct | cold | 0.650 s · 19 GETs · 24.04 MB · 60 MB peak | 0.574 s · 66 GETs · 22.00 MB · 93 MB peak | 998,382 | ✓ |
| scan-head-range1pct | warm | 0.186 s · 0.186 s CPU · 60 MB peak | 0.161 s · 0.161 s CPU · 90 MB peak | 998,382 | ✓ |
| scan-1d-range1pct | cold | 0.658 s · 19 GETs · 24.04 MB · 60 MB peak | 0.509 s · 66 GETs · 22.00 MB · 93 MB peak | 998,382 | ✓ |
| scan-1d-range1pct | warm | 0.185 s · 0.185 s CPU · 60 MB peak | 0.161 s · 0.162 s CPU · 90 MB peak | 998,382 | ✓ |
| get-1k | cold | 1.692 s · 2360 GETs · 58.81 MB · 288 MB peak | 3.353 s · 4946 GETs · 196.74 MB · 716 MB peak | 988 | ✓ |
| get-1k | warm | 0.449 s · 0.449 s CPU · 288 MB peak | 0.857 s · 0.856 s CPU · 716 MB peak | 988 | ✓ |
| diff-across-rewrite | cold | 22.640 s · 149 GETs · 1127.20 MB · 495 MB peak | 20.790 s · 270 GETs · 1043.24 MB · 621 MB peak | 99,839,001 | ✓ |
| diff-across-rewrite | warm | 12.460 s · 12.453 s CPU · 495 MB peak | 11.034 s · 11.024 s CPU · 621 MB peak | 99,839,001 | ✓ |
| diff-across-rewrite-first10k | cold | 0.091 s · 16 GETs · 3.59 MB · 22 MB peak | 0.161 s · 28 GETs · 10.83 MB · 38 MB peak | 10,000 | ✓ |
| diff-across-rewrite-first10k | warm | 0.023 s · 0.022 s CPU · 26 MB peak | 0.032 s · 0.032 s CPU · 46 MB peak | 10,000 | ✓ |
| merge-newest4 | cold | 2.85 s · 1.35 s CPU · read 70.2 MB in 31 GETs · wrote 70.2 MB · 148 MB peak | 2.66 s · 1.37 s CPU · read 64.0 MB in 74 GETs · wrote 63.8 MB · 210 MB peak | 6,919,285 | ✓ |
| fold-into-base | cold | 37.91 s · 30.51 s CPU · read 2336.3 MB in 263 GETs · wrote 612.3 MB · 707 MB peak | 37.23 s · 30.96 s CPU · read 2102.4 MB in 492 GETs · wrote 652.2 MB · 928 MB peak | 99,839,001 | ✓ |

Stored: ours 2336.3 MB (11.27 B/entry, metadata 2.68 MB), parquet 2098.8 MB (10.12 B/entry, metadata 15.77 MB); base: ours 1209.1 MB, parquet 1057.6 MB. 9 layers (at most 12), 269 commits, merges wrote 2.55 entries per entry committed. Layers: 0-1000:100000741, 1001-1178:17421551, 1179-1206:27678645, 1207-1234:27677606, 1235-1262:27678414, 1263-1266:3953893, 1267-1267:988316, 1268-1268:988656, 1269-1269:988420.
