# The configurations measured (T44): each format at its balanced point, from
# the 10M sweeps (results/sweep*).
OURS="block=32768 level=1"
PARQUET="rg_rows=131072 page_bytes=32768 page_rows=4096 unit_pages=4 level=1"
COMMON="coalesce=65536 one_get=1048576"
