#!/bin/bash
# T44: what Parquet costs the worker. The native module (native/, which workers
# load as solera._native) built from clean as it is, and with the `parquet`
# crate added (the same arrow-rs 60 it already uses) plus a function that
# writes and reads a Parquet batch, so the code is linked, not dropped.
# Then pyarrow, the Python route, as installed.
set -e
cd "$(dirname "$0")/../.."
source ~/.cargo/env 2>/dev/null || true
work=${TMPDIR:-/tmp}/layer-format/depcost
rm -rf "$work"; mkdir -p "$work"
for v in base parquet; do
  cp -r native "$work/$v"; rm -rf "$work/$v/target"
done
cat >> "$work/parquet/Cargo.toml.add" <<'TOML'
parquet = { version = "60", default-features = false, features = ["arrow", "zstd"] }
TOML
sed -i '/^\[dependencies\]/r '"$work/parquet/Cargo.toml.add" "$work/parquet/Cargo.toml"
cat >> "$work/parquet/src/lib.rs" <<'RS'

/// T44 probe: Parquet linked in, written and read back.
#[no_mangle]
pub extern "C" fn _parquet_probe(n: usize) -> usize {
    use arrow_array::{ArrayRef, Int64Array, RecordBatch};
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use parquet::arrow::ArrowWriter;
    let col: ArrayRef = std::sync::Arc::new(Int64Array::from_iter_values(0..n as i64));
    let batch = RecordBatch::try_from_iter([("v", col)]).unwrap();
    let mut buf = Vec::new();
    let mut w = ArrowWriter::try_new(&mut buf, batch.schema(), None).unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();
    let r = ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(buf)).unwrap().build().unwrap();
    r.map(|b| b.unwrap().num_rows()).sum()
}
RS
grep -q '^bytes' "$work/parquet/Cargo.toml" || sed -i '/^\[dependencies\]/a bytes = "1"' "$work/parquet/Cargo.toml"
for v in base parquet; do
  cd "$work/$v"
  cargo fetch -q
  t=$(date +%s.%N)
  CARGO_TARGET_DIR="$work/target-$v" systemd-run --user --scope -q -p CPUQuota=800% nice -n 10 cargo build --release -q -j 8 --features pyo3/extension-module
  t=$(echo "$(date +%s.%N) - $t" | bc)
  so=$(ls "$work/target-$v/release/"*.so | head -1)
  crates=$(cargo tree -e normal --prefix none 2>/dev/null | sed 's/ (\*)//' | sort -u | wc -l)
  printf '%s: clean release build %.0f s (8 jobs, as maturin builds it: pyo3/extension-module), %s %s bytes (stripped: %s), %s crates\n' "$v" "$t" "$(basename "$so")" "$(stat -c %s "$so")" \
    "$(strip -o "$work/stripped.so" "$so" && stat -c %s "$work/stripped.so")" "$crates"
  cd - > /dev/null
done
site=$(ls -d .venv/lib/python3*/site-packages)
echo "pyarrow (Python route): $(du -sm "$site/pyarrow" | cut -f1) MB installed; import pyarrow.parquet: $(.venv/bin/python -c 'import time; t=time.perf_counter(); import pyarrow.parquet; print(f"{time.perf_counter()-t:.2f} s")')"
echo "duckdb (server only today): $(du -cm "$site"/duckdb* "$site"/_duckdb* 2>/dev/null | tail -1 | cut -f1) MB installed"
