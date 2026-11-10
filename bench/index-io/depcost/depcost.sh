#!/bin/bash
set -e
D=~/.capy/work/depcost
for x in before after; do
  rm -rf $D/t-$x
  cd $D/$x
  s=$(date +%s.%N)
  CARGO_TARGET_DIR=$D/t-$x cargo build --release --features pyo3/extension-module --offline -q 2>&1 | grep -v "^warning" | tail -5 || true
  e=$(date +%s.%N)
  so=$D/t-$x/release/libsolera_native.so
  strip -o $D/$x.stripped.so $so
  n=$(CARGO_TARGET_DIR=$D/t-$x cargo tree -e normal --prefix none --offline 2>/dev/null | sort -u | wc -l)
  echo "$x build_s=$(python3 -c "print(round($e - $s,1))") so_bytes=$(stat -c %s $so) stripped_bytes=$(stat -c %s $D/$x.stripped.so) crates=$n"
done
