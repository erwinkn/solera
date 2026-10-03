// Words in and out for run.bend: Rust writes the commits as u32 words, Bend
// reads them one at a time and writes the merged delta back the same way.

#include <stdio.h>
#include <stdlib.h>
#include <time.h>

static uint32_t* bx_in = NULL;
static uint32_t bx_in_n = 0;
static uint32_t* bx_out = NULL;
static uint32_t bx_out_n = 0, bx_out_cap = 0;

Term bx_load_run(Env e, Term* f, IoWork* w) {
  u64 len;
  char* path = io_cstr(e, f[0], &len);
  FILE* fp = fopen(path, "rb");
  free(path);
  if (!fp) return (Term)0;
  fseek(fp, 0, SEEK_END);
  long size = ftell(fp);
  fseek(fp, 0, SEEK_SET);
  bx_in = malloc(size);
  bx_in_n = (uint32_t)(fread(bx_in, 1, size, fp) / 4);
  fclose(fp);
  return (Term)bx_in_n;
}

Term bx_word_run(Env e, Term* f, IoWork* w) {
  uint32_t i = (uint32_t)f[0];
  return (Term)(i < bx_in_n ? bx_in[i] : 0);
}

Term bx_put_run(Env e, Term* f, IoWork* w) {
  if (bx_out_n == bx_out_cap) {
    bx_out_cap = bx_out_cap ? bx_out_cap * 2 : 1024;
    bx_out = realloc(bx_out, (size_t)bx_out_cap * 4);
  }
  bx_out[bx_out_n++] = (uint32_t)f[0];
  return term_pak(CID(Unit), 0);
}

Term bx_save_run(Env e, Term* f, IoWork* w) {
  u64 len;
  char* path = io_cstr(e, f[0], &len);
  FILE* fp = fopen(path, "wb");
  free(path);
  if (fp) {
    fwrite(bx_out, 4, bx_out_n, fp);
    fclose(fp);
  }
  return term_pak(CID(Unit), 0);
}

Term bx_micros_run(Env e, Term* f, IoWork* w) {
  return (Term)(uint32_t)(io_tick() / 1000);
}

Term bx_cpu_run(Env e, Term* f, IoWork* w) {
  return (Term)(uint32_t)((double)clock() * 1e6 / CLOCKS_PER_SEC);
}

static void __attribute__((constructor)) bx_use(void) {
  io_eff(CID(Bx.cpu), bx_cpu_run, 0);
  io_eff(CID(Bx.load), bx_load_run, 0);
  io_eff(CID(Bx.word), bx_word_run, 0);
  io_eff(CID(Bx.put), bx_put_run, 0);
  io_eff(CID(Bx.save), bx_save_run, 0);
  io_eff(CID(Bx.micros), bx_micros_run, 0);
}
