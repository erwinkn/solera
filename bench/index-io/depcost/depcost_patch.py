import sys
d = sys.argv[1]
p = f"{d}/Cargo.toml"
s = open(p).read()
s = s.replace('arrow-buffer = "60"\n', 'arrow-buffer = "60"\nobject_store = { version = "0.14", features = ["aws"] }\ntokio = { version = "1", features = ["rt-multi-thread"] }\nurl = "2"\n')
open(p, "w").write(s)
p = f"{d}/src/lib.rs"
s = open(p).read()
s = s.replace('#[pymodule]\n#[pyo3(name = "_native")]', 'mod io_probe;\n\n#[pymodule]\n#[pyo3(name = "_native")]', 1)
s = s.replace("    m.add_class::<SortedEntries>()?;", "    m.add_function(wrap_pyfunction!(io_probe::read_range, m)?)?;\n    m.add_class::<SortedEntries>()?;", 1)
open(p, "w").write(s)
