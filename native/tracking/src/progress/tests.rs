use super::*;

fn k(s: &str) -> Key {
    s.as_bytes().into()
}

fn range(lo: Option<&str>, hi: Option<&str>) -> KeyRange {
    KeyRange {
        lo: lo.map(k),
        hi: hi.map(k),
    }
}

fn at(c: u64) -> Value {
    Value::At(c, Arc::new(Conditions::default()))
}

/// The value of every key of a small universe, read through `pieces`.
fn decode_all(p: &Keyed, universe: &[&str]) -> Vec<Value> {
    universe
        .iter()
        .map(|u| p.value_of(u.as_bytes()).clone())
        .collect()
}

#[test]
fn write_matches_a_brute_force_map() {
    let universe: Vec<String> = (0..12).map(|i| format!("k{i:02}")).collect();
    let u: Vec<&str> = universe.iter().map(|s| s.as_str()).collect();
    let bounds: Vec<Option<&str>> = std::iter::once(None)
        .chain(u.iter().map(|s| Some(*s)))
        .collect();
    let mut seed = 7u64;
    let mut rnd = |n: usize| {
        seed = seed
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        ((seed >> 33) as usize) % n
    };
    for _ in 0..300 {
        let mut p = Keyed::default();
        let mut brute: Vec<Value> = vec![Value::Empty; u.len()];
        for _ in 0..12 {
            let a = rnd(bounds.len());
            let b = rnd(bounds.len() + 1);
            let lo = bounds[a];
            let hi = if b == bounds.len() { None } else { bounds[b] };
            let r = range(lo, hi);
            let v = match rnd(4) {
                0 => Value::Empty,
                1 => Value::Held,
                n => at(n as u64),
            };
            p.write(&r, v.clone());
            for (i, key) in u.iter().enumerate() {
                if r.contains(key.as_bytes()) {
                    brute[i] = v.clone();
                }
            }
            assert_eq!(decode_all(&p, &u), brute);
            assert!(
                p.values.windows(2).all(|w| w[0] != w[1]),
                "adjacent equal pieces merge"
            );
            assert_eq!(p.bounds.len() + 1, p.values.len());
            let whole: Vec<Piece> = p.pieces(&KeyRange::all());
            assert_eq!(whole.len(), p.values.len());
            let sub = range(lo, hi);
            for piece in p.pieces(&sub) {
                assert!(!piece.range.is_empty());
                for key in &u {
                    if piece.range.contains(key.as_bytes()) {
                        assert!(sub.contains(key.as_bytes()));
                        assert_eq!(p.value_of(key.as_bytes()), &piece.value);
                    }
                }
            }
            let mut e = Enc::new();
            p.encode(&mut e);
            let back = Keyed::decode(&mut Dec::new(&e.buf)).unwrap();
            assert_eq!(back, p);
        }
    }
}

#[test]
fn patterns_take_as_the_python_matcher_does() {
    let p = Patterns::new(
        vec![Pat::Glob("a/**/*.csv".into()), Pat::Regex("b[0-9]+".into())],
        vec![Pat::Glob("**/x*".into())],
    )
    .unwrap();
    for (key, taken) in [
        ("a/1.csv", true),
        ("a/b/c/2.csv", true),
        ("a/b/x.csv", false),
        ("b12", true),
        ("b12x", false),
        ("c.csv", false),
    ] {
        assert_eq!(p.takes(key.as_bytes()), taken, "{key}");
    }
    assert!(
        p.index_pattern().is_some_and(|ip| ip.include.is_empty()),
        "a regex include leaves the read unfiltered"
    );
    assert!(!p.exact_in_index());
    assert!(Patterns::new(vec![Pat::Regex("(".into())], vec![]).is_err());
}

#[test]
fn a_range_whose_low_end_is_not_below_its_high_end_is_refused() {
    for (lo, hi) in [("z", "a"), ("m", "m")] {
        let mut e = Enc::new();
        range(Some(lo), Some(hi)).encode(&mut e);
        assert!(KeyRange::decode(&mut Dec::new(&e.buf)).is_err());
    }
    let mut p = Keyed::default();
    p.points.insert(
        k("q"),
        Point {
            holding: Holding::Held,
            reason: Reason::Failed,
            seen: None,
        },
    );
    assert_eq!(p.points_in(&range(Some("z"), Some("a"))).count(), 0);
}
