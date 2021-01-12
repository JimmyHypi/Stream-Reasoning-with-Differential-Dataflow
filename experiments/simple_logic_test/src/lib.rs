use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::iterate::Iterate;
use differential_dataflow::operators::join::Join;
use differential_dataflow::operators::reduce::Threshold;
use differential_dataflow::Collection;
use timely::dataflow::Scope;

// [IMPROVEMENT]:
// Now, THIS IS WRONG, but for this simple case it works. Every rule needs to be parametrized
// on the type of the encoder and the encoded triple should be:
// <E::EncoderDataset as IntoIterator>::Item
type EncodedTriple<T> = (T, T, T);

/// First rule: T(a, SCO, c) <= T(a, SCO, b),T(b, SCO, c)
// [IMPROVEMENT]:
// The current implementation of the function passes the translated value of S_C_O. My original
// idea was to pass parameter:
//      map: E::MapStructure,
// that as defined in the encoder module implements the BiMapTrait.
// This allows the filter operator to contain something like:
//      let v = if let Some(v) = map.get_right(&String::from(&model::S_C_O)) {
//          v
//      } else {
//          panic!("Throw error here");
//      };
//      triple.1 == v
// But this messes up all the lifetime as we would be required to pass the E::MapStructure inside
// the filter closure. This creates an odd error that I don't fully comprehend. Uncomment the next
// rule_1 function. To see the error and the overall situation.
// Passing the sco_value as a V would make the trait BiMapTrait useless..

pub fn rule_1<G, V>(
    data_collection: &Collection<G, EncodedTriple<V>>,
    sco_value: V,
) -> Collection<G, EncodedTriple<V>>
where
    G: Scope,
    G::Timestamp: Lattice,
    V: std::cmp::Eq + std::hash::Hash + Clone + Copy + differential_dataflow::ExchangeData,
    EncodedTriple<V>: timely::Data + Ord + std::fmt::Debug,
{
    let sco_transitive_closure =
        data_collection
            //.filter(|triple| triple.predicate == model::RDFS_SUB_CLASS_OF)
            .filter(move |triple| triple.1 == sco_value )
            .iterate(|inner| {

                inner
                    .map(|triple| (triple.2, (triple.0, triple.1)))
                    .join(&inner.map(|triple| (triple.0, (triple.1, triple.2))))
                    .map(|(_obj, ((subj1, pred1), (_pred2, obj2)))|
                        (subj1, pred1, obj2)
                    )
                    .concat(&inner)
                    .threshold(|_,c| { if c > &0 { 1 } else if c < &0 { -1 } else { 0 } })

            })
            //.inspect(|x| println!("AFTER_RULE_1: {:?}", x))
        ;

    sco_transitive_closure
}

/// Second rule: T(a, SPO, c) <= T(a, SPO, b),T(b, SPO, c)
pub fn rule_2<G, V>(
    data_collection: &Collection<G, EncodedTriple<V>>,
    spo_value: V,
) -> Collection<G, EncodedTriple<V>>
where
    G: Scope,
    G::Timestamp: Lattice,
    V: std::cmp::Eq + std::hash::Hash + Clone + Copy + differential_dataflow::ExchangeData,
    EncodedTriple<V>: timely::Data + Ord + std::fmt::Debug,
{
    let spo_transitive_closure = data_collection
        .filter(move |triple| triple.1 == spo_value)
        .iterate(|inner| {
            inner
                .map(|triple| (triple.2, (triple.0, triple.1)))
                .join(&inner.map(|triple| (triple.0, (triple.1, triple.2))))
                .map(|(_obj, ((subj1, pred1), (_pred2, obj2)))| (subj1, pred1, obj2))
                .concat(&inner)
                .threshold(|_, c| {
                    if c > &0 {
                        1
                    } else if c < &0 {
                        -1
                    } else {
                        0
                    }
                })
        });

    spo_transitive_closure
}

/// Third rule: T(x, TYPE, b) <= T(a, SCO, b),T(x, TYPE, a)
pub fn rule_3<G, V>(
    data_collection: &Collection<G, EncodedTriple<V>>,
    type_value: V,
    sco_value: V,
) -> Collection<G, EncodedTriple<V>>
where
    G: Scope,
    G::Timestamp: Lattice,
    V: std::cmp::Eq + std::hash::Hash + Clone + Copy + differential_dataflow::ExchangeData,
    EncodedTriple<V>: timely::Data + Ord + std::fmt::Debug,
{
    let sco_only = data_collection.filter(move |triple| triple.1 == sco_value);

    let candidates = data_collection
        .filter(move |triple| triple.1 == type_value)
        .map(|triple| (triple.2.clone(), (triple)))
        .join(&sco_only.map(|triple| (triple.0, ())))
        .map(|(_key, (triple, ()))| triple);

    let sco_type_rule = candidates.iterate(|inner| {
        let sco_only_in = sco_only.enter(&inner.scope());

        inner
            .map(|triple| (triple.2, (triple.0, triple.1)))
            .join(&sco_only_in.map(|triple| (triple.0, (triple.1, triple.2))))
            .map(|(_key, ((x, typ), (_sco, b)))| (x, typ, b))
            .concat(&inner)
            .threshold(|_, c| {
                if c > &0 {
                    1
                } else if c < &0 {
                    -1
                } else {
                    0
                }
            })
    });

    sco_type_rule
}

/// Fourth rule: T(x, p, b) <= T(p1, SPO, p),T(x, p1, y)
pub fn rule_4<G, V>(
    data_collection: &Collection<G, EncodedTriple<V>>,
    spo_value: V,
) -> Collection<G, EncodedTriple<V>>
where
    G: Scope,
    G::Timestamp: Lattice,
    V: std::cmp::Eq + std::hash::Hash + Clone + Copy + differential_dataflow::ExchangeData,
    EncodedTriple<V>: timely::Data + Ord + std::fmt::Debug,
{
    // Select only the triples whose predicate participates in a SPO triple
    let spo_only_out = data_collection.filter(move |triple| triple.1 == spo_value);

    let candidates = data_collection
        .map(|triple| ((triple.1.clone()), triple))
        .join(&spo_only_out.map(|triple| ((triple.0), ())))
        .map(|(_, (triple, ()))| triple);

    let spo_type_rule = candidates.iterate(|inner| {
        let spo_only = spo_only_out.enter(&inner.scope());
        inner
            .map(|triple| (triple.1, (triple.0, triple.2)))
            .join(&spo_only.map(|triple| (triple.0, (triple.1, triple.2))))
            .map(|(_key, ((x, y), (_spo, p)))| (x, p, y))
            .concat(&inner)
            .threshold(|_, c| {
                if c > &0 {
                    1
                } else if c < &0 {
                    -1
                } else {
                    0
                }
            })
    });
    spo_type_rule
}

/// Fifth rule: T(a, TYPE, D) <= T(p, DOMAIN, D),T(a, p, b)
pub fn rule_5<G, V>(
    data_collection: &Collection<G, EncodedTriple<V>>,
    domain_value: V,
    type_value: V,
) -> Collection<G, EncodedTriple<V>>
where
    G: Scope,
    G::Timestamp: Lattice,
    V: std::cmp::Eq + std::hash::Hash + Clone + Copy + differential_dataflow::ExchangeData,
    EncodedTriple<V>: timely::Data + Ord + std::fmt::Debug,
{
    let only_domain = data_collection.filter(move |triple| triple.1 == domain_value);

    let candidates = data_collection
        .map(|triple| ((triple.1.clone()), triple))
        .join(&only_domain.map(|triple| (triple.0, ())))
        .map(|(_, (triple, ()))| triple);

    // This does not require a iterative dataflow, the rule does not produce
    // terms that are used by the rule itself
    let domain_type_rule = candidates
        .map(|triple| (triple.1, (triple.0, triple.2)))
        .join(&only_domain.map(|triple| (triple.0, (triple.1, triple.2))))
        .map(move |(_key, ((a, _b), (_dom, d)))| (a, type_value, d));

    domain_type_rule
}

/// Sixth rule: T(b, TYPE, R) <= T(p, RANGE, R),T(a, p, b)
pub fn rule_6<G, V>(
    data_collection: &Collection<G, EncodedTriple<V>>,
    range_value: V,
    type_value: V,
) -> Collection<G, EncodedTriple<V>>
where
    G: Scope,
    G::Timestamp: Lattice,
    V: std::cmp::Eq + std::hash::Hash + Clone + Copy + differential_dataflow::ExchangeData,
    EncodedTriple<V>: timely::Data + Ord + std::fmt::Debug,
{
    let only_range = data_collection.filter(move |triple| triple.1 == range_value);

    let candidates = data_collection
        .map(|triple| ((triple.1.clone()), triple))
        .join(&only_range.map(|triple| (triple.0, ())))
        .map(|(_, (triple, ()))| triple);

    // This does not require a iterative dataflow, the rule does not produce
    // terms that are used by the rule itself
    let domain_type_rule = candidates
        .map(|triple| (triple.1, (triple.0, triple.2)))
        .join(&only_range.map(|triple| (triple.0, (triple.1, triple.2))))
        .map(move |(_key, ((_a, b), (_ran, r)))| (b, type_value, r));

    domain_type_rule
}

use differential_dataflow::operators::arrange::arrangement::ArrangeBySelf;
use differential_dataflow::operators::arrange::TraceAgent;
use differential_dataflow::trace::implementations::ord::OrdKeySpine;
use timely::dataflow::operators::probe::Probe;
use timely::dataflow::ProbeHandle;

/// Computes the full materialization of the collection
pub fn full_materialization<G, V>(
    data_input: &Collection<G, EncodedTriple<V>>,
    mut probe: &mut ProbeHandle<G::Timestamp>,
    // Contract:
    // rdfs_keywords[0] = sub_class_of
    // rdfs_keywords[1] = sub_property_of
    // rdfs_keywords[2] = sub_type
    // rdfs_keywords[3] = sub_domain
    // rdfs_keywords[4] = sub_range
    // [IMPROVEMENT]:
    // Maybe an HashMap here? Seems overkill still
    rdfs_keywords: &[V; 5],
) -> TraceAgent<OrdKeySpine<EncodedTriple<V>, G::Timestamp, isize>>
where
    G: Scope,
    G::Timestamp: Lattice,
    V: std::cmp::Eq + std::hash::Hash + Clone + Copy + differential_dataflow::ExchangeData,
    EncodedTriple<V>: timely::Data + Ord + std::fmt::Debug,
{
    // ASSUMPTION: WE ARE HARDCODING THE RULES IN HERE
    // We only have two kinds of rules:
    // the ones that deal with only the T_box:
    // T(a, SCO, c) <= T(a, SCO, b),T(b, SCO, c)
    // T(a, SPO, c) <= T(a, SPO, b),T(b, SPO, c)
    // the ones that deal with both the a_box and the t_box
    // T(x, TYPE, b) <= T(a, SCO, b),T(x, TYPE, a)
    // T(x, p, y) <= T(p1, SPO, p),T(x, p1, y)
    // T(a, TYPE, D) <= T(p, DOMAIN, D),T(a, p, b)
    // T(b, TYPE, R) <= T(p, RANGE, R),T(a, p, b)

    // Orders matters, to guarantee a correct execution of the materialization:
    // T(a, SCO, c) <= T(a, SCO, b),T(b, SCO, c)        -- rule_1
    // T(a, SPO, c) <= T(a, SPO, b),T(b, SPO, c)        -- rule_2
    // T(x, p, y) <= T(p1, SPO, p),T(x, p1, y)          -- rule_4
    // T(a, TYPE, D) <= T(p, DOMAIN, D),T(a, p, b)      -- rule_5
    // T(b, TYPE, R) <= T(p, RANGE, R),T(a, p, b)       -- rule_6
    // T(x, TYPE, b) <= T(a, SCO, b),T(x, TYPE, a)      -- rule_3
    // as we can see there is no rule with a literal in the body that
    // corresponds to a literal in the head of any subsequent rule

    let sco_transitive_closure = rule_1(&data_input, rdfs_keywords[0]);

    let spo_transitive_closure = rule_2(&data_input, rdfs_keywords[1]);

    let data_input = data_input
        .concat(&sco_transitive_closure)
        .concat(&spo_transitive_closure)
        //  VERY IMPORTANT: THE DISTINCT PUTS THE REMOVAL INTO ADDITION
        // SO WE REWRITE THE DISTINCT TO KEEP THE REMOVAL -1
        // .distinct()
        .threshold(|_, c| {
            if c > &0 {
                1
            } else if c < &0 {
                -1
            } else {
                0
            }
        });

    let spo_type_rule = rule_4(&data_input, rdfs_keywords[1]);

    let data_input = data_input.concat(&spo_type_rule).threshold(|_, c| {
        if c > &0 {
            1
        } else if c < &0 {
            -1
        } else {
            0
        }
    });

    let domain_type_rule = rule_5(&data_input, rdfs_keywords[3], rdfs_keywords[2]);

    // We don't need this, but still :P
    let data_input = data_input.concat(&domain_type_rule).threshold(|_, c| {
        if c > &0 {
            1
        } else if c < &0 {
            -1
        } else {
            0
        }
    });

    let range_type_rule = rule_6(&data_input, rdfs_keywords[4], rdfs_keywords[2]);

    let data_input = data_input.concat(&range_type_rule).threshold(|_, c| {
        if c > &0 {
            1
        } else if c < &0 {
            -1
        } else {
            0
        }
    });

    let sco_type_rule = rule_3(&data_input, rdfs_keywords[2], rdfs_keywords[0]);

    let data_input = data_input
        .concat(&sco_type_rule)
        // .distinct()
        .threshold(|_, c| {
            if c > &0 {
                1
            } else if c < &0 {
                -1
            } else {
                0
            }
        });

    let arrangement = data_input.arrange_by_self();

    arrangement.stream.probe_with(&mut probe);

    arrangement.trace
}
