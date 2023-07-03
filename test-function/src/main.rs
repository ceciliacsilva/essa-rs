use std::collections::HashSet;

use anna_api::lattice::{Lattice, SetLattice};
use essa_test_function::{
    append_foo, concurrent_kvs_test_extern, repeat_string_extern, to_uppercase_extern,
};

fn main() {
    let a = essa_api::datafusion_run(
        "SELECT * FROM demo",
        "/home/ceciliacsilva/external-projects/delta-rs/rust/tests/data/delta-0.8.0",
    )
    .unwrap();

    let serialized_result = a.wait().unwrap();
    println!("datafusion run: {:?}", serialized_result);

    println!("Testing R integration!");
    let result = essa_api::run_r("function(x) { x$value + c(1,2,3,4) }", &serialized_result).unwrap();
    let serialized_result = result.wait().unwrap();
    println!("Serialized R result: {:?}", serialized_result);

    let result = essa_api::run_r("function(x) { x }", &serialized_result).unwrap();
    let _serialized_result = result.wait().unwrap();

    // println!("Hello world from test function!");
    // let result = to_uppercase_extern("foobar".into()).expect("extern function call failed");
    // println!("Waiting for result...");
    // let result = result.get().unwrap();
    // println!("Function result: {:?}", result);

    // println!("Storing a set in the kvs");
    // let key = "some-test-key".into();
    // essa_api::kvs_put(
    //     &key,
    //     &SetLattice::new(["one".into(), "two".into(), "three".into()].into()).into(),
    // )
    // .unwrap();

    // let result = append_foo(result).expect("extern function call failed");
    // println!("Waiting for result...");
    // let result = result.get().unwrap();
    // println!("Function result: {}", result);

    // println!("Reading the set from the kvs");
    // let lattice = essa_api::kvs_get(&key)
    //     .unwrap()
    //     .into_set()
    //     .unwrap()
    //     .into_revealed();
    // println!(
    //     "Result: {:?}",
    //     lattice
    //         .iter()
    //         .map(|v| std::str::from_utf8(v))
    //         .collect::<Result<HashSet<_>, _>>()
    //         .unwrap()
    // );

    // println!("Appending to the set in the kvs");
    // essa_api::kvs_put(
    //     &key,
    //     &SetLattice::new(["four".into(), "two".into(), "three".into()].into()).into(),
    // )
    // .unwrap();

    // let result = repeat_string_extern(result, 15000).expect("extern function call failed");
    // println!("Waiting for result...");
    // let result = result.get().unwrap();
    // println!("Function result: {}", result.len());

    // println!("Reading the set from the kvs");
    // let lattice = essa_api::kvs_get(&key)
    //     .unwrap()
    //     .into_set()
    //     .unwrap()
    //     .into_revealed();
    // println!(
    //     "Result: {:?}",
    //     lattice
    //         .iter()
    //         .map(|v| std::str::from_utf8(v))
    //         .collect::<Result<HashSet<_>, _>>()
    //         .unwrap()
    // );

    // // TODO: this is not working right now. Should be fixed.
    // println!("Running concurrent KVS test");
    // let key: anna_api::ClientKey = "concurrent-kvs_test-key".into();
    // let range_start = 1;
    // let range_end = 10;
    // let result = concurrent_kvs_test_extern(key.clone(), range_start, range_end)
    //     .expect("concurrent kvs test call failed");
    // result.get().unwrap().expect("function failed");

    // println!("Reading the concurrent KVS test result set from the kvs");
    // let lattice = essa_api::kvs_get(&key)
    //     .unwrap()
    //     .into_set()
    //     .unwrap()
    //     .into_revealed();
    // let result_set = lattice
    //     .iter()
    //     .map(|v| {
    //         let s = std::str::from_utf8(v).context("result entry not utf8")?;
    //         let i = s.parse().context("result entry not an usize")?;
    //         Result::<usize, anyhow::Error>::Ok(i)
    //     })
    //     .collect::<Result<BTreeSet<_>, _>>()
    //     .unwrap();
    // assert_eq!(result_set, (range_start..range_end).collect());
    println!("DONE");
}
