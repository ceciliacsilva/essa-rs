use std::collections::HashSet;

use anna_api::lattice::{Lattice, SetLattice};
use essa_api::deltalake_save;
use essa_test_function::{
    append_foo, repeat_string_extern, to_uppercase_extern,
};

fn main() {
    let to_get_acquisitions = essa_api::datafusion_run(
        "SELECT frequencia, resistencia, temperatura FROM aquisicoes",
        "/home/ceciliacsilva/Desktop/delta-rs/shm",
    )
    .unwrap();

    println!("Testing R integration!");

    let batch_size = essa_api::run_r(
        "function(pzt_batch_measures){
            return(length(pzt_batch_measures$resistencia))
}",
        &[to_get_acquisitions.0],
    ).unwrap();

    let temperature = essa_api::run_r(
        "function(pzt_batch_measures){
            return(pzt_batch_measures$temperatura)
}",
        &[to_get_acquisitions.0],
    ).unwrap();

    let frequency_points_qnt = essa_api::run_r(
        "function(pzt_batch_measures){
            return(length(pzt_batch_measures$frequencia[[1]]))
}",
        &[to_get_acquisitions.0],
    ).unwrap();

    let to_measures = essa_api::run_r(
        r#"function(pzt_batch_measures, batch_size, frequency_points_qnt){
  batch_size <- unlist(batch_size)[[1]]
  frequency_points_qnt <- unlist(frequency_points_qnt)[[1]]
  impedance_r <- pzt_batch_measures$resistencia
  frequency_points <- pzt_batch_measures$frequencia[[1]]

  measure_matrix <- matrix(NA, frequency_points_qnt, batch_size)

  for(i in 1:batch_size){
    measure_matrix[,i]<-impedance_r[[i]]
  }
  return(measure_matrix)
}"#,
        &[to_get_acquisitions.0, batch_size.0, frequency_points_qnt.0],
    )
    .unwrap();

    let to_median_impedance = essa_api::run_r(
        r#"function(measure_df, batch_size, frequency_points_qnt){
  batch_size <- unlist(batch_size)[[1]]
  frequency_points_qnt <- unlist(frequency_points_qnt)[[1]]
  median_impedance_r = vector()
  measure_matrix = matrix(unlist(measure_df), ncol=batch_size)

  for(i in 1:frequency_points_qnt){
    median_impedance_r[i]=median(as.numeric(measure_matrix[i,1:batch_size]))
  }

  return(median_impedance_r)
}"#,
        &[to_measures.0, batch_size.0, frequency_points_qnt.0],
    )
    .unwrap();

    let _ = deltalake_save(
        "/home/ceciliacsilva/Desktop/shm/median",
        &[to_median_impedance.0],
    );

    let metric_calculation = essa_api::run_r(
    r#"function(measure_matrix, median_impedance_r, batch_size, frequency_points_qnt){
  batch_size <- unlist(batch_size)[[1]]
  frequency_points_qnt <- unlist(frequency_points_qnt)[[1]]
  metric_vector = vector()
  CCD = vector()
  measure_matrix = matrix(unlist(measure_matrix), ncol = batch_size)
  median_impedance_r = matrix(unlist(measure_matrix), ncol=1)

  for(i in 1:batch_size){
    metric_vector[i] = sqrt(  sum(  (measure_matrix[,i] - median_impedance_r)^2/frequency_points_qnt  ))

    CCD[i] = 1 - sum( (measure_matrix[,i] - mean(measure_matrix[,i])) *
                          (median_impedance_r - mean(median_impedance_r)) /
                                  (sd(measure_matrix[,i]) * sd(median_impedance_r)) ) / frequency_points_qnt
  }

  return(metric_vector)
}"#,
    &[to_measures.0, to_median_impedance.0, batch_size.0, frequency_points_qnt.0]
    ).unwrap();

    // TODO: this should be better.
    let _ = deltalake_save(
        "/home/ceciliacsilva/Desktop/shm/result",
        &[metric_calculation.0, temperature.0],
    );

    println!("Hello world from test function!");
    let result = to_uppercase_extern("foobar".into()).expect("extern function call failed");
    println!("Waiting for result...");
    let result = result.get().unwrap();
    println!("Function result: {:?}", result);

    println!("Storing a set in the kvs");
    let key = "some-test-key".into();
    essa_api::kvs_put(
        &key,
        &SetLattice::new(["one".into(), "two".into(), "three".into()].into()).into(),
    )
    .unwrap();

    let result = append_foo(result).expect("extern function call failed");
    println!("Waiting for result...");
    let result = result.get().unwrap();
    println!("Function result: {}", result);

    println!("Reading the set from the kvs");
    let lattice = essa_api::kvs_get(&key)
        .unwrap()
        .into_set()
        .unwrap()
        .into_revealed();
    println!(
        "Result: {:?}",
        lattice
            .iter()
            .map(|v| std::str::from_utf8(v))
            .collect::<Result<HashSet<_>, _>>()
            .unwrap()
    );

    println!("Appending to the set in the kvs");
    essa_api::kvs_put(
        &key,
        &SetLattice::new(["four".into(), "two".into(), "three".into()].into()).into(),
    )
    .unwrap();

    let result = repeat_string_extern(result, 15000).expect("extern function call failed");
    println!("Waiting for result...");
    let result = result.get().unwrap();
    println!("Function result: {}", result.len());

    println!("Reading the set from the kvs");
    let lattice = essa_api::kvs_get(&key)
        .unwrap()
        .into_set()
        .unwrap()
        .into_revealed();
    println!(
        "Result: {:?}",
        lattice
            .iter()
            .map(|v| std::str::from_utf8(v))
            .collect::<Result<HashSet<_>, _>>()
            .unwrap()
    );

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
