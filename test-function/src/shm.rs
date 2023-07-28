//! Contains some examples for essa-rs functions that can be called remotely.
//!
//! The `essa_wrap` attribute generates a wrapper function with the supplied
//! name to call the attributed function remotely. All functions can still be
//! called locally too, using their normal name.

use essa_api::*;

#[essa_wrap(name = "rm_outliers_extern")]
pub fn rm_outliers(pzt_id: u64, cycle: u64) -> bool {
    let to_get_acquisitions = essa_api::datafusion_run(
        &format!("SELECT sensor_id, resistencia, frequencia, ciclo FROM aquisicoes WHERE sensor_id={} AND ciclo={}", pzt_id, cycle),
        "/home/ceciliacsilva/Desktop/delta-rs/pzt",
    )
    .unwrap();

    println!("Testing R integration!");

    let batch_size = essa_api::run_r(
        "function(pzt_batch_measures){
            return(length(pzt_batch_measures$resistencia))
}",
        &[to_get_acquisitions.0],
    ).unwrap();

    let cycle = essa_api::run_r(
        "function(pzt_batch_measures){
            return(pzt_batch_measures$ciclo)
}",
        &[to_get_acquisitions.0],
    ).unwrap();

    let sensor_id = essa_api::run_r(
        "function(pzt_batch_measures){
            print(pzt_batch_measures$sensor_id)
            return(pzt_batch_measures$sensor_id)
}",
        &[to_get_acquisitions.0]
    ).unwrap();

    let _ = deltalake_save(
        "/home/ceciliacsilva/Desktop/delta-rs/id",
        &[sensor_id.0],
    );

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
        "/home/ceciliacsilva/Desktop/delta-rs/median",
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
        "/home/ceciliacsilva/Desktop/delta-rs/result",
        &[metric_calculation.0, temperature.0, cycle.0, sensor_id.0],
    );

    true
}
