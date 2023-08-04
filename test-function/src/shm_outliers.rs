use essa_api::*;

fn to_measure_matrix() -> &'static str {
    r#"
function(measure_df, batch_size, frequency_points_qnt){
  batch_size <- unlist(batch_size)[[1]]
  frequency_points_qnt <- unlist(frequency_points_qnt)[[1]]

  impedance_r <- measure_df$resistencia

  measure_matrix <- matrix(NA, frequency_points_qnt, batch_size)

  for(i in 1:batch_size){
    measure_matrix[,i]<-impedance_r[[i]]
  }
  return(measure_matrix)
}
"#
}

fn to_median_impedance() -> &'static str {
    r#"
function(measure_matrix, batch_size, frequency_points_qnt){
  batch_size <- unlist(batch_size)[[1]]
  frequency_points_qnt <- unlist(frequency_points_qnt)[[1]]
  median_impedance_r = vector()
  measure_matrix = matrix(unlist(measure_matrix), ncol=batch_size)

  for(i in 1:frequency_points_qnt){
    median_impedance_r[i]=median(measure_matrix[i,1:batch_size])
  }

  return(median_impedance_r)
}"#
}

fn metric_calculation() -> &'static str {
    r#"function(measure_matrix, median_impedance_r, batch_size, frequency_points_qnt){
  batch_size <- unlist(batch_size)[[1]]
  frequency_points_qnt <- unlist(frequency_points_qnt)[[1]]
  metric_vector = vector()
  CCD = vector()
  measure_matrix = matrix(unlist(measure_matrix), ncol=batch_size)
  median_impedance_r = matrix(unlist(median_impedance_r), ncol=1)

  for(i in 1:batch_size){
    metric_vector[i] = sqrt(  sum(  (measure_matrix[,i] - median_impedance_r)^2/frequency_points_qnt  ))

    CCD[i] = 1 - sum( (measure_matrix[,i] - mean(measure_matrix[,i])) *
                          (median_impedance_r - mean(median_impedance_r)) /
                                  (sd(measure_matrix[,i]) * sd(median_impedance_r)) ) / frequency_points_qnt
  }

  return(metric_vector)
}"#
}

fn outliers_positions() -> &'static str {
    "
function(metric_vector) {
    metric_vector = matrix(unlist(metric_vector), ncol=1)
    outliers_value <- boxplot(metric_vector, plot=FALSE)$out
    outliers_positions = match(outliers_value, metric_vector)
    return(outliers_positions)
}
"
}

#[essa_wrap(name = "rm_outliers_extern")]
pub fn rm_outliers(pzt_id: u64, cycle: u64) -> bool {
    let pzt_batch_measures = essa_api::datafusion_run(
        &format!("SELECT sensor_id, resistencia, frequencia, temperatura, ciclo FROM aquisicoes WHERE sensor_id={} AND ciclo={}", pzt_id, cycle),
        "/home/ceciliacsilva/Desktop/delta-rs/pzt13",
    )
    .unwrap();

    println!("Running shm outliers removal!");

    let sensor_id = essa_api::run_r(
        "function(pzt_batch_measures){
            return(pzt_batch_measures$sensor_id[[1]])
}",
        &[pzt_batch_measures.0],
    )
    .unwrap();

    let cycle = essa_api::run_r(
        "function(pzt_batch_measures){
            return(pzt_batch_measures$ciclo[[1]])
}",
        &[pzt_batch_measures.0],
    )
    .unwrap();

    let batch_size = essa_api::run_r(
        "function(pzt_batch_measures){
            return(length(pzt_batch_measures$resistencia))
}",
        &[pzt_batch_measures.0],
    )
    .unwrap();

    let frequency_points_qnt = essa_api::run_r(
        "function(pzt_batch_measures){
            return(length(pzt_batch_measures$frequencia[[1]]))
}",
        &[pzt_batch_measures.0],
    )
    .unwrap();

    let temperature_median = essa_api::run_r(
        "function(pzt_batch_measures){
            return(median(pzt_batch_measures$temperatura))
}",
        &[pzt_batch_measures.0],
    )
    .unwrap();

    let measure_matrix = essa_api::run_r(
        to_measure_matrix(),
        &[pzt_batch_measures.0, batch_size.0, frequency_points_qnt.0],
    )
    .unwrap();

    let median_impedance_r = essa_api::run_r(
        to_median_impedance(),
        &[measure_matrix.0, batch_size.0, frequency_points_qnt.0],
    )
    .unwrap();

    let metric_vector = essa_api::run_r(
        metric_calculation(),
        &[
            measure_matrix.0,
            median_impedance_r.0,
            batch_size.0,
            frequency_points_qnt.0,
        ],
    )
    .unwrap();

    let outliers_position = essa_api::run_r(outliers_positions(), &[metric_vector.0]).unwrap();

    // TODO: this should be better.
    let _ = deltalake_save(
        "/home/ceciliacsilva/Desktop/delta-rs/metric-vector",
        &[metric_vector.0, cycle.0, sensor_id.0],
    )
    .unwrap();

    // TODO: this should be better.
    let _ = deltalake_save(
        "/home/ceciliacsilva/Desktop/delta-rs/median",
        &[
            median_impedance_r.0,
            sensor_id.0,
            cycle.0,
            temperature_median.0,
        ],
    )
    .unwrap();

    // TODO: this should be better.
    let _ = deltalake_save(
        "/home/ceciliacsilva/Desktop/delta-rs/outliers",
        &[outliers_position.0, cycle.0, sensor_id.0],
    )
    .unwrap();

    true
}
