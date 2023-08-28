mod shm_outliers;
mod shm_polinomios;

use crate::{
    shm_outliers::get_pzt_batch,
    shm_outliers::rm_outliers_extern,
    shm_polinomios::{get_median, polinomios_extern},
};
use essa_api::{dataframe_new, series_new, deltalake_save};

fn main() {
    let pzt_id = 13;
    let cycle = 0;

//     let pzt_batch_measures = essa_api::datafusion_run(
//         &format!("SELECT sensor_id, resistencia, frequencia, temperatura, ciclo FROM aquisicoes WHERE sensor_id={} AND ciclo={}", pzt_id, cycle),
//         "/home/ceciliacsilva/Desktop/delta-rs/pzt13",
//     ).unwrap();
//     println!("a = {pzt_batch_measures:?}");

//     let sensor_id = essa_api::run_r(
//         "function(pzt_batch_measures){
//             return(pzt_batch_measures$sensor_id[[1]])
// }",
//         &[pzt_batch_measures.get_key()],
//     )
//     .unwrap();

//     let _ = deltalake_save(
//         "/home/ceciliacsilva/Desktop/delta-rs/metric-vector",
//         &[
//             sensor_id.get_key(),
//         ],
//     )
//     .unwrap();

    // let median = get_median(pzt_id).unwrap().get();
    // println!("median: {:?}", median);
    // if median.is_err() {
    //for cycle in 0..1 {

    println!("Removing outliers from: {}", cycle);
    let pzt_batch_measure = get_pzt_batch(pzt_id, cycle).unwrap().get(); //.unwrap().expect("pzt_measure is None");
    println!("pzt batch measure: {:?}", pzt_batch_measure);
    let removedp = rm_outliers_extern(pzt_batch_measure.unwrap().expect("pzt is none")).unwrap();
    println!("Outliers removed? {:?}, cycle: {}", removedp.get(), cycle);
    //}
    // }

    // let median = get_median(pzt_id).unwrap().get().unwrap().expect("Failed to get median");
    // let polinomiosp = polinomios_extern(median).unwrap().get().unwrap();
    // println!("Polinomios salvos? {:?}", polinomiosp);

    // let s1 = series_new("A", &vec![1.0, 2.0]).unwrap();
    // let s2 = series_new("B", &vec![3.0, 4.0]).unwrap();
    // let dataframe = dataframe_new(&vec![s1.get_key(), s2.get_key()]).unwrap();

    // println!("Saving a `dataframe`");
    // let _dat = essa_api::deltalake_save(
    //     "/home/ceciliacsilva/Desktop/delta-rs/dataframe",
    //     &[dataframe.get_key()],
    // )
    // .unwrap();

    println!("DONE");
}
