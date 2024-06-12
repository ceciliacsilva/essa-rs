mod shm_outliers;
mod shm_polinomios;

use crate::{
    shm_outliers::get_pzt_batch,
    shm_outliers::rm_outliers_extern,
    shm_polinomios::{get_median, polinomios_extern},
};
use essa_api::{dataframe_new, deltalake_save, series_new};

fn main() {
    let pzt_id = 13;

    let median = get_median(pzt_id).unwrap().get();
    println!("median: {:?}", median);
    if median.is_err() {
        for cycle in 0..20 {
            println!("Removing outliers from: {}", cycle);
            let pzt_batch_measure: Result<anna_api::ClientKey, essa_api::EssaResult> =
                get_pzt_batch(pzt_id, cycle).unwrap().get();
            println!("pzt batch measure: {:?}", pzt_batch_measure);
            let key_pzt_batch_measure = pzt_batch_measure.unwrap();
            println!("key pzt batch measure: {:?}", key_pzt_batch_measure);

            let removedp = rm_outliers_extern(key_pzt_batch_measure).unwrap();
            println!("Outliers removed? {:?}, cycle: {}", removedp.get(), cycle);
        }
    }

    let median = get_median(pzt_id).unwrap().get().unwrap();
    let polinomiosp = polinomios_extern(median).unwrap().get().unwrap();
    println!("Polinomios salvos? {:?}", polinomiosp);

    let s1 = series_new("A", &vec![1.0, 2.0]).unwrap();
    let s2 = series_new("B", &vec![3.0, 4.0]).unwrap();
    let dataframe = dataframe_new(&vec![s1.get_key().unwrap(), s2.get_key().unwrap()]).unwrap();

    let f = essa_api::run_r("
function(dat) {
    dat <- data.frame(dat)
    return(unlist(dat[,1] + c(1.0, 1.0))[1])
}", &[dataframe.get_key().unwrap()]).unwrap();

    let _res = essa_api::deltalake_save(
        "/home/ceciliacsilva/Desktop/delta-rs/dataframe",
        &[f.get_key().unwrap()]
    ).unwrap();

    println!("DONE");
}
