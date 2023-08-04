mod shm_outliers;
mod shm_polinomios;

use crate::{shm_outliers::rm_outliers_extern, shm_polinomios::polinomios_extern};

fn main() {
    // println!("Removing outliers");
    // for cycle in 0..20 {
    //     println!("{}", cycle);
    //     let removedp = rm_outliers_extern(13, cycle).unwrap();
    //     println!("Outliers removed? {:?}, cycle: {}", removedp.get().unwrap(), cycle);
    // }

    let okp = polinomios_extern(13, vec![]).unwrap();
    println!("polinimios ok? {:?}", okp.get());

    println!("DONE");
}
