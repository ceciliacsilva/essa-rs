use anna_api::ClientKey;
use essa_api::*;

fn to_measure_matrix() -> &'static str {
    r#"
function(impedance_r, batch_size, frequency_points_qnt){
  batch_size <- unlist(batch_size)[[1]]
  frequency_points_qnt <- unlist(frequency_points_qnt)[[1]]
  impedance_r <- matrix(unlist(impedance_r), ncol=batch_size)

  measure_matrix <- matrix(NA, frequency_points_qnt, batch_size)

  for(i in 1:batch_size){
    measure_matrix[,i]<-impedance_r[[i]]
  }
  return(measure_matrix)
}
"#
}

fn calc_coef() -> &'static str {
    r#"
function(matriz, medianas2, x, l, qnt_medidas){
  library('lmtest')
  l <- unlist(l)[[1]]
  qnt_medidas <- unlist(qnt_medidas)[[1]]
  x <- matrix(unlist(x), nrow=qnt_medidas)
  medianas2 = matrix(unlist(medianas2), nrow=l)
  matriz = matrix(unlist(matriz), ncol=qnt_medidas)

  print(l)
  print(qnt_medidas)
  print(x)
  print(dim(medianas2))
  print(dim(matriz))

  coef=matrix(NA,l,4)
  pressupostos=matrix(NA,l,3)
  r2=vector()

  for(i in 1:l)
    {
     y = medianas2[i,]
     print(y)
     print(x)
     modelo0=lm(y~x+I(x^2)+I(x^3))
     u=residuals(modelo0)
     modelo=lm(y[2:15]~x[2:15]+I(x[2:15]^2)+I(x[2:15]^3)+u[1:14])
     coef[i,]=modelo$coefficients[1:4]
     r2[i]=summary(modelo)$adj.r.squared
     sw=shapiro.test(residuals(modelo))$p.value
     dw=dwtest(modelo)$p.value
     bp=bptest(modelo)$p.value
     pressupostos[i,]=c(sw,dw,bp)
     }

  k=1:l
  c1=rep(0.05,l)
  c2=rep(0.7,l)

  dados=data.frame(k,pressupostos,r2)
  modelo=dados[dados[,2]>c1 & dados[,3]>c1 & dados[,4]>c1 & dados[,5]>c2 ,1]

  print(modelo)

  coeficientes = coef[modelo,]

  temp2 = x
  X=cbind(1, x, x^2, x^3)
  Imp_Pred_Bas=coeficientes%*%t(X)

  nr=nrow(Imp_Pred_Bas)
  nc=ncol(Imp_Pred_Bas)
  matriz2 = matriz

  print(dim(matriz2))

  CCD_Bas=vector()

  for(i in 1:nc)
  {

     CCD_Bas[i]= 1-  sum( ((matriz2[modelo,i] -  mean(matriz2[modelo,i])) *(Imp_Pred_Bas[,i] - mean(Imp_Pred_Bas[,i]))) /
                         (sd(matriz2[modelo,i]) * sd(Imp_Pred_Bas[,i]))   ) /nr
  }

  print('CCD_Bas')
  print(CCD_Bas)
  print('coeficientes')
  print(coeficientes)
  return(coeficientes)
}"#
}

fn _calc_ccd_bas() -> &'static str {
    "function(x, coeficientes, matriz, l, qnt_medidas) {
  l <- unlist(l)[[1]]
  qnt_medidas <- unlist(qnt_medidas)[[1]]
  x <- matrix(unlist(x), nrow=qnt_medidas)
  coeficientes = matrix(unlist(coeficientes), nrow=l)
  matriz = matrix(unlist(matriz), ncol=qnt_medidas)

}
"
}

#[essa_wrap(name = "get_median")]
pub fn median(pzt_id: u64) -> ClientKey {
    let get_median = essa_api::datafusion_run(
        &format!("SELECT col00 as r, col01 as sensor_id, col02 as ciclo, col03 as temperatura FROM median WHERE col01 = {}", pzt_id),
        "/home/ceciliacsilva/Desktop/delta-rs/median",
    ).unwrap();

    get_median.get_key().unwrap()
}

#[essa_wrap(name = "polinomios_extern")]
pub fn polinomios(pzt_medianas: ClientKey) -> bool {
    let sensor_id = essa_api::run_r(
        "function(pzt_medianas){
            return(pzt_medianas$sensor_id[[1]])
}",
        &[pzt_medianas.clone()],
    )
    .unwrap();

    let l = essa_api::run_r(
        "function(pzt_medianas){
            return(length(pzt_medianas$r[[1]]))
}",
        &[pzt_medianas.clone()],
    )
    .unwrap();

    let pzt_medianas_r = essa_api::run_r(
        "function(pzt_medianas, l){
            l <- unlist(l)[[1]]
            return(matrix(unlist(pzt_medianas$r), nrow=l))
}",
        &[pzt_medianas.clone(), l.get_key().unwrap()],
    )
    .unwrap();

    let qnt_medidas = essa_api::run_r(
        "function(pzt_medianas){
            return(length(pzt_medianas$temp))
}",
        &[pzt_medianas.clone()],
    )
    .unwrap();

    let x = essa_api::run_r(
        "function(pzt_medianas){
            return(pzt_medianas$temperatura)
}",
        &[pzt_medianas.clone()],
    )
    .unwrap();

    // XXX: is weird but the ordering is right like this.
    let matrix = essa_api::run_r(
        to_measure_matrix(),
        &[pzt_medianas_r.get_key().unwrap(), l.get_key().unwrap(), qnt_medidas.get_key().unwrap()],
    )
    .unwrap();

    let mediana2 = essa_api::run_r(
        "function(pzt_medianas, l, qnt_medidas) {
            qnt_medidas <- unlist(qnt_medidas)[[1]]
            l <- unlist(l)[[1]]

            medianas2 = matrix(NA,l,qnt_medidas)
            for(i in 1:qnt_medidas){
                medianas2[,i] = pzt_medianas$r[[i]]
            }
            return(medianas2)
}
",
        &[pzt_medianas.clone(), l.get_key().unwrap(), qnt_medidas.get_key().unwrap()],
    )
    .unwrap();

    let coef = essa_api::run_r(
        calc_coef(),
        &[
            matrix.get_key().unwrap(),
            mediana2.get_key().unwrap(),
            x.get_key().unwrap(),
            l.get_key().unwrap(),
            qnt_medidas.get_key().unwrap(),
        ],
    )
    .unwrap();

    println!("coef: {:?}, {:?}, {:?}", sensor_id.get_key(), x.get_key(), coef.get_key());

    let _pol = essa_api::deltalake_save(
        "/home/ceciliacsilva/Desktop/delta-rs/coef_collection",
        &[sensor_id.get_key().unwrap(), x.get_key().unwrap(), coef.get_key().unwrap()],
    )
    .unwrap();

    true
}
