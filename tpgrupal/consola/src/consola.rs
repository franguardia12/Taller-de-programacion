use crate::logica::{obtener_aeropuertos, obtener_posiciones_aeropuertos};
use crate::threadpool::ThreadPool;
use crate::{
    input::{ingresar_nuevo_vuelo, obtener_datos_de_vuelo},
    logica::gestionar_vuelo,
};

pub fn ejecutar_consola(pool: ThreadPool) {
    let (nombres_aeropuertos, info_aeropuertos) = obtener_aeropuertos();

    loop {
        let mut vuelo = obtener_datos_de_vuelo(&nombres_aeropuertos);
        let (origen_number, destino_number) =
            obtener_posiciones_aeropuertos(&info_aeropuertos, &mut vuelo);

        pool.execute(move || gestionar_vuelo(&mut vuelo, origen_number, destino_number));

        let nuevo_vuelo = ingresar_nuevo_vuelo();
        if !nuevo_vuelo {
            pool.shutdown();
            break;
        }
    }
}
