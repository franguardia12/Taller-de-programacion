use cliente_servidor::client_services::{connection::connect_to_server, sending::send_request};
use haversine_rs::{distance, point::*, units::*};
use protocolo::serial_deserial::{
    cassandra::serializador_cliente_server::query_to_bytes_client_server,
    level_consistency::LevelConsistency,
};
use rand::Rng;
use std::{
    io::{self, Write},
    thread, time,
};

use interfaz::{handler::*, vuelo::Vuelo};

/// Gestiona la evolución de un 'Vuelo' a lo largo del trayecto entre los aeropuertos de origen y destino.
///
/// # Parameters
///
/// - `vuelo`: Un struct 'Vuelo' que será modificado en sus parámetros variables de trayectoria y combustible.
/// - `o_number`: Coordenadas del aeropuerto de origen.
/// - `d_number`: Coordenadas del aeropuerto de destino.
pub fn gestionar_vuelo(vuelo: &mut Vuelo, o_number: (f32, f32), d_number: (f32, f32)) {
    let query_insert_origen = construir_consulta_insert(vuelo, "VUELOS_ORIGEN".to_string());
    let query_insert_destino = construir_consulta_insert(vuelo, "VUELOS_DESTINO".to_string());
    console_send_query(query_insert_origen, LevelConsistency::Strong);

    console_send_query(query_insert_destino, LevelConsistency::Strong);

    let variacion_en_x = d_number.0 - o_number.0;
    let variacion_en_y = d_number.1 - o_number.1;

    let tiempo_en_la_consola = calculo_tiempo_vuelo(o_number, d_number, vuelo.velocidad_actual);
    actualizar_datos_vuelo(
        tiempo_en_la_consola,
        o_number,
        vuelo,
        variacion_en_x,
        variacion_en_y,
    );
}

fn calculo_tiempo_vuelo(
    o_coord_number: (f32, f32),
    d_coord_number: (f32, f32),
    velocidad_actual: f32,
) -> f32 {
    let p1 = Point::new(o_coord_number.0 as f64, o_coord_number.1 as f64);
    let p2 = Point::new(d_coord_number.0 as f64, d_coord_number.1 as f64);

    let distancia = distance(p1, p2, Unit::Kilometers) as f32;

    let tiempo_estimado = distancia / velocidad_actual;

    (tiempo_estimado * 10.0).round()
}

fn actualizar_datos_vuelo(
    duracion: f32,
    origen: (f32, f32),
    vuelo: &mut Vuelo,
    variacion_en_x: f32,
    variacion_en_y: f32,
) {
    let intervalo = 0.7;
    let paso = intervalo / duracion;
    let mut rng = rand::thread_rng();
    let resto_combustible = rng.gen_range(5..=10) as f32;

    for i in 0..=(duracion / intervalo) as usize {
        let progreso = paso * i as f32;
        (vuelo.latitud_actual, vuelo.longitud_actual) =
            actualizar_pos_vuelo(origen, variacion_en_x, variacion_en_y, progreso);
        actualizar_combustible(vuelo, progreso, resto_combustible);

        send_update_general(vuelo);

        thread::sleep(time::Duration::from_secs_f64(1.0));
    }
    (vuelo.latitud_actual, vuelo.longitud_actual) =
        actualizar_pos_vuelo(origen, variacion_en_x, variacion_en_y, 1.0);
    actualizar_combustible(vuelo, 1.0, resto_combustible);

    send_update_general(vuelo);
    send_update_estado(vuelo);

    thread::sleep(time::Duration::from_secs_f64(5.0));

    send_delete(vuelo);

    println!("\nVuelo completado!");
    io::stdout().flush().unwrap();
}

fn get_lat_long(buffer: &String) -> (String, String) {
    let buf = buffer.to_string();
    let j = (buf.trim()).split(",").collect::<Vec<&str>>();
    (j[2].to_string(), j[3].to_string())
}

fn pasar_a_float(coord: (String, String)) -> (f32, f32) {
    (
        coord.0.parse::<f32>().unwrap_or(0.0),
        coord.1.parse::<f32>().unwrap_or(0.0),
    )
}

fn actualizar_pos_vuelo(
    origen: (f32, f32),
    variacion_en_x: f32,
    variacion_en_y: f32,
    progreso: f32,
) -> (f32, f32) {
    (
        origen.0 + (progreso * variacion_en_x),
        origen.1 + (progreso * variacion_en_y),
    )
}

fn actualizar_combustible(vuelo: &mut Vuelo, progreso: f32, combustible_resto: f32) {
    vuelo.combustible = 100.0 - ((100.0 - combustible_resto) * progreso);
}

/// Dado un vector con todos los aeropuertos en la base de datos, determina la latitud y longitud de los
/// seleccionados por el cliente en la consola para el origen y el destino.
///
/// # Parameters
///
/// - `aeropuertos`: Un vector con los nombres de todos los aeropuertos disponibles en la base de datos.
/// - `vuelo`: Un struct 'Vuelo' al que se le cargará la latitud y longitud de salida.
///
/// # Returns
///
/// Devuelve una tupla de 2 tuplas, donde la primera corresponde a la latitudos y longitud del aeropuerto de origen,
/// y la segunda corresponde a la latitud y longitud del aeropuerto de destino.
pub fn obtener_posiciones_aeropuertos(
    aeropuertos: &Vec<String>,
    vuelo: &mut Vuelo,
) -> ((f32, f32), (f32, f32)) {
    let mut o = (String::new(), String::new());
    let mut d = (String::new(), String::new());
    for e in aeropuertos {
        if !o.0.is_empty() && !d.0.is_empty() {
            break;
        }
        if e.contains(&vuelo.origen) {
            o = get_lat_long(e);
        }
        if e.contains(&vuelo.destino) {
            d = get_lat_long(e);
        }
    }
    let o_number = pasar_a_float(o);
    vuelo.latitud_actual = o_number.0;
    vuelo.longitud_actual = o_number.1;

    (o_number, pasar_a_float(d))
}

/// Ejecuta una consulta a la base de datos para obtener todos los aeropuertos disponibles en ella.
///
/// # Returns
///
/// Devuelve una tupla de vectores, donde el primer campo son los nombres de los aeropuertos y el segundo
/// campo es la información que acompaña a dichos aeropuertos.
pub fn obtener_aeropuertos() -> (Vec<String>, Vec<String>) {
    let consulta_cql_aeropuertos =
        construir_consulta_select("AEROPUERTOS".to_string(), "".to_string(), "".to_string());
    let mut lineas_seleccionadas_aeropuertos: Vec<String> = vec![];
    while lineas_seleccionadas_aeropuertos.is_empty() {
        let resultado = ejecutar_consulta(
            consulta_cql_aeropuertos.to_string(),
            LevelConsistency::Strong,
        );
        lineas_seleccionadas_aeropuertos = resultado.unwrap_or_default();
    }

    let mut nombres_aeropuertos: Vec<String> = vec![];
    let mut info_aeropuertos: Vec<String> = vec![];

    for aer in lineas_seleccionadas_aeropuertos {
        info_aeropuertos.push(aer.to_string());
        let campos = aer.trim().split(",").collect::<Vec<&str>>();
        nombres_aeropuertos.push(campos[1].to_string());
    }

    (nombres_aeropuertos, info_aeropuertos)
}

fn send_update_general(vuelo: &Vuelo) {
    let query_update_origen = construir_consulta_update_consola(vuelo, "VUELOS_ORIGEN".to_string());
    let query_update_destino =
        construir_consulta_update_consola(vuelo, "VUELOS_DESTINO".to_string());

    console_send_query(query_update_origen, LevelConsistency::Weak);
    console_send_query(query_update_destino, LevelConsistency::Weak);
}

fn send_update_estado(vuelo: &Vuelo) {
    let update_origen_estado = construir_consulta_update(
        vuelo,
        "VUELOS_ORIGEN".to_string(),
        "ESTADO_VUELO".to_string(),
        "Arrived".to_string(),
    );
    let update_destino_estado = construir_consulta_update(
        vuelo,
        "VUELOS_DESTINO".to_string(),
        "ESTADO_VUELO".to_string(),
        "Arrived".to_string(),
    );

    console_send_query(update_origen_estado, LevelConsistency::Weak);
    console_send_query(update_destino_estado, LevelConsistency::Weak);
}

fn send_delete(vuelo: &Vuelo) {
    let query_delete_origen = construir_consulta_delete(vuelo, "VUELOS_ORIGEN".to_string());
    let query_delete_destino = construir_consulta_delete(vuelo, "VUELOS_DESTINO".to_string());

    console_send_query(query_delete_origen, LevelConsistency::Strong);
    console_send_query(query_delete_destino, LevelConsistency::Strong);
}

pub fn console_send_query(consulta: String, consistencia: LevelConsistency) {
    let mut conexion = connect_to_server();

    let consulta_serializada = query_to_bytes_client_server(&consulta, consistencia, 0x00);
    match conexion {
        Ok(ref mut c) => {
            if send_request(c, consulta_serializada).is_err() {
                println!("Fallo en la conexion al enviar la query.");
            }
        }
        Err(_) => {
            println!("fallo en la conexión")
        }
    }
}
