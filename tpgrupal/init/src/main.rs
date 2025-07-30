use std::{
    fs::OpenOptions,
    io::{BufRead, BufReader},
    time,
};

use cliente_servidor::funciones::{
    construir_consulta_create_keyspace, construir_consulta_create_table, send_query,
};
use protocolo::serial_deserial::level_consistency::LevelConsistency;
use std::thread::sleep;

fn main() {
    crear_keyspace_aerolineas();

    crear_tabla_aeropuertos();

    insertar_aeropuertos();

    crear_tablas_vuelo_origen();

    crear_tabla_vuelo_destino();
}

fn crear_keyspace_aerolineas() {
    let query = construir_consulta_create_keyspace(
        "Aerolineas".to_string(),
        "SimpleStrategy".to_string(),
        3,
    );

    send_query(query, LevelConsistency::Strong);
}

fn crear_tabla_aeropuertos() {
    let campos = [
        "ID_AEROPUERTO".to_string(),
        "NOMBRE".to_string(),
        "LATITUD".to_string(),
        "LONGITUD".to_string(),
    ]
    .to_vec();

    let tipos = [
        "INT".to_string(),
        "TEXT".to_string(),
        "INT".to_string(),
        "INT".to_string(),
    ]
    .to_vec();

    let partition_key = ["ID_AEROPUERTO".to_string()].to_vec();
    let clustering_colum = ["NOMBRE".to_string()].to_vec();

    let query = construir_consulta_create_table(
        "AEROPUERTOS".to_string(),
        campos,
        tipos,
        partition_key,
        clustering_colum,
    );

    send_query(query, LevelConsistency::Strong);
}

fn insertar_aeropuertos() {
    let path = "init/src/aeropuertos.csv".to_string();
    let file = OpenOptions::new().read(true).open(path).unwrap();
    let reader = BufReader::new(file);

    for line in reader.lines().map_while(Result::ok) {
        if line.trim().is_empty() {
            break;
        }
        let campos = line.split(",").collect::<Vec<&str>>();
        let query = format!("INSERT INTO AEROPUERTOS (ID_AEROPUERTO, NOMBRE, LATITUD, LONGITUD) VALUES ({}, {}, {}, {})", campos[0], campos[1], campos[2], campos[3]);
        send_query(query, LevelConsistency::Strong);
        sleep(time::Duration::from_millis(75));
    }
}

fn crear_tablas_vuelo_origen() {
    let campos = [
        "ORIGEN".to_string(),
        "FECHA".to_string(),
        "ID_VUELO".to_string(),
        "DESTINO".to_string(),
        "ESTADO_VUELO".to_string(),
        "VELOCIDAD_ACTUAL".to_string(),
        "ALTITUD_ACTUAL".to_string(),
        "LATITUD_ACTUAL".to_string(),
        "LONGITUD_ACTUAL".to_string(),
        "COMBUSTIBLE".to_string(),
    ]
    .to_vec();

    let tipos = [
        "TEXT".to_string(),
        "TEXT".to_string(),
        "INT".to_string(),
        "TEXT".to_string(),
        "TEXT".to_string(),
        "INT".to_string(),
        "INT".to_string(),
        "INT".to_string(),
        "INT".to_string(),
        "INT".to_string(),
    ]
    .to_vec();

    let partition_key = ["ORIGEN".to_string()].to_vec();
    let clustering_colum = ["ID_VUELO".to_string()].to_vec();

    let crear_tabla_aeropuertos = construir_consulta_create_table(
        "VUELOS_ORIGEN".to_string(),
        campos,
        tipos,
        partition_key,
        clustering_colum,
    );

    send_query(crear_tabla_aeropuertos, LevelConsistency::Strong);
}

fn crear_tabla_vuelo_destino() {
    let campos = [
        "DESTINO".to_string(),
        "FECHA".to_string(),
        "ID_VUELO".to_string(),
        "ORIGEN".to_string(),
        "ESTADO_VUELO".to_string(),
        "VELOCIDAD_ACTUAL".to_string(),
        "ALTITUD_ACTUAL".to_string(),
        "LATITUD_ACTUAL".to_string(),
        "LONGITUD_ACTUAL".to_string(),
        "COMBUSTIBLE".to_string(),
    ]
    .to_vec();

    let tipos = [
        "TEXT".to_string(),
        "TEXT".to_string(),
        "INT".to_string(),
        "TEXT".to_string(),
        "TEXT".to_string(),
        "INT".to_string(),
        "INT".to_string(),
        "INT".to_string(),
        "INT".to_string(),
        "INT".to_string(),
    ]
    .to_vec();

    let partition_key = ["DESTINO".to_string()].to_vec();
    let clustering_colum = ["ID_VUELO".to_string()].to_vec();

    let crear_tabla_aeropuertos = construir_consulta_create_table(
        "VUELOS_DESTINO".to_string(),
        campos,
        tipos,
        partition_key,
        clustering_colum,
    );

    send_query(crear_tabla_aeropuertos, LevelConsistency::Strong);
}
