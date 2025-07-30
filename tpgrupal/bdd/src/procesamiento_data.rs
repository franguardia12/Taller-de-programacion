use murmur3::murmur3_32;
use std::time::{SystemTime, UNIX_EPOCH};
use std::{
    collections::HashMap,
    fs::{self, File},
    io::{BufRead, BufReader, Cursor},
    path::Path,
};

use protocolo::parser_cql::condicion_where::CondicionWhere;

use crate::{endpoint_data::EndpointData, node_status::NodeStatus, tabla::Tabla};

pub fn new_metadata(ip_nodo: &String) -> HashMap<String, EndpointData> {
    let mut metadata_nodos: HashMap<String, EndpointData> = HashMap::new();
    let now = SystemTime::now();
    let mut unix_timestamp: f64 = 0.0;
    if let Ok(duration) = now.duration_since(UNIX_EPOCH) {
        // Obtener el valor numérico de segundos y fracciones
        unix_timestamp =
            duration.as_secs() as f64 + (duration.subsec_nanos() as f64 / 1_000_000_000.0);
    }

    let endpoint = EndpointData::new(unix_timestamp, 0, NodeStatus::Normal);
    metadata_nodos.insert(ip_nodo.to_string(), endpoint);
    metadata_nodos
}

pub fn obtener_hash_origen(query_insert: &str) -> u32 {
    let columnas_y_valores = query_insert.split(" VALUES ").collect::<Vec<&str>>();
    let columnas_sin_parentesis = columnas_y_valores[1].replace(['(', ')'], "");
    let columnas = columnas_sin_parentesis.split(",").collect::<Vec<&str>>();

    let origen = columnas[0].to_string();
    hashear(&origen).unwrap()
}

pub fn obtener_hash_key_select(condicion: &CondicionWhere) -> u32 {
    let valor1 = condicion.condicion1.to_string();
    let partes = valor1.split(" = ").collect::<Vec<&str>>();
    let nombre1 = partes[1].to_string();
    hashear(&nombre1).unwrap()
}

pub fn obtener_row(query_insert: &str) -> String {
    let columns_y_values_query = query_insert.split("VALUES").collect::<Vec<&str>>();
    let values_query = columns_y_values_query[1].trim().replace(['(', ')'], "");
    let values_separados = values_query.split(",").collect::<Vec<&str>>();
    let mut v = vec![];
    for e in values_separados {
        v.push(e.trim());
    }
    v.join(",")
}

pub fn hashear(dato: &str) -> Result<u32, String> {
    let mut cursor = Cursor::new(dato.as_bytes());
    match murmur3_32(&mut cursor, 0) {
        Ok(x) => Ok(x),
        Err(_) => Err("Error al calcular el hash".to_string()),
    }
}

pub fn get_data(path: String) -> Vec<String> {
    let mut data: Vec<String> = vec![];
    if let Ok(f) = File::open(path) {
        let reader = BufReader::new(f);

        for line in reader.lines().map_while(Result::ok) {
            data.push(line.trim().to_string());
        }
    }
    data
}

pub fn get_seeds() -> Vec<String> {
    let mut seeds: Vec<String> = vec![];
    let archivo = File::open(Path::new("cliente-servidor/src/client_services/seeds.txt"));
    if let Ok(archivo) = archivo {
        let reader = BufReader::new(archivo);
        for line in reader.lines().map_while(Result::ok) {
            seeds.push(line.split(":").collect::<Vec<&str>>()[0].to_string());
        }
    }
    seeds // [node1, node2, node3]
}

fn get_headers_aeropuertos() -> Vec<String> {
    [
        "ID_AEROPUERTO".to_string(),
        "NOMBRE".to_string(),
        "LATITUD".to_string(),
        "LONGITUD".to_string(),
    ]
    .to_vec()
}

fn get_headers_vuelos(tipo: &str) -> Vec<String> {
    match tipo {
        "DESTINO" => [
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
        .to_vec(),
        _ => [
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
        .to_vec(),
    }
}

fn inicializar_headers() -> (Vec<String>, Vec<String>, Vec<String>) {
    let headers_aeropuerto = get_headers_aeropuertos();

    let headers_origen = get_headers_vuelos("ORIGEN");

    let headers_destino = get_headers_vuelos("DESTINO");

    (headers_aeropuerto, headers_origen, headers_destino)
}

fn insert_data(data: Vec<String>, tabla: &mut Tabla) {
    for r in data {
        tabla.insertar(r);
    }
}

fn crear_tabla(
    name: String,
    headers_aeropuerto: Vec<String>,
    headers_origen: Vec<String>,
    headers_destino: Vec<String>,
) -> Tabla {
    let mut tabla = Tabla::new("".to_string(), vec![]);
    if name.contains("AEROPUERTOS") {
        tabla = Tabla::new("AEROPUERTOS".to_string(), headers_aeropuerto.clone());
    } else if name.contains("VUELOS_ORIGEN") {
        tabla = Tabla::new("VUELOS_ORIGEN".to_string(), headers_origen.clone());
    } else if name.contains("VUELOS_DESTINO") {
        tabla = Tabla::new("VUELOS_DESTINO".to_string(), headers_destino.clone());
    }
    tabla
}

pub fn load_tablas(path_keyspace: String, ip: String) -> Result<HashMap<String, Tabla>, String> {
    let mut tablas: HashMap<String, Tabla> = HashMap::new();
    let entradas =
        fs::read_dir(&path_keyspace).map_err(|_| "Base de datos incorrecta.".to_string())?;

    for entrada in entradas.map_while(Result::ok) {
        let ruta = entrada.path();
        if ruta.is_file() && ruta.extension().and_then(|ext| ext.to_str()) == Some("csv") {
            let name = ruta
                .file_name()
                .unwrap_or(std::ffi::OsStr::new(""))
                .to_string_lossy()
                .to_string();
            if name.contains(&ip) {
                let data = get_data(format!("{}/{}", &path_keyspace, name));
                let (headers_aeropuerto, headers_origen, headers_destino) = inicializar_headers();
                let mut tabla =
                    crear_tabla(name, headers_aeropuerto, headers_origen, headers_destino);
                if !tabla.nombre.is_empty() {
                    insert_data(data, &mut tabla);
                    tablas.insert(tabla.nombre.to_string(), tabla);
                }
            }
        }
    }
    if tablas.is_empty() {
        tablas = new_tablas();
    }

    Ok(tablas)
}

fn new_tablas() -> HashMap<String, Tabla> {
    let mut tablas: HashMap<String, Tabla> = HashMap::new();

    let (headers_aeropuerto, headers_origen, headers_destino) = inicializar_headers();

    let tabla1 = Tabla::new("AEROPUERTOS".to_string(), headers_aeropuerto.clone());
    let tabla2 = Tabla::new("VUELOS_ORIGEN".to_string(), headers_origen.clone());
    let tabla3 = Tabla::new("VUELOS_DESTINO".to_string(), headers_destino.clone());
    tablas.insert(tabla1.nombre.to_string(), tabla1);
    tablas.insert(tabla2.nombre.to_string(), tabla2);
    tablas.insert(tabla3.nombre.to_string(), tabla3);
    tablas
}
