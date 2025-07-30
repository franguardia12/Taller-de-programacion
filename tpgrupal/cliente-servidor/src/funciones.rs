use std::{
    fs::File,
    io::{self, BufRead},
    net::TcpStream,
    path::Path,
    sync::Arc,
};

use protocolo::serial_deserial::{
    cassandra::serializador_cliente_server::query_to_bytes_client_server,
    level_consistency::LevelConsistency,
};
use rustls::{ClientConnection, ServerName, StreamOwned};
use seguridad::create_client_config;

use crate::client_services::sending::send_request;

pub fn construir_consulta_create_keyspace(
    nombre: String,
    strategy: String,
    replicacion_factor: usize,
) -> String {
    let consulta = format!(
        "CREATE KEYSPACE {} WITH replication = {{'class': '{}', 'replication_factor' : {}}}",
        nombre, strategy, replicacion_factor
    );
    consulta
}

pub fn construir_consulta_create_table(
    tabla: String,
    campos: Vec<String>,
    tipos: Vec<String>,
    partition_key: Vec<String>,
    clustering_colum: Vec<String>,
) -> String {
    let mut columnas: Vec<String> = vec![];

    for i in 0..campos.len() {
        let nueva_columnas = [campos[i].to_string(), tipos[i].to_string()];
        columnas.push(nueva_columnas.join(" "));
    }
    let partition_k = partition_key.join(", ");
    let cluster_c = clustering_colum.join(", ");
    let key = format!("PRIMARY KEY (({}), {})", partition_k, cluster_c);
    columnas.push(key);

    let c = columnas.join(", ");

    let consulta = format!("CREATE TABLE {} ({})", tabla, c);
    consulta
}

pub fn send_query(consulta: String, consistencia: LevelConsistency) {
    let mut conexion = conectarse_al_servidor();

    let consulta_serializada = query_to_bytes_client_server(&consulta, consistencia, 0x00);
    match conexion {
        Ok(ref mut c) => {
            let _ = send_request(c, consulta_serializada);
        }
        Err(_) => {
            println!("Fallo en la conexion al enviar la query.")
        }
    }
}

pub fn conectarse_al_servidor() -> Result<StreamOwned<ClientConnection, TcpStream>, String> {
    let config = Arc::new(create_client_config()?);

    let archivo = File::open(Path::new("cliente-servidor/src/client_services/seeds.txt"))
        .map_err(|_| "Error al abrir el archivo".to_string())?;
    let reader = io::BufReader::new(archivo);
    for line in reader.lines() {
        let node_address = line.map_err(|_| "Error al leer la linea.".to_string())?;
        let domain = "localhost";
        let server_name = ServerName::try_from(domain)
            .map_err(|_| format!("Nombre de dominio inválido: {}", domain))?;

        if let Ok(stream) = TcpStream::connect(&node_address) {
            let client_conn = ClientConnection::new(Arc::clone(&config), server_name)
                .map_err(|e| format!("Error al crear la conexión TLS del cliente: {}", e))?;
            let tls_stream = StreamOwned::new(client_conn, stream);
            return Ok(tls_stream);
        }
    }
    Err("No se pudo conectar a ningun nodo".to_string())
}
