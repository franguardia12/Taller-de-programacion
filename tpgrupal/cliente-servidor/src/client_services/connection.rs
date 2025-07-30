use std::fs::File;
use std::io::{self, BufRead};
use std::net::TcpStream;
use std::path::Path;
use std::sync::Arc;

use rustls::{ClientConnection, ServerName, StreamOwned};
use seguridad::create_client_config;

pub fn connect_to_server() -> Result<StreamOwned<ClientConnection, TcpStream>, String> {
    let config = Arc::new(create_client_config()?);

    let archivo = File::open(Path::new(
        "cliente-servidor/src/client_services/seeds_client.txt",
    ))
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
