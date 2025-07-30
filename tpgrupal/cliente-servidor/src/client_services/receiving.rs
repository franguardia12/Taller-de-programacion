use std::net::TcpStream;

use protocolo::serial_deserial::cassandra::deserializador_server_cliente::deserializar_respuesta;
use rustls::{ClientConnection, StreamOwned};

pub fn receive_response(
    socket: &mut StreamOwned<ClientConnection, TcpStream>,
) -> Result<(Vec<String>, i8), Box<dyn std::error::Error>> {
    let (deserialized_response, tipo) = deserializar_respuesta(socket)?;
    Ok((deserialized_response, tipo))
}
