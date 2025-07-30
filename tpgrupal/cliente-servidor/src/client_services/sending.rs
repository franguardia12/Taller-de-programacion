use std::io::Write;
use std::net::TcpStream;

use rustls::{ClientConnection, StreamOwned};

pub fn send_request(
    socket: &mut StreamOwned<ClientConnection, TcpStream>,
    serialized_request: Vec<u8>,
) -> Result<(), Box<dyn std::error::Error>> {
    socket.write_all(&serialized_request)?;
    Ok(())
}
