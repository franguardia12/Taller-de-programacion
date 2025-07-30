use std::io::Read;
use std::mem::size_of;

use crate::serial_deserial::cassandra::deserializador_server_cliente::obtener_rows_body;

const VOID: i32 = 0x0001;
const ROWS: i32 = 0x0002;

pub fn deserializar_respuesta_nodos<T: Read>(mut stream: T) -> Result<Vec<String>, String> {
    let mut header = [0u8; 7];
    stream
        .read_exact(&mut header)
        .map_err(|_| "Cantidad incorrecta de bytes en el header del mensaje.".to_string())?;

    let body_len = i32::from_be_bytes([header[3], header[4], header[5], header[6]]) as usize;
    let mut body = vec![0u8; body_len];
    stream
        .read_exact(&mut body)
        .map_err(|_| "Cantidad incorrecta de bytes en el body del mensaje.".to_string())?;
    let kind_respuesta = i32::from_be_bytes([body[0], body[1], body[2], body[3]]);
    match kind_respuesta {
        VOID => Ok([].to_vec()),
        ROWS => deserializar_body_rows_nodo(&body),
        _ => Ok([].to_vec()),
    }
}

fn deserializar_body_rows_nodo(body: &[u8]) -> Result<Vec<String>, String> {
    let rows_count = i32::from_be_bytes([body[4], body[5], body[6], body[7]]);

    let column_count = i32::from_be_bytes([body[8], body[9], body[10], body[11]]);

    // i = size_of(Kind) + size_of(Rows_count) + size_of(Column_count)
    let i = size_of::<i32>() + size_of::<i32>() + size_of::<i32>();

    obtener_rows_body(i, rows_count, column_count, body)
}
