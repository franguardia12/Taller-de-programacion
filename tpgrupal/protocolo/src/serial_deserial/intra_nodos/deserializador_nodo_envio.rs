use std::io::Read;

pub fn deserializar_envio_nodos<T: Read>(mut stream: T) -> Result<(String, u8, u16), String> {
    let mut header = [0u8; 7];
    stream
        .read_exact(&mut header)
        .map_err(|_| "Cantidad incorrecta de bytes en el header del mensaje.".to_string())?;

    let body_len = i32::from_be_bytes([header[3], header[4], header[5], header[6]]) as usize;
    let mut body = vec![0u8; body_len];
    stream
        .read_exact(&mut body)
        .map_err(|_| "Cantidad incorrecta de bytes en el body del mensaje.".to_string())?;

    let (query, responsabilidad, consistencia) = deserializar_body(&body)?;
    Ok((query, responsabilidad, consistencia))
}

fn deserializar_body(body: &[u8]) -> Result<(String, u8, u16), String> {
    let length_query = i32::from_be_bytes([body[0], body[1], body[2], body[3]]) as usize;

    let query = &body[4..(length_query + 4)];

    let consistencia = u16::from_be_bytes([body[4 + length_query], body[4 + length_query + 1]]);

    let responsabilidad = &body[(length_query + 4 + 2)..(length_query + 4 + 2 + 1)];

    let query = match String::from_utf8(query.to_vec()) {
        Ok(query) => query,
        Err(_) => return Err("No se pudo convertir el body a una cadena válida".to_string()),
    };
    Ok((query, responsabilidad[0], consistencia))
}
