use std::io::Read;

const QUERY: i8 = 0x07;
const STARTUP: i8 = 0x01;

pub fn deserializar_consulta<T: Read>(mut stream: T) -> Result<(String, u16, i8), String> {
    let mut header = [0u8; 9];
    stream
        .read_exact(&mut header)
        .map_err(|_| "Cantidad incorrecta de bytes en el header del mensaje.".to_string())?;

    let op_code = i8::from_be_bytes([header[4]]);
    // Extraer el tamaño del cuerpo (body_len) del header (bytes 5-8)
    let body_len = i32::from_be_bytes([header[5], header[6], header[7], header[8]]) as usize;

    let mut body = vec![0u8; body_len];
    stream
        .read_exact(&mut body)
        .map_err(|_| "Cantidad incorrecta de bytes en el body del mensaje.".to_string())?;

    let (consulta_cql, consistency) = match op_code {
        QUERY => deserializar_body_query(&body)?,
        STARTUP => deserializar_body_startup(&body)?,
        _ => return Err("Tipo de consulta no soportado.".to_string()),
    };

    Ok((consulta_cql, consistency, op_code))
}

fn deserializar_body_query(body: &[u8]) -> Result<(String, u16), String> {
    let length_query = i32::from_be_bytes([body[0], body[1], body[2], body[3]]);

    let query = &body[4..(length_query as usize + 4)];

    let consistency = &body[(length_query as usize + 4)..(length_query as usize + 4 + 2)];
    let consist = u16::from_be_bytes([consistency[0], consistency[1]]);
    let query_string = match String::from_utf8(query.to_vec()) {
        Ok(query) => query,
        Err(_) => return Err("No se pudo convertir el body a una String válida".to_string()),
    };
    Ok((query_string, consist))
}

fn deserializar_body_startup(body: &[u8]) -> Result<(String, u16), String> {
    let len_cql_version = u16::from_be_bytes([body[2], body[3]]) as usize;
    let cql_version = &body[4..(len_cql_version + 4)];
    let cql_version_string = match String::from_utf8(cql_version.to_vec()) {
        Ok(mensaje) => mensaje,
        Err(_) => return Err("No se pudo convertir el body a una String válida".to_string()),
    };
    let len_version =
        u16::from_be_bytes([body[4 + len_cql_version], body[4 + len_cql_version + 1]]) as usize;
    let version = &body[(4 + len_cql_version + 2)..(4 + len_cql_version + 2 + len_version)];
    let version_string = match String::from_utf8(version.to_vec()) {
        Ok(mensaje) => mensaje,
        Err(_) => return Err("No se pudo convertir el body a una String válida".to_string()),
    };
    let mensaje = format!("{} - {}", cql_version_string, version_string);
    Ok((mensaje, 0x00))
}
