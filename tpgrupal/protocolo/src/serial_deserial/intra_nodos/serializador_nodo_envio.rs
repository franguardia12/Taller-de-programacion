use crate::serial_deserial::level_consistency::LevelConsistency;

const FLAGS_HEADER_DEFAULT: u8 = 0x00;

pub fn serializar_envio_nodos(
    consulta_cql: &str,
    consistencia: LevelConsistency,
    responsabilidad: u8,
) -> Vec<u8> {
    let stream_id: u16 = 0x00;

    let body = serializar_body_query_nodos(consulta_cql, consistencia.valor(), responsabilidad);

    let header = serializar_header_nodos(FLAGS_HEADER_DEFAULT, stream_id, body.len() as i32);

    let mut frame = header;
    frame.extend(body);
    frame
}

pub fn serializar_header_nodos(flags: u8, stream_id: u16, lenght: i32) -> Vec<u8> {
    let mut header = Vec::with_capacity(9);
    header.push(flags);
    header.extend(&stream_id.to_be_bytes());
    header.extend(&lenght.to_be_bytes());
    header
}

fn serializar_body_query_nodos(
    consulta_cql: &str,
    consistency: u16,
    responsabilidad: u8,
) -> Vec<u8> {
    let cadena_bytes = consulta_cql.as_bytes();

    let mut result: Vec<u8> = vec![];
    result.extend((cadena_bytes.len() as i32).to_be_bytes());
    result.extend(cadena_bytes);
    result.extend(&consistency.to_be_bytes());
    result.extend(responsabilidad.to_be_bytes());

    result
}
