use crate::serial_deserial::level_consistency::LevelConsistency;

const FLAGS_HEADER_DEFAULT: u8 = 0x00;
const VERSION_CLIENT: u8 = 0x04;

const QUERY: i8 = 0x07;
const STARTUP: i8 = 0x01;

pub fn query_to_bytes_client_server(
    consulta_cql: &str,
    consistencia: LevelConsistency,
    flags_query: u8,
) -> Vec<u8> {
    let stream_id: u16 = 0x00;

    let body = serializar_body_query(consulta_cql, consistencia.valor(), flags_query);

    let header = serializar_header(
        VERSION_CLIENT,
        FLAGS_HEADER_DEFAULT,
        stream_id,
        QUERY,
        body.len() as i32,
    );

    let mut frame = header;
    frame.extend(body);
    frame
}

pub fn serializar_header(
    version: u8,
    flags: u8,
    stream_id: u16,
    op_code: i8,
    lenght: i32,
) -> Vec<u8> {
    let mut header = Vec::with_capacity(9);
    header.push(version);
    header.push(flags);
    header.extend(&stream_id.to_be_bytes());
    header.extend(op_code.to_be_bytes());
    header.extend(&lenght.to_be_bytes());
    header
}

fn serializar_body_query(consulta_cql: &str, consistency: u16, flags_query: u8) -> Vec<u8> {
    let cadena_bytes = consulta_cql.as_bytes();

    let mut result: Vec<u8> = vec![];
    result.extend((cadena_bytes.len() as i32).to_be_bytes());
    result.extend(cadena_bytes);
    result.extend(&consistency.to_be_bytes());
    result.push(flags_query);

    result
}

pub fn serializar_startup_client_server() -> Vec<u8> {
    let stream_id: u16 = 0x00;

    let body = serializar_body_startup();

    let header = serializar_header(
        VERSION_CLIENT,
        FLAGS_HEADER_DEFAULT,
        stream_id,
        STARTUP,
        body.len() as i32,
    );

    let mut frame = header;
    frame.extend(body);
    frame
}

fn serializar_body_startup() -> Vec<u8> {
    let cant_opciones: u16 = 0x01;
    let cql_version = "CQL_VERSION".as_bytes();
    let len_cql_version = cql_version.len() as u16;
    let version = "3.0.0".as_bytes();
    let len_version = version.len() as u16;
    let mut string_map: Vec<u8> = vec![];
    string_map.extend(cant_opciones.to_be_bytes());
    string_map.extend(len_cql_version.to_be_bytes());
    string_map.extend(cql_version);
    string_map.extend(len_version.to_be_bytes());
    string_map.extend(version);

    string_map
}

#[cfg(test)]
mod tests {
    use std::str::from_utf8;

    use super::*;

    #[test]
    fn test_funcionamiento_correcto() {
        //Arrange
        let query = "SELECT ID_VUELO, ORIGEN, DESTINO, COMBUSTIBLE FROM tabla_ejemplo WHERE ORIGEN = 'AEROPUERTO JORGE NEWBERY' AND ID_VUELO = 123";
        let len_bytes_query = query.len();

        //Act
        let serializada = query_to_bytes_client_server(query, LevelConsistency::Strong, 0x00);

        //Assert HEADER
        assert!(serializada[0] == VERSION_CLIENT);
        assert!(serializada[1] == FLAGS_HEADER_DEFAULT);
        assert!(u16::from_be_bytes([serializada[2], serializada[3]]) == 0x00);
        assert!(i8::from_be_bytes([serializada[4]]) == QUERY);

        //Assert BODY
        assert!(
            i32::from_be_bytes([
                serializada[9],
                serializada[10],
                serializada[11],
                serializada[12]
            ]) == len_bytes_query as i32
        );
        assert!(from_utf8(&serializada[13..(len_bytes_query + 13)]).unwrap() == query);
        assert!(
            u16::from_be_bytes([
                serializada[len_bytes_query + 13],
                serializada[len_bytes_query + 14]
            ]) == LevelConsistency::Strong.valor()
        );
        assert!(serializada[len_bytes_query + 15] == 0x00);
    }
}
