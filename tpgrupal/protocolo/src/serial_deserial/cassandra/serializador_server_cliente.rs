use super::serializador_cliente_server::serializar_header;

const FLAGS_HEADER_DEFAULT: u8 = 0x00;
const FLAGS_METADATA_DEFAULT: i32 = 0x0004;
const VERSION_SERVER: u8 = 0x84;

const RESULT: i8 = 0x08;
const READY: i8 = 0x02;

const VOID: i32 = 0x0001;
const ROWS: i32 = 0x0002;

pub fn result_to_bytes_server_client(respuesta: Vec<&str>, kind: i32) -> Result<Vec<u8>, String> {
    let stream_id: u16 = 0x00;

    let body = serializar_body_result(respuesta, kind, FLAGS_METADATA_DEFAULT)?;

    let header = serializar_header(
        VERSION_SERVER,
        FLAGS_HEADER_DEFAULT,
        stream_id,
        RESULT,
        body.len() as i32,
    );

    let mut frame = header;
    frame.extend(body);
    Ok(frame)
}

fn serializar_body_result(
    respuesta: Vec<&str>,
    kind: i32,
    flags_body: i32,
) -> Result<Vec<u8>, String> {
    let mut res = vec![];
    res.extend(kind.to_be_bytes());

    match kind {
        VOID => res.extend([0u8, 0]),
        ROWS => res.extend(procesar_rows(respuesta, flags_body)?),
        _ => res.extend([0u8, 0]),
    };
    Ok(res)
}

//El formato de la respuesta es: cada linea un renglon y cada renglon separa elementos por comas
fn procesar_rows(respuesta: Vec<&str>, flags_body: i32) -> Result<Vec<u8>, String> {
    let mut body: Vec<u8> = vec![];
    body.extend(flags_body.to_be_bytes());

    if respuesta.is_empty() {
        body.extend((0_i32).to_be_bytes());
        body.extend((0_i32).to_be_bytes());
        return Ok(body);
    }
    let row_count = respuesta.len() as i32;
    let columns_count = respuesta[0].split(",").collect::<Vec<&str>>().len() as i32;

    let mut rows_content: Vec<u8> = Vec::with_capacity((row_count * columns_count) as usize);

    for iter in respuesta.iter().take(row_count as usize) {
        let mut row_i_bytes: Vec<u8> = vec![];
        let row_i_datos = iter;
        let datos = row_i_datos.split(",").collect::<Vec<&str>>();
        for e in datos {
            if e.is_empty() {
                row_i_bytes.extend((-1_i32).to_be_bytes());
            } else {
                row_i_bytes.extend((e.len() as i32).to_be_bytes());
                row_i_bytes.extend(e.as_bytes());
            }
        }
        rows_content.extend(row_i_bytes);
    }

    body.extend(columns_count.to_be_bytes());
    body.extend(row_count.to_be_bytes());
    body.extend(rows_content);
    Ok(body)
}

pub fn serializar_ready_server_client() -> Vec<u8> {
    let stream_id: u16 = 0x00;

    let body: Vec<u8> = vec![];

    let header = serializar_header(
        VERSION_SERVER,
        FLAGS_HEADER_DEFAULT,
        stream_id,
        READY,
        body.len() as i32,
    );

    let mut frame = header;
    frame.extend(body);
    frame
}
