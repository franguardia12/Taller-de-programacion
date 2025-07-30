use super::serializador_nodo_envio::serializar_header_nodos;

const FLAGS_HEADER_DEFAULT: u8 = 0x00;

const VOID: i32 = 0x0001;
const ROWS: i32 = 0x0002;

pub fn serializar_respuesta_nodos(respuesta: Vec<&str>, kind: i32) -> Vec<u8> {
    let stream_id: u16 = 0x00;

    let body = serializar_body_result(respuesta, kind);
    let lenght = body.len();

    let header = serializar_header_nodos(FLAGS_HEADER_DEFAULT, stream_id, lenght as i32);

    let mut frame = header;
    frame.extend(body);
    frame
}

fn serializar_body_result(respuesta: Vec<&str>, kind: i32) -> Vec<u8> {
    let mut res = vec![];
    res.extend(kind.to_be_bytes());

    match kind {
        VOID => res.extend([0u8, 0]),
        ROWS => res.extend(procesar_rows(respuesta)),
        _ => res.extend([0u8, 0]),
    };
    res
}

//El formato de la respuesta es: cada linea un renglon y cada renglon separa elementos por comas
fn procesar_rows(respuesta: Vec<&str>) -> Vec<u8> {
    let mut body: Vec<u8> = vec![];
    if respuesta.is_empty() {
        body.extend((0_i32).to_be_bytes()); //rows_count
        body.extend((0_i32).to_be_bytes()); //colums_content
        return body;
    }
    let row_count = respuesta.len() as i32;
    let columns_count = respuesta[0].split(",").collect::<Vec<&str>>().len() as i32;

    let mut rows_content: Vec<u8> = Vec::with_capacity((row_count * columns_count) as usize);

    for iter in respuesta.iter() {
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

    body.extend(row_count.to_be_bytes());
    body.extend(columns_count.to_be_bytes());
    body.extend(rows_content);
    body
}
