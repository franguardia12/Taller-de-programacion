use std::io::Read;
use std::mem::size_of;

const RESULT: i8 = 0x08;
const READY: i8 = 0x02;

const VOID: i32 = 0x0001;
const ROWS: i32 = 0x0002;

pub fn deserializar_respuesta<T: Read>(mut stream: T) -> Result<(Vec<String>, i8), String> {
    let mut header = [0u8; 9];
    stream
        .read_exact(&mut header)
        .map_err(|_| "Cantidad incorrecta de bytes en el header del mensaje.".to_string())?;

    let op_code = i8::from_be_bytes([header[4]]);

    let body_len = i32::from_be_bytes([header[5], header[6], header[7], header[8]]) as usize;
    let mut body = vec![0u8; body_len];
    stream
        .read_exact(&mut body)
        .map_err(|_| "Cantidad incorrecta de bytes en el body del mensaje.".to_string())?;

    let res: Vec<String> = match op_code {
        RESULT => {
            let kind_respuesta = i32::from_be_bytes([body[0], body[1], body[2], body[3]]);
            match kind_respuesta {
                VOID => vec![],
                ROWS => deserializar_body_rows(&body)?,
                _ => return Err("Tipo de respuesta no soportada.".to_string()),
            }
        }
        READY => vec![],
        _ => return Err("Tipo de respuesta no soportada.".to_string()),
    };

    Ok((res, op_code))
}

fn deserializar_body_rows(body: &[u8]) -> Result<Vec<String>, String> {
    let column_count = i32::from_be_bytes([body[8], body[9], body[10], body[11]]);

    // Considero que no fue enviado el <paging_state>, ya que es opcional
    let rows_count = i32::from_be_bytes([body[12], body[13], body[14], body[15]]);

    // i = size_of(Kind) + size_of(Flags) + size_of(Rows_count) + size_of(Column_count)
    let i = size_of::<i32>() + size_of::<i32>() + size_of::<i32>() + size_of::<i32>();

    obtener_rows_body(i, rows_count, column_count, body)
}

pub fn obtener_rows_body(
    mut i: usize,
    rows_count: i32,
    column_count: i32,
    body: &[u8],
) -> Result<Vec<String>, String> {
    let mut rows_elements = vec![];

    while i < body.len() {
        let mut length_value = i32::from_be_bytes([body[i], body[i + 1], body[i + 2], body[i + 3]]);
        if length_value == -1_i32 {
            rows_elements.push("".to_string());
            i += 1;
        } else {
            let (mut j, mut value_bytes) = (i + size_of::<i32>(), vec![]);
            i += length_value as usize;
            while length_value > 0 {
                value_bytes.push(body[j]);
                length_value -= 1;
                j += 1;
            }
            let elemento = String::from_utf8(value_bytes)
                .map_err(|_| "No se pudo convertir un elemento a una String válido.".to_string())?;
            rows_elements.push(elemento);
        }
        i += size_of::<i32>();
    }

    Ok(elementos_sueltos_a_filas_de_elementos(
        rows_count,
        column_count,
        rows_elements,
    ))
}

fn elementos_sueltos_a_filas_de_elementos(
    rows_count: i32,
    column_count: i32,
    rows_elements: Vec<String>,
) -> Vec<String> {
    let mut rows_content = vec![];
    for k in 0..(rows_count as usize) {
        let mut row_k = vec![];
        for elem in rows_elements
            .iter()
            .skip(k * column_count as usize)
            .take(column_count as usize)
        {
            row_k.push(elem.to_string());
        }
        rows_content.push(row_k.join(","));
    }

    rows_content
}
