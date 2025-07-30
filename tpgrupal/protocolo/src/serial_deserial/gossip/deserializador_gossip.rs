use std::io::Read;

use super::type_message::TypeGossip;

pub fn deserializar_gossip<T: Read>(
    stream: &mut T,
) -> Result<(String, TypeGossip, String), String> {
    let mut header = [0u8; 5];

    stream
        .read_exact(&mut header)
        .map_err(|_| "Cantidad incorrecta de bytes en el header del mensaje.".to_string())?;
    let tipo = u8::from_be_bytes([header[0]]);
    let body_len = i32::from_be_bytes([header[1], header[2], header[3], header[4]]);

    let mut body = vec![0u8; body_len as usize];
    stream
        .read_exact(&mut body)
        .map_err(|_| "Cantidad incorrecta de bytes en el body del mensaje.".to_string())?;
    let (mensaje, ip) = match TypeGossip::create(tipo) {
        TypeGossip::Syn => deserializar_syn(body),
        _ => (String::from_utf8(body.to_vec()).unwrap(), String::new()),
    };

    Ok((mensaje, TypeGossip::create(tipo), ip))
}

fn deserializar_syn(body: Vec<u8>) -> (String, String) {
    let len_ip = i32::from_be_bytes([body[0], body[1], body[2], body[3]]) as usize;
    let ip_vec = body[4..(4 + len_ip)].to_vec();
    let ip = String::from_utf8(ip_vec).unwrap();
    let mensaje_vec = body[(4 + len_ip + 4)..(body.len())].to_vec();
    let mensaje = String::from_utf8(mensaje_vec).unwrap();

    (mensaje, ip)
}
