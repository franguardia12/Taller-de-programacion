use super::type_message::TypeGossip;

pub fn serializar_gossip(mensaje: String, tipo: TypeGossip, ip: String) -> Vec<u8> {
    //BODY: <string (mensaje)>
    let body = match tipo {
        TypeGossip::Syn => serializar_syn(mensaje, ip),
        TypeGossip::Ack => serializar_ack(mensaje),
        TypeGossip::Ack2 => serializar_ack2(mensaje),
    };

    let len_body: i32 = body.len() as i32;

    //HEADER: <u8 (tipo del mensaje)><int (largo del body)>
    let header = serializar_header(tipo, len_body);

    let mut mensaje_serilizado: Vec<u8> = vec![];
    mensaje_serilizado.extend(header);
    mensaje_serilizado.extend(body);

    mensaje_serilizado
}

fn serializar_syn(mensaje: String, ip: String) -> Vec<u8> {
    let mut body: Vec<u8> = vec![];
    body.extend((ip.len() as i32).to_be_bytes());
    body.extend(ip.trim().as_bytes());
    body.extend((mensaje.len() as i32).to_be_bytes());
    body.extend(mensaje.trim().as_bytes());

    body
}

fn serializar_ack2(mensaje: String) -> Vec<u8> {
    mensaje.trim().as_bytes().to_vec()
}

fn serializar_ack(mensaje: String) -> Vec<u8> {
    let msj_chars = mensaje.chars().collect::<Vec<char>>();
    let mut desactualizados = "";
    let mut para_actualiar = "";
    let ambos_lados = mensaje.split('\n').collect::<Vec<&str>>();

    if msj_chars[0] != '\n' && msj_chars[msj_chars.len() - 1] != '\n' {
        desactualizados = ambos_lados[0].trim();
        para_actualiar = ambos_lados[1].trim();
    } else if msj_chars[0] == '\n' {
        para_actualiar = ambos_lados[1].trim();
    } else {
        desactualizados = ambos_lados[0].trim();
    }
    let mensaje_ordenado = format!("{}\n{}", desactualizados, para_actualiar);
    mensaje_ordenado.as_bytes().to_vec()
}

fn serializar_header(tipo: TypeGossip, len_body: i32) -> Vec<u8> {
    let mut header: Vec<u8> = vec![];
    header.push(tipo.valor());
    header.extend(len_body.to_be_bytes());
    header
}
