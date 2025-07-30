use std::{
    fs::{self, File, OpenOptions},
    io::Write,
    net::{TcpListener, TcpStream},
    sync::{Arc, Mutex},
    thread::{self},
};

use bdd::{
    endpoint_data::EndpointData,
    keyspace::Keyspace,
    node_status::NodeStatus,
    nodo::{obtener_nombre_servicio, Nodo},
    procesamiento_data::{hashear, obtener_row},
    tabla::Tabla,
};
use protocolo::{
    parser_cql::{
        consulta::Consulta,
        parseo_consulta::{
            obtener_headers_table, obtener_tipo_strategy_y_replication, procesar_consulta,
        },
        type_cql::TypeCQL,
    },
    serial_deserial::{
        cassandra::{
            deserializador_cliente_server::deserializar_consulta,
            serializador_server_cliente::{
                result_to_bytes_server_client, serializar_ready_server_client,
            },
        },
        gossip::{
            deserializador_gossip::deserializar_gossip, serializador_gossip::serializar_gossip,
            type_message::TypeGossip,
        },
        intra_nodos::{
            deserializador_nodo_envio::deserializar_envio_nodos,
            serializador_nodo_respuesta::serializar_respuesta_nodos,
        },
        level_consistency::LevelConsistency,
    },
};
use rustls::{ServerConnection, StreamOwned};
use seguridad::create_server_config;

const REPLICA: u8 = 1;
pub const PUERTO_CLIENTE: &str = "9042";
pub const PUERTO_INTERNODOS: &str = "9043";
pub const PUERTO_GOSSIP: &str = "9044";

const STARTUP: i8 = 0x01;

pub fn run_server(node_address: String, nodo: Arc<Mutex<Nodo>>) -> Result<(), String> {
    match TcpListener::bind(&node_address) {
        Ok(listener) => {
            let server_config = Arc::new(create_server_config()?);
            for connection in listener.incoming() {
                match connection {
                    Ok(socket) => {
                        let nodo = Arc::clone(&nodo);
                        let server_config = Arc::clone(&server_config);
                        thread::spawn(move || {
                            let server_conn = ServerConnection::new(server_config).unwrap();
                            let mut tls_stream = StreamOwned::new(server_conn, socket);
                            let _ = handle_client_request(nodo, &mut tls_stream);
                        });
                    }
                    Err(e) => return Err(format!("Error al aceptar la conexión: {}", e)),
                }
            }
            Ok(())
        }
        Err(_) => Err("No se ha podido levantar el servidor.".to_string()),
    }
}

pub fn abrir_puerto_interconexion_nodos(
    address: String,
    nodo: Arc<Mutex<Nodo>>,
) -> Result<(), String> {
    match TcpListener::bind(&address) {
        Ok(listener) => {
            let server_config = Arc::new(create_server_config()?);
            for connection in listener.incoming() {
                match connection {
                    Ok(socket) => {
                        let server_config = Arc::clone(&server_config);
                        let nodo = Arc::clone(&nodo);
                        thread::spawn(move || {
                            let server_conn = ServerConnection::new(server_config).unwrap();
                            let mut tls_stream = StreamOwned::new(server_conn, socket);
                            let _ = handle_node_request(nodo, &mut tls_stream);
                        });
                    }
                    Err(e) => return Err(format!("Error al aceptar la conexión: {}", e)),
                }
            }
            Ok(())
        }
        Err(_) => Err("No se ha podido levantar el servidor.".to_string()),
    }
}

pub fn abrir_puerto_gossip(address: String, nodo: Arc<Mutex<Nodo>>) -> Result<(), String> {
    match TcpListener::bind(&address) {
        Ok(listener) => {
            let server_config = Arc::new(create_server_config()?);
            for connection in listener.incoming() {
                match connection {
                    Ok(socket) => {
                        let server_config = Arc::clone(&server_config);
                        let nodo = Arc::clone(&nodo);
                        let server_conn = ServerConnection::new(server_config).unwrap();
                        let mut tls_stream = StreamOwned::new(server_conn, socket);
                        let _ = handle_gossip(nodo, &mut tls_stream);
                    }
                    Err(e) => return Err(format!("Error al aceptar la conexión: {}", e)),
                }
            }
            Ok(())
        }
        Err(_) => Err("No se ha podido levantar el servidor.".to_string()),
    }
}

fn handle_node_request(
    nodo: Arc<Mutex<Nodo>>,
    socket: &mut StreamOwned<ServerConnection, TcpStream>,
) -> Result<(), String> {
    let (deserializada, resp, cons) = deserializar_envio_nodos(&mut *socket)?;
    let mut consulta = procesar_consulta(&deserializada)?;

    if let Ok(mut nodo_guard) = nodo.lock() {
        match consulta.get_type() {
            TypeCQL::Insert => {
                insert(&mut nodo_guard, socket, &mut consulta, resp, cons)?;
            }
            TypeCQL::Select => {
                select(&mut nodo_guard, socket, &mut consulta, resp)?;
            }
            TypeCQL::Delete => {
                delete(&mut nodo_guard, socket, &mut consulta, resp, cons)?;
            }
            TypeCQL::Update => {
                update(&mut nodo_guard, socket, &mut consulta, resp, cons)?;
            }
            TypeCQL::CreateTable => {
                create_table(&mut nodo_guard, socket, &mut consulta)?;
            }
            TypeCQL::CreateKeyspace => {
                create_keyspace(&mut nodo_guard, socket, &mut consulta)?;
            }
        }
    }
    Ok(())
}

fn insert(
    nodo_guard: &mut Nodo,
    socket: &mut StreamOwned<ServerConnection, TcpStream>,
    consulta: &mut Consulta,
    responsabilidad: u8,
    consistencia: u16,
) -> Result<(), String> {
    if responsabilidad == REPLICA {
        let tabla = consulta.get_tabla();
        let query = consulta.get_query();
        nodo_guard.insertar_a_tabla(tabla.to_string(), obtener_row(query));
        nodo_guard.persistir_insert(tabla.to_string(), obtener_row(query));
        nodo_guard.timestamp += 1;
        println!("Se han insertado los datos en la replica {}", nodo_guard.ip);
        println!("- - - - - - - - - - - - - - - - - - - - - - - - - - - -");
        // Luego de haber insertado en la tabla de esta réplica y persistir los datos en su
        // archivo, es necesario devolver un acknowledge al nodo responsable
        let ack_serializado =
            serializar_respuesta_nodos(vec![String::from("ACK").as_str()], 0x0002);
        socket
            .write_all(&ack_serializado)
            .map_err(|_| "Error al escribir en el socket.".to_string())?;

        return Ok(());
    }
    nodo_guard.execute_query(consulta, LevelConsistency::create(consistencia))?;
    Ok(())
}

fn select(
    nodo_guard: &mut Nodo,
    socket: &mut StreamOwned<ServerConnection, TcpStream>,
    consulta: &mut Consulta,
    responsabilidad: u8,
) -> Result<(), String> {
    let keyspace = nodo_guard.get_key()?;
    let tabla_elegida = keyspace.tablas.get(consulta.get_tabla());
    let condicion = consulta.get_where();
    let mut respuesta: Vec<String> = Vec::new();

    if let Some(&tabla_elegida) = tabla_elegida.as_ref() {
        let auxiliar = tabla_elegida.select(condicion, consulta.get_query().to_string());
        for linea in auxiliar.iter() {
            if !respuesta.contains(linea) {
                respuesta.push(linea.to_string());
            }
        }
    }
    if responsabilidad == REPLICA {
        // Si bien la lógica es similar al caso en que no sea una réplica el que reciba
        // la consulta, en este caso también es necesario devolver el timestamp del nodo
        // ya que luego será usado para el read repair

        respuesta.push(nodo_guard.timestamp.to_string()); // Agregar el timestamp al final
    }
    let vec_strs: Vec<&str> = respuesta.iter().map(|s| s.as_str()).collect();
    let respuesta_serializada = serializar_respuesta_nodos(vec_strs, 0x0002);

    socket
        .write_all(&respuesta_serializada)
        .map_err(|_| "Error al escribir en el socket.".to_string())?;
    Ok(())
}

fn delete(
    nodo_guard: &mut Nodo,
    socket: &mut StreamOwned<ServerConnection, TcpStream>,
    consulta: &mut Consulta,
    responsabilidad: u8,
    consistencia: u16,
) -> Result<(), String> {
    if responsabilidad == REPLICA {
        let tabla = consulta.get_tabla();
        nodo_guard.eliminar_en_tabla(tabla.to_string(), consulta.get_where());
        nodo_guard.persistir_delete(tabla.to_string())?;
        nodo_guard.timestamp += 1;
        // Luego de haber eliminado en la tabla de esta réplica y persistir los datos en su archivo
        // es necesario devolver un acknowledge al nodo responsable
        let ack_serializado =
            serializar_respuesta_nodos(vec![String::from("ACK").as_str()], 0x0002);
        socket
            .write_all(&ack_serializado)
            .map_err(|_| "Error al escribir en el socket.".to_string())?;

        return Ok(());
    }
    nodo_guard.execute_query(consulta, LevelConsistency::create(consistencia))?;
    Ok(())
}

fn update(
    nodo_guard: &mut Nodo,
    socket: &mut StreamOwned<ServerConnection, TcpStream>,
    consulta: &mut Consulta,
    responsabilidad: u8,
    consistencia: u16,
) -> Result<(), String> {
    if responsabilidad == REPLICA {
        let tabla = consulta.get_tabla();
        nodo_guard.update_en_tabla(
            tabla.to_string(),
            consulta.get_where(),
            consulta.get_query(),
        );
        nodo_guard.persistir_update(tabla.to_string())?;
        nodo_guard.timestamp += 1;
        // Luego de haber actualizado en la tabla de esta réplica y persistir los datos en su archivo
        // es necesario devolver un acknowledge al nodo responsable
        let ack_serializado =
            serializar_respuesta_nodos(vec![String::from("ACK").as_str()], 0x0002);
        socket
            .write_all(&ack_serializado)
            .map_err(|_| "Error al escribir en el socket.".to_string())?;

        return Ok(());
    }
    nodo_guard.execute_query(consulta, LevelConsistency::create(consistencia))?;
    Ok(())
}

fn create_table(
    nodo_guard: &mut Nodo,
    socket: &mut StreamOwned<ServerConnection, TcpStream>,
    consulta: &mut Consulta,
) -> Result<(), String> {
    let headers = obtener_headers_table(consulta.get_query());
    let tabla = consulta.get_tabla();
    let tabla_nueva = Tabla::new(tabla.to_string(), headers.clone());

    let keyspace_actual = nodo_guard.keyspace_actual.to_string();
    let keyspace = nodo_guard
        .keyspaces
        .get_mut(&keyspace_actual)
        .ok_or("No pudo crearse la tabla correctamente.".to_string())?;
    keyspace.tablas.insert(tabla.to_string(), tabla_nueva);

    let path = format!(
        "bdd/src/{}/{}_{}.csv",
        keyspace_actual, tabla, nodo_guard.ip
    );
    nodo_guard.persistir_tabla_nueva(path);
    nodo_guard.timestamp += 1;
    let ack_serializado = serializar_respuesta_nodos(vec![String::from("ACK").as_str()], 0x0002);
    socket
        .write_all(&ack_serializado)
        .map_err(|_| "Error al escribir en el socket.".to_string())?;
    Ok(())
}

fn create_keyspace(
    nodo_guard: &mut Nodo,
    socket: &mut StreamOwned<ServerConnection, TcpStream>,
    consulta: &mut Consulta,
) -> Result<(), String> {
    let (strategy, replication_factor) = obtener_tipo_strategy_y_replication(consulta.get_query());
    if strategy != "SimpleStrategy" {
        return Err("No pudo crearse el keyspace correctamente.".to_string());
    }
    let tabla = consulta.get_tabla();
    let k = Keyspace::new(tabla.to_string(), replication_factor, strategy);
    nodo_guard.keyspaces.insert(tabla.to_string(), k);
    nodo_guard.timestamp += 1;
    let ack_serializado = serializar_respuesta_nodos(vec![String::from("ACK").as_str()], 0x0002);
    socket
        .write_all(&ack_serializado)
        .map_err(|_| "Error al escribir en el socket.".to_string())?;
    Ok(())
}

fn handle_gossip(
    nodo: Arc<Mutex<Nodo>>,
    socket: &mut StreamOwned<ServerConnection, TcpStream>,
) -> Result<(), String> {
    //Primero lee el syn
    let (deserializada, tipo, sending_ip) = deserializar_gossip(&mut *socket)?;
    if tipo != TypeGossip::Syn {
        return Err("Se esperaba un mensaje SYN".to_string());
    }
    let servicio = obtener_nombre_servicio(sending_ip);
    let (mis_desactualizados, mis_actualizados) = {
        let mut nodo_guard = nodo.lock().unwrap();
        nodo_guard.gossip_recientes.push(servicio);
        let mut mis_desactualizados = String::new();
        let mut mis_actualizados = String::new();

        let metadatos = deserializada.split(" ");
        for metadata in metadatos {
            let ip_gen_ver = metadata.split(":").collect::<Vec<&str>>();
            let ip = ip_gen_ver[0];
            let gen: f64 = ip_gen_ver[1]
                .parse()
                .map_err(|_| "Error al parsear generación".to_string())?;
            let ver: u32 = ip_gen_ver[2]
                .parse()
                .map_err(|_| "Error al parsear versión".to_string())?;
            if let Some(metadata_propia) = nodo_guard.metadata_nodos.get(ip) {
                if gen > metadata_propia.heartbeat_state.generation
                    || ver > metadata_propia.heartbeat_state.version
                {
                    mis_desactualizados.push_str(&format!("{}:{}:{} ", ip, gen, ver));
                } else {
                    mis_actualizados.push_str(&format!(
                        "{}:{}:{}:{} ",
                        ip,
                        metadata_propia.heartbeat_state.generation,
                        metadata_propia.heartbeat_state.version,
                        metadata_propia.application_state.status
                    ));
                }
            } else {
                mis_desactualizados.push_str(&format!("{}:{}:{} ", ip, gen, ver));
            }
        }

        for (ip, metadata) in nodo_guard.metadata_nodos.iter() {
            if !deserializada.contains(ip) {
                mis_actualizados.push_str(&format!(
                    "{}:{}:{}:{} ",
                    ip,
                    metadata.heartbeat_state.generation,
                    metadata.heartbeat_state.version,
                    metadata.application_state.status
                ));
            }
        }

        (
            mis_desactualizados.trim_end().to_string(),
            mis_actualizados.trim_end().to_string(),
        )
    };

    let respuesta = format!(
        "{}\n{}",
        mis_desactualizados.trim_end(),
        mis_actualizados.trim_end()
    );
    let respuesta_serializada = serializar_gossip(respuesta, TypeGossip::Ack, "".to_string());
    socket
        .write_all(&respuesta_serializada)
        .map_err(|_| "Error al escribir en el socket.".to_string())?;

    let (deserializada, tipo, _) = deserializar_gossip(socket)?;
    if tipo != TypeGossip::Ack2 {
        return Err("Se esperaba un mensaje ACK2".to_string());
    }

    // Actualizar metadatos propios con el bloqueo
    if deserializada.is_empty() {
        return Ok(());
    }
    let sus_actualizados = deserializada.split(" ");
    for metadata in sus_actualizados {
        let ip_gen_ver_status = metadata.split(":").collect::<Vec<&str>>();
        let ip = ip_gen_ver_status[0];
        let gen: f64 = ip_gen_ver_status[1]
            .parse()
            .map_err(|_| "No se pudo convertir a f64".to_string())?;
        let ver: u32 = ip_gen_ver_status[2]
            .parse()
            .map_err(|_| "No se pudo convertir a u32".to_string())?;
        let state_str = ip_gen_ver_status[3];
        let state = NodeStatus::create(state_str);
        let mut nodo_guard = nodo.lock().unwrap();
        let hash_result = hashear(ip)?;
        let already_exists = nodo_guard.nodos.contains_key(&hash_result);
        if let Some(metadata_propia) = nodo_guard.metadata_nodos.get_mut(ip) {
            metadata_propia.heartbeat_state.version = ver;
            metadata_propia.heartbeat_state.generation = gen;
            let delete_ip = metadata_propia.application_state.status == NodeStatus::Normal
                && state == NodeStatus::Down;
            metadata_propia.application_state.status = state.clone();
            if nodo_guard.ip == ip {
                continue;
            }
            if delete_ip {
                let hash_result = hashear(ip)?;
                nodo_guard.nodos.remove(&hash_result);
                println!(
                    "Marcando al nodo {} como caído y eliminandolo de la lista de nodos",
                    ip
                );

                //Hay que eliminar el nodo de la lista de nodos que ya tengo en mi metadata (nodo que recibe)

                if let Some(keyspace) = nodo_guard.keyspaces.get(&nodo_guard.keyspace_actual) {
                    let tablas: Vec<_> = keyspace.tablas.keys().cloned().collect();
                    for tabla in tablas {
                        if tabla == "AEROPUERTOS" {
                            continue;
                        }

                        let path = format!(
                            "bdd/src/{}/{}_{}.csv",
                            nodo_guard.keyspace_actual, tabla, ip
                        );
                        if fs::metadata(&path).is_ok() {
                            OpenOptions::new()
                                .write(true)
                                .truncate(true)
                                .open(&path)
                                .map_err(|e| format!("Error al abrir el archivo: {}", e))?;
                        }
                    }
                }

                nodo_guard.actualizar_replicas();
            }
            if !already_exists && state == NodeStatus::Normal {
                nodo_guard.nodos.insert(hash_result, ip.to_string());
            };
            continue;
        }

        let endpoint = EndpointData::new(gen, ver, state);

        nodo_guard.metadata_nodos.insert(ip.to_string(), endpoint);
        let hash_result = hashear(ip)?;
        nodo_guard.nodos.insert(hash_result, ip.to_string());
        nodo_guard.actualizar_replicas();
        let keyspace_path = format!("bdd/src/{}", nodo_guard.keyspace_actual);
        if fs::metadata(keyspace_path).is_ok() {
            if let Some(keyspace) = nodo_guard.keyspaces.get(&nodo_guard.keyspace_actual) {
                for tabla in keyspace.tablas.keys() {
                    let path = format!(
                        "bdd/src/{}/{}_{}.csv",
                        nodo_guard.keyspace_actual, tabla, ip
                    );
                    if fs::metadata(&path).is_err() && File::create(path).is_err() {
                        println!("Hubo un error de creacion.");
                    }
                }
            }
        }
    }
    socket
        .sock
        .shutdown(std::net::Shutdown::Both)
        .map_err(|e| format!("Error al cerrar el socket: {}", e))?;
    Ok(())
}

fn handle_client_request(
    nodo: Arc<Mutex<Nodo>>,
    socket: &mut StreamOwned<ServerConnection, TcpStream>,
) -> Result<(), String> {
    let (deserialized_request, consistency, tipo) = deserializar_consulta(&mut *socket)?;
    startup_server_client(socket, tipo)?;
    let mut nodo_guard = nodo.lock().unwrap();
    let mut consulta = procesar_consulta(&deserialized_request)?;
    let option_vector = nodo_guard
        .execute_query(&mut consulta, LevelConsistency::create(consistency))
        .map_err(|e| format!("No se ha podido ejecutar la consulta, debido a {}.", e))?;
    if let Some(vector) = option_vector {
        let vec_strs: Vec<&str> = vector.iter().map(|s| s.as_str()).collect();
        let respuesta_serializada = result_to_bytes_server_client(vec_strs, 0x0002)?;
        // Crear la conexion al nodo que nos pidio informacion.
        socket
            .write_all(&respuesta_serializada)
            .map_err(|_| "Error al escribir en el socket.".to_string())?;
    }
    Ok(())
}

fn startup_server_client(
    socket: &mut StreamOwned<ServerConnection, TcpStream>,
    tipo: i8,
) -> Result<(), String> {
    match tipo {
        STARTUP => {
            println!("Autenticación: Servidor recibe STARTUP del cliente");
            let ready = serializar_ready_server_client();
            match socket.write_all(&ready) {
                Ok(_) => {
                    println!("Servidor envía el READY al cliente");
                    Ok(())
                }
                Err(_) => Err("Error al escribir en el socket.".to_string()),
            }
        }
        _ => Ok(()),
    }
}
