use protocolo::parser_cql::parseo_consulta::{
    obtener_headers_table, obtener_tipo_strategy_y_replication, procesar_consulta,
};
use protocolo::serial_deserial::gossip::deserializador_gossip::deserializar_gossip;
use protocolo::serial_deserial::gossip::serializador_gossip::serializar_gossip;
use protocolo::serial_deserial::gossip::type_message::TypeGossip;
use protocolo::serial_deserial::intra_nodos::deserializador_nodo_respuesta::deserializar_respuesta_nodos;
use protocolo::serial_deserial::intra_nodos::serializador_nodo_envio::serializar_envio_nodos;
use protocolo::{
    parser_cql::{condicion_where::CondicionWhere, consulta::Consulta, type_cql::TypeCQL},
    serial_deserial::level_consistency::LevelConsistency,
};

use rand::Rng;
use rustls::{ClientConnection, ServerName, StreamOwned};
use seguridad::create_client_config;
use std::collections::BTreeMap;
use std::fs::{self, OpenOptions};
use std::path::Path;
use std::sync::MutexGuard;

use std::sync::mpsc::{self, Receiver, Sender};

use std::sync::{Arc, Mutex};
use std::time::Duration;
use std::{thread, vec};

use std::{
    collections::HashMap,
    fs::File,
    io::{self, Write},
    net::TcpStream,
};

use crate::endpoint_data::EndpointData;
use crate::keyspace::Keyspace;
use crate::node_status::NodeStatus;
use crate::procesamiento_data::*;
use crate::tabla::Tabla;
const PUERTO_INTERNODOS: &str = "9043";
const PUERTO_GOSSIP: &str = "9044";
const RESPONSABLE: u8 = 0;
const REPLICA: u8 = 1;

type TxRx = (Sender<Result<(), String>>, Receiver<Result<(), String>>);

type TxRx2 = (
    Sender<Result<(Vec<String>, String, String), String>>,
    Receiver<Result<(Vec<String>, String, String), String>>,
);

/// Estructura que representa un nodo del cluster, contiene
/// todas las estructuras necesarias para almacenar toda la 
/// información que se necesite para el correcto funcionamiento
/// de la base de datos
pub struct Nodo {
    pub metadata_nodos: HashMap<String, EndpointData>, //(ip, EndpointData)
    pub ip: String,
    pub keyspaces: HashMap<String, Keyspace>,
    pub token: u32,
    pub replicas: Vec<String>,        // Vector de ips de nodos
    pub nodos: BTreeMap<u32, String>, //K: ip hasheada; V: ip String
    pub timestamp: u32,
    pub gossip_recientes: Vec<String>,
    pub keyspace_actual: String,
}

impl Nodo {
    /// Método que crea un nuevo nodo en el cluster a partir
    /// de la IP ingresada
    pub fn new(ip: &String) -> Result<Self, String> {
        let mut ips = BTreeMap::new();
        let metadata_nodos = new_metadata(ip); // ---> Se crea la metadata del nodo actual y se lo inserta
        let replicas: Vec<String> = Vec::new();
        let keyspaces: HashMap<String, Keyspace> = HashMap::new();

        let hash_result = hashear(ip)?;
        ips.insert(hash_result, ip.to_string()); // ---> Se insertar la IP del nodo actual en el árbol de nodos

        let mut nodo = Nodo {
            metadata_nodos,
            ip: ip.to_string(),
            keyspaces,
            token: hash_result,
            replicas,
            nodos: ips,
            timestamp: 0,
            gossip_recientes: Vec::new(),
            keyspace_actual: "Aerolineas".to_string(),
        };

        nodo.load_data()?;

        Ok(nodo)
    }

    /// Método que inserta una nueva línea de información en una tabla del nodo
    pub fn insertar_a_tabla(&mut self, nombre_tabla: String, row: String) {
        if let Some(keyspace) = self.keyspaces.get_mut(&self.keyspace_actual) {
            if let Some(tabla) = keyspace.tablas.get_mut(&nombre_tabla) {
                tabla.insertar(row);
            }
        }
    }

    /// Método que elimina una o más líneas de información en una tabla del nodo que
    /// cumplan con la condición recibida
    pub fn eliminar_en_tabla(&mut self, nombre_tabla: String, condicion: &CondicionWhere) {
        if let Some(keyspace) = self.keyspaces.get_mut(&self.keyspace_actual) {
            if let Some(tabla) = keyspace.tablas.get_mut(&nombre_tabla) {
                tabla.eliminar(condicion);
            }
        }
    }

    /// Método que actualiza una o más líneas de información en una tabla del nodo que
    /// cumplan con la condición recibida
    pub fn update_en_tabla(
        &mut self,
        nombre_tabla: String,
        condicion: &CondicionWhere,
        query: &str,
    ) {
        if let Some(keyspace) = self.keyspaces.get_mut(&self.keyspace_actual) {
            if let Some(tabla) = keyspace.tablas.get_mut(&nombre_tabla) {
                tabla.actualizar(condicion, query.to_string());
            }
        }
    }

    fn get_nodo_responsable(&self, key_hash: u32) -> Option<String> {
        for (token, node_address) in self.nodos.iter() {
            if key_hash <= *token {
                return Some(node_address.to_string());
            }
        }
        if let Some((_, value)) = self.nodos.iter().next() {
            return Some(value.to_string());
        }
        None
    }

    /// Método que ejecuta la consulta recibida por el nodo (escrita en CQL)
    /// recibiendo también la consistencia de la misma
    pub fn execute_query(
        &mut self,
        consulta: &mut Consulta,
        consistencia: LevelConsistency,
    ) -> Result<Option<Vec<String>>, String> {
        let query = consulta.get_query();
        let condicion = consulta.get_where();
        let tipo_consulta = consulta.get_type();
        let tabla_consulta = consulta.get_tabla();
        let consulta_explicita = consulta.get_consulta_explicita();
        let mut nivel_replicacion = 0;
        let mut quorum = 0;
        if let Some(keyspace) = self.keyspaces.get(&self.keyspace_actual) {
            nivel_replicacion = keyspace.nivel_replicacion;
            quorum = (nivel_replicacion + 1) / 2;
        }
        match tipo_consulta {
            TypeCQL::Insert => {
                let key_origen = obtener_hash_origen(query);

                let ip_nodo_responsable =
                    self.get_nodo_responsable(key_origen).ok_or_else(|| {
                        "No se ha encontrado el nodo responsable para la key.".to_string()
                    })?;
                if ip_nodo_responsable == self.ip {
                    // INSERTO EN ESTE NODO Y EN LAS REPLICAS
                    self.insertar_a_tabla(tabla_consulta.to_string(), obtener_row(query));
                    self.persistir_insert(tabla_consulta.to_string(), obtener_row(query));
                    self.timestamp += 1;
                    println!("Se ha recibido la consulta: {}", consulta_explicita);
                    println!(
                        "Se está insertando en el nodo responsable, en la tabla: {}",
                        tabla_consulta
                    );

                    // Estando en el nodo coordinador voy a esperar tantos ACKs dependiendo del nivel de consistencia
                    // Si es WEAK, espero 1 ACK
                    // Si es STRONG, espero 2 ACKs
                    // Igualmente se inserta en todos los nodos dependiendo del replication factor

                    self.enviar_escrituras_replicas(consulta.clone(), consistencia, quorum)?;
                } else {
                    // Se le envia la consulta al nodo responsable.
                    let nombre_servicio = obtener_nombre_servicio(ip_nodo_responsable.clone());

                    let direccion = format!("{}:{}", nombre_servicio, PUERTO_INTERNODOS);
                    println!("Se ha recibido una consulta: {}", consulta_explicita);
                    println!(
                        "El nodo no es responsable. Se la envía al responsable de IP: {}",
                        ip_nodo_responsable
                    );
                    if send_and_deserial(
                        direccion,
                        consulta_explicita,
                        LevelConsistency::Strong,
                        RESPONSABLE,
                        tipo_consulta,
                    )
                    .is_err()
                    {
                        // Al igual que en el caso de una operación SELECT, si no pude conectarme al nodo responsable pero yo tampoco lo soy
                        // entonces yo (el nodo coordinador) paso a ser un nuevo "nodo responsable" enviándole la consulta a las réplicas del
                        // nodo responsable real y recibiendo las respuestas, luego actuar pero sin involucrar datos propios
                        println!(
                            "El nodo responsable está caído: Se le pasa la consulta a sus réplicas"
                        );
                        let replicas = get_replicas(
                            &self.nodos,
                            ip_nodo_responsable.to_string(),
                            nivel_replicacion,
                        );

                        if consistencia == LevelConsistency::Weak {
                            for ip_replica in replicas {
                                println!("Se envía la consulta a la réplica de IP: {}", ip_replica);
                                let nombre_servicio = obtener_nombre_servicio(ip_replica.clone());

                                let direccion =
                                    format!("{}:{}", nombre_servicio, PUERTO_INTERNODOS);
                                if let Some(endpoint_data) = self.metadata_nodos.get(&ip_replica) {
                                    if endpoint_data.application_state.status == NodeStatus::Normal
                                    {
                                        let deserialize_response = send_and_deserial(
                                            direccion,
                                            consulta_explicita,
                                            LevelConsistency::Weak,
                                            REPLICA,
                                            tipo_consulta,
                                        )?;
                                        if deserialize_response[0] == "ACK" {
                                            // Se cumple la consistencia
                                            println!("- - - - - - - - - - - - - - - - - - - - - - - - - - - -");
                                            return Ok(None);
                                        }
                                    }
                                }
                            }
                            return Err("No se cumplió la consistencia de la consulta".to_string());
                        }
                        let mut exitos = 0;
                        for ip_replica in replicas {
                            let nombre_servicio = obtener_nombre_servicio(ip_replica.clone());

                            let direccion = format!("{}:{}", nombre_servicio, PUERTO_INTERNODOS);
                            if ip_replica == self.ip {
                                println!("El nodo coordinador es réplica del nodo responsable caído. Se insertan los datos en sus tablas");
                                self.insertar_a_tabla(
                                    tabla_consulta.to_string(),
                                    obtener_row(query),
                                );
                                self.persistir_insert(
                                    tabla_consulta.to_string(),
                                    obtener_row(query),
                                );
                                self.timestamp += 1;
                                exitos += 1;
                                if exitos >= quorum {
                                    println!("Se han obtenido más de {} ACKs: Se cumple el Consistency Level", quorum);
                                    println!(
                                        "- - - - - - - - - - - - - - - - - - - - - - - - - - - -"
                                    );
                                    break;
                                }
                            } else if let Some(endpoint_data) = self.metadata_nodos.get(&ip_replica)
                            {
                                if endpoint_data.application_state.status == NodeStatus::Normal {
                                    if let Ok(deserialize_response) = send_and_deserial(
                                        direccion,
                                        consulta_explicita,
                                        LevelConsistency::Strong,
                                        REPLICA,
                                        tipo_consulta,
                                    ) {
                                        if deserialize_response[0] == "ACK" {
                                            exitos += 1;
                                            if exitos >= quorum {
                                                println!("Se han obtenido más de {} ACKs: Se cumple el Consistency Level", quorum);
                                                // Se cumple la consistencia
                                                break;
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        if exitos < quorum {
                            return Err("No se cumplió la consistencia de la consulta".to_string());
                        }
                    }
                    println!("- - - - - - - - - - - - - - - - - - - - - - - - - - - -");
                }
                Ok(None)
            }
            TypeCQL::Select => {
                if condicion.condicion1.is_empty() {
                    //NO HAY WHERE
                    //1ero buscar los datos de nuestro nodo
                    let mut vector_datos: Vec<String> = Vec::new();
                    if let Some(tabla_elegida) = self.get_tabla(tabla_consulta)? {
                        vector_datos = tabla_elegida.select(condicion, query.to_string());
                    }
                    // Hacer un for de todo lo que hay en la tabla que fue solicitada, appendeamos los campos pedidos
                    // Luego llamamos a otro nodo
                    for (_, ip) in self.nodos.clone().iter() {
                        if &self.ip == ip {
                            continue;
                        }

                        let nombre_servicio = obtener_nombre_servicio(ip.clone());

                        let direccion = format!("{}:{}", nombre_servicio, PUERTO_INTERNODOS);
                        if let Ok(deserialized_response) = send_and_deserial(
                            direccion,
                            consulta_explicita,
                            LevelConsistency::Weak,
                            RESPONSABLE,
                            tipo_consulta,
                        ) {
                            for row in deserialized_response {
                                if !vector_datos.contains(&row) {
                                    vector_datos.push(row);
                                }
                            }
                        } else {
                            // Como en este caso las consultas no tienen WHERE es necesario pasar por todos los nodos para resolverlas
                            // Si hay algún nodo que no esté disponible porque se cayó entonces prueba con sus réplicas, las cuales
                            // corresponden a las siguientes ips luego del ip del nodo responsable

                            let replicas =
                                get_replicas(&self.nodos, ip.to_string(), nivel_replicacion);

                            for ip_replica in replicas {
                                let nombre_servicio = obtener_nombre_servicio(ip_replica.clone());

                                let direccion =
                                    format!("{}:{}", nombre_servicio, PUERTO_INTERNODOS);
                                if ip_replica == self.ip {
                                    // Si bien la lógica es similar al caso en que no sea una réplica el que reciba
                                    // la consulta, en este caso también es necesario devolver el timestamp del nodo
                                    // ya que luego será usado para el read repair

                                    if let Some(tabla_elegida) = self.get_tabla(tabla_consulta)? {
                                        let auxiliar =
                                            tabla_elegida.select(condicion, query.to_string());
                                        for linea in auxiliar.iter() {
                                            if !vector_datos.contains(linea) {
                                                vector_datos.push(linea.to_string());
                                            }
                                        }
                                    }
                                    break;
                                }

                                if let Some(endpoint_data) = self.metadata_nodos.get(&ip_replica) {
                                    if endpoint_data.application_state.status == NodeStatus::Normal
                                    {
                                        if let Ok(mut deserialized_response) = send_and_deserial(
                                            direccion,
                                            consulta_explicita,
                                            LevelConsistency::Strong,
                                            REPLICA,
                                            tipo_consulta,
                                        ) {
                                            deserialized_response.pop();
                                            for row in deserialized_response {
                                                if !vector_datos.contains(&row) {
                                                    vector_datos.push(row);
                                                }
                                            }
                                            break;
                                        }
                                    }
                                }
                            }
                        }
                    }
                    return Ok(Some(vector_datos));
                }
                //HAY WHERE
                let hash_valor = obtener_hash_key_select(condicion);
                let ip_nodo_responsable =
                    self.get_nodo_responsable(hash_valor).ok_or_else(|| {
                        "No se ha encontrado el nodo responsable para la key.".to_string()
                    })?;
                if ip_nodo_responsable == self.ip {
                    // Soy el nodo responsable
                    let mut datos = Vec::new();
                    if let Some(tabla_elegida) = self.get_tabla(tabla_consulta)? {
                        datos = tabla_elegida.select(condicion, query.to_string());
                    }

                    // Considerando que un nodo puede caerse y reconectarse, aunque no tenga que recuperar los datos que tenía antes
                    // ese nodo vuelve a estar presente y si era réplica de otro nodo, cuando se reconecte vuelve a serlo

                    // En ese caso el SELECT a un nodo responsable también realiza SELECT a las réplicas, ya que para ese caso concreto
                    // se obtendrán valores distintos para un dato (incluso la inexistencia de este en el nodo que se reconecta) y será
                    // necesario hacer un read repair para que ese nodo tenga ese dato actualizado

                    // En todos los demás casos esta operación se hace pero no produce cambios, pero para poder contemplar ese caso particular
                    // es necesario hacer esto

                    // Enviar lecturas a réplicas y esperar respuestas
                    let resultado_consistency = self.enviar_lecturas_replicas(
                        consulta.clone(),
                        consistencia,
                        datos,
                        tabla_consulta,
                        quorum,
                    )?;
                    let mut aux: Vec<String> = vec![];
                    for elem in resultado_consistency {
                        if !aux.contains(&elem) {
                            aux.push(elem);
                        }
                    }
                    return Ok(Some(aux));
                }
                let mut vector_datos: Vec<String> = Vec::new();
                // Se le envía la consulta al nodo responsable.

                let nombre_servicio = obtener_nombre_servicio(ip_nodo_responsable.clone());

                let direccion = format!("{}:{}", nombre_servicio, PUERTO_INTERNODOS);

                if let Ok(deserialized_response) = send_and_deserial(
                    direccion,
                    consulta_explicita,
                    LevelConsistency::Strong,
                    RESPONSABLE,
                    tipo_consulta,
                ) {
                    for row in deserialized_response {
                        if !vector_datos.contains(&row) {
                            vector_datos.push(row);
                        }
                    }
                    return Ok(Some(vector_datos));
                }
                // En este caso no pude conectarme al nodo responsable pero yo (nodo coordinador) tampoco lo soy, y como hay
                // un WHERE en la consulta es necesario enviársela a los nodos que les corresponda resolverla pero como no
                // pude conectarme al nodo responsable y alguien tiene que hacerlo, entonces una de sus réplicas tiene que
                // tomar su lugar como nodo responsable

                // De nuevo aprovechando la implementación sé que las réplicas del nodo responsables son las siguientes ips en
                // el cluster por lo que tengo también las ips de ellas, tengo que iterar hasta encontrar una réplica a la que
                // me pueda conectar

                let replicas = get_replicas(
                    &self.nodos,
                    ip_nodo_responsable.to_string(),
                    nivel_replicacion,
                );
                if consistencia == LevelConsistency::Weak {
                    for ip_replica in replicas {
                        let nombre_servicio = obtener_nombre_servicio(ip_replica.clone());

                        let direccion = format!("{}:{}", nombre_servicio, PUERTO_INTERNODOS);
                        if ip_replica == self.ip {
                            // Si bien la lógica es similar al caso en que no sea una réplica el que reciba
                            // la consulta, en este caso también es necesario devolver el timestamp del nodo
                            // ya que luego será usado para el read repair

                            if let Some(tabla_elegida) = self.get_tabla(tabla_consulta)? {
                                let auxiliar = tabla_elegida.select(condicion, query.to_string());
                                for linea in auxiliar.iter() {
                                    if !vector_datos.contains(linea) {
                                        vector_datos.push(linea.to_string());
                                    }
                                }
                            }
                            return Ok(Some(vector_datos));
                        }
                        if let Some(endpoint_data) = self.metadata_nodos.get(&ip_replica) {
                            if endpoint_data.application_state.status == NodeStatus::Normal {
                                let mut deserialized_response = send_and_deserial(
                                    direccion,
                                    consulta_explicita,
                                    LevelConsistency::Strong,
                                    REPLICA,
                                    tipo_consulta,
                                )?;
                                deserialized_response.pop();
                                return Ok(Some(deserialized_response));
                            }
                        }
                    }
                } else {
                    //SELECT con where pero no soy el nodo responsable CONSISTENCY STRONG
                    let mut exitos = 0;

                    // respuestas: Vec<(datos, timestamp, direccion_replica)>
                    // Mapear los datos por timestamp
                    let mut mapa_respuestas: HashMap<String, (Vec<String>, String)> =
                        HashMap::new();

                    for ip_replica in replicas {
                        let nombre_servicio = obtener_nombre_servicio(ip_replica.clone());

                        let direccion = format!("{}:{}", nombre_servicio, PUERTO_INTERNODOS);
                        if ip_replica == self.ip {
                            let mut respuesta: Vec<String> = Vec::new();
                            if let Some(tabla_elegida) = self.get_tabla(tabla_consulta)? {
                                let auxiliar = tabla_elegida.select(condicion, query.to_string());
                                for linea in auxiliar.iter() {
                                    if !respuesta.contains(linea) {
                                        respuesta.push(linea.to_string());
                                    }
                                }
                            }
                            mapa_respuestas
                                .entry(self.timestamp.to_string())
                                .or_insert((respuesta.clone(), direccion.to_string()))
                                .0
                                .extend(respuesta.into_iter());
                            exitos += 1;
                            if exitos >= quorum {
                                break;
                            }
                        } else if let Some(endpoint_data) = self.metadata_nodos.get(&ip_replica) {
                            if endpoint_data.application_state.status != NodeStatus::Normal {
                                continue;
                            }
                            let mut deserialized_response = send_and_deserial(
                                direccion.to_string(),
                                consulta_explicita,
                                LevelConsistency::Strong,
                                REPLICA,
                                tipo_consulta,
                            )?;
                            let timestamp = deserialized_response.pop().unwrap();

                            mapa_respuestas
                                .entry(timestamp)
                                .or_insert((deserialized_response.clone(), direccion))
                                .0
                                .extend(deserialized_response.into_iter());
                            exitos += 1;
                            if exitos >= quorum {
                                break;
                            }
                        }
                    }
                    if exitos < quorum {
                        return Err("No se cumplió la consistencia de la consulta".to_string());
                    }

                    // Se mapearon los datos por timestamp, ahora se debe elegir el más reciente
                    let (timestamp_mas_reciente, datos_mas_recientes) = mapa_respuestas
                        .iter()
                        .max_by_key(|entry| entry.0)
                        .ok_or_else(|| "No se encontraron datos en las respuestas".to_string())?;

                    let dato_mas_reciente = &datos_mas_recientes;
                    // Caso de diferentes timestamps entre las réplicas
                    if mapa_respuestas.len() > 1 {
                        // Hay discrepancias, realizar read repair
                        self.read_repair(
                            &mapa_respuestas,
                            dato_mas_reciente,
                            timestamp_mas_reciente,
                            tabla_consulta,
                        )?;
                    }
                    return Ok(Some(dato_mas_reciente.0.clone()));
                }
                Ok(None)
            }
            TypeCQL::Update => {
                let hash_valor = obtener_hash_key_select(condicion);
                let ip_nodo_responsable =
                    self.get_nodo_responsable(hash_valor).ok_or_else(|| {
                        "No se ha encontrado el nodo responsable para la key.".to_string()
                    })?;
                if ip_nodo_responsable == self.ip {
                    // Soy el nodo responsable
                    if let Some(tabla_elegida) = self.get_tabla(tabla_consulta)? {
                        tabla_elegida.actualizar(condicion, query.to_string());
                        self.persistir_update(tabla_consulta.to_string())?;
                        self.timestamp += 1;
                    }

                    // Hay que ver si en este caso es necesario actualizar el dato en las réplicas o no
                    // Ya que de eso puede encargarse el read repair al momento de hacer un SELECT
                    // Por lo que entonces habría que modificar esa operación para que soporte esto => Sí

                    self.enviar_escrituras_replicas(consulta.clone(), consistencia, quorum)?;
                } else {
                    // Se le envía la consulta al nodo responsable.

                    let nombre_servicio = obtener_nombre_servicio(ip_nodo_responsable.clone());

                    let direccion = format!("{}:{}", nombre_servicio, PUERTO_INTERNODOS);
                    if send_and_not_deserial(
                        direccion,
                        consulta_explicita,
                        LevelConsistency::Strong,
                        RESPONSABLE,
                    )
                    .is_err()
                    {
                        // Al igual que en el caso de una operación SELECT, si no pude conectarme al nodo responsable pero yo tampoco lo soy
                        // entonces yo (el nodo coordinador) paso a ser un nuevo "nodo responsable" enviándole la consulta a las réplicas del
                        // nodo responsable real y recibiendo las respuestas, luego actuar pero sin involucrar datos propios

                        let replicas = get_replicas(
                            &self.nodos,
                            ip_nodo_responsable.to_string(),
                            nivel_replicacion,
                        );

                        if consistencia == LevelConsistency::Weak {
                            for ip_replica in replicas {
                                if let Some(endpoint_data) = self.metadata_nodos.get(&ip_replica) {
                                    if endpoint_data.application_state.status != NodeStatus::Normal
                                    {
                                        continue;
                                    }

                                    let nombre_servicio =
                                        obtener_nombre_servicio(ip_replica.clone());

                                    let direccion =
                                        format!("{}:{}", nombre_servicio, PUERTO_INTERNODOS);
                                    if let Ok(deserialize_response) = send_and_deserial(
                                        direccion,
                                        consulta_explicita,
                                        LevelConsistency::Strong,
                                        REPLICA,
                                        tipo_consulta,
                                    ) {
                                        if deserialize_response[0] == "ACK" {
                                            // Se cumple la consistencia
                                            return Ok(None);
                                        }
                                    }
                                }
                            }
                            return Err("No se cumplió la consistencia de la consulta".to_string());
                        }
                        let mut exitos = 0;
                        let replicas = get_replicas(
                            &self.nodos,
                            ip_nodo_responsable.to_string(),
                            nivel_replicacion,
                        );

                        for ip_replica in replicas {
                            if self.ip == ip_replica {
                                if let Some(tabla_elegida) = self.get_tabla(tabla_consulta)? {
                                    tabla_elegida.actualizar(condicion, query.to_string());
                                    self.persistir_update(tabla_consulta.to_string())?;
                                    self.timestamp += 1;
                                    exitos += 1;
                                    if exitos >= quorum {
                                        break;
                                    }
                                }
                                continue;
                            }
                            if let Some(endpoint_data) = self.metadata_nodos.get(&ip_replica) {
                                if endpoint_data.application_state.status != NodeStatus::Normal {
                                    continue;
                                }

                                let nombre_servicio = obtener_nombre_servicio(ip_replica.clone());

                                let direccion =
                                    format!("{}:{}", nombre_servicio, PUERTO_INTERNODOS);
                                if let Ok(deserialize_response) = send_and_deserial(
                                    direccion,
                                    consulta_explicita,
                                    LevelConsistency::Strong,
                                    REPLICA,
                                    tipo_consulta,
                                ) {
                                    if deserialize_response[0] == "ACK" {
                                        exitos += 1;
                                        if exitos >= quorum {
                                            // Se cumple la consistencia
                                            break;
                                        }
                                    }
                                }
                            }
                        }
                        if exitos < quorum {
                            return Err("No se cumplió la consistencia de la consulta".to_string());
                        }
                    }
                }
                Ok(None)
            }
            TypeCQL::Delete => {
                let hash_valor = obtener_hash_key_select(condicion);
                let ip_nodo_responsable =
                    self.get_nodo_responsable(hash_valor).ok_or_else(|| {
                        "No se ha encontrado el nodo responsable para la key.".to_string()
                    })?;
                if ip_nodo_responsable == self.ip {
                    // Soy el nodo responsable
                    if let Some(tabla_elegida) = self.get_tabla(tabla_consulta)? {
                        tabla_elegida.eliminar(condicion);
                        self.persistir_delete(tabla_consulta.to_string())?;
                        self.timestamp += 1;
                    }

                    // En este caso al ser un DELETE también es necesario eliminar el dato en las réplicas
                    // Ya que si no eso podría causar que se devuelva un dato que ya no existe

                    self.enviar_escrituras_replicas(consulta.clone(), consistencia, quorum)?;
                } else {
                    // Se le envía la consulta al nodo responsable.

                    let nombre_servicio = obtener_nombre_servicio(ip_nodo_responsable.clone());

                    let direccion = format!("{}:{}", nombre_servicio, PUERTO_INTERNODOS);

                    if send_and_not_deserial(
                        direccion,
                        consulta_explicita,
                        LevelConsistency::Strong,
                        RESPONSABLE,
                    )
                    .is_err()
                    {
                        // Al igual que en el caso de una operación SELECT, si no pude conectarme al nodo responsable pero yo tampoco lo soy
                        // entonces yo (el nodo coordinador) paso a ser un nuevo "nodo responsable" enviándole la consulta a las réplicas del
                        // nodo responsable real y recibiendo las respuestas, luego actuar pero sin involucrar datos propios

                        if consistencia == LevelConsistency::Weak {
                            let replicas = get_replicas(
                                &self.nodos,
                                ip_nodo_responsable.to_string(),
                                nivel_replicacion,
                            );
                            for ip_replica in replicas {
                                let nombre_servicio = obtener_nombre_servicio(ip_replica.clone());

                                let direccion =
                                    format!("{}:{}", nombre_servicio, PUERTO_INTERNODOS);
                                if let Some(endpoint_data) = self.metadata_nodos.get(&ip_replica) {
                                    if endpoint_data.application_state.status != NodeStatus::Normal
                                    {
                                        continue;
                                    }
                                    let deserialize_response = send_and_deserial(
                                        direccion,
                                        consulta_explicita,
                                        LevelConsistency::Strong,
                                        REPLICA,
                                        tipo_consulta,
                                    )?;
                                    if deserialize_response[0] == "ACK" {
                                        // Se cumple la consistencia
                                        return Ok(None);
                                    }
                                }
                            }
                            return Err("No se cumplió la consistencia de la consulta".to_string());
                        }
                        let mut exitos = 0;
                        let replicas = get_replicas(
                            &self.nodos,
                            ip_nodo_responsable.to_string(),
                            nivel_replicacion,
                        );
                        for ip_replica in replicas {
                            if self.ip == ip_replica {
                                if let Some(tabla_elegida) = self.get_tabla(tabla_consulta)? {
                                    tabla_elegida.eliminar(condicion);
                                    self.timestamp += 1;
                                    exitos += 1;
                                    if exitos >= quorum {
                                        break;
                                    }
                                }
                            } else if let Some(endpoint_data) = self.metadata_nodos.get(&ip_replica)
                            {
                                if endpoint_data.application_state.status != NodeStatus::Normal {
                                    continue;
                                }

                                let nombre_servicio = obtener_nombre_servicio(ip_replica.clone());

                                let direccion =
                                    format!("{}:{}", nombre_servicio, PUERTO_INTERNODOS);
                                let deserialize_response = send_and_deserial(
                                    direccion,
                                    consulta_explicita,
                                    LevelConsistency::Strong,
                                    REPLICA,
                                    tipo_consulta,
                                )?;
                                if deserialize_response[0] == "ACK" {
                                    exitos += 1;
                                    if exitos >= quorum {
                                        // Se cumple la consistencia
                                        break;
                                    }
                                }
                            }
                        }
                        if exitos < quorum {
                            return Err("No se cumplió la consistencia de la consulta".to_string());
                        }
                    }
                }
                Ok(None)
            }
            TypeCQL::CreateTable => self.create_table(consulta, consistencia),
            TypeCQL::CreateKeyspace => self.create_keyspace(consulta, consistencia),
        }
    }

    fn create_table(
        &mut self,
        consulta: &mut Consulta,
        consistencia: LevelConsistency,
    ) -> Result<Option<Vec<String>>, String> {
        let headers = obtener_headers_table(consulta.get_query());
        let tabla = consulta.get_tabla();
        let tabla_nueva = Tabla::new(tabla.to_string(), headers.clone());

        let keyspace = self.get_key()?;
        keyspace.tablas.insert(tabla.to_string(), tabla_nueva);
        let path = format!("bdd/src/{}/{}_{}.csv", self.keyspace_actual, tabla, self.ip);
        self.persistir_tabla_nueva(path);
        for (_, ip) in self.nodos.iter() {
            if &self.ip != ip {
                let nombre_servicio = obtener_nombre_servicio(ip.clone());

                let direccion = format!("{}:{}", nombre_servicio, PUERTO_INTERNODOS);
                send_and_deserial(
                    direccion,
                    consulta.get_consulta_explicita(),
                    LevelConsistency::create(consistencia.valor()),
                    RESPONSABLE,
                    &TypeCQL::CreateTable,
                )?;
            }
        }
        Ok(None)
    }

    fn create_keyspace(
        &mut self,
        consulta: &mut Consulta,
        consistencia: LevelConsistency,
    ) -> Result<Option<Vec<String>>, String> {
        let (strategy, replication_factor) =
            obtener_tipo_strategy_y_replication(consulta.get_query());
        if strategy != "SimpleStrategy" {
            return Err("No pudo crearse el keyspace correctamente.".to_string());
        }
        let tabla = consulta.get_tabla();
        let k = Keyspace::new(tabla.to_string(), replication_factor, strategy);
        self.keyspaces.insert(tabla.to_string(), k);
        let path = format!("bdd/src/{}", tabla);

        let _ = fs::create_dir(path);
        for (_, ip) in self.nodos.iter() {
            if &self.ip != ip {
                let nombre_servicio = obtener_nombre_servicio(ip.clone());

                let direccion = format!("{}:{}", nombre_servicio, PUERTO_INTERNODOS);
                send_and_deserial(
                    direccion,
                    consulta.get_consulta_explicita(),
                    LevelConsistency::create(consistencia.valor()),
                    RESPONSABLE,
                    &TypeCQL::CreateKeyspace,
                )?;
            }
        }
        Ok(None)
    }

    /// Método que persiste una fila nueva que se haya insertado en una tabla
    /// en el archivo específico del nodo
    pub fn persistir_insert(&self, tabla: String, row: String) {
        let path = format!("bdd/src/{}/{}_{}.csv", self.keyspace_actual, tabla, self.ip);
        let fila = row.clone() + "\n";

        if let Ok(contenido) = std::fs::read_to_string(&path) {
            if contenido.contains(&row) {
                // Si ya existe, no escribir
                return;
            }
        }

        let file = OpenOptions::new().append(true).open(path);
        if let Ok(mut file) = file {
            let _ = file.write_all(fila.as_bytes());
            let _ = file.flush();
        }
    }

    /// Método que persiste la actualización que se hizo en una o más filas de una tabla
    /// en el archivo específico del nodo
    pub fn persistir_update(&mut self, tabla: String) -> Result<(), String> {
        let path = format!("bdd/src/{}/{}_{}.csv", self.keyspace_actual, tabla, self.ip);

        let path_temporal = format!(
            "bdd/src/{}/archivo_update_{}.csv",
            self.keyspace_actual, self.ip
        );
        let archivo_temporal =
            File::create(&path_temporal).map_err(|_| "No se pudo persistir el update.")?;
        let mut escritor = io::BufWriter::new(&archivo_temporal);

        let keyspace = self.get_key()?;
        if let Some(tabla) = keyspace.tablas.get_mut(&tabla) {
            for datos in tabla.datos.values() {
                for linea in datos {
                    let nueva_linea = format!("{}\n", linea);
                    let _ = escritor.write_all(nueva_linea.as_bytes());
                }
            }
        }
        let _ = escritor.flush();
        let _ = std::fs::rename(&path_temporal, path);
        Ok(())
    }

    /// Método que persiste la eliminación de una o más filas de una tabla
    /// en el archivo específico del nodo
    pub fn persistir_delete(&mut self, tabla: String) -> Result<(), String> {
        let path = format!("bdd/src/{}/{}_{}.csv", self.keyspace_actual, tabla, self.ip);

        let path_delete = format!(
            "bdd/src/{}/path_delete_{}.csv",
            self.keyspace_actual, self.ip
        );
        let archivo_temporal =
            File::create(&path_delete).map_err(|_| "No se pudo persistir el delete.")?;
        let mut escritor = io::BufWriter::new(archivo_temporal);

        let keyspace = self.get_key()?;
        if let Some(tabla) = keyspace.tablas.get_mut(&tabla) {
            for datos in tabla.datos.values() {
                for linea in datos {
                    let nueva_linea = format!("{}\n", linea);
                    let _ = escritor.write_all(nueva_linea.as_bytes());
                }
            }
        }
        let _ = escritor.flush();
        let _ = std::fs::rename(&path_delete, path);
        Ok(())
    }

    /// Método que persiste la creación de una tabla nueva en el archivo específico del nodo,
    /// se recibe la ruta en que estará el archivo asociado con esa tabla
    pub fn persistir_tabla_nueva(&self, path: String) {
        let _ = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(path);
    }

    fn enviar_escrituras_replicas(
        &self,
        consulta: Consulta,
        consistencia: LevelConsistency,
        quorum: usize,
    ) -> Result<(), String> {
        let (tx, rx): TxRx = mpsc::channel();

        for ip_replica in &self.replicas {
            println!("Enviando a réplica: {}", ip_replica);
            let config = Arc::new(create_client_config()?);
            let server_name = ServerName::try_from("localhost")
                .map_err(|_| "Nombre de dominio inválido.".to_string())?;
            let ip_replica = ip_replica.to_string();
            let consulta = consulta.clone();
            let nivel_consistencia = consistencia.clone();
            let tx = tx.clone();

            let nombre_servicio = obtener_nombre_servicio(ip_replica.clone());

            thread::spawn(move || {
                let direccion_replica = format!("{}:{}", nombre_servicio, PUERTO_INTERNODOS);

                let resultado = match TcpStream::connect(direccion_replica) {
                    Ok(socket) => {
                        let client_conn =
                            ClientConnection::new(Arc::clone(&config), server_name).unwrap();
                        let mut tls_stream = StreamOwned::new(client_conn, socket);
                        let query_serializada = serializar_envio_nodos(
                            consulta.get_consulta_explicita(),
                            nivel_consistencia.clone(),
                            REPLICA,
                        );
                        if tls_stream.write_all(&query_serializada).is_ok() {
                            // Leer respuesta (ACK)
                            if let Ok(respuesta) = deserializar_respuesta_nodos(&mut tls_stream) {
                                if respuesta.contains(&"ACK".to_string()) {
                                    Ok(())
                                } else {
                                    Err("No se recibió ACK".to_string())
                                }
                            } else {
                                Err("Error al recibir la respuesta".to_string())
                            }
                        } else {
                            Err("Error al enviar la consulta".to_string())
                        }
                    }
                    Err(_) => Err("No se pudo conectar con la réplica".to_string()),
                };
                // Enviar el resultado al hilo principal
                let _ = tx.send(resultado);
            });
        }
        drop(tx); // Cerramos el sender para indicar que no habrá más envíos

        let mut ack_count = 0;
        let required_acks = match consistencia {
            LevelConsistency::Weak => 1,
            LevelConsistency::Strong => quorum - 1,
        };

        //Recibir resultados a medida que lleguen
        for resultado in rx {
            if resultado.is_ok() {
                ack_count += 1;
                if ack_count >= required_acks {
                    println!(
                        "Se han obtenido más de {} ACKs: Se cumple el Consistency Level",
                        required_acks
                    );
                    // Alcanzamos el número requerido de ACKs
                    println!("- - - - - - - - - - - - - - - - - - - - - - - - - - - -");
                    return Ok(());
                }
            }
        }
        // Si salimos del bucle, es que no alcanzamos el número requerido de ACKs
        Err("No se alcanzó el nivel de consistencia requerido".to_string())
    }

    fn enviar_lecturas_replicas(
        &mut self,
        consulta: Consulta,
        consistencia: LevelConsistency,
        datos_responsable: Vec<String>,
        nombre_tabla: &str, //nombre de la tabla de la consulta
        quorum: usize,
    ) -> Result<Vec<String>, String> {
        let (tx, rx): TxRx2 = mpsc::channel();
        let _ = io::stdout().flush();

        for ip_replica in &self.replicas {
            let server_name = ServerName::try_from("localhost")
                .map_err(|_| "Nombre de dominio inválido.".to_string())?;
            let ip_replica = ip_replica.clone();
            let consulta = consulta.clone();

            let nombre_servicio = obtener_nombre_servicio(ip_replica.clone());

            let tx = tx.clone();
            let nivel_consistencia = consistencia.clone();
            thread::spawn(move || {
                let config = Arc::new(create_client_config().unwrap());
                let direccion_replica = format!("{}:{}", &nombre_servicio, PUERTO_INTERNODOS);
                let direccion_replica2 = format!("{}:{}", &nombre_servicio, PUERTO_INTERNODOS);

                let resultado = match TcpStream::connect(direccion_replica) {
                    Ok(socket) => {
                        let client_conn = ClientConnection::new(Arc::clone(&config), server_name)
                            .map_err(|_| "Error al crear la conexión TLS del cliente.")
                            .unwrap();
                        let mut tls_stream = StreamOwned::new(client_conn, socket);
                        let query_serializada = serializar_envio_nodos(
                            consulta.get_consulta_explicita(),
                            nivel_consistencia.clone(), // Usamos el nivel requerido
                            REPLICA,
                        );
                        if tls_stream.write_all(&query_serializada).is_ok() {
                            if let Ok(respuesta) = deserializar_respuesta_nodos(&mut tls_stream) {
                                // Suponiendo que la respuesta es (datos, timestamp)
                                let timestamp = respuesta.last().unwrap().clone(); // Obtener timestamp
                                let datos = respuesta[..respuesta.len() - 1].to_vec();
                                Ok((datos, timestamp, direccion_replica2))
                            } else {
                                Err("Error al recibir la respuesta".to_string())
                            }
                        } else {
                            Err("Error al enviar la consulta".to_string())
                        }
                    }
                    Err(_) => Err("No se pudo conectar con la réplica".to_string()),
                };
                let _ = tx.send(resultado);
            });
        }

        drop(tx); // Cerramos el sender

        let mut respuestas_replicas: Vec<(Vec<String>, String, String)> = Vec::new();
        let mut exitos = 0;
        let required_responses = match consistencia {
            LevelConsistency::Weak => 1,
            LevelConsistency::Strong => quorum - 1,
        };

        for resultado in rx.into_iter().flatten() {
            let datos = resultado.0;
            let timestamp = resultado.1;
            let direccion_replica = resultado.2;
            exitos += 1;
            respuestas_replicas.push((datos.clone(), timestamp.clone(), direccion_replica.clone()));
            if exitos >= required_responses {
                break;
            }
        }
        if respuestas_replicas.is_empty() {
            return Err("No se pudo obtener el dato de ninguna réplica".to_string());
        }
        // Procesar las respuestas y realizar read repair si es necesario
        let dato_mas_reciente =
            self.realizar_read_repair(respuestas_replicas, datos_responsable, nombre_tabla)?;

        Ok(dato_mas_reciente)
    }

    fn realizar_read_repair(
        &mut self,
        respuestas: Vec<(Vec<String>, String, String)>,
        datos_responsable: Vec<String>,
        tabla: &str,
    ) -> Result<Vec<String>, String> {
        // respuestas: Vec<(datos, timestamp, direccion_replica)>
        // Mapear los datos por timestamp
        let mut mapa_respuestas: HashMap<String, (Vec<String>, String)> = HashMap::new();

        for (datos, timestamp, direccion_replica) in respuestas {
            mapa_respuestas
                .entry(timestamp)
                .or_insert((Vec::new(), direccion_replica))
                .0
                .extend(datos.into_iter());
        }

        let nombre_servicio = obtener_nombre_servicio(self.ip.clone());

        let direccion_responsable = format!("{}:{}", &nombre_servicio, PUERTO_INTERNODOS);
        mapa_respuestas
            .entry(self.timestamp.to_string())
            .or_insert((Vec::new(), direccion_responsable))
            .0
            .extend(datos_responsable);
        // Obtener el timestamp más alto
        if let Some((timestamp_mas_reciente, datos_mas_recientes)) =
            mapa_respuestas.iter().max_by_key(|entry| entry.0)
        {
            let dato_mas_reciente = &datos_mas_recientes;

            // Caso de diferentes timestamps entre las réplicas
            if mapa_respuestas.len() > 1 {
                // Hay discrepancias, realizar read repair
                self.read_repair(
                    &mapa_respuestas,
                    dato_mas_reciente,
                    timestamp_mas_reciente,
                    tabla,
                )?;
            }
            return Ok(dato_mas_reciente.0.clone());
        }
        Err("No se encontraron datos en las respuestas".to_string())
    }

    fn read_repair(
        &mut self,
        mapa_respuestas: &HashMap<String, (Vec<String>, String)>,
        dato_mas_reciente: &(Vec<String>, String),
        timestamp_mas_reciente: &String,
        tabla: &str,
    ) -> Result<(), String> {
        for (timestamp, (_, direccion)) in mapa_respuestas {
            let direccion = direccion.to_string();
            if timestamp != timestamp_mas_reciente {
                // Estas réplicas tienen datos desactualizados

                let nombre_servicio = obtener_nombre_servicio(self.ip.clone());

                let direccion_responsable = format!("{}:{}", &nombre_servicio, PUERTO_INTERNODOS);

                let keyspace = self.get_key()?;
                let headers = keyspace.tablas.get(tabla).unwrap().headers.clone();
                let tabla_cloned = tabla.to_string();
                let (dato_para_actualizar, _) = dato_mas_reciente.clone();
                if *direccion != direccion_responsable {
                    thread::spawn(move || {
                        println!("Se realiza un read repair en la réplica {} del nodo responsable actual", direccion);
                        let config = Arc::new(create_client_config().unwrap());
                        let server_name = ServerName::try_from("localhost").unwrap();
                        if let Ok(socket) = TcpStream::connect(direccion) {
                            let client_conn =
                                ClientConnection::new(Arc::clone(&config), server_name).unwrap();
                            let mut tls_stream = StreamOwned::new(client_conn, socket);
                            for linea in dato_para_actualizar.iter() {
                                let linea = linea.split(",").collect::<Vec<&str>>();
                                let consulta_update = construir_update_todos_los_campos(
                                    &tabla_cloned,
                                    &headers,
                                    linea,
                                );
                                let query_serializada = serializar_envio_nodos(
                                    &consulta_update,
                                    LevelConsistency::Strong,
                                    REPLICA,
                                );
                                let _ = tls_stream.write_all(&query_serializada);
                                let _ = deserializar_respuesta_nodos(&mut tls_stream);
                            }
                        }
                    });
                } else {
                    println!(
                        "Se realiza un read repair en el nodo responsable actual {}",
                        direccion
                    );
                    for linea in dato_para_actualizar.iter() {
                        let linea = linea.split(",").collect::<Vec<&str>>();
                        let consulta_update =
                            construir_update_todos_los_campos(&tabla_cloned, &headers, linea);

                        if let Ok(consulta) = procesar_consulta(&consulta_update) {
                            if let Some(tabla_elegida) = self.get_tabla(tabla)? {
                                tabla_elegida
                                    .actualizar(consulta.get_where(), consulta_update.to_string());
                                self.persistir_update(tabla.to_string())?;
                                self.timestamp += 1;
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }

    /// Método que hace que el nodo actual comience a hacer Gossip, inicialmente
    /// con nadie pero progresivamente con los nodos que vaya conociendo
    pub fn iniciar_gossip(nodo: Arc<Mutex<Self>>) {
        let seeds = get_seeds();
        thread::spawn(move || loop {
            println!("- - - - Inicia ronda de Gossip - - - -");
            let mut conectados: Vec<String> = vec![];
            let start_time = std::time::Instant::now(); // Marca de tiempo inicial
            let timeout = std::time::Duration::from_secs(10);
            while conectados.len() < 2 {
                if start_time.elapsed() >= timeout {
                    // Si se excede el tiempo límite, rompe el ciclo
                    break;
                }
                let (mut random_ip, nodo_ip) = {
                    let nodo_lock = nodo.lock().unwrap();

                    (
                        nodo_lock.get_random_ip(&nodo_lock.nodos),
                        nodo_lock.ip.to_string(),
                    )
                };
                let mut servicio = obtener_nombre_servicio(random_ip.to_string());
                if conectados.contains(&servicio)
                    || nodo.lock().unwrap().gossip_recientes.contains(&servicio)
                {
                    continue;
                }
                if (!conectados.is_empty() && !seeds.contains(&servicio)) || random_ip.is_empty() {
                    let mut rng = rand::thread_rng();
                    let random_number = rng.gen_range(0..=2);
                    random_ip = seeds[random_number].to_string();
                }
                if random_ip.contains("node") {
                    servicio = random_ip;
                    random_ip = obtener_ip_del_servicio(&servicio);
                } else {
                    servicio = obtener_nombre_servicio(random_ip.to_string());
                }

                // Llamar a gossip_session sin mantener el bloqueo
                match Self::gossip_session(nodo.clone(), &random_ip, nodo_ip) {
                    Ok(_) => {}
                    Err(_e) => {
                        continue;
                    }
                }
                conectados.push(servicio);
            }
            nodo.lock().unwrap().update_heartbeat();
            nodo.lock().unwrap().gossip_recientes.clear();
            println!("Réplicas del nodo: {:?}", nodo.lock().unwrap().replicas);
            thread::sleep(Duration::from_secs(5));
        });
    }

    /// Método que actualiza el heartbeat del nodo actual
    pub fn update_heartbeat(&mut self) {
        if let Some(endpoint_data) = self.metadata_nodos.get_mut(&self.ip) {
            endpoint_data.heartbeat_state.version += 1;
        }
    }

    pub fn gossip_session(
        nodo: Arc<Mutex<Nodo>>,
        random_ip: &String,
        sending_ip: String,
    ) -> Result<(), String> {
        println!("Nodos del árbol: {:?}", nodo.lock().unwrap().nodos.values());
        let config = Arc::new(create_client_config()?);
        let server_name = ServerName::try_from("localhost")
            .map_err(|_| "Nombre de dominio inválido.".to_string())?;

        let nombre_servicio = obtener_nombre_servicio(random_ip.clone());

        let address = format!("{}:{}", nombre_servicio, PUERTO_GOSSIP);
        // Preparar el mensaje SYN sin mantener el bloqueo
        let syn = {
            let mut syn_msg = String::new();
            if let Ok(nodo_lock) = nodo.lock() {
                for (ip, endpoint_data) in nodo_lock.metadata_nodos.iter() {
                    syn_msg.push_str(&format!(
                        "{}:{}:{} ",
                        ip,
                        endpoint_data.heartbeat_state.generation,
                        endpoint_data.heartbeat_state.version
                    ));
                }
            }
            syn_msg.trim_end().to_string()
        };

        // Realizar operaciones de red sin mantener el bloqueo
        if let Ok(socket) = TcpStream::connect(&address) {
            let client_conn = ClientConnection::new(Arc::clone(&config), server_name)
                .map_err(|_| "Error al crear la conexión TLS del cliente.")?;
            let mut tls_stream = StreamOwned::new(client_conn, socket);
            if let Ok(mut nodo_lock) = nodo.lock() {
                if nodo_lock.metadata_nodos.contains_key(random_ip) {
                    if let Some(endpoint_data) = nodo_lock.metadata_nodos.get_mut(random_ip) {
                        if endpoint_data.application_state.status == NodeStatus::Down {
                            println!("Marcando como reconectado un nodo que se había caido.");
                            endpoint_data.application_state.status = NodeStatus::Normal;
                            endpoint_data.heartbeat_state.version += 1;
                        }
                    }
                }
            }

            let syn_serializada = serializar_gossip(syn.to_string(), TypeGossip::Syn, sending_ip);
            // Enviar SYN
            tls_stream
                .write_all(&syn_serializada)
                .map_err(|_| "Error al ejecutar la consulta.".to_string())?;

            // Recibir ACK
            let (ack_received, _, _) = deserializar_gossip(&mut tls_stream)?;

            if ack_received.is_empty() {
                return Ok(());
            }
            let (desactualizados_peer, actualizar_actual_local) = {
                let respuesta = ack_received.split("\n").collect::<Vec<&str>>();
                (
                    respuesta[0].split(" ").collect::<Vec<&str>>(),
                    respuesta[1].split(" ").collect::<Vec<&str>>(),
                )
            };
            // Preparar ACK2 sin mantener el bloqueo
            let mut ack2: String = "".to_string();

            if let Ok(nodo_lock) = nodo.lock() {
                for desactualizado in desactualizados_peer {
                    let partes = desactualizado.split(":").collect::<Vec<&str>>();
                    let ip = partes[0];
                    if let Some(endpoint_data) = nodo_lock.metadata_nodos.get(ip) {
                        ack2.push_str(&format!(
                            "{}:{}:{}:{} ",
                            ip,
                            endpoint_data.heartbeat_state.generation,
                            endpoint_data.heartbeat_state.version,
                            endpoint_data.application_state.status
                        ));
                    }
                }
            }
            // Actualizar metadatos propios
            if !actualizar_actual_local.is_empty() {
                for nodo_a_actualizar in actualizar_actual_local.iter() {
                    let partes = nodo_a_actualizar.split(":").collect::<Vec<&str>>();
                    if partes[0].is_empty() {
                        break;
                    }
                    let ip = partes[0];
                    let generacion: f64 = partes[1]
                        .parse()
                        .map_err(|_| "Error al parsear generacion".to_string())?;
                    let ver: u32 = partes[2]
                        .parse()
                        .map_err(|_| "Error al parsear version".to_string())?;
                    let estado_str = partes[3];
                    let estado = NodeStatus::create(estado_str);
                    let mut nodo_lock = nodo.lock().unwrap();

                    let hash_result = hashear(ip)?;
                    let already_exists = nodo_lock.nodos.contains_key(&hash_result);

                    if let Some(endpoint_data) = nodo_lock.metadata_nodos.get_mut(ip) {
                        endpoint_data.heartbeat_state.generation = generacion;
                        endpoint_data.heartbeat_state.version = ver;

                        let delete_ip = endpoint_data.application_state.status
                            == NodeStatus::Normal
                            && estado == NodeStatus::Down;
                        endpoint_data.application_state.status = estado.clone();
                        if nodo_lock.ip == ip {
                            continue;
                        }
                        if delete_ip {
                            //Hay que eliminar el nodo de la lista de nodos que ya tengo en mi metadata (nodo que envía)
                            println!("Marcando al nodo {} como caído y eliminandolo de la lista de nodos", ip);
                            let hash_result = hashear(ip)?;
                            nodo_lock.nodos.remove(&hash_result);

                            if let Some(keyspace) =
                                nodo_lock.keyspaces.get(&nodo_lock.keyspace_actual)
                            {
                                let tablas: Vec<String> = keyspace.tablas.keys().cloned().collect();
                                let _ = keyspace;

                                for tabla in tablas {
                                    if tabla == "AEROPUERTOS" {
                                        continue;
                                    }

                                    let path = format!(
                                        "bdd/src/{}/{}_{}.csv",
                                        nodo_lock.keyspace_actual, tabla, ip
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

                            //nodo_lock.metadata_nodos.remove(ip);
                            nodo_lock.actualizar_replicas();
                        }
                        if !already_exists && estado == NodeStatus::Normal {
                            nodo_lock.nodos.insert(hash_result, ip.to_string());
                        }
                        continue;
                    }

                    let endpoint = EndpointData::new(generacion, ver, estado);
                    nodo_lock.metadata_nodos.insert(ip.to_string(), endpoint);
                    let hash_result = hashear(ip)?;
                    nodo_lock.nodos.insert(hash_result, ip.to_string());
                    nodo_lock.actualizar_replicas();
                    let keyspace_path = format!("bdd/src/{}", nodo_lock.keyspace_actual);
                    if fs::metadata(keyspace_path).is_ok() {
                        if let Some(keyspace) = nodo_lock.keyspaces.get(&nodo_lock.keyspace_actual)
                        {
                            for tabla in keyspace.tablas.keys() {
                                let path = format!(
                                    "bdd/src/{}/{}_{}.csv",
                                    nodo_lock.keyspace_actual, tabla, ip
                                );
                                let ruta = Path::new(&path);
                                if ruta.exists() {
                                    continue;
                                } else {
                                    let archivo = File::create(&path);
                                    if archivo.is_err() {
                                        return Err(format!("Error al crear archivo: {}", path));
                                    }
                                }
                            }
                            let (ip_distribuidor, token_distribuidor) =
                                obtener_distribuidor(&nodo_lock, ip.to_string());
                            println!(
                                "El nodo de IP {} debe distribuir los datos al nuevo nodo",
                                ip_distribuidor
                            );
                            redistribuir(
                                &mut nodo_lock,
                                ip.to_string(),
                                &ip_distribuidor.to_string(),
                                token_distribuidor.to_string(),
                            )?;
                            println!("Reorganización completada");
                        }
                    }
                }
            }

            // Enviar ACK2 sin mantener el bloqueo
            let ack2_serializada = serializar_gossip(ack2, TypeGossip::Ack2, "".to_string());
            tls_stream
                .write_all(&ack2_serializada)
                .map_err(|_| "Error al ejecutar la consulta.".to_string())?;

            // Cerrar el socket
            tls_stream
                .sock
                .shutdown(std::net::Shutdown::Both)
                .map_err(|_| "Error al cerrar el socket.".to_string())?;
        } else {
            let mut nodo_lock = nodo.lock().unwrap();
            if nodo_lock.metadata_nodos.contains_key(random_ip) {
                if let Some(endpoint_data) = nodo_lock.metadata_nodos.get_mut(random_ip) {
                    if endpoint_data.application_state.status == NodeStatus::Normal {
                        println!(
                            "Marcando al nodo {} como caído y eliminandolo de la lista de nodos",
                            random_ip
                        );
                        endpoint_data.application_state.status = NodeStatus::Down;
                        endpoint_data.heartbeat_state.version += 1;
                    }
                    let hash_result = hashear(random_ip)?;
                    nodo_lock.nodos.remove(&hash_result);
                }
                nodo_lock.actualizar_replicas();
            }
        }
        println!(" - - - - Ronda de gossip finalizada - - - -");
        Ok(())
    }

    fn get_random_ip(&self, ips: &BTreeMap<u32, String>) -> String {
        let nodes: Vec<&String> = ips.values().collect();
        if nodes.is_empty() {
            return "".to_string();
        }
        let random_index = rand::random::<usize>() % nodes.len();

        if self.ip == *nodes[random_index] {
            "".to_string()
        } else {
            nodes[random_index].to_string()
        }
    }

    pub fn load_data(&mut self) -> Result<(), String> {
        let path_bdd = "bdd/src";

        let entradas =
            fs::read_dir(path_bdd).map_err(|_| "Base de datos incorrecta.".to_string())?;

        for entrada in entradas.map_while(Result::ok) {
            // ---> Se itera por todos los elementos de la ruta especificada
            let ruta = entrada.path();
            if ruta.is_dir() {
                // ---> Solo se ejecuta la lógica si el elemento es un directorio, en este caso el keyspace
                if let Some(nombre) = ruta.file_name() {
                    let name = nombre.to_string_lossy().to_string();
                    let mut k = Keyspace::new(name.to_string(), 3, "SimpleStrategy".to_string());
                    let tablas =
                        load_tablas(format!("{}/{}", path_bdd, name), self.ip.to_string())?;
                    k.add_tablas(tablas);
                    self.keyspaces.insert(name.to_string(), k);
                }
            }
        }
        Ok(())
    }

    pub fn get_key(&mut self) -> Result<&mut Keyspace, String> {
        let keyspace = self
            .keyspaces
            .get_mut(&self.keyspace_actual)
            .ok_or("Keyspace no encontrado.")?;
        Ok(keyspace)
    }

    pub fn get_tabla(&mut self, tabla_buscada: &str) -> Result<Option<&mut Tabla>, String> {
        let keyspace = self.get_key()?;
        if let Some(tabla_encontrada) = keyspace.tablas.get_mut(tabla_buscada) {
            return Ok(Some(tabla_encontrada));
        }
        Ok(None)
    }

    pub fn actualizar_replicas(&mut self) {
        let mut nivel_replicacion = 3;
        if let Some(keyspace) = self.keyspaces.get(&self.keyspace_actual) {
            nivel_replicacion = keyspace.nivel_replicacion;
        }
        let nuevas_replicas = get_replicas(&self.nodos, self.ip.to_string(), nivel_replicacion);
        self.replicas = nuevas_replicas;
    }
}

fn obtener_distribuidor(nodo: &MutexGuard<Nodo>, ip_nuevo: String) -> (String, String) {
    let mut ip_anterior = "".to_string();
    let mut _token_anterior = "".to_string();
    for (token, ip) in nodo.nodos.iter() {
        if ip_anterior != ip_nuevo {
            ip_anterior = ip.to_string();
            _token_anterior = token.to_string();
            continue;
        } else {
            return (ip.to_string(), token.to_string());
        }
    }
    ("".to_string(), "".to_string())
}

fn redistribuir(
    nodo: &mut MutexGuard<Nodo>,
    ip_nuevo: String,
    ip_redistribuidor: &String,
    token_nuevo: String,
) -> Result<(), String> {
    let keyspace = nodo
        .keyspaces
        .get(&nodo.keyspace_actual)
        .ok_or_else(String::new)?;
    for (nombre_tabla, tabla) in keyspace.tablas.iter() {
        for (partition_key, datos) in tabla.datos.iter() {
            println!("Se redistribuirán los datos de la tabla {}", nombre_tabla);
            let hash_result = hashear(partition_key)?;
            if let Ok(token_u32) = token_nuevo.parse::<u32>() {
                if hash_result <= token_u32 {
                    for linea in datos.iter() {
                        let linea = linea.split(",").collect::<Vec<&str>>();
                        let columnas = tabla.headers.join(", ");
                        let valores_formateados = linea.join(", ");

                        let consulta = format!(
                            "INSERT INTO {} ({}) VALUES ({})",
                            nombre_tabla, columnas, valores_formateados
                        );

                        let consulta_insert = procesar_consulta(&consulta).unwrap();

                        let nombre_servicio = obtener_nombre_servicio(ip_nuevo.clone());

                        let address_nodo_responsable =
                            format!("{}:{}", nombre_servicio, PUERTO_INTERNODOS);
                        let _ = send_and_not_deserial(
                            address_nodo_responsable,
                            consulta_insert.get_consulta_explicita(),
                            LevelConsistency::Strong,
                            REPLICA,
                        );

                        // eliminamos los datos nuestros y de mis réplicas
                        let mut consulta_delete = format!(
                            "DELETE FROM {} WHERE {} = {} AND ID_VUELO = {}",
                            nombre_tabla, tabla.headers[0], linea[0], linea[2]
                        );
                        println!("Consulta delete: {}", consulta_delete);
                        if nombre_tabla.contains("AEROPUERTOS") {
                            consulta_delete = format!(
                                "DELETE FROM {} WHERE ID_AEROPUERTO = {} AND NOMBRE = {}",
                                nombre_tabla, linea[0], linea[1]
                            );
                        }
                        if nodo.replicas.is_empty() {
                            continue;
                        }

                        let mut nivel_replicacion = 3;
                        if let Some(keyspace) = nodo.keyspaces.get(&nodo.keyspace_actual) {
                            nivel_replicacion = keyspace.nivel_replicacion;
                        }

                        let consulta_delete = procesar_consulta(&consulta_delete).unwrap();
                        let replicas_redistribuidor = get_replicas(
                            &nodo.nodos,
                            ip_redistribuidor.to_string(),
                            nivel_replicacion,
                        );
                        let ultima_replica =
                            replicas_redistribuidor[nodo.replicas.len() - 1].to_string();

                        println!("Se eliminarán los datos de la réplica {} dado que un nuevo nodo se ingresó al cluster", ultima_replica);

                        let nombre_servicio = obtener_nombre_servicio(ultima_replica.clone());

                        let address_replica = format!("{}:{}", nombre_servicio, PUERTO_INTERNODOS);

                        send_and_not_deserial(
                            address_replica,
                            consulta_delete.get_consulta_explicita(),
                            LevelConsistency::Strong,
                            REPLICA,
                        )
                        .map_err(|_| "No se pudo conectar con el nodo responsable.".to_string())?;
                    }
                }
            }
        }
    }

    Ok(())
}

pub fn get_replicas(
    ips: &BTreeMap<u32, String>,
    ip_actual: String,
    replication: usize,
) -> Vec<String> {
    let mut comienzo = false;
    let mut replicas: Vec<String> = vec![];
    for (_, node_ip) in ips.iter() {
        if *node_ip == ip_actual {
            comienzo = true;
            continue;
        } else if comienzo {
            if replicas.len() >= replication - 1 {
                break;
            }
            replicas.push(node_ip.to_string());
        }
    }
    for (_, node_ip) in ips.iter() {
        if *node_ip == ip_actual || replicas.contains(node_ip) {
            continue;
        }
        if replicas.len() >= replication - 1 {
            break;
        }
        replicas.push(node_ip.to_string());
    }
    replicas
}

fn construir_update_todos_los_campos(
    tabla: &String,
    headers: &[String],
    linea: Vec<&str>,
) -> String {
    let mut campos_valores = Vec::new();
    for i in 0..headers.len() {
        campos_valores.push(format!("{} = {}", headers[i], linea[i]));
    }

    let consulta_update = format!(
        "UPDATE {} SET {} WHERE {} = {} AND ID_VUELO = {}",
        tabla,
        campos_valores.join(", "),
        headers[0],
        linea[0],
        headers[2],
    );

    consulta_update
}

fn send_and_deserial(
    direccion: String,
    consulta: &str,
    consistencia: LevelConsistency,
    responsabilidad: u8,
    tipo_consulta: &TypeCQL,
) -> Result<Vec<String>, String> {
    let server_name =
        ServerName::try_from("localhost").map_err(|_| "Nombre de dominio inválido.".to_string())?;
    let config = Arc::new(create_client_config()?);
    let mut res: Vec<String> = vec![];
    if let Ok(socket) = TcpStream::connect(direccion.to_string()) {
        let client_conn = ClientConnection::new(Arc::clone(&config), server_name)
            .map_err(|_| "Error al crear la conexión TLS del cliente.")?;
        let mut tls_stream = StreamOwned::new(client_conn, socket);
        let query_serializada = serializar_envio_nodos(consulta, consistencia, responsabilidad);
        let _ = tls_stream.write_all(&query_serializada);
        if *tipo_consulta == TypeCQL::Select {
            res = deserializar_respuesta_nodos(tls_stream)?;
        }
    }
    Ok(res)
}

fn send_and_not_deserial(
    direccion: String,
    consulta: &str,
    consistencia: LevelConsistency,
    responsabilidad: u8,
) -> Result<(), String> {
    let server_name =
        ServerName::try_from("localhost").map_err(|_| "Nombre de dominio inválido.".to_string())?;
    let config = Arc::new(create_client_config()?);
    if let Ok(socket) = TcpStream::connect(direccion.to_string()) {
        let client_conn = ClientConnection::new(Arc::clone(&config), server_name)
            .map_err(|_| "Error al crear la conexión TLS del cliente.")?;
        let mut tls_stream = StreamOwned::new(client_conn, socket);
        let query_serializada = serializar_envio_nodos(consulta, consistencia, responsabilidad);
        let _ = tls_stream.write_all(&query_serializada);
    }
    Ok(())
}

pub fn obtener_nombre_servicio(ip: String) -> String {
    match ip.as_str() {
        "127.0.0.1" => "node1".to_string(),
        "127.0.0.2" => "node2".to_string(),
        "127.0.0.3" => "node3".to_string(),
        "127.0.0.4" => "node4".to_string(),
        "127.0.0.5" => "node5".to_string(),
        "127.0.0.6" => "node6".to_string(),
        "127.0.0.7" => "node7".to_string(),
        "127.0.0.8" => "node8".to_string(),
        _ => "".to_string(),
    }
}

pub fn obtener_ip_del_servicio(servicio: &str) -> String {
    "127.0.0.".to_string()
        + &servicio
            .chars()
            .nth(servicio.len() - 1)
            .unwrap()
            .to_string()
}
