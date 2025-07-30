use std::collections::HashMap;

use crate::tabla::Tabla;

/// Estructura que representa un Keyspace en
/// la base de datos
#[derive(Debug)]
pub struct Keyspace {
    pub nombre: String,
    pub tablas: HashMap<String, Tabla>, // K: nombre de tabla, V: Tabla
    pub nivel_replicacion: usize,       // Número de réplicas
    pub strategy: String,
}

impl Keyspace {
    /// Constructor de la estructura Keyspace, recibe tanto el nombre
    /// como el nivel de replicación (replication factor) que tenga ese
    /// keyspace y también la estrategia de replicación que se va a usar
    pub fn new(nombre: String, nivel_replicacion: usize, strategy: String) -> Self {
        let tablas: HashMap<String, Tabla> = HashMap::new();
        Keyspace {
            nombre,
            tablas,
            nivel_replicacion,
            strategy,
        }
    }

    /// Método que agrega las tablas nuevas creadas al keyspace
    pub fn add_tablas(&mut self, tabla: HashMap<String, Tabla>) {
        self.tablas = tabla;
    }
}
