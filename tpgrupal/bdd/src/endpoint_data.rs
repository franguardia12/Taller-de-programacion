use crate::node_status::NodeStatus;

/// Definición de la estructura HeartbeatState que se encarga
/// de almacenar el estado de un nodo en un momento dado
pub struct HeartbeatState {
    pub generation: f64,
    pub version: u32,
}

/// Definición de la estructura ApplicationState que se encarga
/// de almacenar el estado de la aplicación en un nodo, por ejemplo
/// cuando este está caído o normal
pub struct ApplicationState {
    pub status: NodeStatus,
}

/// Definición de la estructura EndpointData que se encarga
/// de almacenar los datos de un endpoint para la comunicación
/// entre nodos con Gossip
pub struct EndpointData {
    pub heartbeat_state: HeartbeatState,
    pub application_state: ApplicationState,
}

impl EndpointData {
    /// Constructor de la estructura EndpointData 
    pub fn new(generacion: f64, version: u32, estado: NodeStatus) -> Self {
        EndpointData {
            heartbeat_state: HeartbeatState {
                generation: generacion,
                version,
            },
            application_state: ApplicationState { status: estado },
        }
    }
}
