use std::fmt;

/// Enum que contiene todos los estados posibles
/// en los que puede estar un nodo
#[derive(PartialEq, Clone)]
pub enum NodeStatus {
    Bootstrap,
    Normal,
    Down,
}

impl fmt::Display for NodeStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            NodeStatus::Bootstrap => write!(f, "Bootstrap"),
            NodeStatus::Normal => write!(f, "Normal"),
            NodeStatus::Down => write!(f, "Down"),
        }
    }
}

impl NodeStatus {
    /// Crea un nuevo estado de nodo a partir de un string
    pub fn create(estado: &str) -> Self {
        match estado {
            "Bootstrap" => NodeStatus::Bootstrap,
            "Normal" => NodeStatus::Normal,
            _ => NodeStatus::Down,
        }
    }
}
