#[derive(Debug, PartialEq, Clone)]
pub struct CondicionWhere {
    pub condicion1: String,
    pub operador_logico: Option<String>,
    pub condicion2: String,
}
