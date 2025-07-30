use super::{condicion_where::CondicionWhere, type_cql::TypeCQL};

#[derive(Debug, Clone)]
pub struct Consulta {
    pub consulta_explicita: String,
    pub tabla: String,
    pub tipo: TypeCQL,
    pub query: String,
    pub condicion_where: CondicionWhere,
}

impl Consulta {
    pub fn get_consulta_explicita(&self) -> &str {
        &self.consulta_explicita
    }

    pub fn get_tabla(&self) -> &str {
        &self.tabla
    }

    pub fn get_type(&self) -> &TypeCQL {
        &self.tipo
    }

    pub fn get_query(&self) -> &str {
        &self.query
    }

    pub fn get_where(&self) -> &CondicionWhere {
        &self.condicion_where
    }
}
