use std::fmt;

#[derive(Debug, Clone, PartialEq)]
pub enum TypeCQL {
    Select,
    Insert,
    Update,
    Delete,
    CreateTable,
    CreateKeyspace,
}

impl fmt::Display for TypeCQL {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TypeCQL::Select => write!(f, "SELECT"),
            TypeCQL::Insert => write!(f, "INSERT"),
            TypeCQL::Update => write!(f, "UPDATE"),
            TypeCQL::Delete => write!(f, "DELETE"),
            TypeCQL::CreateTable => write!(f, "CREATE TABLE"),
            TypeCQL::CreateKeyspace => write!(f, "CREATE KEYSPACE"),
        }
    }
}
