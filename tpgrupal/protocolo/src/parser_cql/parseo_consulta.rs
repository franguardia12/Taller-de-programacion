use crate::parser_cql::condicion_where::CondicionWhere;
use crate::parser_cql::consulta::Consulta;
use crate::parser_cql::type_cql::TypeCQL;

fn parsear_insert(consulta_cql: &str) -> Consulta {
    let partes_insert: Vec<&str> = consulta_cql.split_whitespace().collect();

    let tabla = partes_insert[2].to_string();

    let columna_comienzo = consulta_cql.find('(').unwrap_or(0);
    let columna_fin = consulta_cql.find(')').unwrap_or(0);
    let columnas_str = &consulta_cql[columna_comienzo..columna_fin + 1];

    let valores_comienzo = consulta_cql.find("VALUES").unwrap_or(0) + "VALUES".len();
    let valores_str = consulta_cql[valores_comienzo..].trim();

    let query = [columnas_str, "VALUES", valores_str];
    let q = query.join(" ");

    Consulta {
        consulta_explicita: consulta_cql.to_string(),
        tabla,
        tipo: TypeCQL::Insert,
        query: q,
        condicion_where: CondicionWhere {
            condicion1: String::new(),
            operador_logico: None,
            condicion2: String::new(),
        },
    }
}

fn parsear_select(consulta_cql: &str) -> Consulta {
    let partes_select: Vec<&str> = consulta_cql.split_whitespace().collect();
    let from_index = partes_select.iter().position(|&x| x == "FROM").unwrap_or(0);
    let tabla = partes_select[from_index + 1].to_string();

    let columnas: String = partes_select[1..from_index].join(" ");

    let mut cond1 = String::new();
    let mut oper: Option<String> = None;
    let mut cond2 = String::new();

    if consulta_cql.contains("WHERE") {
        let x: Vec<&str> = consulta_cql.split("WHERE").collect();
        let y: Vec<&str> = x[1].split("AND").collect();
        cond1 = y[0].trim().to_string();
        if y.len() > 1 {
            cond2 = y[1].trim().to_string();
            oper = Some("AND".to_string());
        }
    };
    let condicion_where = CondicionWhere {
        condicion1: cond1,
        operador_logico: oper,
        condicion2: cond2,
    };

    Consulta {
        consulta_explicita: consulta_cql.to_string(),
        tabla,
        tipo: TypeCQL::Select,
        query: columnas,
        condicion_where,
    }
}

fn parsear_update(consulta_cql: &str) -> Consulta {
    let cql_primer_mitad: Vec<&str> = consulta_cql.split("WHERE").collect();
    let espaciado: Vec<&str> = cql_primer_mitad[0].split_whitespace().collect();

    let tabla = espaciado[1].to_string();

    let query = espaciado[3..].join(" ");

    let mut cond1 = String::new();
    let mut oper: Option<String> = None;
    let mut cond2 = String::new();

    if cql_primer_mitad.len() > 1 {
        let x: Vec<&str> = consulta_cql.split("WHERE").collect();
        let y: Vec<&str> = x[1].split("AND").collect();
        cond1 = y[0].trim().to_string();
        if y.len() > 1 {
            cond2 = y[1].trim().to_string();
            oper = Some("AND".to_string());
        }
    };
    let condicion_where = CondicionWhere {
        condicion1: cond1,
        operador_logico: oper,
        condicion2: cond2,
    };

    Consulta {
        consulta_explicita: consulta_cql.to_string(),
        tabla,
        tipo: TypeCQL::Update,
        query,
        condicion_where,
    }
}

fn parsear_delete(consulta_cql: &str) -> Consulta {
    let partes_delete: Vec<&str> = consulta_cql.split_whitespace().collect();
    let tabla = partes_delete[2].to_string();

    let mut cond1 = String::new();
    let mut oper: Option<String> = None;
    let mut cond2 = String::new();

    if consulta_cql.contains("WHERE") {
        let x: Vec<&str> = consulta_cql.split("WHERE").collect();
        let y: Vec<&str> = x[1].split("AND").collect();
        cond1 = y[0].trim().to_string();
        if y.len() > 1 {
            cond2 = y[1].trim().to_string();
            oper = Some("AND".to_string());
        }
    };
    let condicion_where = CondicionWhere {
        condicion1: cond1,
        operador_logico: oper,
        condicion2: cond2,
    };

    Consulta {
        consulta_explicita: consulta_cql.to_string(),
        tabla,
        tipo: TypeCQL::Delete,
        query: "".to_string(),
        condicion_where,
    }
}

fn parsear_create_table(consulta_cql: &str) -> Consulta {
    let splited = consulta_cql.split_whitespace().collect::<Vec<&str>>();
    let tabla = splited[2].to_string();

    let query = (splited[3..]).to_vec().join(" ");

    let condicion_where = CondicionWhere {
        condicion1: String::new(),
        operador_logico: None,
        condicion2: String::new(),
    };

    Consulta {
        consulta_explicita: consulta_cql.to_string(),
        tabla,
        tipo: TypeCQL::CreateTable,
        query,
        condicion_where,
    }
}

pub fn obtener_headers_table(query: &str) -> Vec<String> {
    let mut headers = vec![];

    let contenido_query = &query[1..query.len() - 1];
    let campos = contenido_query.split(',').collect::<Vec<&str>>();

    for mut campo in campos {
        campo = campo.trim();
        if !campo.starts_with("PRIMARY KEY") {
            let nombre_columna = campo.split_whitespace().next().unwrap_or("").to_string();
            let nombre_columna_limpio = nombre_columna.trim().replace(['(', ')'], "").to_string();
            if !headers.contains(&nombre_columna_limpio) {
                headers.push(nombre_columna_limpio);
            }
        } else {
            let contenido_primary_key = &campo[("PRIMARY KEY ((".len())..campo.len() - 1];
            for columna_primary_key in contenido_primary_key.split(',') {
                let c_primary_key_limpia = columna_primary_key
                    .trim()
                    .replace(['(', ')'], "")
                    .to_string();
                headers.push(c_primary_key_limpia);
            }
        }
    }
    mover_al_inicio(headers)
}

fn mover_al_inicio(headers: Vec<String>) -> Vec<String> {
    let mut headers_ordenados: Vec<String> = Vec::with_capacity(headers.len());
    headers_ordenados.push(headers[headers.len() - 1].to_string());
    for e in headers.iter().take(headers.len() - 1) {
        if !headers_ordenados.contains(&e.to_string()) {
            headers_ordenados.push(e.to_string());
        }
    }
    headers_ordenados
}

fn parsear_create_keyspace(consulta_cql: &str) -> Consulta {
    let splited = consulta_cql.split_whitespace().collect::<Vec<&str>>();
    let option_index = splited.iter().position(|&x| x == "=").unwrap_or(0);

    let tabla = splited[2].to_string();
    let query = splited[(option_index + 1)..].join(" ");

    let condicion_where = CondicionWhere {
        condicion1: String::new(),
        operador_logico: None,
        condicion2: String::new(),
    };

    Consulta {
        consulta_explicita: consulta_cql.to_string(),
        tabla,
        tipo: TypeCQL::CreateKeyspace,
        query,
        condicion_where,
    }
}

pub fn obtener_tipo_strategy_y_replication(query: &str) -> (String, usize) {
    let splited = query.split(",").collect::<Vec<&str>>();
    let class_strategy = splited[0].split(":").collect::<Vec<&str>>();
    let repli_number = splited[1].split(":").collect::<Vec<&str>>();

    let strategy = class_strategy[1].trim().replace("'", "").to_string();

    let number = repli_number[1]
        .trim()
        .replace("}", "")
        .parse::<usize>()
        .unwrap_or(1);

    (strategy, number)
}

pub fn procesar_consulta(consulta_cql: &str) -> Result<Consulta, String> {
    let partes: Vec<&str> = consulta_cql.split_whitespace().collect();
    let res = match partes[0].to_uppercase().as_str() {
        "SELECT" => parsear_select(consulta_cql),
        "INSERT" => parsear_insert(consulta_cql),
        "UPDATE" => parsear_update(consulta_cql),
        "DELETE" => parsear_delete(consulta_cql),
        "CREATE" => match partes[1].to_uppercase().as_str() {
            "TABLE" => parsear_create_table(consulta_cql),
            "KEYSPACE" => parsear_create_keyspace(consulta_cql),
            _ => return Err("Consulta CQL no soportada".to_string()),
        },
        _ => return Err("Consulta CQL no soportada".to_string()),
    };
    Ok(res)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parsear_insert() {
        //Arrange
        let query = "INSERT INTO tabla_ejemplo (ID_VUELO, ORIGEN, DESTINO, COMBUSTIBLE) VALUES (5, 'Buenos Aires', 'Rio de Janeiro', 99)";
        let cond = CondicionWhere {
            condicion1: String::new(),
            operador_logico: None,
            condicion2: String::new(),
        };

        //Act
        let consulta = parsear_insert(query);

        //Assert
        assert!(consulta.tabla == "tabla_ejemplo");
        assert!(consulta.get_tabla() == "tabla_ejemplo");

        assert!(consulta.get_type() == &TypeCQL::Insert);

        assert!(consulta.query == "(ID_VUELO, ORIGEN, DESTINO, COMBUSTIBLE) VALUES (5, 'Buenos Aires', 'Rio de Janeiro', 99)");
        assert!(consulta.get_query() == "(ID_VUELO, ORIGEN, DESTINO, COMBUSTIBLE) VALUES (5, 'Buenos Aires', 'Rio de Janeiro', 99)");

        assert!(consulta.consulta_explicita == query);
        assert!(consulta.get_consulta_explicita() == query);

        assert!(consulta.condicion_where == cond);
        assert!(consulta.get_where() == &cond);
    }

    #[test]
    fn test_parsear_select() {
        //Arrange
        let query = "SELECT ID_VUELO, ORIGEN, DESTINO, COMBUSTIBLE FROM tabla_ejemplo WHERE ORIGEN = 'AEROPUERTO JORGE NEWBERY' AND ID_VUELO = 123";
        let cond = CondicionWhere {
            condicion1: "ORIGEN = 'AEROPUERTO JORGE NEWBERY'".to_string(),
            operador_logico: Some("AND".to_string()),
            condicion2: "ID_VUELO = 123".to_string(),
        };

        //Act
        let consulta = parsear_select(query);

        //Assert
        assert!(consulta.tabla == "tabla_ejemplo");
        assert!(consulta.get_tabla() == "tabla_ejemplo");

        assert!(consulta.get_type() == &TypeCQL::Select);

        assert!(consulta.query == "ID_VUELO, ORIGEN, DESTINO, COMBUSTIBLE");
        assert!(consulta.get_query() == "ID_VUELO, ORIGEN, DESTINO, COMBUSTIBLE");

        assert!(consulta.consulta_explicita == query);
        assert!(consulta.get_consulta_explicita() == query);

        assert!(consulta.condicion_where == cond);
        assert!(consulta.get_where() == &cond);
    }

    #[test]
    fn test_parsear_update() {
        //Arrange
        let query = "UPDATE tabla_ejemplo SET COMBUSTIBLE = 50 WHERE ORIGEN = 'AEROPUERTO JORGE NEWBERY' AND ID_VUELO = 123";
        let cond = CondicionWhere {
            condicion1: "ORIGEN = 'AEROPUERTO JORGE NEWBERY'".to_string(),
            operador_logico: Some("AND".to_string()),
            condicion2: "ID_VUELO = 123".to_string(),
        };

        //Act
        let consulta = parsear_update(query);

        //Assert
        assert!(consulta.tabla == "tabla_ejemplo");
        assert!(consulta.get_tabla() == "tabla_ejemplo");

        assert!(consulta.get_type() == &TypeCQL::Update);

        assert!(consulta.query == "COMBUSTIBLE = 50");
        assert!(consulta.get_query() == "COMBUSTIBLE = 50");

        assert!(consulta.consulta_explicita == query);
        assert!(consulta.get_consulta_explicita() == query);

        assert!(consulta.condicion_where == cond);
        assert!(consulta.get_where() == &cond);
    }

    #[test]
    fn test_parsear_delete() {
        //Arrange
        let query = "DELETE FROM tabla_ejemplo WHERE ORIGEN = 'AEROPUERTO JORGE NEWBERY' AND ID_VUELO = 123";
        let cond = CondicionWhere {
            condicion1: "ORIGEN = 'AEROPUERTO JORGE NEWBERY'".to_string(),
            operador_logico: Some("AND".to_string()),
            condicion2: "ID_VUELO = 123".to_string(),
        };

        //Act
        let consulta = parsear_delete(query);

        //Assert
        assert!(consulta.tabla == "tabla_ejemplo");
        assert!(consulta.get_tabla() == "tabla_ejemplo");

        assert!(consulta.get_type() == &TypeCQL::Delete);

        assert!(consulta.query.is_empty());
        assert!(consulta.get_query() == "");

        assert!(consulta.consulta_explicita == query);
        assert!(consulta.get_consulta_explicita() == query);

        assert!(consulta.condicion_where == cond);
        assert!(consulta.get_where() == &cond);
    }

    #[test]
    fn test_consulta_inexistente() {
        //Arrange
        let query =
            "MERGE FROM tabla_ejemplo WHERE ORIGEN = 'AEROPUERTO JORGE NEWBERY' AND ID_VUELO = 123";

        //Act
        let consulta = procesar_consulta(query);

        //Assert
        assert!(consulta.is_err());
    }

    #[test]
    fn test_parsear_create_table() {
        //Arrange
        let query = "CREATE TABLE tabla_ejemplo (id_usuario UUID, nombre TEXT, edad INT, PRIMARY KEY ((id_usuario), nombre)";
        let cond = CondicionWhere {
            condicion1: String::new(),
            operador_logico: None,
            condicion2: String::new(),
        };

        //Act
        let consulta = parsear_create_table(query);

        //Assert
        assert!(consulta.tabla == "tabla_ejemplo");
        assert!(consulta.get_tabla() == "tabla_ejemplo");

        assert!(consulta.get_type() == &TypeCQL::CreateTable);

        assert!(
            consulta.query
                == "(id_usuario UUID, nombre TEXT, edad INT, PRIMARY KEY ((id_usuario), nombre)"
        );
        assert!(
            consulta.get_query()
                == "(id_usuario UUID, nombre TEXT, edad INT, PRIMARY KEY ((id_usuario), nombre)"
        );

        assert!(consulta.consulta_explicita == query);
        assert!(consulta.get_consulta_explicita() == query);

        assert!(consulta.condicion_where == cond);
        assert!(consulta.get_where() == &cond);
    }
}
