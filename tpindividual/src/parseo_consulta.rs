use std::path::PathBuf;

use crate::logica_delete::validar_delete;
use crate::logica_insert::validar_insert;
use crate::logica_select::validar_select;
use crate::logica_update::validar_update;

/// Obtiene las partes relevantes de la consulta y las devuelve
/// en una forma conveniente para el posterior procesamiento.
pub fn split_consulta(consulta: &str) -> Vec<&str> {
    let mut result = Vec::new();
    let mut start = 0;
    let mut in_quotes = false;

    for (i, c) in consulta.char_indices() {
        if c == '\'' {
            in_quotes = !in_quotes;
        } else if c.is_whitespace() && !in_quotes {
            if start != i {
                result.push(&consulta[start..i]);
            }
            start = i + 1;
        }
    }

    if start < consulta.len() {
        result.push(&consulta[start..]);
    }

    result
}

/// Parsea la consulta recibida identificando la operación a realizar y
/// llamando a la función correspondiente en cada caso.
pub fn procesar_consulta(consulta_sql: &str, ruta_directorio: &str) {
    let partes = split_consulta(consulta_sql);

    if partes.is_empty() {
        println!("INVALID_SYNTAX: La consulta SQL está vacía.");
        return;
    }

    if partes.len() < 3 {
        println!("INVALID_SYNTAX: La consulta SQL no tiene la estructura correcta.");
        return;
    }

    let operacion = partes[0].to_uppercase();

    match operacion.as_str() {
        "SELECT" => validar_select(consulta_sql, ruta_directorio),
        "UPDATE" => validar_update(&partes, ruta_directorio),
        "INSERT" => validar_insert(consulta_sql, ruta_directorio),
        "DELETE" => validar_delete(&partes, ruta_directorio),
        _ => println!("INVALID_SYNTAX: Consulta SQL no válida."),
    }
}

/// Construye la ruta completa del archivo CSV a leer a partir de la ruta del
/// directorio que lo contiene y su nombre extraído de la consulta.
pub fn construir_ruta_archivo(ruta_directorio: &str, nombre_archivo: &str) -> PathBuf {
    let mut ruta_archivo = PathBuf::from(ruta_directorio);
    ruta_archivo.push(nombre_archivo);
    ruta_archivo.set_extension("csv");
    ruta_archivo
}

#[test]
fn test_split_consulta_sin_saltos_de_linea() {
    let consulta = "DELETE FROM person WHERE lastname = 'Burton'";
    let resultado = split_consulta(consulta);
    assert_eq!(
        resultado,
        ["DELETE", "FROM", "person", "WHERE", "lastname", "=", "'Burton'"]
    );
}

#[test]
fn test_split_consulta_con_saltos_de_linea() {
    let consulta = "UPDATE clientes\nSET email = 'mrodriguez@hotmail.com'\n WHERE id = 4";
    let resultado = split_consulta(consulta);
    assert_eq!(
        resultado,
        [
            "UPDATE",
            "clientes",
            "SET",
            "email",
            "=",
            "'mrodriguez@hotmail.com'",
            "WHERE",
            "id",
            "=",
            "4"
        ]
    );
}

#[test]
fn test_construir_ruta_archivo() {
    let ruta_directorio = "/home/usuario/archivos";
    let nombre_archivo = "personas";
    let resultado = construir_ruta_archivo(ruta_directorio, nombre_archivo);
    assert_eq!(
        resultado,
        PathBuf::from("/home/usuario/archivos/personas.csv")
    );
}

#[test]
fn test_procesar_consulta_vacia() {
    let consulta = "";
    let ruta_directorio = "/home/usuario/archivos";
    assert_eq!(procesar_consulta(consulta, ruta_directorio), ());
}

#[test]
fn test_procesar_consulta_con_estructura_incorrecta() {
    let consulta = "DELETE";
    let ruta_directorio = "/home/usuario/archivos";
    assert_eq!(procesar_consulta(consulta, ruta_directorio), ());
}

#[test]
fn test_procesar_consulta_con_operacion_invalida() {
    let consulta = "DROP TABLE personas";
    let ruta_directorio = "/home/usuario/archivos";
    assert_eq!(procesar_consulta(consulta, ruta_directorio), ());
}
