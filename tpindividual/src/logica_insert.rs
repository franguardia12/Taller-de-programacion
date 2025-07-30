use std::fs::{self, File};
use std::io::{self, BufRead, BufWriter, Write};
use std::path::{Path, PathBuf};

use crate::parseo_consulta::construir_ruta_archivo;
use crate::validaciones::es_directorio_valido;

/// Crea una nueva fila reorganizando los valores de acuerdo
/// con el orden de las columnas en el archivo CSV. Si alguna
/// columna no fue especificada en la consulta, inserta un valor
/// vacío en su lugar.
pub fn mapear_valores<'a>(
    columnas_insertadas: &'a [String],
    nombres_columnas: &'a Vec<String>,
    valores_insertados: &'a [String],
) -> Vec<&'a str> {
    let mut nueva_fila = Vec::new();

    for nombre_columna in nombres_columnas {
        if let Some(pos) = columnas_insertadas.iter().position(|c| c == nombre_columna) {
            nueva_fila.push(&valores_insertados[pos][..]);
        } else {
            nueva_fila.push("");
        }
    }

    nueva_fila
}

/// Inserta las nuevas líneas en el archivo CSV. Para cada fila
/// a insertar se reorganizan los valores de acuerdo con el orden
/// de las columnas en el archivo CSV.
pub fn insertar_filas<W: Write>(
    escritor: &mut W,
    nombres_columnas: &Vec<String>,
    columnas_insertadas: &[String],
    filas_a_insertar: &[String],
) -> Result<(), &'static str> {
    for fila in filas_a_insertar {
        let valores_insertados = fila
            .trim()
            .trim_start_matches('(')
            .trim_end_matches(')')
            .split(',')
            .map(|s| s.trim().to_string())
            .collect::<Vec<String>>();

        let nueva_fila = mapear_valores(columnas_insertadas, nombres_columnas, &valores_insertados);

        escritor
            .write_all(nueva_fila.join(",").as_bytes())
            .map_err(|_| "ERROR: Error escribiendo en el archivo")?;
        escritor
            .write_all(b"\n")
            .map_err(|_| "ERROR: Error escribiendo en el archivo")?;
    }

    Ok(())
}

/// Función que escribe las líneas originales del archivo CSV
/// en el nuevo archivo.
pub fn escribir_lineas_originales<W: Write>(
    escritor: &mut W,
    lineas: io::Lines<io::BufReader<File>>,
) -> Result<(), &'static str> {
    for linea in lineas {
        let linea = linea.map_err(|_| "ERROR: Error leyendo la línea del archivo")?;
        escritor
            .write_all(linea.as_bytes())
            .map_err(|_| "ERROR: Error escribiendo en el archivo")?;
        escritor
            .write_all(b"\n")
            .map_err(|_| "ERROR: Error escribiendo en el archivo")?;
    }
    Ok(())
}

/// Valida si las columnas recibidas en la consulta
/// pertenecen a la tabla del archivo CSV. En caso de que no
/// se devuelve un error.
pub fn validar_columnas(
    columnas_insertadas: &Vec<String>,
    nombres_columnas: &[String],
) -> Result<(), &'static str> {
    for columna in columnas_insertadas {
        if !nombres_columnas.contains(columna) {
            return Err(
                "INVALID_COLUMN: Una de las columnas especificadas no pertenece a la tabla.",
            );
        }
    }
    Ok(())
}

/// Función para insertar las nuevas líneas en el archivo CSV, las cuales
/// se reciben como parámetro.
pub fn insertar_lineas<W: Write>(
    escritor: &mut W,
    lineas: io::Lines<io::BufReader<File>>,
    nombres_columnas: &Vec<String>,
    partes: &[String],
) -> Result<(), &'static str> {
    let columnas_insertadas = partes[0]
        .split(',')
        .map(|s| s.trim().to_string())
        .collect::<Vec<String>>();

    let filas_a_insertar = &partes[2..];

    validar_columnas(&columnas_insertadas, nombres_columnas)?;

    escribir_lineas_originales(escritor, lineas)?;

    insertar_filas(
        escritor,
        nombres_columnas,
        &columnas_insertadas,
        filas_a_insertar,
    )?;

    Ok(())
}

/// Ejecuta la operación INSERT en el archivo CSV especificado. Si ocurre
/// un error durante el procesamiento se lanza un error.
pub fn procesar_insert(ruta: &PathBuf, partes: &[String]) -> Result<(), &'static str> {
    let path = Path::new(ruta);
    let archivo: File = File::open(path).map_err(|_| "ERROR: No se pudo abrir el archivo")?;
    let buffer = io::BufReader::new(archivo);

    let archivo_temporal = File::create("archivo_temporal.csv")
        .map_err(|_| "ERROR: No se pudo crear el archivo temporal")?;
    let mut escritor = BufWriter::new(archivo_temporal);

    let mut lineas = buffer.lines();
    let nombres_columnas = if let Some(encabezado) = lineas.next() {
        let encabezado =
            encabezado.map_err(|_| "ERROR: No se pudo leer el encabezado del archivo")?;
        escritor
            .write_all(encabezado.as_bytes())
            .map_err(|_| "ERROR: No se pudo escribir en el archivo")?;
        escritor
            .write_all(b"\n")
            .map_err(|_| "ERROR: No se pudo escribir en el archivo")?;
        encabezado
            .split(',')
            .map(|s| s.to_string())
            .collect::<Vec<String>>()
    } else {
        return Ok(());
    };

    insertar_lineas(&mut escritor, lineas, &nombres_columnas, partes)?;

    escritor
        .flush()
        .map_err(|_| "ERROR: No se pudo escribir en el archivo")?;
    fs::rename("archivo_temporal.csv", ruta)
        .map_err(|_| "ERROR: No se pudo renombrar el archivo")?;

    Ok(())
}

/// Extrae el nombre del archivo (la tabla) y el conjunto de columnas
/// de la consulta. En caso que ocurra un error lo devuelve.
pub fn extraer_tabla_y_columnas(
    consulta: &str,
    into_index: usize,
    values_index: usize,
) -> Result<(String, String), String> {
    let tabla_y_columnas = &consulta[into_index + 4..values_index].trim();
    let mut partes = tabla_y_columnas.splitn(2, '(');
    let nombre_tabla = partes
        .next()
        .ok_or_else(|| "ERROR: No se pudo extraer el nombre de la tabla.".to_string())?
        .trim()
        .to_string();
    let columnas_str = partes
        .next()
        .ok_or_else(|| "ERROR: No se pudo extraer la lista de columnas.".to_string())?
        .trim()
        .trim_end_matches(')')
        .to_string();

    Ok((nombre_tabla, columnas_str))
}

/// Extrae cada una de las nuevas líneas de valores a insertar
/// en el archivo CSV y las devuelve en una estructura apropiada. En caso
/// que no haya ningún valor devuelve error.
pub fn extraer_valores(valores_str: &str) -> Result<Vec<String>, String> {
    let mut valores = Vec::new();
    let mut temp_valores = String::new();
    let mut entre_comillas = false;
    let mut dentro_parentesis = false;

    for c in valores_str.chars() {
        match c {
            '\'' => {
                entre_comillas = !entre_comillas;
            }
            '(' => {
                if !dentro_parentesis {
                    dentro_parentesis = true;
                } else {
                    temp_valores.push(c);
                }
            }
            ')' => {
                if dentro_parentesis && !entre_comillas {
                    dentro_parentesis = false;
                    valores.push(temp_valores.trim().to_string());
                    temp_valores.clear();
                } else {
                    temp_valores.push(c);
                }
            }
            _ => temp_valores.push(c),
        }
    }

    if valores.is_empty() {
        return Err("INVALID_SYNTAX: No se encontraron valores para insertar.".to_string());
    }

    Ok(valores)
}

/// Busca si una cierta palabra forma parte de la consulta, en
/// caso de que no devuelve un error.
pub fn encontrar_palabra(consulta: &str, palabra: &str) -> Result<usize, String> {
    consulta.find(palabra).ok_or_else(|| {
        format!(
            "INVALID_SYNTAX: La consulta INSERT no contiene la palabra '{}'.",
            palabra
        )
    })
}

/// Función para parsear la consulta INSERT extrayendo las partes más
/// relevantes y devolviéndolas en una estructura adecuada para el
/// posterior procesamiento.
pub fn parsear_consulta_insert(consulta: &str) -> Result<Vec<String>, String> {
    let lower_consulta = consulta.to_lowercase();

    let into_index = encontrar_palabra(&lower_consulta, "into")?;
    let values_index = encontrar_palabra(&lower_consulta, "values")?;

    let (nombre_tabla, columnas) = extraer_tabla_y_columnas(consulta, into_index, values_index)?;
    let valores = extraer_valores(&consulta[values_index + 6..])?;

    let mut resultado = Vec::new();
    resultado.push(columnas);
    resultado.push(nombre_tabla);
    resultado.extend(valores);

    Ok(resultado)
}

/// Valida que la estructura de la consulta INSERT sea la correcta y lanza error
/// en caso de que no lo sea. Si lo es se ejecuta la operación llamando a la
/// función de procesamiento.
pub fn validar_insert(consulta: &str, ruta_directorio: &str) {
    let partes = match parsear_consulta_insert(consulta) {
        Ok(partes) => partes,
        Err(error) => {
            println!("{}", error);
            return;
        }
    };

    if partes.len() < 3 {
        println!("INVALID_SYNTAX: La consulta INSERT no tiene la estructura correcta.");
        return;
    }

    if !es_directorio_valido(ruta_directorio, &partes[1]) {
        println!("INVALID_TABLE: El directorio no contiene el archivo CSV especificado.");
        return;
    }

    let ruta_archivo = construir_ruta_archivo(ruta_directorio, &partes[1]);

    match procesar_insert(&ruta_archivo, &partes) {
        Ok(_) => (),
        Err(error) => println!("{}", error),
    }
}

#[test]
fn test_consulta_insert_sin_palabras_necesarias() {
    let consulta = "INSERT ordenes (id, id_cliente, producto, cantidad) (111, 6, 'Laptop', 3)";

    let resultado1 = encontrar_palabra(consulta, "into");
    assert!(resultado1.is_err());
    assert_eq!(
        resultado1.unwrap_err(),
        "INVALID_SYNTAX: La consulta INSERT no contiene la palabra 'into'."
    );

    let resultado2 = encontrar_palabra(consulta, "values");
    assert!(resultado2.is_err());
    assert_eq!(
        resultado2.unwrap_err(),
        "INVALID_SYNTAX: La consulta INSERT no contiene la palabra 'values'."
    );
}

#[test]
fn test_encontrar_palabras_de_consulta() {
    let consulta =
        "INSERT into ordenes (id, id_cliente, producto, cantidad) values (111, 6, 'Laptop', 3)";
    let into_index = encontrar_palabra(consulta, "into");
    assert!(into_index.is_ok());
    assert_eq!(into_index.unwrap(), 7);

    let values_index = encontrar_palabra(consulta, "values");
    assert!(values_index.is_ok());
    assert_eq!(values_index.unwrap(), 57);
}

#[test]
fn test_extraer_tabla_y_columnas() {
    let consulta =
        "INSERT INTO ordenes (id, id_cliente, producto, cantidad) VALUES (111, 6, 'Laptop', 3)";
    let into_index = 7;
    let values_index = 57;

    let resultado = extraer_tabla_y_columnas(consulta, into_index, values_index);
    assert!(resultado.is_ok());
    assert_eq!(
        resultado.unwrap(),
        (
            "ordenes".to_string(),
            "id, id_cliente, producto, cantidad".to_string()
        )
    );
}

#[test]
fn test_extraer_valores() {
    let valores = "(111, 6, 'Laptop', 3)";
    let resultado = extraer_valores(valores);
    assert!(resultado.is_ok());
    assert_eq!(resultado.unwrap(), vec!["111, 6, Laptop, 3"]);
}

#[test]
fn test_extraer_valores_sin_valores() {
    let valores = "";
    let resultado = extraer_valores(valores);
    assert!(resultado.is_err());
    assert_eq!(
        resultado.unwrap_err(),
        "INVALID_SYNTAX: No se encontraron valores para insertar."
    );
}

#[test]
fn test_columna_invalida() {
    let columnas_insertadas = vec!["id".to_string(), "nombre".to_string()];
    let nombres_columnas = vec![
        "id".to_string(),
        "apellido".to_string(),
        "email".to_string(),
    ];

    let resultado = validar_columnas(&columnas_insertadas, &nombres_columnas);
    assert!(resultado.is_err());
    assert_eq!(
        resultado.unwrap_err(),
        "INVALID_COLUMN: Una de las columnas especificadas no pertenece a la tabla."
    );
}

#[test]
fn test_todas_las_columnas_validas() {
    let columnas_insertadas = vec!["id".to_string(), "apellido".to_string()];
    let nombres_columnas = vec![
        "id".to_string(),
        "apellido".to_string(),
        "email".to_string(),
    ];

    let resultado = validar_columnas(&columnas_insertadas, &nombres_columnas);
    assert!(resultado.is_ok());
    assert_eq!(resultado.unwrap(), ());
}

#[test]
fn test_mapeo_de_valores_sin_todas_las_columnas() {
    let columnas_insertadas = vec!["id".to_string(), "apellido".to_string()];
    let nombres_columnas = vec![
        "id".to_string(),
        "apellido".to_string(),
        "email".to_string(),
    ];
    let valores_insertados = vec!["1".to_string(), "'Rodriguez'".to_string()];

    let resultado = mapear_valores(&columnas_insertadas, &nombres_columnas, &valores_insertados);
    assert_eq!(resultado, ["1", "'Rodriguez'", ""]);
}

#[test]
fn test_mapeo_de_valores_con_todas_las_columnas() {
    let columnas_insertadas = vec![
        "id".to_string(),
        "apellido".to_string(),
        "email".to_string(),
    ];
    let nombres_columnas = vec![
        "id".to_string(),
        "apellido".to_string(),
        "email".to_string(),
    ];
    let valores_insertados = vec![
        "1".to_string(),
        "'Rodriguez'".to_string(),
        "'mrodriguez@hotmail.com'".to_string(),
    ];

    let resultado = mapear_valores(&columnas_insertadas, &nombres_columnas, &valores_insertados);
    assert_eq!(resultado, ["1", "'Rodriguez'", "'mrodriguez@hotmail.com'"]);
}

#[test]
fn test_mapeo_de_valores_con_distinto_orden_de_columnas() {
    let columnas_insertadas = vec![
        "apellido".to_string(),
        "id".to_string(),
        "email".to_string(),
    ];
    let nombres_columnas = vec![
        "id".to_string(),
        "apellido".to_string(),
        "email".to_string(),
    ];
    let valores_insertados = vec![
        "'Rodriguez'".to_string(),
        "1".to_string(),
        "'mrodriguez@hotmail.com'".to_string(),
    ];

    let resultado = mapear_valores(&columnas_insertadas, &nombres_columnas, &valores_insertados);
    assert_eq!(resultado, ["1", "'Rodriguez'", "'mrodriguez@hotmail.com'"]);
}
