use std::fs;
use std::io::{self, BufRead};
use std::path::{Path, PathBuf};

use crate::condiciones::evaluar_condiciones;
use crate::parseo_consulta::construir_ruta_archivo;
use crate::validaciones::es_directorio_valido;

/// Ordena las líneas del archivo CSV según el o los criterios de
/// ordenamiento especificados en la consulta.
pub fn ordenar_lineas(
    mut lineas: Vec<Vec<String>>,
    nombres_columnas: &[&str],
    orden: Option<&str>,
) -> Vec<Vec<String>> {
    if let Some(orden_str) = orden {
        let mut orden_criterios: Vec<(usize, bool)> = Vec::new();

        for criterio in orden_str.split(',') {
            let criterio = criterio.trim();
            let (columna, desc) = if let Some(col) = criterio.strip_suffix(" DESC") {
                (col, true)
            } else if let Some(col) = criterio.strip_suffix(" ASC") {
                (col, false)
            } else {
                (criterio, false)
            };
            if let Some(index) = nombres_columnas
                .iter()
                .position(|nombre| *nombre == columna)
            {
                orden_criterios.push((index, desc));
            }
        }

        lineas.sort_by(|a, b| {
            for &(index, descendente) in &orden_criterios {
                let cmp = a[index].cmp(&b[index]);
                if cmp != std::cmp::Ordering::Equal {
                    return if descendente { cmp.reverse() } else { cmp };
                }
            }
            std::cmp::Ordering::Equal
        });
    }

    lineas
}

/// Selecciona las columnas especificadas de las líneas filtradas.
pub fn seleccionar_columnas(
    lineas: Vec<Vec<String>>,
    nombres_columnas: &[String],
    columnas: &[&str],
) -> Result<Vec<Vec<String>>, &'static str> {
    let indices: Vec<usize> = columnas
        .iter()
        .map(|columna| {
            nombres_columnas
                .iter()
                .position(|nombre| nombre == columna)
                .ok_or("INVALID_COLUMN: Columna no encontrada")
        })
        .collect::<Result<Vec<usize>, &'static str>>()?;

    let resultado: Vec<Vec<String>> = lineas
        .into_iter()
        .map(|linea| {
            indices
                .iter()
                .map(|&i| linea.get(i).map(|s| s.to_string()).unwrap_or_default())
                .collect()
        })
        .collect();

    Ok(resultado)
}

/// Filtra las líneas del archivo CSV por aquellas que cumplan las condiciones
/// recibidas en la consulta. En caso de que no haya condiciones devuelve todas
/// las líneas.
pub fn seleccionar_lineas(
    lineas: impl Iterator<Item = io::Result<String>>,
    nombres_columnas: &Vec<String>,
    condiciones: Option<&str>,
) -> Result<Vec<Vec<String>>, &'static str> {
    let mut resultado: Vec<Vec<String>> = Vec::new();

    for linea in lineas {
        let linea = linea.map_err(|_| "ERROR: Error leyendo la línea del archivo")?;
        let cumple_condiciones = if let Some(cond) = condiciones {
            evaluar_condiciones(&linea, cond, nombres_columnas)?
        } else {
            true
        };

        if cumple_condiciones {
            let valores: Vec<String> = linea.split(',').map(|s| s.to_string()).collect();
            resultado.push(valores);
        }
    }

    Ok(resultado)
}

/// Ejecuta la operación SELECT en el archivo CSV especificado. Si ocurre
/// un error durante el procesamiento se lanza un error.
pub fn procesar_select(
    ruta_archivo: &PathBuf,
    columnas: &[&str],
    condiciones: Option<&str>,
    orden: Option<&str>,
) -> Result<(), String> {
    let path = Path::new(ruta_archivo);
    let archivo = fs::File::open(path).map_err(|_| "ERROR: No se pudo abrir el archivo")?;
    let buffer = io::BufReader::new(archivo);

    let mut lineas = buffer.lines();
    let nombres_columnas = if let Some(encabezado) = lineas.next() {
        let encabezado =
            encabezado.map_err(|_| "ERROR: No se pudo leer el encabezado del archivo")?;
        encabezado
            .split(',')
            .map(|s| s.to_string())
            .collect::<Vec<String>>()
    } else {
        return Err("ERROR: El archivo está vacío".to_string());
    };

    // Filtrar las líneas
    let lineas_filtradas =
        seleccionar_lineas(lineas, &nombres_columnas, condiciones).map_err(|e| e.to_string())?;

    // Ordenar las líneas
    let nombres_columnas_ref: Vec<&str> = nombres_columnas.iter().map(|s| s.as_str()).collect();

    let lineas_ordenadas = ordenar_lineas(lineas_filtradas, &nombres_columnas_ref, orden);

    // Seleccionar las columnas
    let lineas_seleccionadas: Vec<Vec<String>> = if columnas == ["*"] {
        seleccionar_columnas(lineas_ordenadas, &nombres_columnas, &nombres_columnas_ref)
            .map_err(|e| e.to_string())?
    } else {
        seleccionar_columnas(lineas_ordenadas, &nombres_columnas, columnas)
            .map_err(|e| e.to_string())?
    };

    // Imprimir las líneas
    if columnas == ["*"] {
        println!("{}", nombres_columnas_ref.join(","));
    } else {
        println!("{}", columnas.join(","));
    }
    for linea in lineas_seleccionadas {
        println!(
            "{}",
            linea
                .iter()
                .map(|s| s.as_str())
                .collect::<Vec<_>>()
                .join(",")
        );
    }

    Ok(())
}

/// Extrae la o las formas de ordenamiento de las filas de la consulta.
/// En caso de que no haya ordenamientos devuelve None.
pub fn extraer_orden(consulta: &str, order_by_index: Option<usize>) -> Option<&str> {
    order_by_index.map(|ob_idx| {
        let slice = &consulta[ob_idx + 9..];
        slice.trim()
    })
}

/// Extrae las condiciones de búsqueda de la consulta y las devuelve.
/// En caso de que no haya condiciones devuelve None.
pub fn extraer_condiciones(
    consulta: &str,
    where_index: Option<usize>,
    order_by_index: Option<usize>,
) -> Option<&str> {
    where_index.map(|w_idx| {
        let start = w_idx + 6;
        let end = order_by_index.unwrap_or(consulta.len());
        let slice = &consulta[start..end];
        slice.trim_end()
    })
}

/// Extrae el nombre del archivo (la tabla) de la consulta y lo devuelve.
pub fn extraer_tabla(
    consulta: &str,
    from_index: usize,
    where_index: Option<usize>,
    order_by_index: Option<usize>,
) -> &str {
    match (where_index, order_by_index) {
        (Some(w_idx), _) => consulta[from_index + 5..w_idx].trim(),
        (_, Some(ob_idx)) => consulta[from_index + 5..ob_idx].trim(),
        _ => consulta[from_index + 5..].trim(),
    }
}

/// Extrae el conjunto de columnas de la consulta y las devuelve
/// en una forma conveniente.
pub fn extraer_columnas(consulta: &str, from_index: usize) -> Vec<&str> {
    let columnas_str = &consulta[7..from_index].trim();
    columnas_str.split(',').map(|s| s.trim()).collect()
}

/// Busca el índice de una palabra clave en la consulta, si no lo encuentra
/// devuelve error.
pub fn encontrar_indice_clave(consulta: &str, clave: &str) -> Result<usize, String> {
    consulta.to_lowercase().find(clave).ok_or(format!(
        "INVALID_SYNTAX: Falta la palabra {} en la consulta",
        clave.to_uppercase()
    ))
}

type PartesSelect<'a> = (Vec<&'a str>, &'a str, Option<&'a str>, Option<&'a str>);
/// Función para parsear la consulta SELECT extrayendo las partes más
/// relevantes y devolviéndolas para el posterior procesamiento.
pub fn parsear_consulta_select(consulta: &str) -> Result<PartesSelect, String> {
    let from_index = encontrar_indice_clave(consulta, "from")?;
    let where_index = encontrar_indice_clave(consulta, "where").ok();
    let order_by_index = encontrar_indice_clave(consulta, "order by").ok();

    let columnas = extraer_columnas(consulta, from_index);
    let tabla = extraer_tabla(consulta, from_index, where_index, order_by_index);
    let condiciones = extraer_condiciones(consulta, where_index, order_by_index);
    let orden = extraer_orden(consulta, order_by_index);

    Ok((columnas, tabla, condiciones, orden))
}

/// Valida que la estructura de la consulta SELECT sea la correcta y lanza error
/// en caso de que no lo sea. Si lo es se ejecuta la operación llamando a la
/// función de procesamiento.
pub fn validar_select(consulta: &str, ruta_directorio: &str) {
    let (columnas, tabla, condiciones, orden) = match parsear_consulta_select(consulta) {
        Ok(resultado) => resultado,
        Err(error) => {
            println!("{}", error);
            return;
        }
    };

    if !es_directorio_valido(ruta_directorio, tabla) {
        println!("INVALID_TABLE: El directorio no contiene el archivo CSV especificado.");
        return;
    }

    let ruta_archivo = construir_ruta_archivo(ruta_directorio, tabla);

    match procesar_select(&ruta_archivo, &columnas, condiciones, orden) {
        Ok(_) => (),
        Err(error) => println!("{}", error),
    }
}

#[test]
fn test_consulta_select_sin_palabras_necesarias() {
    let consulta = "SELECT";
    let clave = "FROM";
    let resultado = encontrar_indice_clave(consulta, clave);
    assert!(resultado.is_err());
    assert_eq!(
        resultado.unwrap_err(),
        "INVALID_SYNTAX: Falta la palabra FROM en la consulta"
    );
}

#[test]
fn test_obtener_indice_clave() {
    let consulta = "SELECT * FROM tabla WHERE condicion ORDER BY columna";
    let clave = "from";
    let resultado = encontrar_indice_clave(consulta, clave);
    assert!(resultado.is_ok());
    assert_eq!(resultado.unwrap(), 9);
}

#[test]
fn test_extraer_columnas_de_consulta() {
    let consulta =
        "SELECT columna1, columna2, columna3 FROM tabla WHERE condicion ORDER BY columna";
    let from_index = 36;
    let resultado = extraer_columnas(consulta, from_index);
    assert_eq!(resultado, ["columna1", "columna2", "columna3"]);
}

#[test]
fn test_extraer_tabla_de_consulta() {
    let consulta =
        "SELECT columna1, columna2, columna3 FROM tabla WHERE condicion ORDER BY columna";
    let from_index = 36;
    let where_index = 47;
    let order_by_index = 63;
    let resultado = extraer_tabla(
        consulta,
        from_index,
        Some(where_index),
        Some(order_by_index),
    );
    assert_eq!(resultado, "tabla");
}

#[test]
fn test_extraer_condiciones_de_consulta() {
    let consulta =
        "SELECT columna1, columna2, columna3 FROM tabla WHERE condicion ORDER BY columna";
    let where_index = 47;
    let order_by_index = 63;
    let resultado = extraer_condiciones(consulta, Some(where_index), Some(order_by_index));
    assert_eq!(resultado, Some("condicion"));
}

#[test]
fn test_extraer_orden_de_consulta() {
    let consulta =
        "SELECT columna1, columna2, columna3 FROM tabla WHERE condicion ORDER BY columna DESC";
    let order_by_index = 63;
    let resultado = extraer_orden(consulta, Some(order_by_index));
    assert_eq!(resultado, Some("columna DESC"));
}

#[test]
fn test_directorio_no_valido_select() {
    let consulta = "SELECT * FROM clientes";
    let ruta_directorio = "directorio_test";
    match fs::create_dir(ruta_directorio) {
        Ok(_) => {
            assert_eq!(validar_select(consulta, ruta_directorio), ());

            match fs::remove_dir(ruta_directorio) {
                Ok(_) => {}
                Err(e) => {
                    println!("ERROR: No se pudo eliminar el directorio: {}", e);
                }
            }
        }
        Err(e) => {
            println!("ERROR: No se pudo crear el directorio: {}", e);
        }
    }
}
