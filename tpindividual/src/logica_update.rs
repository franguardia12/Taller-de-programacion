use std::fs::{self, File};
use std::io::{self, BufRead, BufWriter, Write};
use std::path::{Path, PathBuf};

use crate::condiciones::evaluar_condiciones;
use crate::parseo_consulta::construir_ruta_archivo;
use crate::validaciones::es_directorio_valido;

/// Función que aplica los cambios especificados a una línea del archivo CSV.
pub fn aplicar_cambios_a_linea(linea: &str, cambios: &str, nombres_columnas: &[String]) -> String {
    let mut campos: Vec<&str> = linea.split(',').collect();

    let cambios: Vec<(&str, &str)> = cambios
        .split(',')
        .map(|c| {
            let partes: Vec<&str> = c.split('=').map(str::trim).collect();
            (partes[0], partes[1])
        })
        .collect();

    for (columna, valor) in cambios {
        if let Some(index) = nombres_columnas.iter().position(|col| col == columna) {
            campos[index] = valor.trim_matches('\'');
        }
    }

    campos.join(",")
}

/// Función para actualizar las líneas del archivo CSV que cumplan con la condición
/// especificada o todas si no se especifica una condición. Los cambios a aplicar
/// se reciben como parámetro.
pub fn actualizar_lineas<W: Write>(
    escritor: &mut W,
    lineas: io::Lines<io::BufReader<File>>,
    nombres_columnas: &Vec<String>,
    condicion: Option<&str>,
    cambios: &str,
) -> Result<(), &'static str> {
    for linea in lineas {
        let linea = linea.map_err(|_| "ERROR: Error leyendo la línea del archivo")?;

        let actualizar = if let Some(cond) = condicion {
            match evaluar_condiciones(&linea, cond, nombres_columnas) {
                Ok(resultado) => resultado,
                Err(error) => {
                    return Err(error);
                }
            }
        } else {
            true
        };

        let linea_actualizada = if actualizar {
            aplicar_cambios_a_linea(&linea, cambios, nombres_columnas)
        } else {
            linea
        };

        escritor
            .write_all(linea_actualizada.as_bytes())
            .map_err(|_| "ERROR: Error escribiendo la línea")?;
        escritor
            .write_all(b"\n")
            .map_err(|_| "ERROR: Error escribiendo el salto de línea")?;
    }

    Ok(())
}

/// Ejecuta la operación UPDATE en el archivo CSV especificado. Si ocurre
/// un error durante el procesamiento se lanza un error.
pub fn procesar_update(
    ruta: &PathBuf,
    condicion: Option<&str>,
    cambios: &str,
) -> Result<(), &'static str> {
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

    actualizar_lineas(&mut escritor, lineas, &nombres_columnas, condicion, cambios)?;

    escritor
        .flush()
        .map_err(|_| "ERROR: No se pudo escribir en el archivo")?;
    fs::rename("archivo_temporal.csv", ruta)
        .map_err(|_| "ERROR: No se pudo renombrar el archivo")?;

    Ok(())
}

/// Extrae las columnas a actualizar y la posición de la palabra "WHERE" en la consulta.
pub fn obtener_columnas_a_actualizar(words: &[&str]) -> (String, usize) {
    let set_index = words.iter().position(|word| *word == "SET").unwrap_or(0);
    let where_index = words.iter().position(|word| *word == "WHERE").unwrap_or(0);

    let palabras_intermedias: Vec<&str> = words[(set_index + 1)..where_index].to_vec();
    (palabras_intermedias.join(" "), where_index)
}

/// Valida que la estructura de la consulta UPDATE sea la correcta y lanza error
/// en caso de que no lo sea. Si lo es se ejecuta la operación llamando a la
/// función de procesamiento.
pub fn validar_update(partes: &[&str], ruta_directorio: &str) {
    if partes.len() < 6 || partes[2] != "SET" {
        println!("INVALID_SYNTAX: La consulta UPDATE no tiene la estructura correcta.");
        return;
    }

    if !es_directorio_valido(ruta_directorio, partes[1]) {
        println!("INVALID_TABLE: El directorio no contiene el archivo CSV especificado.");
        return;
    }

    let ruta_archivo = construir_ruta_archivo(ruta_directorio, partes[1]);

    let set_substring = partes[3..].join(" ");

    if !set_substring.contains("WHERE") {
        // Procesar caso UPDATE sin WHERE (afecta a todas las filas)
        match procesar_update(&ruta_archivo, None, &set_substring) {
            Ok(()) => (),
            Err(error) => println!("{}", error),
        }
    } else {
        let partes_substring: Vec<&str> = set_substring.split_whitespace().collect();

        if let Some(where_index) = partes_substring.iter().position(|&s| s == "WHERE") {
            if where_index + 1 < partes_substring.len() {
                // Procesar caso UPDATE con WHERE (afecta a filas específicas)
                let cambios = partes_substring[..where_index].join(" ");
                let condicion = partes_substring[(where_index + 1)..].join(" ");
                match procesar_update(&ruta_archivo, Some(&condicion), &cambios) {
                    Ok(()) => (),
                    Err(error) => println!("{}", error),
                }
            } else {
                println!("INVALID_SYNTAX: La consulta UPDATE no tiene la estructura correcta.");
            }
        } else {
            println!("INVALID_SYNTAX: La consulta UPDATE no tiene la estructura correcta.");
        }
    }
}

#[test]
fn test_consulta_update_corta_con_estructura_incorrecta() {
    let partes = ["UPDATE", "tabla", "SET"];
    let ruta_directorio = "directorio_test";
    assert_eq!(validar_update(&partes, ruta_directorio), ());
}

#[test]
fn test_directorio_no_valido_update() {
    let partes = [
        "UPDATE",
        "clientes",
        "SET",
        "email",
        "=",
        "'mrodriguez@hotmail.com'",
    ];
    let ruta_directorio = "directorio_test";
    match fs::create_dir(ruta_directorio) {
        Ok(_) => {
            assert_eq!(validar_update(&partes, ruta_directorio), ());

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

#[test]
fn test_consulta_update_larga_con_estructura_incorrecta() {
    let partes = [
        "UPDATE",
        "archivo_test",
        "SET",
        "email",
        "=",
        "'mrodriguez@hotmail.com'",
        "WHERE",
    ];
    let ruta_directorio = "directorio_prueba";
    if let Err(e) = fs::create_dir(ruta_directorio) {
        println!("ERROR: No se pudo crear el directorio: {}", e);
        return;
    }

    let ruta_archivo_csv = Path::new(ruta_directorio).join("archivo_test.csv");
    if let Err(e) = fs::File::create(&ruta_archivo_csv) {
        println!("ERROR: No se pudo crear el archivo: {}", e);
        let _ = fs::remove_dir_all(ruta_directorio);
        return;
    }

    assert_eq!(validar_update(&partes, ruta_directorio), ());

    if let Err(e) = fs::remove_dir_all(ruta_directorio) {
        println!("Error al eliminar el directorio: {}", e);
    }
}

#[test]
fn test_obtener_columnas_de_consulta() {
    let partes = [
        "UPDATE",
        "clientes",
        "SET",
        "email",
        "=",
        "'mrodriguez@hotmail.com'",
        "WHERE",
        "id",
        "=",
        "4",
    ];
    let (columnas, where_index) = obtener_columnas_a_actualizar(&partes);
    assert_eq!(columnas, "email = 'mrodriguez@hotmail.com'");
    assert_eq!(where_index, 6);
}

#[test]
fn test_aplicar_cambios_a_linea() {
    let linea = "1,John,Doe,30";
    let cambios = "age = 31,lastname = 'Smith'";
    let nombres_columnas = vec![
        "id".to_string(),
        "firstname".to_string(),
        "lastname".to_string(),
        "age".to_string(),
    ];
    let resultado = aplicar_cambios_a_linea(linea, cambios, &nombres_columnas);
    assert_eq!(resultado, "1,John,Smith,31");
}
