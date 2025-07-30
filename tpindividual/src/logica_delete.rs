use std::fs::{self, File};
use std::io::{self, BufRead, BufWriter, Write};
use std::path::{Path, PathBuf};

use crate::condiciones::evaluar_condiciones;
use crate::parseo_consulta::construir_ruta_archivo;
use crate::validaciones::es_directorio_valido;

/// Función para eliminar las líneas del archivo que cumplan con la condición
/// especificada o todas si no se especifica una condición.
pub fn eliminar_lineas<W: Write>(
    escritor: &mut W,
    lineas: io::Lines<io::BufReader<File>>,
    nombres_columnas: &Vec<String>,
    condicion: Option<&str>,
) -> Result<(), &'static str> {
    for linea in lineas {
        let linea = linea.map_err(|_| "ERROR: Error leyendo la línea del archivo")?;
        let conservar = match condicion {
            Some(cond) => match evaluar_condiciones(&linea, cond, nombres_columnas) {
                Ok(resultado) => !resultado,
                Err(error) => return Err(error),
            },
            None => false,
        };

        if conservar {
            escritor
                .write_all(linea.as_bytes())
                .map_err(|_| "ERROR: Error escribiendo la línea")?;
            escritor
                .write_all(b"\n")
                .map_err(|_| "ERROR: Error escribiendo el salto de línea")?;
        }
    }
    Ok(())
}
/// Ejecuta la operación DELETE en el archivo CSV especificado. Si ocurre
/// un error durante el procesamiento se lanza un error.
pub fn procesar_delete(ruta: &PathBuf, condicion: Option<&str>) -> Result<(), &'static str> {
    let path = Path::new(ruta);
    let archivo = File::open(path).map_err(|_| "ERROR: No se pudo abrir el archivo")?;
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

    eliminar_lineas(&mut escritor, lineas, &nombres_columnas, condicion)?;

    escritor
        .flush()
        .map_err(|_| "ERROR: No se pudo escribir en el archivo")?;
    fs::rename("archivo_temporal.csv", ruta)
        .map_err(|_| "ERROR: No se pudo renombrar el archivo")?;

    Ok(())
}

/// Valida que la estructura de la consulta DELETE sea la correcta y lanza error
/// en caso de que no lo sea. Si lo es se ejecuta la operación llamando a la
/// función de procesamiento.
pub fn validar_delete(partes: &[&str], ruta_directorio: &str) {
    if partes.len() < 3 || partes[1] != "FROM" {
        println!("INVALID_SYNTAX: La consulta DELETE no tiene la estructura correcta.");
        return;
    }

    if !es_directorio_valido(ruta_directorio, partes[2]) {
        println!("INVALID_TABLE: El directorio no contiene el archivo CSV especificado.");
        return;
    }

    let ruta_archivo = construir_ruta_archivo(ruta_directorio, partes[2]);

    if partes.len() == 3 {
        // Procesar caso DELETE sin WHERE (elimina todas las filas)
        match procesar_delete(&ruta_archivo, None) {
            Ok(()) => (),
            Err(error) => println!("{}", error),
        }
    }

    if partes.len() >= 7 && partes[3] == "WHERE" {
        // Procesar caso DELETE con WHERE (elimina filas específicas)
        let condicion = partes[4..].join(" ");
        match procesar_delete(&ruta_archivo, Some(&condicion)) {
            Ok(()) => (),
            Err(error) => println!("{}", error),
        }
    } else {
        println!("INVALID_SYNTAX: La consulta DELETE no tiene la estructura correcta.");
    }
}

#[test]
fn test_consulta_delete_corta_con_estructura_incorrecta() {
    let consulta = ["DELETE", "DELETE", "person"];
    let ruta_directorio = "directorio_prueba";
    assert_eq!(validar_delete(&consulta, ruta_directorio), ());
}

#[test]
fn test_directorio_no_valido_delete() {
    let consulta = ["DELETE", "FROM", "person"];
    let ruta_directorio = "directorio_prueba";
    match fs::create_dir(ruta_directorio) {
        Ok(_) => {
            assert_eq!(validar_delete(&consulta, ruta_directorio), ());

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
fn test_consulta_delete_larga_con_estructura_incorrecta() {
    let partes = [
        "DELETE",
        "FROM",
        "archivo_test",
        "person",
        "columna",
        ">",
        "valor",
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

    assert_eq!(validar_delete(&partes, ruta_directorio), ());

    if let Err(e) = fs::remove_dir_all(ruta_directorio) {
        println!("Error al eliminar el directorio: {}", e);
    }
}
