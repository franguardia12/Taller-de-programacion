use std::{
    fs::{self},
    path::Path,
};

/// Verifica si la ruta existe y si es un directorio
pub fn es_ruta_valida(ruta: &str) -> bool {
    let path = Path::new(ruta);

    path.exists() && path.is_dir()
}

/// Verifica si el directorio contiene un archivo CSV específico
/// Verifica si el nombre del archivo coincide y si tiene extensión .csv
/// Devuelve true si lo encontró, false en caso contrario
pub fn es_directorio_valido(ruta: &str, nombre_archivo: &str) -> bool {
    let path = Path::new(ruta);

    if let Ok(entries) = fs::read_dir(path) {
        for entry in entries.flatten() {
            let archivo_path = entry.path();

            if let Some(stem) = archivo_path.file_stem() {
                if let Some(stem_str) = stem.to_str() {
                    if stem_str == nombre_archivo {
                        if let Some(extension) = archivo_path.extension() {
                            if extension == "csv" {
                                return true;
                            }
                        }
                    }
                }
            }
        }
    }

    false
}

#[test]
fn test_ruta_invalida() {
    assert_eq!(es_ruta_valida("no_existe"), false);
}

#[test]
fn test_es_ruta_valida() {
    let ruta_directorio = "directorio_test";

    match fs::create_dir(ruta_directorio) {
        Ok(_) => {
            assert!(es_ruta_valida(ruta_directorio));

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
fn test_es_ruta_no_es_directorio() {
    let ruta_archivo = "archivo_test.txt";

    match fs::File::create(ruta_archivo) {
        Ok(_) => {
            assert!(!es_ruta_valida(ruta_archivo));

            match fs::remove_file(ruta_archivo) {
                Ok(_) => {}
                Err(e) => {
                    println!("ERROR: No se pudo eliminar el archivo: {}", e);
                }
            }
        }
        Err(e) => {
            println!("ERROR: No se pudo crear el archivo: {}", e);
        }
    }
}

#[test]
fn test_es_directorio_valido() {
    let ruta_directorio = "directorio_test";
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

    assert!(es_directorio_valido(ruta_directorio, "archivo_test"));

    if let Err(e) = fs::remove_dir_all(ruta_directorio) {
        println!("Error al eliminar el directorio: {}", e);
    }
}

#[test]
fn test_no_es_directorio_valido() {
    let ruta_directorio = "directorio_test";
    if let Err(e) = fs::create_dir(ruta_directorio) {
        println!("ERROR: No se pudo crear el directorio: {}", e);
        return;
    }

    let ruta_archivo_txt = Path::new(ruta_directorio).join("archivo_test.txt");
    if let Err(e) = fs::File::create(&ruta_archivo_txt) {
        println!("ERROR: No se pudo crear el archivo: {}", e);
        let _ = fs::remove_dir_all(ruta_directorio);
        return;
    }

    assert!(!es_directorio_valido(ruta_directorio, "archivo_test"));

    if let Err(e) = fs::remove_dir_all(ruta_directorio) {
        println!("Error al eliminar el directorio: {}", e);
    }
}

#[test]
fn test_directorio_valido_no_contiene_archivo_buscado() {
    let ruta_directorio = "directorio_test";
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

    assert!(!es_directorio_valido(
        ruta_directorio,
        "archivo_no_existente"
    ));

    if let Err(e) = fs::remove_dir_all(ruta_directorio) {
        println!("Error al eliminar el directorio: {}", e);
    }
}
