use chrono::{prelude::*, Duration};
use rand::{self, Rng};
use std::io::{self, *};

use interfaz::vuelo::Vuelo;

/// Gestiona el ingreso de datos por stdin para la creación de un nuevo vuelo con dicha información.
///
/// # Parameters
///
/// - `nombres_aeropuertos`: Un vector con los nombres de todos los aeropuertos disponibles en la base de datos.
///
/// # Returns
///
/// Devuelve un struct de tipo 'Vuelo' con toda la información cargada en sus campos.
pub fn obtener_datos_de_vuelo(nombres_aeropuertos: &[String]) -> Vuelo {
    let id_vuelo = return_id();
    let aeropuerto_origen = return_aeropuerto_valido(nombres_aeropuertos, "", "Origen");
    let aeropuerto_destino =
        return_aeropuerto_valido(nombres_aeropuertos, &aeropuerto_origen, "Destino");
    let velocidad_promedio = return_velocidad_promedio();

    let actual_utc: DateTime<Utc> = Utc::now();
    let fecha_argentina = actual_utc - Duration::hours(3);
    let fecha = format!("{}", fecha_argentina.format("%Y-%m-%d"));

    Vuelo {
        id: id_vuelo,
        origen: aeropuerto_origen,
        destino: aeropuerto_destino,
        fecha,
        estado_vuelo: "On-Time".to_string(),
        velocidad_actual: velocidad_promedio,
        altitud_actual: 11500.0,
        latitud_actual: 0.0,
        longitud_actual: 0.0,
        combustible: 100.0,
    }
}

fn return_aeropuerto_valido(
    aeropuertos_posibles: &[String],
    aeropuerto_usado: &str,
    aeropuerto_buscado: &str,
) -> String {
    let str_stdout = format!("Ingrese el {}: ", aeropuerto_buscado);
    let mut input = String::new();
    loop {
        print!("{}", str_stdout);
        io::stdout().flush().unwrap();
        let _ = stdout().flush();
        let _ = stdin().read_line(&mut input);
        let name_aeropuerto = construir_nombre(&input);
        let nombre_aeropuerto = name_aeropuerto.trim().to_string();
        if aeropuerto_usado == nombre_aeropuerto && !aeropuerto_usado.is_empty() {
            println!("El aeropuerto de Origen no puede ser igual al de Destino.");
            io::stdout().flush().unwrap();
            let _ = stdout().flush();
            continue;
        }
        if aeropuertos_posibles.contains(&nombre_aeropuerto) {
            return nombre_aeropuerto;
        } else {
            println!("Aeropuerto inexistente.");
            io::stdout().flush().unwrap();
            let _ = stdout().flush();
        }
        input.clear();
    }
}

fn construir_nombre(input: &str) -> String {
    let upper_input = input.to_uppercase();
    let nombre_separado = upper_input.split(" ").collect::<Vec<&str>>();
    let aer = "AEROPUERTO";
    if nombre_separado.contains(&aer) {
        nombre_separado.join(" ")
    } else {
        let mut nombre = vec![aer];
        nombre.extend(nombre_separado);
        nombre.join(" ")
    }
}

fn return_velocidad_promedio() -> f32 {
    let mut input = String::new();
    loop {
        print!("Ingrese la velocidad promedio estimada (en km/h): ");
        io::stdout().flush().unwrap();
        let _ = stdout().flush();
        let _ = stdin().read_line(&mut input);
        match input.trim().parse::<f32>() {
            Ok(n) => {
                if n > 0.0 {
                    return n;
                } else {
                    println!("Ingrese una velocidad valida.");
                    io::stdout().flush().unwrap();
                    let _ = stdout().flush();
                }
            }
            Err(_) => {
                println!("Ingrese un valor numerico.");
                io::stdout().flush().unwrap();
                let _ = stdout().flush();
            }
        };
        input.clear();
    }
}

fn return_id() -> String {
    print!(
        "Ingrese el id entre [500, 999] (No ingresar un id valido devolvera un numero random): "
    );
    io::stdout().flush().unwrap();
    let _ = stdout().flush();
    let mut input = String::new();
    let _ = stdin().read_line(&mut input);
    match input.trim().parse::<i32>() {
        Ok(n) => {
            if (500..=999).contains(&n) {
                "VUE".to_string() + &(n.to_string())
            } else {
                obtener_id_random()
            }
        }
        Err(_) => obtener_id_random(),
    }
}

fn obtener_id_random() -> String {
    let mut rng = rand::thread_rng();
    let random_id = rng.gen_range(500..=999);
    let id_str = random_id.to_string();
    "VUE".to_string() + &id_str
}

/// Espera la confirmación o no de que se desea ejecutar un nuevo vuelo.
///
/// # Returns
///
/// Devuelve un bool que define si quiere ingresar un nuevo vuelo.
pub fn ingresar_nuevo_vuelo() -> bool {
    let mut input = String::new();
    let mut nuevo_vuelo = false;
    loop {
        print!("Desea ingresar un nuevo vuelo? [Y/N]: ");
        io::stdout().flush().unwrap();
        let _ = stdout().flush();
        let _ = stdin().read_line(&mut input);
        match input.to_uppercase().trim() {
            "Y" => {
                nuevo_vuelo = true;
                break;
            }
            "N" => {
                break;
            }
            _ => {}
        }
        input.clear();
    }
    nuevo_vuelo
}
