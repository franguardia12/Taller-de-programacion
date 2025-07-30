use cliente_servidor::client_services::connection::connect_to_server;
use cliente_servidor::client_services::receiving::receive_response;
use cliente_servidor::client_services::sending::send_request;
use protocolo::serial_deserial::cassandra::serializador_cliente_server::{
    query_to_bytes_client_server, serializar_startup_client_server,
};
use protocolo::serial_deserial::level_consistency::LevelConsistency;

const READY: i8 = 0x02;
use crate::vuelo::Vuelo;

/// Crea una consulta INSERT con el formato de CQL para insertar
/// un vuelo en la base de datos.
pub fn construir_consulta_insert(vuelo: &Vuelo, tabla: String) -> String {
    let consulta: String = if tabla == "VUELOS_ORIGEN" {
        format!("INSERT INTO VUELOS_ORIGEN (ORIGEN, FECHA, ID_VUELO, DESTINO, ESTADO_VUELO, VELOCIDAD_ACTUAL, ALTITUD_ACTUAL, LATITUD_ACTUAL, LONGITUD_ACTUAL, COMBUSTIBLE) VALUES ({}, {}, {}, {}, {}, {}, {}, {}, {}, {})", vuelo.origen, vuelo.fecha, vuelo.id, vuelo.destino, vuelo.estado_vuelo, vuelo.velocidad_actual, vuelo.altitud_actual, vuelo.latitud_actual, vuelo.longitud_actual, vuelo.combustible)
    } else {
        format!("INSERT INTO VUELOS_DESTINO (DESTINO, FECHA, ID_VUELO, ORIGEN, ESTADO_VUELO, VELOCIDAD_ACTUAL, ALTITUD_ACTUAL, LATITUD_ACTUAL, LONGITUD_ACTUAL, COMBUSTIBLE) VALUES ({}, {}, {}, {}, {}, {}, {}, {}, {}, {})", vuelo.destino, vuelo.fecha, vuelo.id, vuelo.origen, vuelo.estado_vuelo, vuelo.velocidad_actual, vuelo.altitud_actual, vuelo.latitud_actual, vuelo.longitud_actual, vuelo.combustible)
    };
    consulta
}

/// Crea una consulta SELECT con el formato de CQL para obtener
/// información de la base de datos.
pub fn construir_consulta_select(tabla: String, aeropuerto: String, fecha: String) -> String {
    //Dependiendo de si la consulta es para aeropuertos o para vuelos ya sé cuál es la forma de la consulta en cada caso
    //Hago la distinción y le envío la consulta al serializador para que luego entonces pueda ser enviada al servidor
    let consulta;
    if tabla == "AEROPUERTOS" {
        consulta = "SELECT * FROM AEROPUERTOS".to_string();
    } else if tabla == "VUELOS_ORIGEN" {
        if aeropuerto == *"" {
            consulta = "SELECT * FROM VUELOS_ORIGEN".to_string();
        } else {
            consulta = format!(
                "SELECT * FROM VUELOS_ORIGEN WHERE ORIGEN = {} AND FECHA = {}",
                aeropuerto, fecha
            );
        }
    } else if aeropuerto == *"" {
        consulta = "SELECT * FROM VUELOS_DESTINO".to_string();
    } else {
        consulta = format!(
            "SELECT * FROM VUELOS_DESTINO WHERE DESTINO = {} AND FECHA = {}",
            aeropuerto, fecha
        );
    }
    consulta
}

/// Crea una consulta DELETE con el formato de CQL para eliminar
/// un vuelo de la base de datos.
pub fn construir_consulta_delete(vuelo: &Vuelo, tabla: String) -> String {
    let consulta: String = if tabla == "VUELOS_ORIGEN" {
        format!(
            "DELETE FROM {} WHERE ORIGEN = {} AND ID_VUELO = {}",
            tabla, vuelo.origen, vuelo.id
        )
    } else {
        format!(
            "DELETE FROM {} WHERE DESTINO = {} AND ID_VUELO = {}",
            tabla, vuelo.destino, vuelo.id
        )
    };

    consulta
}

pub fn construir_consulta_update_consola(vuelo: &Vuelo, tabla: String) -> String {
    let consulta: String = if tabla == "VUELOS_ORIGEN" {
        format!("UPDATE {} SET LATITUD_ACTUAL = {}, LONGITUD_ACTUAL = {}, COMBUSTIBLE = {} WHERE ORIGEN = {} AND ID_VUELO = {}", tabla, vuelo.latitud_actual, vuelo.longitud_actual, vuelo.combustible, vuelo.origen, vuelo.id)
    } else {
        format!("UPDATE {} SET LATITUD_ACTUAL = {}, LONGITUD_ACTUAL = {}, COMBUSTIBLE = {} WHERE DESTINO = {} AND ID_VUELO = {}", tabla,vuelo.latitud_actual, vuelo.longitud_actual, vuelo.combustible, vuelo.destino, vuelo.id)
    };

    consulta
}

/// Crea una consulta UPDATE con el formato de CQL para actualizar
/// un vuelo de la base de datos.
pub fn construir_consulta_update(
    vuelo: &Vuelo,
    tabla: String,
    campo: String,
    valor: String,
) -> String {
    if tabla == "VUELOS_ORIGEN" {
        let consulta = format!(
            "UPDATE {} SET {} = {} WHERE ORIGEN = {} AND ID_VUELO = {}",
            tabla, campo, valor, vuelo.origen, vuelo.id
        );
        consulta
    } else {
        let consulta = format!(
            "UPDATE {} SET {} = {} WHERE DESTINO = {} AND ID_VUELO = {}",
            tabla, campo, valor, vuelo.destino, vuelo.id
        );
        consulta
    }
}

/// Serializa una consulta de acuerdo con el protocolo de Cassandra y la envía a la base de datos, luego recibe la respuesta que este
/// debe enviar y la deserializa para devolverla.
///
/// # Parameters
///
/// - `consulta`: Un String con la consulta que se desea realizar.
/// - `tipo_consistencia`: Corresponde al nivel de consistencia que se desea que tenga la consulta.
///
/// # Returns
///
/// Devuelve un Result con un vector de String que es la respuesta a la consulta (en caso de ser exitosa) y un String
/// en caso de que ocurra algún error.
pub fn ejecutar_consulta(
    consulta: String,
    tipo_consistencia: LevelConsistency,
) -> Result<Vec<String>, String> {
    // Hacer la diferencia entre una consulta con consistencia Strong y Weak
    // Para eso puede recibirse un parámetro en la función que indique la
    // consistencia que esa consulta tiene que tener y luego se envía como siempre

    let consulta_serializada = query_to_bytes_client_server(&consulta, tipo_consistencia, 0x00);

    let mut conexion = connect_to_server();

    match conexion {
        Ok(ref mut conexion) => {
            let _ = send_request(conexion, consulta_serializada);
            let respuesta = receive_response(conexion);
            match respuesta {
                Ok(respuesta) => Ok(respuesta.0),
                Err(_) => Err("No se pudo recibir respuesta".to_string()),
            }
        }
        Err(_) => Err("No se pudo conectar al servidor".to_string()),
    }
}

/// Se encarga de enviar el mensaje de inicio de conexión con la base de datos,
/// para que luego el cliente pueda enviar consultas
pub fn ejecutar_startup() {
    let startup = serializar_startup_client_server();
    let mut conexion = match connect_to_server() {
        Ok(c) => c,
        Err(_) => {
            println!("No se pudo recibir respuesta");
            return;
        }
    };
    let _ = send_request(&mut conexion, startup);
    println!("Autenticación: Cliente envía STARTUP al servidor");
    let respuesta = receive_response(&mut conexion);
    match respuesta {
        Ok(respuesta) => {
            if respuesta.1 == READY {
                println!("Cliente recibe READY");
            } else {
                println!("La autenticación falló");
            }
        }
        Err(_) => println!("No se pudo recibir respuesta"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_construir_consulta_insert() {
        let vuelo = Vuelo {
            id: "1".to_string(),
            origen: "MAD".to_string(),
            destino: "BCN".to_string(),
            fecha: "2021-06-01".to_string(),
            estado_vuelo: "En curso".to_string(),
            velocidad_actual: 800.0,
            altitud_actual: 10000.0,
            latitud_actual: 40.4165,
            longitud_actual: -3.70256,
            combustible: 100.0,
        };
        let consulta = construir_consulta_insert(&vuelo, "VUELOS_ORIGEN".to_string());
        assert_eq!(
            consulta,
            "INSERT INTO VUELOS_ORIGEN (ORIGEN, FECHA, ID_VUELO, DESTINO, ESTADO_VUELO, VELOCIDAD_ACTUAL, ALTITUD_ACTUAL, LATITUD_ACTUAL, LONGITUD_ACTUAL, COMBUSTIBLE) VALUES (MAD, 2021-06-01, 1, BCN, En curso, 800, 10000, 40.4165, -3.70256, 100)"
        );
    }

    #[test]
    fn test_construir_consulta_select() {
        let consulta = construir_consulta_select(
            "VUELOS_ORIGEN".to_string(),
            "MAD".to_string(),
            "2021-06-01".to_string(),
        );
        assert_eq!(
            consulta,
            "SELECT * FROM VUELOS_ORIGEN WHERE ORIGEN = MAD AND FECHA = 2021-06-01"
        );
    }

    #[test]
    fn test_construir_consulta_update() {
        let vuelo = Vuelo {
            id: "1".to_string(),
            origen: "MAD".to_string(),
            destino: "BCN".to_string(),
            fecha: "2021-06-01".to_string(),
            estado_vuelo: "En curso".to_string(),
            velocidad_actual: 800.0,
            altitud_actual: 10000.0,
            latitud_actual: 40.4165,
            longitud_actual: -3.70256,
            combustible: 100.0,
        };
        let consulta = construir_consulta_update(
            &vuelo,
            "VUELOS_ORIGEN".to_string(),
            "ESTADO_VUELO".to_string(),
            "Finalizado".to_string(),
        );
        assert_eq!(
            consulta,
            "UPDATE VUELOS_ORIGEN SET ESTADO_VUELO = Finalizado WHERE ORIGEN = MAD AND ID_VUELO = 1"
        );
    }
}
