use std::{
    fs::File,
    io::{BufReader, Seek, SeekFrom},
};

use rustls::{Certificate, ClientConfig, PrivateKey, RootCertStore, ServerConfig};
use rustls_pemfile::{certs, pkcs8_private_keys, rsa_private_keys};

pub fn get_certs() -> Result<Vec<Certificate>, String> {
    let archivo_cert = File::open("seguridad/cert.pem");
    if archivo_cert.is_err() {
        return Err("Error al abrir los archivos de certificado y clave privada.".to_string());
    }
    let cert_file = &mut BufReader::new(archivo_cert.unwrap());
    let cert_chain = match certs(cert_file) {
        Ok(certs) => certs.into_iter().map(Certificate).collect::<Vec<_>>(),
        Err(e) => return Err(format!("Error al cargar los certificados: {}", e)),
    };
    Ok(cert_chain)
}

pub fn get_keys() -> Result<Vec<PrivateKey>, String> {
    let archivo_key = File::open("seguridad/key.pem")
        .map_err(|_| "Error al abrir el archivo de clave privada.".to_string())?;
    let mut key_file = BufReader::new(archivo_key);

    // Intentar cargar claves privadas en formato PKCS#8
    let keys = match pkcs8_private_keys(&mut key_file) {
        Ok(keys) if !keys.is_empty() => {
            //Claves privadas en formato PKCS#8 cargadas correctamente.
            keys.into_iter().map(PrivateKey).collect::<Vec<_>>()
        }
        Ok(_) => {
            // No se encontraron claves PKCS#8, intentar con claves RSA
            key_file
                .seek(SeekFrom::Start(0))
                .map_err(|e| format!("Error al reiniciar el lector de claves: {}", e))?;
            match rsa_private_keys(&mut key_file) {
                Ok(keys) if !keys.is_empty() => {
                    //Claves privadas RSA cargadas correctamente.
                    keys.into_iter().map(PrivateKey).collect::<Vec<_>>()
                }
                Ok(_) => {
                    return Err(
                        "No se encontraron claves privadas en el archivo key.pem.".to_string()
                    )
                }
                Err(e) => return Err(format!("Error al cargar las claves privadas RSA: {}", e)),
            }
        }
        Err(e) => return Err(format!("Error al cargar las claves privadas PKCS#8: {}", e)),
    };
    Ok(keys)
}

pub fn create_server_config() -> Result<ServerConfig, String> {
    let certs = get_certs();
    let key = get_keys();
    if let (Ok(certs), Ok(mut keys)) = (certs, key) {
        Ok(ServerConfig::builder()
            .with_safe_defaults()
            .with_no_client_auth()
            .with_single_cert(certs, keys.remove(0))
            .expect("Failed to create server config"))
    } else {
        Err("Error al cargar los certificados y claves privadas".to_string())
    }
}

pub fn create_client_config() -> Result<ClientConfig, String> {
    let mut root_cert_store = RootCertStore::empty();
    let certs = get_certs();
    match certs {
        Ok(certs) => {
            for cert in certs {
                root_cert_store
                    .add(&cert)
                    .expect("Failed to add certificate to store");
            }
            Ok(
                ClientConfig::builder()
                    .with_safe_defaults()
                    .with_root_certificates(root_cert_store)
                    .with_no_client_auth(), // El cliente no presenta un certificado propio
            )
        }
        Err(e) => Err(e),
    }
}
