use std::{env, thread};

use std::sync::{Arc, Mutex};

use bdd::nodo::Nodo;
use cliente_servidor::servidor::server::{
    abrir_puerto_gossip, abrir_puerto_interconexion_nodos, run_server,
};
use cliente_servidor::servidor::server::{PUERTO_CLIENTE, PUERTO_GOSSIP, PUERTO_INTERNODOS};

fn main() -> Result<(), String> {
    let args: Vec<String> = env::args().collect();
    let nodo = Nodo::new(&args[1])?;
    let nodo_mutex = Arc::new(Mutex::new(nodo));

    let nodo_cliente = Arc::clone(&nodo_mutex);
    let nodo_internodos = Arc::clone(&nodo_mutex);
    let nodo_gossip = Arc::clone(&nodo_mutex);
    let nodo_gossip2 = Arc::clone(&nodo_mutex);

    let address_cliente: String = format!("0.0.0.0:{}", PUERTO_CLIENTE);
    let address_internodos: String = format!("0.0.0.0:{}", PUERTO_INTERNODOS);
    let address_gossip: String = format!("0.0.0.0:{}", PUERTO_GOSSIP);

    let handle1 = thread::spawn(move || match run_server(address_cliente, nodo_cliente) {
        Ok(_) => (),
        Err(e) => eprintln!("{}", e),
    });

    let handle2 = thread::spawn(move || {
        match abrir_puerto_interconexion_nodos(address_internodos, nodo_internodos) {
            Ok(_) => (),
            Err(e) => eprintln!("{}", e),
        }
    });

    let handle3 = thread::spawn(
        move || match abrir_puerto_gossip(address_gossip, nodo_gossip) {
            Ok(_) => (),
            Err(e) => eprintln!("{}", e),
        },
    );

    Nodo::iniciar_gossip(nodo_gossip2);

    handle1.join().unwrap();
    handle2.join().unwrap();
    handle3.join().unwrap();

    Ok(())
}
