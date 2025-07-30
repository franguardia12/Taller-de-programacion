use consola::{consola::ejecutar_consola, threadpool::ThreadPool};
use interfaz::handler::ejecutar_startup;

fn main() {
    ejecutar_startup();
    let pool = ThreadPool::new(10);
    ejecutar_consola(pool);
}
