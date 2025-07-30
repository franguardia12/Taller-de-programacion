use std::env;

use tpindividual::parseo_consulta::procesar_consulta;
use tpindividual::validaciones::es_ruta_valida;

fn main() {
    let args: Vec<String> = env::args().collect();

    if args.len() != 3 {
        println!("ERROR: cantidad de argumentos inválida");
        return;
    }

    let ruta_directorio = &args[1];
    let consulta_sql = &args[2];

    if !es_ruta_valida(ruta_directorio) {
        println!("INVALID_TABLE: La ruta proporcionada no es un directorio válido.");
        return;
    }

    procesar_consulta(consulta_sql, ruta_directorio);
}
