extern crate tpindividual;

use tpindividual::logica_insert::parsear_consulta_insert;

#[test]
fn test_parseo_consulta_insert() {
    let consulta =
        "INSERT INTO ordenes (id, id_cliente, producto, cantidad) VALUES (111, 6, 'Laptop', 3)";
    let resultado = parsear_consulta_insert(consulta);
    assert!(resultado.is_ok());
    assert_eq!(
        resultado.unwrap(),
        vec![
            "id, id_cliente, producto, cantidad".to_string(),
            "ordenes".to_string(),
            "111, 6, Laptop, 3".to_string()
        ]
    );
}
