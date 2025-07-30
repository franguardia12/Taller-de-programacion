extern crate tpindividual;

use tpindividual::logica_select::parsear_consulta_select;

#[test]
fn test_parsear_consulta_select() {
    let consulta =
        "SELECT columna1, columna2, columna3 FROM tabla WHERE condicion ORDER BY columna DESC";
    let resultado = parsear_consulta_select(consulta);
    assert!(resultado.is_ok());
    assert_eq!(
        resultado.unwrap(),
        (
            vec!["columna1", "columna2", "columna3"],
            "tabla",
            Some("condicion"),
            Some("columna DESC")
        )
    );
}
