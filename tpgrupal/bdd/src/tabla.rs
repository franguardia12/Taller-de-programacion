use std::collections::HashMap;

use protocolo::parser_cql::condicion_where::CondicionWhere;

const IGUAL: &str = "=";
const MAYOR: &str = ">";
const MAYOR_IGUAL: &str = ">=";
const MENOR: &str = "<";
const MENOR_IGUAL: &str = "<=";

#[derive(Debug)]
pub struct Tabla {
    pub nombre: String,
    pub headers: Vec<String>,
    pub datos: HashMap<String, Vec<String>>, // K = Partition Key, V = Registro
}

impl Tabla {
    pub fn new(nombre: String, headers: Vec<String>) -> Self {
        Tabla {
            nombre,
            headers,
            datos: HashMap::new(),
        }
    }

    //Nuestro separador es: ','
    pub fn insertar(&mut self, row: String) {
        let registros = row.split(",").collect::<Vec<&str>>();
        let partition_key = registros[0].to_string();
        match self.datos.get_mut(&partition_key) {
            Some(vec) => {
                if !vec.contains(&row) {
                    vec.push(row.clone());
                }
            }
            None => {
                let vector_vacio: Vec<String> = vec![row.clone()];
                self.datos.insert(partition_key, vector_vacio);
            }
        }
    }

    pub fn eliminar(&mut self, condicion: &CondicionWhere) {
        let condicion_hash = &condicion.condicion1.split(" = ").collect::<Vec<&str>>();
        let condicion_busqueda = &condicion.condicion2.split(" = ").collect::<Vec<&str>>();

        let partition_key = condicion_hash[1].to_string();

        let filas = self.datos.get_mut(&partition_key);

        let mut id_index = 1;
        if partition_key.parse::<usize>().is_err() {
            id_index = 2;
        }

        if let Some(filas) = filas {
            for (index, fila) in filas.iter().enumerate() {
                let fila_separada = fila.split(",").collect::<Vec<&str>>();
                if fila_separada[id_index] == condicion_busqueda[1] {
                    filas.remove(index);
                    break;
                }
            }
        } else {
            println!("No se encontro la fila a eliminar.");
        };
    }

    pub fn actualizar(&mut self, condicion: &CondicionWhere, query: String) {
        // En este caso se quieren actualizar todos los campos de una línea
        let condicion_hash = &condicion.condicion1.split(" = ").collect::<Vec<&str>>();
        let condicion_busqueda = &condicion.condicion2.split(" = ").collect::<Vec<&str>>();

        let partition_key = condicion_hash[1].to_string();

        let mut id_index = 0;
        if partition_key.parse::<usize>().is_err() {
            id_index = 2;
        }

        let registros = query.split(", ").collect::<Vec<&str>>();

        if let Some(filas) = self.datos.get_mut(&partition_key) {
            for fila in filas {
                let fila_separada = fila.split(",").collect::<Vec<&str>>();
                if fila_separada[id_index] == condicion_busqueda[1] {
                    let mut fila_actualizada = fila_separada.clone();
                    for registro in &registros {
                        let registro_split = registro.trim().split(" = ").collect::<Vec<&str>>();
                        let header_index = self
                            .headers
                            .iter()
                            .position(|h| h == registro_split[0])
                            .unwrap();
                        fila_actualizada[header_index] = registro_split[1];
                    }
                    let fila_actualizada = fila_actualizada.join(",");
                    *fila = fila_actualizada;
                }
            }
        }
    }

    pub fn select(&self, condicion: &CondicionWhere, query: String) -> Vec<String> {
        let columnas_a_imprimir = detectar_columnas(&self.headers, query);
        let mut rows_seleccionadas: Vec<Vec<&str>> = vec![];

        if condicion.condicion1.is_empty() {
            for (_particion, datos) in self.datos.iter() {
                for linea in datos.iter() {
                    let linea_separada = linea.split(",").collect::<Vec<&str>>();
                    rows_seleccionadas.push(linea_separada);
                }
            }
        } else {
            for (_particion, datos) in self.datos.iter() {
                for linea in datos.iter() {
                    let linea_separada = linea.split(",").collect::<Vec<&str>>();
                    if verificar_condiciones(
                        &condicion.condicion1,
                        &condicion.condicion2,
                        &linea_separada,
                        &self.headers,
                    ) {
                        rows_seleccionadas.push(linea_separada);
                    }
                }
            }
        }
        imprimir_lineas(&columnas_a_imprimir, rows_seleccionadas)
    }
}

fn verificar_condiciones(
    cond1: &str,
    cond2: &str,
    datos_linea: &[&str],
    c_tabla: &[String],
) -> bool {
    if cond2.is_empty() {
        verificar_condicion(cond1, datos_linea, c_tabla)
    } else {
        verificar_condicion(cond1, datos_linea, c_tabla)
            && verificar_condicion(cond2, datos_linea, c_tabla)
    }
}

fn detectar_operador(condicion: &str) -> &str {
    let caracteres: Vec<char> = condicion.chars().collect();
    let mut operador: &str = "";
    for (index, e) in caracteres.iter().enumerate() {
        match e {
            '=' => {
                operador = IGUAL;
                break;
            }
            '>' => {
                match caracteres[index + 1] {
                    '=' => operador = MAYOR_IGUAL,
                    _ => operador = MAYOR,
                }
                break;
            }
            '<' => {
                match caracteres[index + 1] {
                    '=' => operador = MENOR_IGUAL,
                    _ => operador = MENOR,
                }
                break;
            }
            _ => continue,
        }
    }
    operador
}

fn verificar_condicion(cond: &str, datos_linea: &[&str], c_tabla: &[String]) -> bool {
    let oper = detectar_operador(cond);
    let mut cond_splited: Vec<&str> = cond.split(oper).collect();
    cond_splited[0] = cond_splited[0].trim();
    cond_splited[1] = cond_splited[1].trim();

    let mut algo_final: (&str, &str, &str) = ("", "", "");
    algo_final.1 = oper;
    for (index, e) in c_tabla.iter().enumerate() {
        if e == cond_splited[0] {
            algo_final.0 = datos_linea[index];
        }
        if e == cond_splited[1] {
            algo_final.2 = datos_linea[index];
        }
    }
    if algo_final.0.is_empty() {
        algo_final.0 = cond_splited[0];
    }
    if algo_final.2.is_empty() {
        algo_final.2 = cond_splited[1];
    }
    check_info(algo_final)
}

fn check_info(info: (&str, &str, &str)) -> bool {
    let _a = info.0.parse::<isize>().ok();
    let _b = info.2.parse::<isize>().ok();
    if _a.is_some() && _b.is_some() {
        match info.1 {
            IGUAL => _a == _b,
            MAYOR => _a > _b,
            MAYOR_IGUAL => _a >= _b,
            MENOR => _a < _b,
            MENOR_IGUAL => _a <= _b,
            _ => false,
        }
    } else {
        match info.1 {
            IGUAL => info.0 == info.2,
            MAYOR => info.0 > info.2,
            MAYOR_IGUAL => info.0 >= info.2,
            MENOR => info.0 < info.2,
            MENOR_IGUAL => info.0 <= info.2,
            _ => false,
        }
    }
}

fn detectar_columnas(c_tabla: &[String], columnas_query: String) -> Vec<usize> {
    let z: Vec<&str> = columnas_query.split(',').collect();
    let mut colums: Vec<String> = vec![];
    for e in z.iter() {
        if !e.is_empty() {
            colums.push(e.trim().replace(['(', ')', ',', '\"', '\'', '\n'], ""));
        }
    }
    obtener_indices_y_nombres_columnas(colums, c_tabla)
}

fn imprimir_lineas(indices_colums: &[usize], lineas: Vec<Vec<&str>>) -> Vec<String> {
    let mut res: Vec<String> = vec![];
    for datos_linea in lineas {
        let mut linea_imprimir: Vec<&str> = vec![];
        for i in indices_colums {
            if i < &datos_linea.len() {
                linea_imprimir.push(datos_linea[*i]);
            }
        }
        res.push(linea_imprimir.join(","));
    }
    res
}

fn obtener_indices_y_nombres_columnas(colums: Vec<String>, c_tabla: &[String]) -> Vec<usize> {
    let mut indices_colums: Vec<usize> = vec![];
    let mut todas_las_columnas = false;
    if colums.len() == 1 && &colums[0] == "*" {
        todas_las_columnas = true;
    }
    for e in colums {
        if c_tabla.contains(&e) || todas_las_columnas {
            for (index, c) in c_tabla.iter().enumerate() {
                if todas_las_columnas || *c == e {
                    indices_colums.push(index);
                }
            }
        }
    }
    indices_colums
}

/* #[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_actualizar_solo_un_campo() {
        let mut tabla = Tabla::new(
            "VUELOS_ORIGEN".to_string(),
            vec![
                "ORIGEN".to_string(),
                "FECHA".to_string(),
                "ID_VUELO".to_string(),
                "DESTINO".to_string(),
            ],
        );

        tabla.insertar("Argentina,2021-10-10,1,Chile".to_string());
        tabla.insertar("Argentina,2021-10-10,2,Chile".to_string());
        tabla.insertar("Argentina,2021-10-10,3,Chile".to_string());

        let condicion = CondicionWhere {
            condicion1: "ORIGEN = Argentina".to_string(),
            operador_logico: None,
            condicion2: "ID_VUELO = 1".to_string(),
        };

        tabla.actualizar(&condicion, "DESTINO = Uruguay".to_string());

        let resultado = tabla.datos.get("Argentina").unwrap();

        assert_eq!(resultado[0], "Argentina,2021-10-10,1,Uruguay");
    }

    #[test]
    fn test_actualizar_algunos_campos() {
        let mut tabla = Tabla::new(
            "VUELOS_ORIGEN".to_string(),
            vec![
                "ORIGEN".to_string(),
                "FECHA".to_string(),
                "ID_VUELO".to_string(),
                "DESTINO".to_string(),
            ],
        );

        tabla.insertar("Argentina,2021-10-10,1,Chile".to_string());
        tabla.insertar("Argentina,2021-10-10,2,Chile".to_string());
        tabla.insertar("Argentina,2021-10-10,3,Chile".to_string());

        let condicion = CondicionWhere {
            condicion1: "ORIGEN = Argentina".to_string(),
            operador_logico: None,
            condicion2: "ID_VUELO = 1".to_string(),
        };

        tabla.actualizar(&condicion, "DESTINO = Uruguay, FECHA = 2021-10-11".to_string());

        let resultado = tabla.datos.get("Argentina").unwrap();

        assert_eq!(resultado[0], "Argentina,2021-10-11,1,Uruguay");
    }

    #[test]
    fn test_actualizar_todos_campos() {
        let mut tabla = Tabla::new(
            "VUELOS_ORIGEN".to_string(),
            vec![
                "ORIGEN".to_string(),
                "FECHA".to_string(),
                "ID_VUELO".to_string(),
                "DESTINO".to_string(),
            ],
        );

        tabla.insertar("Argentina,2021-10-10,1,Chile".to_string());
        tabla.insertar("Argentina,2021-10-10,2,Chile".to_string());
        tabla.insertar("Argentina,2021-10-10,3,Chile".to_string());

        let condicion = CondicionWhere {
            condicion1: "ORIGEN = Argentina".to_string(),
            operador_logico: None,
            condicion2: "ID_VUELO = 1".to_string(),
        };

        tabla.actualizar(
            &condicion,
            "ORIGEN = Argentina, FECHA = 2021-10-11, ID_VUELO = 1, DESTINO = Uruguay".to_string(),
        );

        let resultado = tabla.datos.get("Argentina").unwrap();

        assert_eq!(resultado[0], "Argentina,2021-10-11,1,Uruguay");
    }
} */
