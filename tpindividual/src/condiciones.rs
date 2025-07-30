#[derive(Debug)]
pub enum NodoAST {
    Comparacion {
        columna: String,
        operador: String,
        valor: String,
    },
    Not(Box<NodoAST>),
    And(Box<NodoAST>, Box<NodoAST>),
    Or(Box<NodoAST>, Box<NodoAST>),
}

/// Dada una línea y una condición o conjunto de condiciones, evalúa si
/// la línea cumple o no con estas.
pub fn evaluar_condiciones(
    linea: &str,
    condicion: &str,
    nombres_columnas: &Vec<String>,
) -> Result<bool, &'static str> {
    let tokens = dividir_en_tokens(condicion)?;
    let ast = parsear_tokens_a_ast(&tokens)?;
    evaluar_ast(&ast, linea, nombres_columnas)
}

/// Divide la condición o conjunto de condiciones en tokens para su posterior
/// procesamiento.
pub fn dividir_en_tokens(condicion: &str) -> Result<Vec<String>, &'static str> {
    let mut tokens = Vec::new();
    let mut current_token = String::new();
    let mut in_quotes = false;

    for c in condicion.chars() {
        match c {
            ' ' if !in_quotes => {
                if !current_token.is_empty() {
                    tokens.push(current_token);
                    current_token = String::new();
                }
            }
            '(' | ')' if !in_quotes => {
                if !current_token.is_empty() {
                    tokens.push(current_token);
                    current_token = String::new();
                }
                tokens.push(c.to_string());
            }
            '=' | '!' | '<' | '>' if !in_quotes => {
                if !current_token.is_empty() {
                    tokens.push(current_token);
                    current_token = String::new();
                }
                current_token.push(c);
            }
            '\'' => {
                if in_quotes {
                    tokens.push(current_token);
                    current_token = String::new();
                    in_quotes = false;
                } else {
                    if !current_token.is_empty() {
                        tokens.push(current_token);
                        current_token = String::new();
                    }
                    in_quotes = true;
                }
            }
            _ => {
                current_token.push(c);
            }
        }
    }

    if !current_token.is_empty() {
        tokens.push(current_token);
    }

    Ok(tokens)
}

/// Parsea los tokens de las condiciones en un árbol de sintaxis abstracta (AST).
/// El análisis se hace desde operadores de menor procedencia a mayor procedencia.
/// Si algo falla en el proceso devuelve un error.
pub fn parsear_tokens_a_ast(tokens: &[String]) -> Result<Box<NodoAST>, &'static str> {
    parsear_expresion(tokens, 0).map(|(nodo, _)| nodo)
}

/// Parsea las expresiones, manejando los operadores AND y OR.
pub fn parsear_expresion(
    tokens: &[String],
    indice: usize,
) -> Result<(Box<NodoAST>, usize), &'static str> {
    let (mut nodo_izq, mut indice_actual) = parsear_termino(tokens, indice)?;

    while indice_actual < tokens.len() {
        if tokens[indice_actual] == "AND" || tokens[indice_actual] == "OR" {
            let operador = &tokens[indice_actual];
            let (nodo_der, nuevo_indice) = parsear_termino(tokens, indice_actual + 1)?;

            nodo_izq = if operador == "AND" {
                Box::new(NodoAST::And(nodo_izq, nodo_der))
            } else {
                Box::new(NodoAST::Or(nodo_izq, nodo_der))
            };

            indice_actual = nuevo_indice;
        } else {
            break;
        }
    }

    if indice_actual < tokens.len()
        && (tokens[indice_actual] == "AND" || tokens[indice_actual] == "OR")
    {
        return Err("INVALID_SYNTAX: Estructura de condiciones incorrecta");
    }

    Ok((nodo_izq, indice_actual))
}

/// Parsea los términos de las expresiones, manejando los operadores AND.
pub fn parsear_termino(
    tokens: &[String],
    indice: usize,
) -> Result<(Box<NodoAST>, usize), &'static str> {
    let (mut nodo_izq, mut indice_actual) = parsear_factor(tokens, indice)?;

    while indice_actual < tokens.len() {
        if tokens[indice_actual] == "AND" {
            let (nodo_der, nuevo_indice) = parsear_factor(tokens, indice_actual + 1)?;
            nodo_izq = Box::new(NodoAST::And(nodo_izq, nodo_der));
            indice_actual = nuevo_indice;
        } else {
            break;
        }
    }

    Ok((nodo_izq, indice_actual))
}

/// Parsea los factores de los términos, manejando los operadores NOT y los paréntesis.
pub fn parsear_factor(
    tokens: &[String],
    indice: usize,
) -> Result<(Box<NodoAST>, usize), &'static str> {
    let token = tokens
        .get(indice)
        .ok_or("INVALID_SYNTAX: Estructura de condiciones incorrecta")?;

    if *token == "NOT" {
        let (nodo, nuevo_indice) = parsear_factor(tokens, indice + 1)?;
        Ok((Box::new(NodoAST::Not(nodo)), nuevo_indice))
    } else if *token == "(" {
        let (nodo, nuevo_indice) = parsear_expresion(tokens, indice + 1)?;
        if tokens.get(nuevo_indice) == Some(&")".to_string()) {
            Ok((nodo, nuevo_indice + 1))
        } else {
            Err("INVALID_SYNTAX: Estructura de condiciones incorrecta")
        }
    } else {
        parsear_condicion(tokens, indice)
    }
}

/// Parsea las condiciones de los factores, es decir, las comparaciones.
/// Crea un nuevo nodo del AST con la información de la comparación.
pub fn parsear_condicion(
    tokens: &[String],
    indice: usize,
) -> Result<(Box<NodoAST>, usize), &'static str> {
    if tokens.len() < indice + 3 {
        return Err("INVALID_SYNTAX: Estructura de condiciones incorrecta");
    }

    let columna = &tokens[indice];
    let operador = &tokens[indice + 1];
    let valor = &tokens[indice + 2];

    Ok((
        Box::new(NodoAST::Comparacion {
            columna: columna.to_string(),
            operador: operador.to_string(),
            valor: valor.to_string(),
        }),
        indice + 3,
    ))
}

/// Evalúa el AST formado con la información de una línea y las columnas.
/// Devuelve un booleano indicando si la línea cumple o no con la condición.
pub fn evaluar_ast(
    nodo: &NodoAST,
    linea: &str,
    nombres_columnas: &Vec<String>,
) -> Result<bool, &'static str> {
    let valores: Vec<&str> = linea.split(',').collect();

    match nodo {
        NodoAST::Comparacion {
            columna,
            operador,
            valor,
        } => {
            let index_columna = nombres_columnas
                .iter()
                .position(|c| c == columna)
                .ok_or("INVALID_COLUMN: Columna no encontrada")?;
            let valor_columna = valores[index_columna];

            let valor_comparacion =
                if let Some(index_valor) = nombres_columnas.iter().position(|c| c == valor) {
                    valores[index_valor]
                } else {
                    valor
                };

            match operador.as_str() {
                "=" => Ok(valor_columna == valor_comparacion),
                "!=" => Ok(valor_columna != valor_comparacion),
                "<" => Ok(valor_columna < valor_comparacion),
                ">" => Ok(valor_columna > valor_comparacion),
                "<=" => Ok(valor_columna <= valor_comparacion),
                ">=" => Ok(valor_columna >= valor_comparacion),
                _ => Err("ERROR: Operador no soportado"),
            }
        }
        NodoAST::Not(nodo) => evaluar_ast(nodo, linea, nombres_columnas).map(|res| !res),
        NodoAST::And(nodo1, nodo2) => {
            let res1 = evaluar_ast(nodo1, linea, nombres_columnas)?;
            let res2 = evaluar_ast(nodo2, linea, nombres_columnas)?;
            Ok(res1 && res2)
        }
        NodoAST::Or(nodo1, nodo2) => {
            let res1 = evaluar_ast(nodo1, linea, nombres_columnas)?;
            let res2 = evaluar_ast(nodo2, linea, nombres_columnas)?;
            Ok(res1 || res2)
        }
    }
}
