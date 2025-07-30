//! #SQL rústico
//! Resolución del ejercicio individual de la materia taller de programación
//!
//! * módulo [validaciones]:
//!     En este módulo se definen funciones que realizan validaciones generales
//!     de las consultas
//! * módulo [condiciones]:
//!     En este módulo se definen funciones que evalúan las condiciones de búsqueda
//!     de la consulta en cada línea del archivo que se está leyendo
//! * módulo [parseo_consulta]:
//!     En este módulo se definen funciones que parsean la consulta recibida y
//!     extraen los datos relevantes particulares para cada operación
//! * módulo [logica_select]:
//!     En este módulo se define la lógica de la operación SELECT
//! * módulo [logica_update]:
//!     En este módulo se define la lógica de la operación UPDATE
//! * módulo [logica_insert]:
//!     En este módulo se define la lógica de la operación INSERT
//! * módulo [logica_delete]:
//!     En este módulo se define la lógica de la operación DELETE

/// Lógica de chequeo de condiciones de búsqueda
pub mod condiciones;
/// Implementación de DELETE
pub mod logica_delete;
/// Implementación de INSERT
pub mod logica_insert;
/// Implementación de SELECT
pub mod logica_select;
/// Implementación de UPDATE
pub mod logica_update;
/// Parseo de las consultas
pub mod parseo_consulta;
/// Validaciones generales de las consultas
pub mod validaciones;
