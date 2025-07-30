#[derive(Debug, Clone)]
pub struct Aeropuerto {
    pub id: u32,
    pub nombre: String,
    pub latitud: f32,
    pub longitud: f32,
    pub fue_clickeado: bool,
}
