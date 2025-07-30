#[derive(Debug, Clone)]
pub struct Vuelo {
    pub id: String,
    pub origen: String,
    pub destino: String,
    pub fecha: String,
    pub estado_vuelo: String,
    pub velocidad_actual: f32,
    pub altitud_actual: f32,
    pub latitud_actual: f32,
    pub longitud_actual: f32,
    pub combustible: f32,
}
