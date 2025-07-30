use egui::{Pos2, Rect, Sense};
use walkers::Position;

/// Detecta un click que se haya hecho en el mapa
/// de la interfaz y devuelve la posición en píxeles
/// de ese click.
pub fn detect_click_on_map(ui: &mut egui::Ui) -> Option<Pos2> {
    let map_min = Pos2::new(0.0, 0.0);
    let map_max = Pos2::new(1900.0, 769.0);
    let map_rect = Rect::from_min_max(map_min, map_max);

    let response = ui.allocate_rect(map_rect, Sense::click());

    // Si el usuario ha hecho clic dentro del rectángulo del mapa
    if response.clicked() {
        // Obtener la posición donde hizo clic
        if let Some(click_pos) = response.interact_pointer_pos() {
            return Some(click_pos);
        }
    }
    // Si no hizo clic, devolvemos None
    None
}

/// Calcula la distancia en kilómetros entre dos posiciones
/// del mapa usando la fórmula de Haversine, en donde se utilizan
/// las latitudes y longitudes de las posiciones
pub fn calcular_distancia(pos1: Position, pos2: Position) -> f64 {
    let lat1 = pos1.lat().to_radians();
    let lon1 = pos1.lon().to_radians();
    let lat2 = pos2.lat().to_radians();
    let lon2 = pos2.lon().to_radians();

    let dlat = lat2 - lat1;
    let dlon = lon2 - lon1;

    let a = (dlat / 2.0).sin().powi(2) + lat1.cos() * lat2.cos() * (dlon / 2.0).sin().powi(2);
    let c = 2.0 * a.sqrt().atan2((1.0 - a).sqrt());

    // Radio de la Tierra en kilómetros
    let r = 6371.0;

    // Distancia en kilómetros
    r * c
}

/// Verifica si la posición del click que se haya realizado
/// está cerca de la posición de un aeropuerto, calculando
/// la distancia entre ambas posiciones
pub fn esta_cerca_del_aeropuerto(click_pos: Position, airport_pos: Position) -> bool {
    let distance = calcular_distancia(click_pos, airport_pos);
    distance < 10.0 // Umbral de proximidad en kilómetros
}

/// Verifica si la posición del click que se haya realizado
/// está cerca de la posición de un vuelo, calculando
/// la distancia entre ambas posiciones
pub fn esta_cerca_del_vuelo(click_pos: Position, vuelo_pos: Position) -> bool {
    let distance = calcular_distancia(click_pos, vuelo_pos);
    distance < 10.0 // Umbral de proximidad en kilómetros
}
