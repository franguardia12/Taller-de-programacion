use egui::{Response, Ui};
use walkers::Projector;
use walkers::{
    extras::{Place, Places, Style},
    Plugin, Position,
};

use crate::{aeropuerto, vuelo};

#[derive(Default, Clone)]
pub struct ClickWatcher {
    pub clicked_at: Option<Position>,
}

impl ClickWatcher {
    /// Devuelve la posición en la que se hizo click
    /// en el mapa, en latitud y longitud
    pub fn get_clicked_position(&self) -> Option<Position> {
        self.clicked_at
    }
}

impl Plugin for &mut ClickWatcher {
    fn run(self: Box<Self>, _ui: &mut Ui, response: &Response, projector: &Projector) {
        if !response.changed() && response.clicked_by(egui::PointerButton::Primary) {
            self.clicked_at = response
                .interact_pointer_pos()
                .map(|p| projector.unproject(p - response.rect.center()));
        }
    }
}

/// Accede a las posiciones de cada aeropuerto en el mapa
/// y dibuja un ícono característico en cada uno de ellos
/// para identificarlos
pub fn places(aeropuertos: &mut Vec<aeropuerto::Aeropuerto>) -> impl Plugin {
    let mut places = vec![];
    for aeropuerto in aeropuertos {
        places.push(Place {
            position: Position::from_lat_lon(aeropuerto.latitud as f64, aeropuerto.longitud as f64),
            label: aeropuerto.nombre.clone(),
            symbol: '🚆',
            style: Style::default(),
        });
    }
    Places::new(places)
}

/// Accede a las posiciones de cada vuelo en el mapa
/// y dibuja un ícono característico en cada uno de ellos
/// para identificarlos
pub fn planes(vuelos: &[vuelo::Vuelo]) -> impl Plugin {
    let mut planes = vec![];
    for vuelo in vuelos
        .iter()
        .filter(|v| v.estado_vuelo != "Arrived" && v.estado_vuelo != "Boarding")
    {
        if vuelo.estado_vuelo == "Arrived" {
            continue;
        }
        if vuelo.estado_vuelo == "Boarding" {
            continue;
        }
        planes.push(Place {
            position: Position::from_lat_lon(
                vuelo.latitud_actual as f64,
                vuelo.longitud_actual as f64,
            ),
            label: format!("Vuelo {} ({})", vuelo.id, vuelo.estado_vuelo),
            symbol: '✈',
            style: Style::default(),
        });
    }
    Places::new(planes)
}
