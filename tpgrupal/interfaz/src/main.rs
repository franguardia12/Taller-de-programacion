use cliente_servidor::client_services::connection::connect_to_server;
use cliente_servidor::client_services::sending::send_request;
use eframe::egui::{viewport, CentralPanel, Context};
use eframe::{App, Frame};
use interfaz::aeropuerto::Aeropuerto;
use interfaz::{aeropuerto, handler, plugins, vuelo};
use protocolo::serial_deserial::cassandra::serializador_cliente_server::query_to_bytes_client_server;
use protocolo::serial_deserial::level_consistency::LevelConsistency;
use rand::Rng;
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};
use walkers::sources::OpenStreetMap;
use walkers::{HttpTiles, Map, MapMemory, Position};

use chrono::{Local, NaiveDate};

use interfaz::window::esta_cerca_del_aeropuerto;
use interfaz::window::esta_cerca_del_vuelo;

const ESTADOS_VUELO: [&str; 4] = ["Boarding", "On-Time", "Arrived", "Delayed"];

fn main() -> Result<(), eframe::Error> {
    let options = eframe::NativeOptions {
        viewport: viewport::ViewportBuilder {
            inner_size: Some(eframe::egui::vec2(1080.0, 720.0)),
            title: Some("Aerolíneas Rústicas".to_owned()),
            resizable: Some(true),
            maximize_button: Some(false),
            ..Default::default()
        },
        centered: true,
        ..Default::default()
    };

    eframe::run_native(
        "MyApp",
        options,
        Box::new(|_cc| Ok(Box::new(MyApp::default()))),
    )
}

struct MyApp {
    tiles: Option<HttpTiles>,
    map_memory: MapMemory,
    zoom: f32,
    subventana_calendario: bool,
    subventana_vuelos: bool,
    subventana_aeropuerto_destino: bool,
    subventana_vuelo: bool,
    aeropuertos: Vec<aeropuerto::Aeropuerto>,
    vuelos: Vec<vuelo::Vuelo>,
    vuelos_fecha: Vec<vuelo::Vuelo>,
    my_position: Position,
    label_mostrar: bool,
    label_tiempo: Option<Instant>,
    vuelo_nuevo: vuelo::Vuelo,
    aeropuerto_seleccionado: Option<Aeropuerto>,
    fecha_seleccionada: Option<NaiveDate>,
    receiver: mpsc::Receiver<Message>,
    click_watcher: plugins::ClickWatcher,
    abrir_subventana_calendario: bool,
    vuelo_seleccionado: Option<vuelo::Vuelo>,
    subventana_vuelo_actualizar: bool,
    consulta_lista: bool,
}

enum Message {
    Aeropuertos(Vec<aeropuerto::Aeropuerto>),
    Vuelos(Vec<vuelo::Vuelo>),
}

impl Default for MyApp {
    fn default() -> Self {
        let (sender, receiver) = mpsc::channel();

        // Antes de la primer consulta a la base de datos tenemos que primero autenticarnos para que nos autorice a poder hacer consultas
        // Como no es necesario una autenticación con usuario y contraseña en cuanto se envíe el mensaje de STARTUP y se reciba el mensaje
        // de READY como respuesta ya se pueden empezar a hacer consultas
        handler::ejecutar_startup();

        // Lanza un hilo separado para realizar las consultas a la base de datos
        thread::spawn(move || {
            // Consulta de aeropuertos
            let consulta_cql_aeropuertos = handler::construir_consulta_select(
                "AEROPUERTOS".to_string(),
                "".to_string(),
                "".to_string(),
            );

            //En el comienzo del programa realizo una consulta SELECT a la base de datos en la que pido todos los aeropuertos guardados
            //debería recibir un vector de líneas con todas las columnas con algún valor y como sé la estructura que tiene la tabla
            //sé en qué columna está cada valor, eso uso para crear los aeropuertos y luego guardarlos en el vector de aeropuertos de
            //la estructura

            let resultado =
                handler::ejecutar_consulta(consulta_cql_aeropuertos, LevelConsistency::Strong);
            let lineas_seleccionadas_aeropuertos: Vec<String> = match resultado {
                // Esta consulta es Strong porque implica
                // consultar por un estado, en este caso de
                // un aeropuerto
                Ok(lineas) => lineas,
                Err(_) => {
                    println!("Error al ejecutar la consulta inicial: No se han podido obtener todos los aeropuertos.");
                    return;
                }
            };

            let mut aeropuertos = Vec::new();
            for linea in lineas_seleccionadas_aeropuertos.iter() {
                let campos: Vec<&str> = linea.split(",").collect();
                let aeropuerto = aeropuerto::Aeropuerto {
                    id: campos[0].parse().unwrap(),
                    nombre: campos[1].to_string(),
                    latitud: campos[2].parse().unwrap(),
                    longitud: campos[3].parse().unwrap(),
                    fue_clickeado: false,
                };
                aeropuertos.push(aeropuerto);
            }
            sender.send(Message::Aeropuertos(aeropuertos)).unwrap();

            loop {
                let consulta_cql_vuelos = handler::construir_consulta_select(
                    // Esta consulta es Weak porque es una consulta que se realiza continuamente y se usa
                    "VUELOS_ORIGEN".to_string(), // hacer un seguimiento de los vuelos, cuando estos están en curso
                    "".to_string(),
                    "".to_string(),
                );

                let resultado =
                    handler::ejecutar_consulta(consulta_cql_vuelos, LevelConsistency::Weak);
                let lineas_seleccionadas_vuelos: Vec<String> = match resultado {
                    Ok(lineas) => lineas,
                    Err(_) => {
                        println!("Error al obtener la información de los vuelos en curso.");
                        return;
                    }
                };
                let mut vuelos = Vec::new();
                for linea in lineas_seleccionadas_vuelos.iter() {
                    let campos: Vec<&str> = linea.split(",").collect();
                    let vuelo = vuelo::Vuelo {
                        origen: campos[0].to_string(),
                        fecha: campos[1].to_string(),
                        id: campos[2].parse().unwrap(),
                        destino: campos[3].to_string(),
                        estado_vuelo: campos[4].to_string(),
                        velocidad_actual: campos[5].parse().unwrap(),
                        altitud_actual: campos[6].parse().unwrap(),
                        latitud_actual: campos[7].parse().unwrap(),
                        longitud_actual: campos[8].parse().unwrap(),
                        combustible: campos[9].parse().unwrap(),
                    };
                    vuelos.push(vuelo);
                }
                sender.send(Message::Vuelos(vuelos)).unwrap();

                // Esperar un tiempo antes de realizar la siguiente consulta
                thread::sleep(Duration::from_secs(1));
            }
        });

        Self {
            tiles: None, // Inicializamos sin contexto de egui aún
            map_memory: MapMemory::default(),
            zoom: 2.5,
            subventana_calendario: false,
            subventana_vuelos: false,
            subventana_aeropuerto_destino: false,
            my_position: Position::from_lon_lat(28.83, 12.21),
            aeropuertos: vec![],
            vuelos: vec![],
            vuelos_fecha: vec![],
            label_mostrar: false,
            label_tiempo: None,
            vuelo_nuevo: vuelo::Vuelo {
                id: "".to_string(),
                origen: "".to_string(),
                destino: "".to_string(),
                fecha: "".to_string(),
                estado_vuelo: "Boarding".to_string(),
                velocidad_actual: 0.0,
                altitud_actual: 0.0,
                latitud_actual: 0.0,
                longitud_actual: 0.0,
                combustible: 100.0,
            },
            aeropuerto_seleccionado: None,
            fecha_seleccionada: None,
            receiver,
            click_watcher: Default::default(),
            abrir_subventana_calendario: false,
            subventana_vuelo: false,
            vuelo_seleccionado: None,
            subventana_vuelo_actualizar: false,
            consulta_lista: false,
        }
    }
}

impl App for MyApp {
    fn update(&mut self, ctx: &Context, _frame: &mut Frame) {
        // Recibe los resultados del canal y actualiza el estado de la aplicación
        while let Ok(message) = self.receiver.try_recv() {
            match message {
                Message::Aeropuertos(aeropuertos) => {
                    self.aeropuertos = aeropuertos;
                }
                Message::Vuelos(vuelos) => {
                    self.vuelos = vuelos;
                }
            }
        }

        // Redibujar el mapa
        ctx.request_repaint();

        CentralPanel::default().show(ctx, |ui| {
            // Si tiles aún no está inicializado, lo hacemos ahora que tenemos el contexto.
            if self.tiles.is_none() {
                self.tiles = Some(HttpTiles::new(OpenStreetMap, ctx.clone()));
                self.map_memory.set_zoom(self.zoom.into()).unwrap();
            }

            let resultado = self.map_memory.detached();
            let posicion_actual: Position = match resultado {
                Some(centro) => centro,
                None => self.my_position,
            };

            if let Some(tiles) = &mut self.tiles {
                let map = Map::new(Some(tiles), &mut self.map_memory, posicion_actual);

                let map = map
                    .with_plugin(plugins::places(&mut self.aeropuertos))
                    .with_plugin(plugins::planes(&self.vuelos))
                    .with_plugin(&mut self.click_watcher);

                ui.add(map);

                // Redibujar el mapa
                ctx.request_repaint();

                if let Some(position) = self.click_watcher.get_clicked_position() {
                    // Iterar sobre los aeropuertos y verificar si el clic está cerca de alguno
                    for aeropuerto in &self.aeropuertos {
                        let airport_position = Position::from_lon_lat(
                            aeropuerto.longitud.into(),
                            aeropuerto.latitud.into(),
                        );
                        if esta_cerca_del_aeropuerto(position, airport_position) {
                            self.aeropuerto_seleccionado = Some((*aeropuerto).clone());
                            self.abrir_subventana_calendario = true; // Establecer el flag en true
                            break;
                        }
                    }

                    for vuelo in &self.vuelos {
                        let vuelo_position = Position::from_lon_lat(
                            vuelo.longitud_actual.into(),
                            vuelo.latitud_actual.into(),
                        );
                        if esta_cerca_del_vuelo(position, vuelo_position)
                            && vuelo.estado_vuelo != "Boarding"
                            && vuelo.estado_vuelo != "Arrived"
                        {
                            self.vuelo_seleccionado = Some(vuelo.clone());
                            self.subventana_vuelo = true; // Establecer el flag en true
                            break;
                        }
                    }
                }

                // Cerrar la subventana de fechas si se abre otra subventana
                if self.subventana_vuelos || self.subventana_aeropuerto_destino {
                    self.subventana_calendario = false;
                }

                if self.abrir_subventana_calendario {
                    self.subventana_calendario = true;
                    self.abrir_subventana_calendario = false; // Restablecer el flag

                    //Que la fecha vuelva a ser la actual
                    let today = Local::now().naive_local();
                    self.fecha_seleccionada = Some(today.into());
                }

                // Subventana 1: fechas en el calendario
                if self.subventana_calendario {
                    self.consulta_lista = false;
                    let mut ventana_abierta_calendario = true;
                    egui::Window::new("Fechas de vuelos")
                        .open(&mut ventana_abierta_calendario) // Se cierra cuando es `false`
                        .resizable(false)
                        .collapsible(false)
                        .show(ctx, |ui| {
                            ui.heading("Selecciona una fecha");
                            let today = Local::now().naive_local();
                            let mut selected_date = self.fecha_seleccionada.unwrap_or(today.into());

                            // Mostrar el calendario
                            ui.horizontal(|ui| {
                                if ui.button("<").clicked() {
                                    selected_date =
                                        selected_date.pred_opt().unwrap_or(selected_date);
                                    self.fecha_seleccionada = Some(selected_date);
                                }
                                ui.label(format!("{}", selected_date));
                                if ui.button(">").clicked() {
                                    selected_date =
                                        selected_date.succ_opt().unwrap_or(selected_date);
                                    self.fecha_seleccionada = Some(selected_date);
                                }
                            });

                            if ui.button("Seleccionar").clicked() {
                                self.fecha_seleccionada = Some(selected_date);
                                self.subventana_calendario = false; // Cerrar la subventana de calendario
                                self.subventana_vuelos = true; // Abrir la subventana de vuelos
                            }
                        });

                    self.subventana_calendario =
                        ventana_abierta_calendario && !self.subventana_vuelos;
                }

                // Subventana 2: mostrar contenido basado en la opción seleccionada

                //En este punto ya tengo el aeropuerto más cercano a donde se hizo click y la fecha seleccionada
                //ahora tengo que hacer una nueva consulta a la base de datos pidiéndole todos los vuelos
                //entrantes y salientes en la fecha indicada de ese aeropuerto, cuando se reciban se muestran
                //uno abajo del otro

                //Al final de todos los vuelos mostrados aparece el botón para agregar un vuelo nuevo, debería
                //ser un scroll area para que si hay muchos vuelos se puedan ver todos
                if self.subventana_vuelos {
                    if !self.consulta_lista {
                        let consulta_cql_vuelos_origen_fecha = handler::construir_consulta_select(
                            // Esta consulta es Strong porque es una consulta de vuelos que
                            "VUELOS_ORIGEN".to_string(), // se hace al hacer click en un aeropuerto y seleccionar una fecha
                            self.aeropuerto_seleccionado.clone().unwrap().nombre.clone(), // por lo que no implica vuelos en curso
                            self.fecha_seleccionada.unwrap().to_string(),
                        );

                        let resultado = handler::ejecutar_consulta(
                            consulta_cql_vuelos_origen_fecha,
                            LevelConsistency::Strong,
                        );
                        let lineas_seleccionadas_vuelos_origen_fecha: Vec<String> = match resultado
                        {
                            Ok(lineas) => lineas,
                            Err(_) => {
                                println!("Error al obtener los vuelos salientes del aeropuerto seleccionado.");
                                return;
                            }
                        };
                        self.vuelos_fecha.clear();
                        for linea in lineas_seleccionadas_vuelos_origen_fecha.iter() {
                            let campos: Vec<&str> = linea.split(",").collect();
                            let vuelo = vuelo::Vuelo {
                                origen: campos[0].to_string(),
                                fecha: campos[1].to_string(),
                                id: campos[2].to_string(),
                                destino: campos[3].to_string(),
                                estado_vuelo: campos[4].to_string(),
                                velocidad_actual: campos[5].parse().unwrap(),
                                altitud_actual: campos[6].parse().unwrap(),
                                latitud_actual: campos[7].parse().unwrap(),
                                longitud_actual: campos[8].parse().unwrap(),
                                combustible: campos[9].parse().unwrap(),
                            };
                            self.vuelos_fecha.push(vuelo);
                        }

                        let consulta_cql_vuelos_destino_fecha = handler::construir_consulta_select(
                            // Esta consulta es Strong porque es una consulta de vuelos que
                            "VUELOS_DESTINO".to_string(), // se hace al hacer click en un aeropuerto y seleccionar una fecha
                            self.aeropuerto_seleccionado.clone().unwrap().nombre.clone(), // por lo que no implica vuelos en curso
                            self.fecha_seleccionada.unwrap().to_string(),
                        );

                        let resultado = handler::ejecutar_consulta(
                            consulta_cql_vuelos_destino_fecha,
                            LevelConsistency::Strong,
                        );

                        let consulta_cql_vuelos_destino_fecha: Vec<String> = match resultado {
                            Ok(lineas) => lineas,
                            Err(_) => {
                                println!("Error al obtener los vuelos entrantes del aeropuerto seleccionado.");
                                return;
                            }
                        };
                        for linea in consulta_cql_vuelos_destino_fecha.iter() {
                            let campos: Vec<&str> = linea.split(",").collect();
                            let vuelo = vuelo::Vuelo {
                                destino: campos[0].to_string(),
                                fecha: campos[1].to_string(),
                                id: campos[2].to_string(),
                                origen: campos[3].to_string(),
                                estado_vuelo: campos[4].to_string(),
                                velocidad_actual: campos[5].parse().unwrap(),
                                altitud_actual: campos[6].parse().unwrap(),
                                latitud_actual: campos[7].parse().unwrap(),
                                longitud_actual: campos[8].parse().unwrap(),
                                combustible: campos[9].parse().unwrap(),
                            };
                            self.vuelos_fecha.push(vuelo);
                        }
                        self.consulta_lista = true;
                    }

                    let mut ventana_abierta_vuelos = true;
                    egui::Window::new("Lista de vuelos")
                        .open(&mut ventana_abierta_vuelos) // Se cierra cuando es `false`
                        .resizable(false)
                        .collapsible(false)
                        .show(ctx, |ui| {
                            ui.heading(format!("Vuelos del {}", self.fecha_seleccionada.unwrap()));

                            egui::ScrollArea::vertical().show(ui, |ui| {
                                for vuelo in &self.vuelos_fecha {
                                    egui::Frame::group(ui.style()).show(ui, |ui| {
                                        ui.label(format!("ID: {}", vuelo.id));
                                        ui.label(format!("Origen: {}", vuelo.origen));
                                        ui.label(format!("Destino: {}", vuelo.destino));
                                        ui.label(format!("Fecha: {}", vuelo.fecha));
                                        ui.label(format!("Estado: {}", vuelo.estado_vuelo));
                                        ui.label(format!(
                                            "Velocidad: {} km/h",
                                            vuelo.velocidad_actual
                                        ));
                                        ui.label(format!("Altitud: {} m", vuelo.altitud_actual));
                                        ui.label(format!("Latitud: {}", vuelo.latitud_actual));
                                        ui.label(format!("Longitud: {}", vuelo.longitud_actual));
                                        ui.label(format!("Combustible: {} L", vuelo.combustible));
                                    });
                                    ui.separator(); // Añade una línea de separación entre los recuadros
                                }

                                if ui.button("Agregar un vuelo").clicked() {
                                    self.subventana_aeropuerto_destino = true;
                                }
                            });
                        });
                    self.subventana_vuelos =
                        ventana_abierta_vuelos && !self.subventana_aeropuerto_destino;
                }

                // Subventana 3: seleccionar el aeropuerto de destino
                if self.subventana_aeropuerto_destino {
                    self.consulta_lista = false;
                    self.vuelo_nuevo.origen =
                        self.aeropuerto_seleccionado.clone().unwrap().nombre.clone();
                    self.vuelo_nuevo.latitud_actual =
                        self.aeropuerto_seleccionado.clone().unwrap().latitud;
                    self.vuelo_nuevo.longitud_actual =
                        self.aeropuerto_seleccionado.clone().unwrap().longitud;
                    self.vuelo_nuevo.fecha = self.fecha_seleccionada.unwrap().to_string();
                    let mut ventana_abierta_aeropuertos = true;
                    egui::Window::new("Agregar un vuelo")
                        .open(&mut ventana_abierta_aeropuertos) // Se cierra cuando es `false`
                        .resizable(false)
                        .collapsible(false)
                        .show(ctx, |ui| {
                            ui.heading("Selecciona el aeropuerto de destino");
                            ui.vertical(|ui| {
                                for aeropuerto in &self.aeropuertos {
                                    if aeropuerto.nombre == self.vuelo_nuevo.origen {
                                        continue;
                                    }
                                    if ui.button(aeropuerto.nombre.clone()).clicked() {
                                        let mut rng = rand::thread_rng();
                                        let random_id = rng.gen_range(0..=499);
                                        let id_str = random_id.to_string();
                                        let nuevo_id = "VUE".to_string() + &id_str;
                                        self.vuelo_nuevo.destino = aeropuerto.nombre.clone();
                                        self.label_mostrar = true;
                                        self.vuelo_nuevo.id = nuevo_id;
                                        self.label_tiempo = Some(Instant::now());

                                        //En este punto el vuelo a agregar ya está completamente armado por lo tanto puede
                                        //llamarse al handler para que este arme la consulta y la envíe al servidor para
                                        //que este la procese y la almacene en la base de datos.

                                        // Como se va a insertar un vuelo nuevo entonces en el siguiente frame deben actualizarse los vuelos
                                        let consulta1 = handler::construir_consulta_insert(
                                            &self.vuelo_nuevo,
                                            "VUELOS_ORIGEN".to_string(),
                                        );

                                        let consulta2 = handler::construir_consulta_insert(
                                            &self.vuelo_nuevo,
                                            "VUELOS_DESTINO".to_string(),
                                        );

                                        let consulta_serializada1 = query_to_bytes_client_server(
                                            // Esta consulta es Strong porque es una consulta que permite
                                            &consulta1, // editar el estado de un vuelo, en este caso agregando uno nuevo
                                            LevelConsistency::Strong,
                                            0x00,
                                        );

                                        let consulta_serializada2 = query_to_bytes_client_server(
                                            // Esta consulta es Strong porque es una consulta que permite
                                            &consulta2, // editar el estado de un vuelo, en este caso agregando uno nuevo
                                            LevelConsistency::Strong,
                                            0x00,
                                        );

                                        let mut conexion = connect_to_server();

                                        if let Ok(ref mut socket) = conexion {
                                            let _ = send_request(socket, consulta_serializada1);
                                        }

                                        let mut conexion2 = connect_to_server();

                                        if let Ok(ref mut socket) = conexion2 {
                                            let _ = send_request(socket, consulta_serializada2);
                                        }
                                    }
                                }
                            });
                            // Mostrar el label si no ha pasado el tiempo deseado
                            if self.label_mostrar {
                                if let Some(tiempo) = self.label_tiempo {
                                    if tiempo.elapsed() < Duration::from_secs(3) {
                                        ui.label("Vuelo agregado.");
                                    } else {
                                        self.label_mostrar = false;
                                    }
                                }
                            }
                        });
                    self.subventana_aeropuerto_destino =
                        ventana_abierta_aeropuertos && !self.label_mostrar;
                }

                //Subventana 4: mostrar información de un vuelo seleccionado
                if self.subventana_vuelo
                    && self.vuelo_seleccionado.clone().unwrap().estado_vuelo != "Finalized"
                {
                    self.consulta_lista = false;
                    let mut ventana_abierta_vuelo = true;
                    egui::Window::new("Información del vuelo")
                        .open(&mut ventana_abierta_vuelo) // Se cierra cuando es `false`
                        .resizable(false)
                        .collapsible(false)
                        .show(ctx, |ui| {
                            ui.heading(format!(
                                "Vuelo {}",
                                self.vuelo_seleccionado.clone().unwrap().id
                            ));
                            ui.label(format!(
                                "Origen: {}",
                                self.vuelo_seleccionado.clone().unwrap().origen
                            ));
                            ui.label(format!(
                                "Destino: {}",
                                self.vuelo_seleccionado.clone().unwrap().destino
                            ));
                            ui.label(format!(
                                "Fecha: {}",
                                self.vuelo_seleccionado.clone().unwrap().fecha
                            ));
                            ui.label(format!(
                                "Estado: {}",
                                self.vuelo_seleccionado.clone().unwrap().estado_vuelo
                            ));
                            ui.label(format!(
                                "Velocidad: {} km/h",
                                self.vuelo_seleccionado.clone().unwrap().velocidad_actual
                            ));
                            ui.label(format!(
                                "Altitud: {} m",
                                self.vuelo_seleccionado.clone().unwrap().altitud_actual
                            ));
                            ui.label(format!(
                                "Latitud: {}",
                                self.vuelo_seleccionado.clone().unwrap().latitud_actual
                            ));
                            ui.label(format!(
                                "Longitud: {}",
                                self.vuelo_seleccionado.clone().unwrap().longitud_actual
                            ));
                            ui.label(format!(
                                "Combustible: {} L",
                                self.vuelo_seleccionado.clone().unwrap().combustible
                            ));

                            if ui.button("Actualizar vuelo").clicked() {
                                self.subventana_vuelo_actualizar = true;
                            }
                        });
                    // Actualizar el estado de 'self.subventana_vuelo'
                    if !ventana_abierta_vuelo || self.subventana_vuelo_actualizar {
                        self.subventana_vuelo = false;
                    }
                }

                //Subventana 5: actualizar información de un vuelo seleccionado
                if self.subventana_vuelo_actualizar {
                    self.consulta_lista = false;
                    let mut ventana_abierta_vuelo_actualizar = true;
                    egui::Window::new("Actualizar información del vuelo")
                        .open(&mut ventana_abierta_vuelo_actualizar) // Se cierra cuando es `false`
                        .resizable(false)
                        .collapsible(false)
                        .show(ctx, |ui| {
                            ui.heading(format!(
                                "Actualizar vuelo {}",
                                self.vuelo_seleccionado.clone().unwrap().id
                            ));
                            ui.label("Selecciona el nuevo estado del vuelo:");

                            // Campos a actualizar
                            let mut estado_vuelo: String =
                                self.vuelo_seleccionado.clone().unwrap().estado_vuelo;

                            ui.vertical(|ui| {
                                for estado in ESTADOS_VUELO.iter() {
                                    if estado
                                        == &self.vuelo_seleccionado.clone().unwrap().estado_vuelo
                                    {
                                        continue;
                                    }
                                    if ui.button(*estado).clicked() {
                                        estado_vuelo = estado.to_string();

                                        // Realizar la consulta UPDATE a la base de datos
                                        self.subventana_vuelo_actualizar = false;
                                        self.subventana_vuelo = false;

                                        let vuelo_seleccionado =
                                            self.vuelo_seleccionado.clone().unwrap();
                                        if estado_vuelo != vuelo_seleccionado.estado_vuelo {
                                            // Se modificó el estado del vuelo
                                            let consulta_update_estado1 =
                                                handler::construir_consulta_update(
                                                    &vuelo_seleccionado,
                                                    "VUELOS_ORIGEN".to_string(),
                                                    "ESTADO_VUELO".to_string(),
                                                    estado_vuelo.clone(),
                                                );

                                            let consulta_update_estado2 =
                                                handler::construir_consulta_update(
                                                    &vuelo_seleccionado,
                                                    "VUELOS_DESTINO".to_string(),
                                                    "ESTADO_VUELO".to_string(),
                                                    estado_vuelo.clone(),
                                                );

                                            let consulta_serializada_estado1 =
                                                query_to_bytes_client_server(
                                                    // Esta consulta es Strong porque es una consulta que permite
                                                    &consulta_update_estado1, // editar el estado de un vuelo, en este caso actualizando uno
                                                    LevelConsistency::Strong, // ya existente
                                                    0x00,
                                                );

                                            let consulta_serializada_estado2 =
                                                query_to_bytes_client_server(
                                                    // Esta consulta es Strong porque es una consulta que permite
                                                    &consulta_update_estado2, // editar el estado de un vuelo, en este caso actualizando uno
                                                    LevelConsistency::Strong, // ya existente
                                                    0x00,
                                                );

                                            let mut conexion = connect_to_server();

                                            if let Ok(ref mut socket) = conexion {
                                                let _ = send_request(socket, consulta_serializada_estado1);
                                            }

                                            let mut conexion2 = connect_to_server();

                                            if let Ok(ref mut socket) = conexion2 {
                                                let _ = send_request(socket, consulta_serializada_estado2);
                                            }
                                        }
                                    }
                                }
                            });
                        });
                }
            }
        });
    }
}
