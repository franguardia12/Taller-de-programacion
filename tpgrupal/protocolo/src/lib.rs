pub mod parser_cql {
    pub mod condicion_where;
    pub mod consulta;
    pub mod parseo_consulta;
    pub mod type_cql;
}

pub mod serial_deserial {
    pub mod cassandra {
        pub mod deserializador_cliente_server;
        pub mod deserializador_server_cliente;
        pub mod serializador_cliente_server;
        pub mod serializador_server_cliente;
    }
    pub mod intra_nodos {
        pub mod deserializador_nodo_envio;
        pub mod deserializador_nodo_respuesta;
        pub mod serializador_nodo_envio;
        pub mod serializador_nodo_respuesta;
    }
    pub mod gossip {
        pub mod deserializador_gossip;
        pub mod serializador_gossip;
        pub mod type_message;
    }
    pub mod level_consistency;
}
