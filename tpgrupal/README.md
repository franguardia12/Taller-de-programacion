# Taller de Programacion {Antifundamentalistas del Ownership}

## Integrantes

* Blas Sebastian Chuc (110253)
* Franco Alexis Guardia (109374)
* Helen Elizabeth Chen (110195)
* Mateo Requejo (110109)

## Como usar 

A continuacion se detallan los pasos para compilar y ejecutar el programa.

### Consideraciones previas

Verificar tener instalado el comando `docker-compose`. Si alguno de los comandos de docker detallados a continuación falla por un problema de permisos, usar `sudo` al principio de cada uno.
Cada uno de los comandos debe ser ejecutado en la carpeta raíz del proyecto. 

### Como correr

1) Hacer el build del entorno Docker. Para eso, ejecutar:
* `docker-compose build`

2) Levantar los nodos. Para eso, ejecutar en terminales separadas:
* `docker-compose up node1`
* `docker-compose up node2`
* `docker-compose up node3`
* `docker-compose up node4`
* `docker-compose up node5`

3) Inicializar el Keyspace (en otra terminal)
* `docker-compose up init`

4) Levantar la interfaz (en otra terminal distinta a los nodos)
* `cargo run --bin interfaz`

5) Levantar la consola (en otra terminal distinta)
* `cargo run --bin consola`

## Limpiar la base de datos (Limpiar todo el sistema y entorno Docker):

El comando `make prune` ejecutará un Makefile que correrá los siguientes comandos:
* `docker-compose down --remove-orphans`
* `docker system prune -f`
* `docker image prune -a -f`

Si se quisiera detener un nodo en particular, ejecutar
* `docker-compose stop nodeX`

Y luego de esto, se lo puede eliminar de la red ejecutando:
* `docker-compose rm nodeX`

## Como testear
 
Para ejecutar todos los tests ejecutar `cargo test`
