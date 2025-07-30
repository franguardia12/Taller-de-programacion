#!/usr/bin/env bash
set -e

# Puedes agregar aquí lo que necesites antes de arrancar,
# como setear logs, preparar directorios, etc.

echo "Levantando nodo en IP = ${NODO_IP}"
exec /usr/local/bin/cliente-servidor "${NODO_IP}"
