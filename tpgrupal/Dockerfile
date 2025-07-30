FROM rust:bullseye AS builder

WORKDIR /app
COPY . .

# Aceptar como argumento el nombre del binario a compilar
ARG BIN_NAME=cliente-servidor
RUN cargo build --release --bin $BIN_NAME

FROM debian:bullseye-slim

WORKDIR /app

# Instalar dependencias mínimas necesarias
RUN apt-get update && apt-get install -y \
    netcat \
    ca-certificates \
    libx11-6 \
    libx11-dev \
    libgl1-mesa-glx \
    libgl1-mesa-dri \
    libglu1-mesa \
    libxrandr2 \
    libxi6 \
    libxcomposite1 \
    libxcursor1 \
    libxinerama1 \
    libxrender1 \
    libxkbcommon-x11-0 \
    libxkbcommon-dev && \
    rm -rf /var/lib/apt/lists/*

# healthcheck
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD nc -z localhost 9042 || exit 1

# Copiar solo el binario compilado
ARG BIN_NAME=cliente-servidor
COPY --from=builder /app/target/release/$BIN_NAME /usr/local/bin/$BIN_NAME

COPY --from=builder /app /app

# Exponer los puertos utilizados
EXPOSE 9042 9043 9044

CMD ["/usr/local/bin/$BIN_NAME", "0.0.0.0"]


