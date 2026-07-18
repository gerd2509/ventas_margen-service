# ventas-service

Microservicio de **ventas** y **margen de ventas** (extraído de sheets-api). Carga de Excel → Postgres/Neon. Usa la **misma BD y tablas** que usaba el monolito (`ventas`, `ventas_cargas`, `margen_ventas`, `margen_ventas_cargas`).

## Endpoints
- `GET  /health`
- `POST /ventas/import` — multipart `archivo` (Excel). Upsert por `CodigoCV`.
- `GET  /ventas/estado` — total + última carga.
- `GET  /ventas?anio=&mes=&sede=` — filas para los consumidores.
- `POST /margen-ventas/import` — multipart `archivo`. Reemplazo por `CodigoCV`.
- `GET  /margen-ventas/estado`
- `GET  /margen-ventas?anio=&mes=&sede=`

## Variables de entorno
- `DATABASE_URL` — cadena Postgres/Neon (obligatoria). **Nunca** en git.
- `PORT` — por defecto `4003`.

## Correr en local
```bash
npm install
# necesita DATABASE_URL (ponla en un .env o en el entorno)
npm start                 # http://localhost:4003
```

## Docker
```bash
docker build -t ventas-service .
docker run -p 4003:4003 --env-file .env ventas-service
```
(o vía `docker compose up -d` desde `api-servicios/`)
