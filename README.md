# Servicio de Reacciones

Una unica API REST con FastAPI para crear, consultar y eliminar reacciones a
mensajes de chat. El codigo de la aplicacion esta en `app/`.

## Ejecutar

```bash
docker compose up --build
```

Docker Compose levanta la aplicacion `reaction-service` y RabbitMQ como broker
para sus eventos. Son dos contenedores para una sola API de negocio.
La persistencia de las reacciones usa SQLite en un volumen de Docker.

- API: <http://localhost:8000>
- Swagger UI: <http://localhost:8000/docs>
- RabbitMQ Management: <http://localhost:15672>

## Endpoints REST

- `GET /health`
- `POST /api/v1/reactions`
- `GET /api/v1/messages/{message_id}/reactions`
- `DELETE /api/v1/reactions/{reaction_id}`

## Documentacion

- [Configuracion, ejecucion local y ejemplos](README-ReactionService.md).
- [Arreglos, validaciones y cautelas](INFORME_ARREGLOS.txt).
- `.env.example` contiene las variables para desarrollo local.

La validacion HTTP de usuarios y mensajes se integra con servicios externos
configurables. Esos servicios no forman parte de este proyecto; el modo local
usa validacion simulada por defecto.
