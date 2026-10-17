# Servicio de Reacciones

Microservicio implementado con FastAPI para gestionar reacciones a mensajes en un chat.

El despliegue se gestiona con Docker Compose: la API y RabbitMQ. Tambien se puede
ejecutar la API directamente con Python y Uvicorn.

El proyecto contiene una sola aplicacion, en `app/`. RabbitMQ es el broker de
eventos de esa aplicacion y se ejecuta en su propio contenedor.

## Caracteristicas
- **API REST**: crear, listar y eliminar reacciones.
- **Validacion externa**: valida mensajes y usuarios por HTTP cuando `EXTERNAL_VALIDATION_ENABLED=true`; en local puede usar modo simulado.
- **Eventos RabbitMQ**: publica eventos persistentes en un exchange topic de RabbitMQ usando `aio-pika`.
- **Errores estructurados**: las respuestas de error siguen el formato `{"error": {"code": "...", "message": "..."}}`.
- **Base de datos**: SQLAlchemy asincrono con SQLite.
- **Documentacion automatica**: Swagger UI (`/docs`) y ReDoc (`/redoc`).

## Ejecutar con Docker Compose

```bash
docker compose up --build
```

Luego:
- API: `http://localhost:8000`
- Swagger: `http://localhost:8000/docs`
- RabbitMQ Management: `http://localhost:15672` (`guest` / `guest`)

El `docker-compose.yml` principal levanta:
- `reaction-service` en el puerto `8000`.
- `rabbitmq` en los puertos `5672` y `15672`.
- volumen para SQLite.
- volumen para RabbitMQ.

Nota: si Docker Desktop se usa desde WSL, la integracion con esta distro debe estar activa.

## Ejecucion local

```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
uvicorn app.main:app --reload
```

## Variables importantes

- `EXTERNAL_VALIDATION_ENABLED=false`: usa modo simulado. El ID `not-found` responde como inexistente.
- `EXTERNAL_VALIDATION_ENABLED=true`: valida con `MESSAGES_SERVICE_URL/{message_id}` y `USERS_SERVICE_URL/{user_id}`.
- `EVENTS_ENABLED=true`: intenta publicar eventos en RabbitMQ.
- `EVENTS_REQUIRED=false`: la API no falla si el evento no pudo publicarse; deja advertencia en logs.
- `EVENTS_REQUIRED=true`: la API devuelve error si RabbitMQ no esta disponible o no puede publicar.
- `EVENT_EXCHANGE=reactions`: exchange topic usado para eventos.
- `EVENT_ROUTING_KEY_PREFIX=reaction`: genera routing keys como `reaction.reaction_created`.

Puedes copiar `.env.example` a `.env` para desarrollo local.

## Endpoints

- `GET /health`
- `POST /api/v1/reactions`
- `GET /api/v1/messages/{message_id}/reactions`
- `DELETE /api/v1/reactions/{reaction_id}`

Los endpoints, sus modelos de entrada/salida y sus codigos HTTP se conservan.

Ejemplo de creacion:

```json
{
  "message_id": "msg-1234",
  "user_id": "usr-5678",
  "reaction_type": "like"
}
```

## Formato de errores

```json
{
  "error": {
    "code": "duplicate_reaction",
    "message": "User already reacted with this type"
  }
}
```

```json
{
  "error": {
    "code": "validation_error",
    "message": "Request validation failed",
    "details": []
  }
}
```

## Validacion externa

En `app/services/external.py` esta implementada la integracion real hacia:
- `http://messages-service:8000/api/v1/messages/{message_id}`
- `http://users-service:8000/api/v1/users/{user_id}`

Para activarla:

```bash
EXTERNAL_VALIDATION_ENABLED=true uvicorn app.main:app --reload
```

Los servicios externos deben responder `2xx` cuando el recurso existe y `404` cuando no existe.

## Cautelas

- SQLite sirve para demo/desarrollo. Para produccion conviene PostgreSQL y migraciones con Alembic.
- `Base.metadata.create_all` crea tablas, pero no migra esquemas existentes.
- `EVENTS_REQUIRED=false` evita que RabbitMQ caido rompa la API, pero puede dejar eventos sin publicar.
- Las credenciales `guest/guest` de RabbitMQ son solo para desarrollo.
