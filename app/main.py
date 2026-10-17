"""
Punto de entrada principal de la aplicación FastAPI.
Este archivo es responsable de:
1. Configurar e inicializar la aplicación FastAPI.
2. Definir los eventos de ciclo de vida (startup/shutdown) como inicializar la BD.
3. Registrar los enrutadores (routers) que contienen los diferentes endpoints de la API.

Modifica este archivo si necesitas:
- Agregar nuevos routers de otros dominios (ej: app.include_router(...))
- Añadir middleware (CORS, autenticación global, etc.)
- Modificar el título, descripción o versión global de la API.
"""

from fastapi import FastAPI, HTTPException
from fastapi.exceptions import RequestValidationError
from sqlalchemy.exc import SQLAlchemyError

from app.api.v1.reactions import router as reactions_router
from app.core.config import settings
from app.core.database import engine, Base
from app.core.errors import (
    AppError,
    app_error_handler,
    http_exception_handler,
    sqlalchemy_exception_handler,
    unhandled_exception_handler,
    validation_exception_handler,
)
from app.services.event_producer import (
    close_event_producer,
    connect_event_producer,
    event_producer,
)
import logging
from contextlib import asynccontextmanager

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Ciclo de vida de la app. Lo que esté antes del 'yield' ocurre al iniciar,
    y lo que esté después ocurre al apagar el servicio.
    """
    # Crea las tablas en la base de datos si no existen al arrancar la app.
    # En producción real, esto se suele hacer con migraciones (ej: Alembic).
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    logger.info("Base de datos inicializada")
    await connect_event_producer()
    try:
        yield
    finally:
        await close_event_producer()
        await engine.dispose()

# Inicialización principal de la app
app = FastAPI(
    title=settings.PROJECT_NAME,
    description="Microservicio de Reacciones para el trabajo final. Permite reaccionar a mensajes, borrarlas y consultarlas.",
    version="1.0.0",
    lifespan=lifespan
)

app.add_exception_handler(AppError, app_error_handler)
app.add_exception_handler(HTTPException, http_exception_handler)
app.add_exception_handler(RequestValidationError, validation_exception_handler)
app.add_exception_handler(SQLAlchemyError, sqlalchemy_exception_handler)
app.add_exception_handler(Exception, unhandled_exception_handler)

# Registramos los endpoints de reacciones bajo el prefijo /api/v1
app.include_router(reactions_router, prefix=settings.API_V1_STR, tags=["Reactions"])

@app.get("/health", tags=["Health"])
async def health_check():
    """
    Endpoint simple para verificar que el servicio está levantado.
    Utilizado por los healthchecks de Docker y Docker Compose.
    """
    return {
        "status": "ok",
        "events_enabled": settings.EVENTS_ENABLED,
        "rabbitmq_connected": event_producer.is_connected,
    }
