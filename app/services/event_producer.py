"""
Productor de Eventos.

Este módulo se encarga de aislar la lógica de publicación de eventos hacia
el Message Broker (RabbitMQ, Kafka, etc.). Los eventos se emiten para notificar
a otros microservicios que "algo ocurrió" (ej: Reacción creada).

Modifica este archivo si necesitas:
- Instanciar un cliente real de RabbitMQ (pika, aio_pika) o Kafka (confluent-kafka).
- Cambiar el formato o estructura de cómo se serializa el evento (ej: agregar metadatos).
"""

import json
import logging
import asyncio
from app.core.config import settings
from app.core.errors import EventPublishError
from datetime import datetime, timezone
from typing import Any

import aio_pika

logger = logging.getLogger(__name__)


class RabbitMQEventProducer:
    def __init__(self):
        self.connection: aio_pika.abc.AbstractRobustConnection | None = None
        self.channel: aio_pika.abc.AbstractRobustChannel | None = None
        self.exchange: aio_pika.abc.AbstractRobustExchange | None = None

    @property
    def is_connected(self) -> bool:
        return bool(self.connection and not self.connection.is_closed)

    async def connect(self) -> None:
        if not settings.EVENTS_ENABLED or self.is_connected:
            return

        try:
            self.connection = await aio_pika.connect_robust(settings.EVENT_BROKER_URL)
            self.channel = await self.connection.channel(publisher_confirms=True)
            self.exchange = await self.channel.declare_exchange(
                settings.EVENT_EXCHANGE,
                aio_pika.ExchangeType.TOPIC,
                durable=True,
            )
            logger.info("Conectado a RabbitMQ exchange=%s", settings.EVENT_EXCHANGE)
        except Exception as exc:
            await self.close()
            message = "No se pudo conectar a RabbitMQ"
            if settings.EVENTS_REQUIRED:
                raise EventPublishError(message, details={"error": str(exc)}) from exc
            logger.warning("%s: %s", message, exc)

    async def close(self) -> None:
        if self.connection and not self.connection.is_closed:
            await self.connection.close()
        self.connection = None
        self.channel = None
        self.exchange = None

    async def publish(self, event_type: str, data: dict[str, Any]) -> bool:
        if not settings.EVENTS_ENABLED:
            logger.info("Eventos deshabilitados; no se publica %s", event_type)
            return False

        event = {
            "event_type": event_type,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "data": data,
        }
        body = json.dumps(event, default=str).encode("utf-8")
        routing_key = f"{settings.EVENT_ROUTING_KEY_PREFIX}.{event_type.lower()}"
        last_error: Exception | None = None

        for attempt in range(1, settings.EVENT_PUBLISH_RETRIES + 1):
            try:
                if not self.is_connected or self.exchange is None:
                    await self.connect()
                if self.exchange is None:
                    raise RuntimeError("RabbitMQ exchange is not available")

                await self.exchange.publish(
                    aio_pika.Message(
                        body=body,
                        content_type="application/json",
                        delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                    ),
                    routing_key=routing_key,
                )
                logger.info("Evento publicado en RabbitMQ routing_key=%s", routing_key)
                return True
            except Exception as exc:
                last_error = exc
                logger.warning(
                    "Fallo publicando evento %s intento %s/%s: %s",
                    event_type,
                    attempt,
                    settings.EVENT_PUBLISH_RETRIES,
                    exc,
                )
                await self.close()
                if attempt < settings.EVENT_PUBLISH_RETRIES:
                    await asyncio.sleep(settings.EVENT_PUBLISH_RETRY_DELAY_SECONDS)

        details = {"routing_key": routing_key, "error": str(last_error)}
        if settings.EVENTS_REQUIRED:
            raise EventPublishError("No se pudo publicar el evento en RabbitMQ", details=details)

        logger.error("Evento no publicado en RabbitMQ: %s", json.dumps(details))
        return False


event_producer = RabbitMQEventProducer()


async def connect_event_producer() -> None:
    await event_producer.connect()


async def close_event_producer() -> None:
    await event_producer.close()


async def emit_reaction_event(event_type: str, data: dict[str, Any]) -> bool:
    """
    Empaqueta y envía un evento a RabbitMQ.
    """
    return await event_producer.publish(event_type, data)
