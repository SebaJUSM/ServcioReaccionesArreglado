"""
Controladores de la API (Endpoints) para el manejo de reacciones.

Este archivo define las rutas HTTP (GET, POST, DELETE, etc.) que estarán
expuestas hacia el cliente. Cada función corresponde a un endpoint específico.

Modifica este archivo si necesitas:
- Agregar un nuevo endpoint relacionado a reacciones (ej: actualizar una reacción).
- Cambiar la lógica que se ejecuta cuando se llama a un endpoint.
- Modificar qué código HTTP retorna en caso de éxito o error.
"""

import logging

from fastapi import APIRouter, Depends, status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy import func
from sqlalchemy.exc import IntegrityError

from app.core.database import get_db
from app.core.errors import ConflictError, ResourceNotFoundError
from app.models.reaction import Reaction
from app.schemas.reaction import ReactionCreate, ReactionResponse, MessageReactionsResponse, ReactionSummary
from app.services.external import validate_message_exists, validate_user_exists
from app.services.event_producer import emit_reaction_event

# Instanciamos el router que agrupará estos endpoints
router = APIRouter()
logger = logging.getLogger(__name__)

@router.post("/reactions", response_model=ReactionResponse, status_code=status.HTTP_201_CREATED, summary="Agregar una reacción")
async def add_reaction(reaction_in: ReactionCreate, db: AsyncSession = Depends(get_db)):
    """
    Agrega una nueva reacción a un mensaje.
    Sigue el flujo de:
    1. Validar mensaje externo.
    2. Validar usuario externo.
    3. Validar duplicidad en BD local.
    4. Guardar en BD.
    5. Emitir evento a otros microservicios.
    """
    # 1. Validar el mensaje consultando al Microservicio de Mensajes
    msg_exists = await validate_message_exists(reaction_in.message_id)
    if not msg_exists:
        raise ResourceNotFoundError("Message not found", code="message_not_found")
        
    # 2. Validar el usuario consultando al Microservicio de Usuarios
    usr_exists = await validate_user_exists(reaction_in.user_id)
    if not usr_exists:
        raise ResourceNotFoundError("User not found", code="user_not_found")
        
    # 3. Validar que el usuario no haya reaccionado ya con el mismo tipo a este mensaje
    query = select(Reaction).where(
        Reaction.message_id == reaction_in.message_id,
        Reaction.user_id == reaction_in.user_id,
        Reaction.reaction_type == reaction_in.reaction_type
    )
    result = await db.execute(query)
    existing_reaction = result.scalars().first()
    
    if existing_reaction:
        raise ConflictError("User already reacted with this type", code="duplicate_reaction")
        
    # 4. Crear y guardar la reacción en nuestra BD
    new_reaction = Reaction(
        message_id=reaction_in.message_id,
        user_id=reaction_in.user_id,
        reaction_type=reaction_in.reaction_type
    )
    db.add(new_reaction)
    try:
        await db.commit()
    except IntegrityError as exc:
        await db.rollback()
        raise ConflictError("User already reacted with this type", code="duplicate_reaction") from exc
    await db.refresh(new_reaction)
    
    # 5. Emitir evento para notificar al resto del sistema (Ej: para contadores de estadísticas, notificaciones push, etc.)
    event_sent = await emit_reaction_event("REACTION_CREATED", {
        "id": new_reaction.id,
        "message_id": new_reaction.message_id,
        "user_id": new_reaction.user_id,
        "reaction_type": new_reaction.reaction_type
    })
    if not event_sent:
        logger.warning("Reaction %s was stored but its creation event was not published", new_reaction.id)
    
    return new_reaction

@router.get("/messages/{message_id}/reactions", response_model=MessageReactionsResponse, summary="Consultar las reacciones de un mensaje")
async def get_message_reactions(message_id: str, db: AsyncSession = Depends(get_db)):
    """
    Obtiene todas las reacciones asociadas a un mensaje, entregando el listado
    completo y un resumen (agrupación por tipo de reacción).
    """
    # Validar que el mensaje exista (Opcional, previene búsquedas en BD local inútiles)
    msg_exists = await validate_message_exists(message_id)
    if not msg_exists:
        raise ResourceNotFoundError("Message not found", code="message_not_found")

    # Búsqueda de todas las reacciones de un mensaje en la tabla 'reactions'
    query = select(Reaction).where(Reaction.message_id == message_id)
    result = await db.execute(query)
    reactions = result.scalars().all()
    
    # Obtener un conteo agrupado por 'reaction_type' (ej: {'like': 5, 'heart': 2})
    summary_query = select(Reaction.reaction_type, func.count(Reaction.id).label("count"))\
        .where(Reaction.message_id == message_id)\
        .group_by(Reaction.reaction_type)
    summary_result = await db.execute(summary_query)
    
    summary = [ReactionSummary(reaction_type=row[0], count=row[1]) for row in summary_result.all()]
    
    # Armamos la respuesta según el esquema 'MessageReactionsResponse'
    return MessageReactionsResponse(
        message_id=message_id,
        total_reactions=len(reactions),
        summary=summary,
        reactions=reactions
    )

@router.delete("/reactions/{reaction_id}", status_code=status.HTTP_204_NO_CONTENT, summary="Eliminar una reacción")
async def delete_reaction(reaction_id: str, db: AsyncSession = Depends(get_db)):
    """
    Elimina permanentemente una reacción utilizando su identificador único.
    Luego de eliminarla, emite un evento indicando que la reacción fue removida.
    """
    # Buscamos la reacción primero para asegurarnos de que exista
    query = select(Reaction).where(Reaction.id == reaction_id)
    result = await db.execute(query)
    reaction = result.scalars().first()
    
    if not reaction:
        raise ResourceNotFoundError("Reaction not found", code="reaction_not_found")
        
    # La borramos de la base de datos
    await db.delete(reaction)
    await db.commit()
    
    # Emitimos el evento de borrado
    event_sent = await emit_reaction_event("REACTION_DELETED", {
        "id": reaction_id,
        "message_id": reaction.message_id,
        "user_id": reaction.user_id,
        "reaction_type": reaction.reaction_type
    })
    if not event_sent:
        logger.warning("Reaction %s was deleted but its deletion event was not published", reaction_id)
    
    # Un HTTP 204 indica éxito pero que no hay cuerpo en la respuesta.
    return None
