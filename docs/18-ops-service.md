# Ops Service y logs centralizados

## Logs: dónde se escriben ahora

El código de negocio (servicios, estrategias, Saga) ya no escribe logs. Los registran tres clases de la librería `z4-shared-kafka`, que se activan solas en cada servicio:

| #   | Clase                             | Qué registra                                       |
|-----|-----------------------------------|----------------------------------------------------|
| 1   | `KafkaConsumerLoggingInterceptor` | `event_consumed` y `event_failed` de cada listener |
| 2   | `KafkaProducerLoggingListener`    | `event_published` y `event_publish_failed`         |
| 3   | `KafkaRetryErrorHandlerFactory`   | `event_retry` y `event_sent_to_dlt`                |

El estado del negocio se consulta en la base de datos, no en los logs:

| #   | Qué ver                        | Tabla                     | API                                 |
|-----|--------------------------------|---------------------------|-------------------------------------|
| 1   | Línea de tiempo de cada pedido | `purchase_saga_histories` | order-service `/ops/purchase-sagas` |
| 2   | Mensajes que fallaron          | `dead_letter_messages`    | ops-service `/ops/dead-letters`     |
| 3   | Eventos pendientes de publicar | `outbox_events`           | Pendiente                           |

## Librerías compartidas

Están en `backend/libs/`. Cada una es un proyecto Maven independiente con su propio repo git, y los servicios las consumen como dependencias:

| #   | Librería             | Contenido                                        | La usan                              |
|-----|----------------------|--------------------------------------------------|--------------------------------------|
| 1   | `z4-shared-core`     | `ResponseDto`, `BusinessException`, errores HTTP | auth, order, inventory, ops          |
| 2   | `z4-shared-security` | Validación de JWT, roles y CORS                  | auth, order, payment, inventory, ops |
| 3   | `z4-shared-kafka`    | Eventos, outbox, inbox, reintentos/DLT y logs    | order, payment, inventory            |

Antes de compilar los servicios en local hay que instalarlas en `~/.m2`, y repetirlo después de cambiar una librería:

```bash
backend/libs/install-all.sh
```

Por qué hace falta instalarlas si no tienen `main`, y cuándo repetirlo: [01-librerias-compartidas.md](../../backend/libs/docs/01-librerias-compartidas.md).

## ops-service

Puerto `8085`, base de datos `ops_db` (puerto `5437`). Escucha todos los topics `*-dlt` y guarda cada mensaje en `dead_letter_messages` con su payload, los headers de error y un estado: `PENDING`, `REPLAYED` o `DISCARDED`.

Todos los endpoints exigen un JWT con el rol `ADMIN`.

| #   | Método | Ruta                             | Qué hace                                  |
|-----|--------|----------------------------------|-------------------------------------------|
| 1   | GET    | `/ops/dead-letters`              | Bandeja paginada con filtros              |
| 2   | GET    | `/ops/dead-letters/stats`        | Cantidad por topic de origen y estado     |
| 3   | GET    | `/ops/dead-letters/{id}`         | Detalle: payload, excepción y stack trace |
| 4   | POST   | `/ops/dead-letters/{id}/replay`  | Republica en el topic original            |
| 5   | POST   | `/ops/dead-letters/{id}/discard` | Lo marca como descartado                  |

Filtros de la bandeja: `status`, `originalTopic`, `eventType`, `correlationId` y `aggregateId` (id del pedido).

`replay` y `discard` reciben `{"note": "motivo"}`; el motivo es obligatorio y queda guardado con quién lo resolvió. Solo se pueden resolver mensajes `PENDING`. Si un mensaje reprocesado vuelve a fallar, llega como una fila nueva con `replayOfId` apuntando a la anterior.

## order-service: API de Sagas

| #   | Método | Ruta                            | Qué hace                                 |
|-----|--------|---------------------------------|------------------------------------------|
| 1   | GET    | `/ops/purchase-sagas`           | Lista paginada; filtro opcional `status` |
| 2   | GET    | `/ops/purchase-sagas/{orderId}` | Estado actual y línea de tiempo completa |
