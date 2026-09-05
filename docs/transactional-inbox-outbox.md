# Kafka: patrones Transactional Outbox e Inbox

Los patrones **Transactional Outbox** e **Inbox** ayudan a mantener consistencia cuando un microservicio combina una base de datos local con mensajería asíncrona. En Z4 Event Commerce, los servicios guardan su información en PostgreSQL y se comunican mediante Kafka.

Estos patrones no reemplazan a Kafka, a la Saga ni a los reintentos. Cada mecanismo resuelve un problema diferente.

## El problema de la doble escritura

Supongamos que Inventory debe realizar dos operaciones:

1. Reservar stock en PostgreSQL.
2. Publicar `STOCK_RESERVED` en Kafka.

PostgreSQL y Kafka son sistemas independientes. Una transacción local de Spring no puede confirmar ambos cambios de manera atómica.

Una implementación directa podría ser:

```java
inventoryRepository.reserveIfAvailable(productId, quantity);
kafkaTemplate.send("inventory-events-topic", event);
```

Esta operación se conoce como **dual write** o doble escritura. Puede fallar de distintas maneras.

### PostgreSQL confirma y Kafka falla

```text
Inventory descuenta stock
        ↓
PostgreSQL confirma
        ↓
Kafka no recibe STOCK_RESERVED
```

El stock queda reservado, pero Order Service nunca conoce el resultado y la Saga puede permanecer incompleta.

### Kafka confirma y PostgreSQL revierte

```text
Inventory publica STOCK_RESERVED
        ↓
Kafka confirma
        ↓
La transacción PostgreSQL falla y hace rollback
```

Order Service recibe que el stock fue reservado, aunque el cambio no existe en la base de Inventory.

Publicar al final del método no elimina este riesgo. Spring normalmente confirma la transacción después de que el método transaccional retorna, y siempre puede ocurrir una caída entre dos sistemas independientes.

## Transactional Outbox

Transactional Outbox es un **patrón de integración y consistencia de datos en sistemas distribuidos**. Su objetivo es guardar de forma atómica el cambio de negocio y la intención de publicar un evento.

En lugar de escribir directamente en PostgreSQL y Kafka, Inventory escribe únicamente en PostgreSQL durante la transacción principal:

```text
Transacción PostgreSQL de Inventory
├── actualiza inventories
├── registra inventory_reservations
├── registra inbox_events
└── inserta STOCK_RESERVED en outbox_events como PENDING
```

Si cualquiera de estas operaciones falla, PostgreSQL revierte todas. Por lo tanto, no puede existir una reserva confirmada sin que también exista la intención persistente de publicar su resultado.

### Publicación del Outbox

Otro componente llamado **Outbox Publisher** consulta los registros pendientes:

```text
outbox_events: PENDING
        ↓
Outbox Publisher
        ↓
inventory-events-topic
        ↓
ACK de Kafka
        ↓
outbox_events: PUBLISHED
```

En la implementación inicial de Inventory, este componente es un scheduler que ejecuta periódicamente los siguientes pasos:

1. Busca eventos `PENDING` por orden de creación.
2. Publica el payload almacenado utilizando la misma key y el mismo `eventId`.
3. Espera el ACK de Kafka.
4. Cambia el estado a `PUBLISHED` solamente después de recibir la confirmación.
5. Si falla, conserva el evento como `PENDING`, incrementa `attempts` y registra `last_error`.

`PENDING` y `PUBLISHED` son estados de una fila en PostgreSQL. No son topics ni tipos de evento enviados a Kafka.

### Tabla Outbox del proyecto

La tabla `outbox_events` conserva la información necesaria para publicar posteriormente:

| Campo            | Finalidad                                              |
|------------------|--------------------------------------------------------|
| `event_id`       | Identificador estable del evento.                      |
| `aggregate_id`   | Pedido o agregado relacionado.                         |
| `correlation_id` | Identificador para seguir la Saga completa.            |
| `event_type`     | Por ejemplo, `STOCK_RESERVED`.                         |
| `topic`          | Topic Kafka de destino.                                |
| `event_key`      | Key utilizada para conservar el orden por pedido.      |
| `payload`        | Evento serializado que se publicará sin reconstruirlo. |
| `status`         | `PENDING` o `PUBLISHED`.                               |
| `attempts`       | Cantidad de publicaciones fallidas.                    |
| `last_error`     | Último error de publicación.                           |
| `created_at`     | Momento en que nació el evento.                        |
| `published_at`   | Momento en que Kafka confirmó la publicación.          |

## Inbox

Inbox es un **patrón de idempotencia para consumidores de mensajes**. Su objetivo es evitar que un evento recibido más de una vez produzca nuevamente sus efectos de negocio.

Kafka y Transactional Outbox normalmente ofrecen un procesamiento de tipo **at least once**: un mensaje no debería perderse, pero puede entregarse o publicarse más de una vez.

Por ejemplo:

```text
Outbox Publisher publica STOCK_RESERVED
        ↓
Kafka confirma
        ↓
Inventory cae antes de marcar PUBLISHED
        ↓
Al reiniciar publica nuevamente el mismo evento
```

Esta duplicación es válida y esperada. El consumidor debe reconocer el mismo `eventId` y no repetir su operación.

### Procesamiento con Inbox

```text
Evento recibido desde Kafka
        ↓
¿event_id existe en inbox_events?
        ├── Sí → ignorar duplicado
        └── No → ejecutar operación de negocio
                    ↓
                 registrar event_id en inbox_events
```

En Inventory, la modificación del dominio, el registro del Inbox y la creación del Outbox pertenecen a la misma transacción:

```text
BEGIN
  verificar inbox_events
  reservar o liberar inventario
  insertar evento saliente en outbox_events
  insertar evento recibido en inbox_events
COMMIT
```

Si la transacción falla, tampoco se registra el Inbox. Kafka puede entregar nuevamente el mensaje y el servicio tendrá otra oportunidad de procesarlo.

### Tabla Inbox del proyecto

| Campo          | Finalidad                                                      |
|----------------|----------------------------------------------------------------|
| `event_id`     | Clave primaria que impide registrar dos veces el mismo evento. |
| `event_type`   | Tipo del evento consumido.                                     |
| `processed_at` | Momento en que terminó el procesamiento local.                 |

El nombre `processed_events` también habría sido válido, pero `inbox_events` hace explícito el patrón arquitectónico utilizado.

## Cómo se complementan

```text
Kafka
  ↓ evento recibido
Inbox
  ↓ evita repetir la entrada
Lógica de negocio
  ↓ cambia el estado local
Outbox
  ↓ garantiza una salida persistente
Kafka
```

| Patrón | Protege | Pregunta que responde |
|---|---|---|
| Inbox | Entrada del servicio | ¿Ya procesé este `eventId`? |
| Outbox | Salida del servicio | ¿Quedó registrada la intención de publicar este evento? |

Utilizar Outbox sin Inbox puede producir efectos duplicados. Utilizar Inbox sin Outbox evita repetir entradas, pero no evita perder eventos salientes.

## Garantía real: at least once

Transactional Outbox no ofrece por sí solo un `exactly once` global entre todos los servicios. La garantía práctica es:

```text
Persistir el evento de salida al menos una vez
                    +
Publicarlo una o más veces
                    +
Procesarlo efectivamente una sola vez mediante idempotencia
```

Puede existir una caída después del ACK de Kafka y antes de actualizar `PUBLISHED`. En ese caso, el evento se publica nuevamente. Conservar el mismo `eventId` permite que el Inbox del consumidor lo reconozca como duplicado.

## Diferencia frente a reintentos y DLT

| Mecanismo                 | Problema que resuelve                                                             |
|---------------------------|-----------------------------------------------------------------------------------|
| Outbox                    | Evita perder un evento saliente cuando existe un cambio confirmado en PostgreSQL. |
| Inbox                     | Evita repetir efectos cuando llega un evento duplicado.                           |
| Reintentos del consumidor | Vuelven a ejecutar temporalmente un mensaje que falló al ser consumido.           |
| DLT                       | Aísla mensajes que no pudieron procesarse para investigarlos o reprocesarlos.     |
| Saga                      | Coordina el proceso de negocio entre varios servicios y sus compensaciones.       |

Un mensaje en DLT representa un fallo al consumir. Un evento `PENDING` en Outbox representa una publicación todavía no confirmada. Aunque ambos requieren monitoreo, no significan lo mismo.

## Implementación actual y evolución

Actualmente el patrón se implementa primero en `inventory-service` como piloto:

- `inbox_events` evita repetir reservas o liberaciones.
- `outbox_events` conserva `STOCK_RESERVED`, `STOCK_NOT_AVAILABLE` y `STOCK_RELEASED` antes de publicarlos.
- Un scheduler publica los registros pendientes.

Después de validar el comportamiento, el mismo principio debe aplicarse a los demás servicios que modifican su base y publican eventos, especialmente Order y Payment.

Para una evolución más cercana a producción se pueden agregar:

- Procesamiento seguro cuando existen varias instancias del mismo servicio.
- Bloqueo o reclamación de filas pendientes por lote.
- Backoff entre intentos de publicación.
- Métricas y alertas por cantidad o antigüedad de eventos `PENDING`.
- Política de retención y limpieza de eventos `PUBLISHED`.
- Publicación mediante CDC con Debezium en lugar de polling.

El scheduler es apropiado para aprender y para una primera implementación. Debezium es una evolución posible, no un requisito para que el patrón sea válido.

## Resumen

```text
Inbox  = no repetir lo que recibo
Outbox = no perder lo que debo publicar
Saga   = coordinar el proceso distribuido
DLT    = aislar lo que no pudo procesarse
```

Juntos, Inbox y Transactional Outbox permiten construir flujos Kafka confiables sin utilizar una transacción distribuida entre PostgreSQL y Kafka.
