# El mensaje: IntegrationEventDto

Todo lo que viaja por Kafka entre los servicios tiene el mismo formato: un **sobre** (`IntegrationEventDto`) con datos fijos para rastrear el evento y un `payload` libre con los datos de negocio. Lo define la librería `z4-shared-kafka`, en el paquete `com.z4greed.shared.kafka.event`.

| #   | Clase                     | Qué hace                                                          |
|-----|---------------------------|-------------------------------------------------------------------|
| 1   | `IntegrationEventDto`     | El formato del mensaje (un `record` con `@Builder`)               |
| 2   | `IntegrationEventFactory` | Crea eventos con el formato completo; nadie usa el builder a mano |
| 3   | `IntegrationEventReader`  | Convierte el JSON recibido en `IntegrationEventDto`               |

`IntegrationEventDtoBuilder` no existe en el código fuente: lo genera Lombok a partir del `@Builder`. Por eso al abrirlo en IntelliJ aparece decompilado y con `@Generated` en cada campo.

## Un mensaje real

Este es el `RESERVE_STOCK` del pedido 7, tal como quedó en `outbox_events` y como lo recibió inventory-service:

```json
{
  "eventId": "519761c5-8b39-433c-9d5c-87abbefaf821",
  "eventType": "RESERVE_STOCK",
  "eventVersion": 1,
  "aggregateId": "7",
  "correlationId": "purchase-0a9d5a0d-cad0-4836-97f5-af4bdb320344",
  "previousEventId": "e3b923c2-5666-4852-9635-fe9d27b266ed",
  "timestamp": "2026-11-22T15:40:30.187309",
  "producer": "order-service",
  "payload": {
    "customerId": 1,
    "amount": 149.90,
    "currency": "PEN",
    "paymentToken": "TEST_APPROVED",
    "items": [
      { "productId": 100, "productName": "Mechanical Keyboard", "unitPrice": 149.90, "quantity": 1 }
    ]
  }
}
```

La key del mensaje en Kafka es `"7"`, el `aggregateId`.

## Los campos

| #   | Campo             | Tipo            | Quién lo pone      | Para qué                                    |
|-----|-------------------|-----------------|--------------------|---------------------------------------------|
| 1   | `eventId`         | `String` (UUID) | La factory         | Id único; el inbox lo usa contra duplicados |
| 2   | `eventType`       | `String`        | Quien publica      | Qué pasó; elige la estrategia               |
| 3   | `eventVersion`    | `Integer`       | La factory         | Versión del formato del `payload`           |
| 4   | `aggregateId`     | `String`        | order, o se hereda | Id del pedido y key de Kafka                |
| 5   | `correlationId`   | `String`        | order, o se hereda | Une los eventos de una compra               |
| 6   | `previousEventId` | `String`        | order, o se hereda | Evento que provocó este                     |
| 7   | `timestamp`       | `LocalDateTime` | La factory         | Cuándo se creó (no cuándo se publicó)       |
| 8   | `producer`        | `String`        | La factory         | Servicio que lo creó                        |
| 9   | `payload`         | `JsonNode`      | Quien publica      | Datos de negocio, según el `eventType`      |

### `aggregateId`, `correlationId` y `previousEventId`

Los tres sirven para rastrear, pero responden preguntas distintas:

| #   | Campo             | Pregunta                    | ¿Cambia entre eventos de la misma compra? |
|-----|-------------------|-----------------------------|-------------------------------------------|
| 1   | `aggregateId`     | ¿De qué pedido habla?       | No                                        |
| 2   | `correlationId`   | ¿De qué compra forma parte? | No                                        |
| 3   | `previousEventId` | ¿Qué evento lo provocó?     | Sí, en cada evento                        |

- **`aggregateId`:** al usarse como key, todos los eventos de un pedido caen en la misma partición y se procesan en orden. order-service lo usa además para buscar el pedido al recibir una respuesta.
- **`correlationId`:** se crea una sola vez al guardar el pedido (`purchase-<uuid>`) y lo heredan todos. El pedido es una entidad; la compra es un proceso que puede tocar varias. Sirve para buscar la compra completa en Grafana: `{service_name=~".+"} |= "purchase-0a9d5a0d"`.
- **`previousEventId`:** encadena los eventos en el orden en que se provocaron. Si uno llega al DLT, se sabe qué paso lo disparó.

La cadena del pedido 7:

```text
ORDER_CREATED       eventId=e3b923c2  previousEventId=null       (solo en el historial de la Saga)
→ RESERVE_STOCK     eventId=519761c5  previousEventId=e3b923c2
→ STOCK_RESERVED    eventId=72cc2f31  previousEventId=519761c5
→ PAYMENT_REQUESTED eventId=c1f14558  previousEventId=72cc2f31
→ PAYMENT_APPROVED  eventId=d1d1f96d  previousEventId=c1f14558
```

`ORDER_CREATED` no se publica en Kafka: se crea para iniciar la cadena y queda en `purchase_saga_histories`. Por qué unos eventos van al historial y otros al outbox: [22 · Historial de la Saga y outbox](22-saga-history-outbox.md).

### `eventVersion`

Hoy siempre es `1` y nadie lo lee. Existe para el día en que cambie el `payload` de un evento de forma incompatible (por ejemplo, renombrar `amount`): el productor publica la versión `2` y el consumidor sabe cómo leer cada una, sin romper los mensajes que ya están en el topic.

## Cómo se crea

Nunca se usa `IntegrationEventDto.builder()` directamente: la factory pone `eventId`, `eventVersion`, `timestamp` y `producer`, así ningún servicio puede olvidarlos.

| #   | Sobrecarga                                                                | Cuándo               | Quién la usa       |
|-----|---------------------------------------------------------------------------|----------------------|--------------------|
| 1   | `create(eventType, aggregateId, correlationId)`                           | Primero, sin datos   | order              |
| 2   | `create(eventType, aggregateId, correlationId, previousEventId, payload)` | Armado a mano        | order              |
| 3   | `create(eventType, previousEvent)`                                        | Respuesta, sin datos | inventory          |
| 4   | `create(eventType, previousEvent, payload)`                               | Respuesta, con datos | inventory, payment |

Las sobrecargas 3 y 4 copian `aggregateId` y `correlationId` del evento recibido y ponen su `eventId` como `previousEventId`. Por eso inventory y payment no pueden romper la cadena.

Cada servicio envuelve la factory en la suya (`OrderEventFactory`, `InventoryEventFactory`, `PaymentEventFactory`) para recibir su propio `EventTypeEnum` en vez de un `String`:

```java
IntegrationEventDto stockReservedEvent = this.inventoryEventFactory.build(EventTypeEnum.STOCK_RESERVED, event, mapPayload);
this.outboxEventService.enqueue(KafkaTopics.INVENTORY_RESERVATION_RESULTS, stockReservedEvent);
```

## Cómo se lee

```java
IntegrationEventDto event = this.integrationEventReader.read(rawEvent);
```

Si el JSON no tiene el formato esperado, `read` lanza `NonRetryableKafkaException`: el mensaje va directo al DLT sin reintentos, porque reintentar no lo va a arreglar. Por Kafka viaja texto: `eventType` es un `String` y cada servicio lo traduce a su propio `EventTypeEnum`. Si no lo conoce, no lo procesa.

## Catálogo de eventos

| #   | `eventType`           | Publica   | Topic                                  | Consume   |
|-----|-----------------------|-----------|----------------------------------------|-----------|
| 1   | `ORDER_CREATED`       | order     | (no se publica)                        | —         |
| 2   | `RESERVE_STOCK`       | order     | `inventory-reservation-requests-topic` | inventory |
| 3   | `STOCK_RESERVED`      | inventory | `inventory-reservation-results-topic`  | order     |
| 4   | `STOCK_NOT_AVAILABLE` | inventory | `inventory-reservation-results-topic`  | order     |
| 5   | `PAYMENT_REQUESTED`   | order     | `payment-processing-requests-topic`    | payment   |
| 6   | `PAYMENT_APPROVED`    | payment   | `payment-processing-results-topic`     | order     |
| 7   | `PAYMENT_FAILED`      | payment   | `payment-processing-results-topic`     | order     |
| 8   | `RELEASE_STOCK`       | order     | `inventory-reservation-requests-topic` | inventory |
| 9   | `STOCK_RELEASED`      | inventory | `inventory-reservation-results-topic`  | order     |
| 10  | `ORDER_CANCELLED`     | order     | `order-status-events-topic`            | nadie aún |
| 11  | `SAGA_TIMED_OUT`      | order     | (no se publica)                        | —         |

Datos de negocio de cada uno:

| #   | `eventType`           | `payload`                                                   |
|-----|-----------------------|-------------------------------------------------------------|
| 1   | `ORDER_CREATED`       | vacío                                                       |
| 2   | `RESERVE_STOCK`       | `customerId`, `amount`, `currency`, `paymentToken`, `items` |
| 3   | `STOCK_RESERVED`      | `reservationIds`                                            |
| 4   | `STOCK_NOT_AVAILABLE` | `productId`                                                 |
| 5   | `PAYMENT_REQUESTED`   | `customerId`, `amount`, `currency`, `paymentToken`          |
| 6   | `PAYMENT_APPROVED`    | `paymentId`                                                 |
| 7   | `PAYMENT_FAILED`      | `paymentId`                                                 |
| 8   | `RELEASE_STOCK`       | vacío                                                       |
| 9   | `STOCK_RELEASED`      | vacío                                                       |
| 10  | `ORDER_CANCELLED`     | `reason`                                                    |
| 11  | `SAGA_TIMED_OUT`      | —                                                           |

`ORDER_CANCELLED` es un evento público: nadie lo consume todavía, pero queda para la tienda o las notificaciones. `SAGA_TIMED_OUT`, igual que `ORDER_CREATED`, solo queda en el historial de la Saga. Los nombres de los topics se explican en [13 · Nombres de topics](13-kafka-topic-naming.md).

## Dónde se guarda

| #   | Tabla                  | Servicio      | Qué guarda del mensaje               |
|-----|------------------------|---------------|--------------------------------------|
| 1   | `outbox_events`        | quien publica | JSON completo y campos del sobre     |
| 2   | `inbox_events`         | quien consume | Campos del sobre, sin `payload`      |
| 3   | `dead_letter_messages` | ops-service   | JSON completo, ids y tipo del evento |

Detalle de las columnas en [20 · Bases de datos y tablas](20-database-tables.md).
