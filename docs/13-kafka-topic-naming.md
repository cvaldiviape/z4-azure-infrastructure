# Convención de nombres para topics de Kafka

## Objetivo

Los topics deben expresar qué capacidad de negocio procesa el mensaje y qué propósito cumple. Nombres genéricos como `inventory-commands-topic` o `payments-events-topic` indican el servicio involucrado, pero se vuelven ambiguos cuando el dominio incorpora más operaciones.

Z4 Event Commerce utiliza esta estructura:

```text
<capacidad-de-negocio>-<propósito>-topic
```

Ejemplos:

```text
inventory-reservation-requests-topic
inventory-reservation-results-topic
payment-processing-requests-topic
payment-processing-results-topic
order-status-events-topic
```

## Significado de cada propósito

### Requests

Un topic `requests` contiene mensajes que solicitan ejecutar una operación. El consumidor todavía debe validar y procesar la solicitud, por lo que no se presupone que haya tenido éxito.

```text
inventory-reservation-requests-topic
├── RESERVE_STOCK
└── RELEASE_STOCK

payment-processing-requests-topic
└── PAYMENT_REQUESTED
```

### Results

Un topic `results` informa el desenlace asíncrono de una solicitud. Se utiliza `results` en lugar de `responses` porque Kafka no establece una respuesta directa o inmediata como HTTP y una solicitud puede tener distintos desenlaces.

```text
inventory-reservation-results-topic
├── STOCK_RESERVED
├── STOCK_NOT_AVAILABLE
└── STOCK_RELEASED

payment-processing-results-topic
├── PAYMENT_APPROVED
└── PAYMENT_FAILED
```

### Events

Un topic `events` comunica hechos públicos del dominio que no necesariamente constituyen la respuesta directa a una solicitud. Otros consumidores presentes o futuros pueden reaccionar a ellos.

```text
order-status-events-topic
└── ORDER_CANCELLED
```

## Topics del proyecto

| Topic | Productor | Consumidor principal | Tipos de evento |
|---|---|---|---|
| `inventory-reservation-requests-topic` | Order Service | Inventory Service | `RESERVE_STOCK`, `RELEASE_STOCK` |
| `inventory-reservation-results-topic` | Inventory Service | Order Service | `STOCK_RESERVED`, `STOCK_NOT_AVAILABLE`, `STOCK_RELEASED` |
| `payment-processing-requests-topic` | Order Service | Payment Service | `PAYMENT_REQUESTED` |
| `payment-processing-results-topic` | Payment Service | Order Service | `PAYMENT_APPROVED`, `PAYMENT_FAILED` |
| `order-status-events-topic` | Order Service | Consumidores de pedidos | `ORDER_CANCELLED` |

## Dead-letter topics

Los DLT conservan el nombre completo del topic de origen y agregan el sufijo `-dlt`:

```text
inventory-reservation-requests-topic-dlt
inventory-reservation-results-topic-dlt
payment-processing-requests-topic-dlt
payment-processing-results-topic-dlt
```

Esto permite identificar inmediatamente qué flujo no pudo procesar el mensaje. El `DeadLetterPublishingRecoverer` obtiene el topic de origen y agrega el sufijo de forma dinámica.

## Migración desde los nombres anteriores

El nombre del destino se guarda dentro de `outbox_events`. Por eso cada servicio incluye una migración Flyway que cambia únicamente las filas `PENDING` hacia los topics nuevos. Las filas `PUBLISHED` y el Inbox no se modifican: deben conservar el nombre real del canal por el que el mensaje fue publicado o recibido.

Los mensajes que ya existan en los topics anteriores de Kafka tampoco se trasladan automáticamente. En un entorno productivo el renombrado requeriría una transición controlada —por ejemplo, consumo temporal de ambos nombres o publicación dual— hasta vaciar el topic anterior. En el entorno local del proyecto se utilizan directamente los nombres nuevos.

## Por qué no existe un topic por evento

Separar cada tipo de evento en un topic distinto brinda aislamiento, pero también multiplica particiones, permisos, configuración, alertas, DLT y tareas operativas. En este proyecto los eventos relacionados se agrupan por capacidad de negocio porque comparten productor, consumidor, orden y política de procesamiento.

Se debe considerar un topic independiente cuando un evento necesite alguna de estas características:

- Consumidores o permisos claramente diferentes.
- Retención, particionado o volumen distinto.
- Una clave de ordenamiento diferente.
- Un ciclo de vida o nivel de criticidad independiente.
- Evolución contractual que no pueda mantenerse junto con los demás mensajes.

## Relación con el DTO

Todos estos mensajes pueden compartir el mismo contrato de integración aunque cumplan propósitos diferentes:

```java
IntegrationEventDto event;
IntegrationEventDto resultEvent;
```

El nombre del topic describe el canal y su propósito; `eventType` describe el hecho o la operación concreta. Por ejemplo, `inventory-reservation-results-topic` identifica el flujo, mientras que `STOCK_RESERVED` identifica el resultado específico.

## Regla para nuevos topics

Antes de agregar un topic se debe responder:

1. ¿Qué capacidad de negocio es responsable?
2. ¿El mensaje solicita trabajo, informa un resultado o publica un hecho del dominio?
3. ¿Los mensajes agrupados comparten consumidores y requisitos operativos?
4. ¿Cuál será su DLT y qué equipo investigará sus fallos?

El sufijo `-topic` se mantiene por claridad educativa. Si en el futuro se requiere ejecutar contratos incompatibles simultáneamente, la versión se agregará al final, por ejemplo `inventory-reservation-requests-topic-v2`.
