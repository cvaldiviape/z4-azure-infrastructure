# Saga de compra

La Saga de Z4 Event Commerce coordina la creación de un pedido entre **Order**, **Inventory** y **Payment** sin utilizar una transacción distribuida. Cada servicio confirma únicamente su propia transacción PostgreSQL y comunica el resultado mediante eventos Kafka.

Order Service actúa como **orquestador**: inicia la Saga, conserva su estado, decide el siguiente comando y ejecuta la compensación cuando el pago falla. Inventory y Payment son participantes que procesan comandos y publican sus resultados.

## Por qué se necesita una Saga

Crear una compra completa requiere cambios en tres bases independientes:

1. Order registra el pedido.
2. Inventory reserva las unidades solicitadas.
3. Payment procesa el pago.

Una transacción local de Spring no puede confirmar o revertir atómicamente las tres bases. La Saga divide la operación en transacciones locales y define acciones posteriores para alcanzar un resultado consistente:

- si todos los pasos tienen éxito, el pedido termina `CONFIRMED`;
- si no hay stock, el pedido termina `CANCELLED` sin solicitar el pago;
- si el pago falla, Order solicita liberar el stock y cancela el pedido cuando recibe la confirmación.

La consistencia es **eventual**: durante unos instantes cada servicio puede observar un estado diferente mientras los mensajes avanzan por Kafka.

## Responsabilidades

| Servicio  | Responsabilidad en la Saga                                                                                          |
|-----------|---------------------------------------------------------------------------------------------------------------------|
| Order     | Crea el pedido y la instancia de Saga, emite comandos, consume resultados, actualiza estados y guarda el historial. |
| Inventory | Reserva o libera stock y publica el resultado de la operación.                                                      |
| Payment   | Procesa una solicitud de pago y publica si fue aprobada o rechazada.                                                |
| Auth      | Autentica al cliente, pero no participa en el flujo Kafka de la Saga.                                               |

## Topics y mensajes

| Topic                      | Productor | Consumidor                         | Tipos usados por la Saga                                  |
|----------------------------|-----------|------------------------------------|-----------------------------------------------------------|
| `inventory-commands-topic` | Order     | Inventory                          | `RESERVE_STOCK`, `RELEASE_STOCK`                          |
| `inventory-events-topic`   | Inventory | Order                              | `STOCK_RESERVED`, `STOCK_NOT_AVAILABLE`, `STOCK_RELEASED` |
| `payments-commands-topic`  | Order     | Payment                            | `PAYMENT_REQUESTED`                                       |
| `payments-events-topic`    | Payment   | Order                              | `PAYMENT_APPROVED`, `PAYMENT_FAILED`                      |
| `orders-events-topic`      | Order     | Consumidores de eventos de pedidos | `ORDER_CANCELLED`                                         |

Los mensajes utilizan un sobre común con datos como `eventId`, `eventType`, `aggregateId`, `correlationId`, `causationId`, `producer`, `timestamp` y `payload`.

- `aggregateId` identifica el pedido.
- `correlationId` permanece estable durante toda la compra.
- `causationId` apunta al evento que provocó el mensaje actual.
- `eventId` identifica de forma única cada mensaje y permite deduplicarlo mediante Inbox.

## Camino exitoso

```text
Cliente
  │ POST /orders
  ▼
Order
  ├─ crea Order: CREATED
  ├─ crea Saga: STARTED / ORDER_CREATED
  └─ Outbox: RESERVE_STOCK ───────────────► inventory-commands-topic
                                                   │
                                                   ▼
Inventory
  ├─ reserva stock
  └─ Outbox: STOCK_RESERVED ──────────────► inventory-events-topic
                                                   │
                                                   ▼
Order
  ├─ Order: PAYMENT_PENDING
  ├─ Saga: IN_PROGRESS / STOCK_RESERVED
  └─ Outbox: PAYMENT_REQUESTED ───────────► payments-commands-topic
                                                   │
                                                   ▼
Payment
  ├─ registra pago APPROVED
  └─ Outbox: PAYMENT_APPROVED ────────────► payments-events-topic
                                                   │
                                                   ▼
Order
  ├─ Order: CONFIRMED
  └─ Saga: COMPLETED / PAYMENT_APPROVED
```

En la implementación actual, `PAYMENT_APPROVED` confirma el pedido y completa la Saga dentro de Order. No se publica un evento `ORDER_CONFIRMED`.

## Cancelación por falta de stock

Si Inventory no puede reservar uno de los productos, publica `STOCK_NOT_AVAILABLE`:

```text
RESERVE_STOCK
      ↓
STOCK_NOT_AVAILABLE
      ↓
Order: CANCELLED
Saga: COMPLETED / ORDER_CANCELLED
      ↓
orders-events-topic: ORDER_CANCELLED
```

No se solicita ningún pago. Si algunos productos ya fueron reservados antes de encontrar el faltante, `StockServiceImpl` los libera dentro de la misma transacción local antes de publicar `STOCK_NOT_AVAILABLE`.

## Compensación por pago fallido

La reserva de stock ya fue confirmada cuando Payment procesa la solicitud. Por eso un pago rechazado necesita una acción compensatoria explícita:

```text
PAYMENT_FAILED
      ↓
Order Saga: COMPENSATING / PAYMENT_FAILED
      ↓
RELEASE_STOCK ────────────────────────────► inventory-commands-topic
                                                   │
                                                   ▼
Inventory libera las reservas
      │
      └─ STOCK_RELEASED ──────────────────► inventory-events-topic
                                                   │
                                                   ▼
Order: CANCELLED
Saga: COMPLETED / ORDER_CANCELLED
      ↓
orders-events-topic: ORDER_CANCELLED
```

La compensación no es un rollback distribuido. `RELEASE_STOCK` es una nueva operación de negocio que deshace semánticamente la reserva ya confirmada.

## Estado persistido por Order

Order conserva tres vistas relacionadas:

| Información       | Finalidad                                                                                                                                                 |
|-------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------|
| Pedido            | Estado de negocio visible: `CREATED`, `PAYMENT_PENDING`, `CONFIRMED` o `CANCELLED`.                                                                       |
| Saga              | Estado de coordinación: `STARTED`, `IN_PROGRESS`, `COMPENSATING`, `TIMED_OUT`, `COMPLETED` o `FAILED`; además conserva el paso actual y el último evento. |
| Historial de Saga | Registra cada transición para seguimiento y diagnóstico.                                                                                                  |

`PurchaseSagaServiceImpl` recibe los eventos de Inventory y Payment. Selecciona el comportamiento mediante `StockEventStrategyRegistry` o `PaymentEventStrategyRegistry`, y `PurchaseSagaStateManager` actualiza el pedido, la Saga y su historial.

Las estrategias implementadas son:

| Estrategia                       | Resultado                                                |
|----------------------------------|----------------------------------------------------------|
| `StockReservedEventStrategy`     | Avanza al pago y encola `PAYMENT_REQUESTED`.             |
| `StockNotAvailableEventStrategy` | Cancela el pedido y encola `ORDER_CANCELLED`.            |
| `PaymentApprovedEventStrategy`   | Confirma el pedido y completa la Saga.                   |
| `PaymentFailedEventStrategy`     | Marca la Saga como compensando y encola `RELEASE_STOCK`. |
| `StockReleasedEventStrategy`     | Cancela el pedido y completa la compensación.            |

## Timeout de Saga

Order ejecuta periódicamente un detector de Sagas que permanecen demasiado tiempo en `STARTED`, `IN_PROGRESS` o `COMPENSATING`. La antigüedad se calcula utilizando `updated_at` y, para una Saga que todavía no tuvo transiciones, `created_at`.

Cuando una Saga supera el tiempo configurado:

1. la fila se bloquea con `FOR UPDATE SKIP LOCKED` para evitar que dos instancias procesen el mismo timeout;
2. la Saga cambia a `TIMED_OUT` y su paso actual a `SAGA_TIMED_OUT`;
3. se registra una entrada en `purchase_saga_histories` con el paso anterior y la causa;
4. el pedido y sus datos recuperables conservan su estado actual, porque el timeout detecta el problema pero no decide automáticamente una cancelación o compensación.

La columna de versión de `purchase_sagas` evita que el timeout sobrescriba silenciosamente una transición Kafka concurrente. Consulta [Control de concurrencia con `version`](database-strategy.md#control-de-concurrencia-con-version) para ver el `UPDATE` generado y un ejemplo completo de la carrera.

| Propiedad | Variable | Valor predeterminado |
|-----------------------------------------------|-------------------------------------------|--------------------|
| `app.saga.timeout-milliseconds`               | `SAGA_TIMEOUT_MILLISECONDS`               | 300000 (5 minutos) |
| `app.saga.timeout-batch-size`                 | `SAGA_TIMEOUT_BATCH_SIZE`                 | 100                |
| `app.saga.timeout-poll-interval-milliseconds` | `SAGA_TIMEOUT_POLL_INTERVAL_MILLISECONDS` | 30000              |
| `app.saga.timeout-initial-delay-milliseconds` | `SAGA_TIMEOUT_INITIAL_DELAY_MILLISECONDS` | 30000              |

El log `action=saga_timed_out` permite localizar cada caso por `correlationId` y `orderId`. La reconciliación posterior debe decidir si corresponde reanudar, compensar o cerrar manualmente la compra. Un evento tardío o reproducido todavía puede llevar la Saga desde `TIMED_OUT` a `IN_PROGRESS`, `COMPENSATING` o `COMPLETED`; el Inbox evita repetir los efectos de un `eventId` que ya fue procesado.

`FAILED` queda reservado para una recuperación agotada o un caso declarado irrecuperable, no para la simple ausencia temporal de una respuesta.

## Consistencia y tolerancia a fallos

La Saga se apoya en tres mecanismos complementarios:

1. **Transactional Outbox** guarda el cambio de negocio y el mensaje pendiente en la misma transacción PostgreSQL. Un relay programado publica el mensaje y solo lo marca `PUBLISHED` después del ACK del broker.
2. **Transactional Inbox** registra el `eventId` consumido en la misma transacción que los cambios locales. Si Kafka entrega nuevamente el evento, el servicio puede ignorarlo.
3. **Reintentos y DLT** reintentan fallos transitorios del consumidor y separan los mensajes que no pueden procesarse automáticamente.

Estos mecanismos ofrecen procesamiento práctico **al menos una vez con efectos idempotentes**. No implican entrega exactamente una vez entre PostgreSQL y Kafka.

Consulta también:

- [Transactional Inbox y Outbox](transactional-inbox-outbox.md)
- [Reintentos y DLT](kafka-retries-dlt.md)
- [Fundamentos de Kafka](kafka-fundamentals.md)

## Cómo seguir una Saga

Para investigar una compra, utiliza su `correlationId` u `orderId` en todos los servicios. Los logs estructurados incluyen campos como `eventType`, `eventId`, `correlationId`, `orderId`, `orderStatus` y `sagaStatus`.

En PostgreSQL de Order consulta el pedido, la instancia de Saga, su historial y las tablas Inbox/Outbox. En Inventory y Payment consulta sus tablas de negocio junto con sus propias tablas Inbox/Outbox. Cada servicio es dueño de sus datos; ninguno consulta directamente la base de otro.

En Kafbat UI verifica, en este orden:

1. que el comando haya llegado al topic esperado;
2. que el consumer group lo haya consumido;
3. que el evento de respuesta conserve el mismo `correlationId`;
4. que no exista un mensaje relacionado en el topic DLT;
5. que el Outbox de origen haya pasado de `PENDING` a `PUBLISHED`.

## Probar los resultados principales

Con los cuatro servicios y la infraestructura local iniciados, crea un pedido desde la API de Order:

- usa `paymentToken: "TEST_APPROVED"` para recorrer el camino exitoso;
- usa cualquier otro token para provocar `PAYMENT_FAILED` y observar la compensación;
- solicita un producto sin disponibilidad suficiente para provocar `STOCK_NOT_AVAILABLE`.

La respuesta HTTP confirma que Order aceptó y creó el pedido, no que toda la Saga ya terminó. Consulta después el pedido y los registros de seguimiento hasta observar `CONFIRMED` o `CANCELLED`.

## Límites actuales

- No hay intervención o reanudación manual de Sagas fallidas desde una API administrativa.
- El timeout detecta y registra Sagas estancadas, pero todavía no existe reconciliación automática.
- El pago es una simulación local: únicamente `TEST_APPROVED` produce aprobación.
- No se publica `ORDER_CONFIRMED` cuando finaliza el camino exitoso.

Estas limitaciones son puntos naturales de evolución, pero no invalidan el flujo educativo actual.
