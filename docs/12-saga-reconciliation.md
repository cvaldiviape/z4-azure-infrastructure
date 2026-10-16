# Reconciliación de Sagas

La reconciliación es el proceso que compara el estado esperado de una Saga con la evidencia disponible en los servicios participantes para determinar dónde se detuvo el flujo y qué acción de recuperación sería segura.

En Z4 Event Commerce, el timeout ya puede detectar que una compra permaneció demasiado tiempo sin avanzar:

```text
Saga activa sin cambios durante el límite configurado
                        ↓
                 Saga: TIMED_OUT
```

El timeout no conoce la causa. La reconciliación comienza después y responde una pregunta diferente:

```text
Timeout       → ¿la Saga tardó demasiado?
Reconciliación → ¿qué ocurrió realmente y dónde quedó el flujo?
Replay         → ¿qué mensaje identificado debemos volver a enviar?
Compensación   → ¿qué operación de negocio confirmada debemos deshacer?
```

> La reconciliación descrita en esta guía es el siguiente diseño previsto. El timeout está implementado; el reconciliador y el replay todavía no.

## Por qué no se debe compensar por timeout

Supongamos que Order quedó esperando después de `STOCK_RESERVED`. La ausencia de una respuesta no demuestra que el pago haya fallado:

```text
Order
├── status: PAYMENT_PENDING
└── Saga: TIMED_OUT después de STOCK_RESERVED

Payment
└── estado desconocido desde Order
```

Payment podría no haber recibido la solicitud, podría haber aprobado el pago o podría haber publicado una respuesta que terminó en DLT. Si Order libera el stock y cancela únicamente por tiempo, podría dejar al cliente cobrado con un pedido cancelado.

La compensación se ejecuta cuando existe evidencia de negocio, por ejemplo:

```text
PAYMENT_FAILED
→ RELEASE_STOCK
→ STOCK_RELEASED
→ ORDER_CANCELLED
```

`TIMED_OUT` es evidencia de demora, no de rechazo.

## Evidencia utilizada

La reconciliación debe combinar varias fuentes utilizando `orderId`, `correlationId` y `eventId`:

| Fuente | Pregunta que responde |
|---|---|
| `purchase_sagas` | ¿Cuál fue el último paso conocido por Order? |
| `purchase_saga_histories` | ¿Qué transiciones observó Order y en qué orden? |
| `outbox_events` | ¿El servicio todavía intenta publicar o Kafka ya confirmó el mensaje? |
| `inbox_events` | ¿El servicio procesó este `eventId`? |
| Datos de Inventory | ¿Existe una reserva y está reservada o liberada? |
| Datos de Payment | ¿El pago no existe, fue aprobado o fue rechazado? |
| Topics DLT | ¿El consumo agotó los reintentos? |

Un registro `PENDING` en Outbox y un mensaje en DLT no representan el mismo problema:

```text
Outbox PENDING
→ el productor todavía no obtuvo confirmación de Kafka
→ el relay existente debe continuar intentando

Mensaje en DLT
→ Kafka recibió el mensaje, pero el consumidor no pudo procesarlo
→ requiere diagnóstico y posiblemente replay
```

## Diagnóstico según el último paso

El timeout conserva `current_step` y `last_event_id` como el último avance real. La reconciliación no debe interpretar `error_message` para tomar decisiones.

### Después de `ORDER_CREATED`

Order espera el resultado de la reserva.

```text
¿RESERVE_STOCK está PENDING en Order Outbox?
├── Sí → OUTBOX_STILL_PENDING
└── No, está PUBLISHED
    ├── Inventory no lo procesó → INVENTORY_COMMAND_MISSING
    ├── Inventory reservó → INVENTORY_RESPONSE_MISSING
    └── Inventory rechazó → STOCK_NOT_AVAILABLE_RESPONSE_MISSING
```

### Después de `STOCK_RESERVED`

Order espera el resultado del pago.

```text
¿PAYMENT_REQUESTED está PENDING en Order Outbox?
├── Sí → OUTBOX_STILL_PENDING
└── No, está PUBLISHED
    ├── Payment no tiene pago → PAYMENT_REQUEST_MISSING
    ├── Payment = APPROVED → PAYMENT_APPROVED_RESPONSE_MISSING
    └── Payment = FAILED → PAYMENT_FAILED_RESPONSE_MISSING
```

### Después de `PAYMENT_FAILED`

Order espera completar la compensación.

```text
¿RELEASE_STOCK está PENDING en Order Outbox?
├── Sí → COMPENSATION_OUTBOX_PENDING
└── No, está PUBLISHED
    ├── reserva sigue RESERVED → STOCK_RELEASE_MISSING
    └── reserva está RELEASED → STOCK_RELEASED_RESPONSE_MISSING
```

## Límites entre microservicios

Order es dueño de la Saga, pero no debe consultar directamente `inventory_db` ni `payments_db`. Cada servicio conserva la propiedad de sus datos.

Una primera implementación puede ofrecer endpoints internos de solo lectura:

```http
GET /internal/orders/{orderId}/reservations
GET /internal/orders/{orderId}/payment
```

El reconciliador de Order combinaría:

```text
Saga, Inbox y Outbox de Order
              +
estado informado por Inventory
              +
estado informado por Payment
              ↓
diagnóstico persistente
```

Otra evolución sería utilizar mensajes de consulta y respuesta, pero añade asincronía al mecanismo que intenta diagnosticar una asincronía previa. Para la primera versión, endpoints internos con timeout corto y autenticación de servicio a servicio son más fáciles de operar.

## Procesamiento por lotes

El reconciliador debe procesar Sagas `TIMED_OUT` en lotes limitados, por ejemplo 100 por ejecución:

```text
Scheduler
→ selecciona lote TIMED_OUT
→ bloquea o reclama cada trabajo
→ recopila evidencia
→ clasifica el problema
→ persiste el diagnóstico
→ continúa con el siguiente lote
```

No debe mantener una transacción PostgreSQL abierta mientras realiza llamadas HTTP. Una opción segura es reclamar primero los identificadores, cerrar la transacción y reconciliar cada Saga de forma independiente.

Procesar por lotes permite:

- limitar carga sobre las bases y servicios;
- continuar desde ejecuciones anteriores;
- distribuir trabajo entre varias instancias;
- observar progreso y fallos parciales;
- evitar intentar miles de recuperaciones simultáneamente.

## Persistencia propuesta

Una ejecución agrupa el trabajo realizado:

```text
reconciliation_runs
├── id
├── status              -- RUNNING, COMPLETED, PARTIALLY_FAILED, FAILED
├── trigger_type        -- SCHEDULED, MANUAL
├── total_sagas
├── issues_found
├── started_at
├── completed_at
└── error_message
```

Cada hallazgo conserva el diagnóstico de una Saga:

```text
reconciliation_issues
├── id
├── reconciliation_run_id
├── purchase_saga_id
├── order_id
├── correlation_id
├── last_step
├── expected_event_type
├── issue_type
├── resolution_status   -- DETECTED, READY_FOR_REPLAY, RESOLVED, MANUAL_REVIEW
├── details
├── detected_at
└── resolved_at
```

Tipos iniciales sugeridos:

| Tipo | Significado |
|---|---|
| `OUTBOX_STILL_PENDING` | El relay todavía no confirmó la publicación. |
| `INVENTORY_COMMAND_MISSING` | Inventory no presenta evidencia de haber procesado la reserva. |
| `INVENTORY_RESPONSE_MISSING` | La reserva existe, pero Order no procesó su respuesta. |
| `PAYMENT_REQUEST_MISSING` | Payment no presenta evidencia de la solicitud publicada. |
| `PAYMENT_APPROVED_RESPONSE_MISSING` | Payment aprobó, pero Order no procesó la aprobación. |
| `PAYMENT_FAILED_RESPONSE_MISSING` | Payment rechazó, pero Order no inició la compensación. |
| `COMPENSATION_OUTBOX_PENDING` | `RELEASE_STOCK` todavía espera publicación. |
| `STOCK_RELEASE_MISSING` | Inventory conserva la reserva después de publicarse la compensación. |
| `STOCK_RELEASED_RESPONSE_MISSING` | Inventory liberó, pero Order no cerró la Saga. |
| `MANUAL_INSPECTION_REQUIRED` | La evidencia es insuficiente o contradictoria. |

## Acciones y responsabilidad

La primera versión debe ser de diagnóstico. Puede sugerir una acción, pero no cambiar pedidos, pagos o reservas automáticamente:

| Diagnóstico | Acción sugerida |
|---|---|
| Outbox `PENDING` | Mantener relay y alertar si supera su umbral. |
| Comando o respuesta en DLT | Marcar como candidato a replay manual. |
| Pago aprobado | Recuperar la respuesta; nunca compensar por silencio. |
| Pago rechazado | Recuperar `PAYMENT_FAILED` para iniciar la compensación normal. |
| Estado contradictorio | Revisión humana. |

El operador o el futuro servicio de replay ejecutará la acción. Esta separación deja una auditoría clara:

```text
Timeout detectó
→ Reconciliación diagnosticó
→ Operador autorizó
→ Replay ejecutó
→ Inbox protegió la idempotencia
```

## Relación con replay e idempotencia

El replay debe reutilizar el mensaje original y conservar su `eventId`. No debe generar un evento equivalente con otro identificador, porque el Inbox no podría reconocerlo como duplicado.

```text
Mismo eventId
├── no fue procesado → ejecuta el efecto y registra Inbox
└── ya fue procesado → Inbox lo ignora
```

La reconciliación no reproduce mensajes. Solo identifica qué mensaje podría requerir replay y aporta evidencia para decidirlo.

## Resultado esperado

Un reporte de lote podría verse así:

```text
Run 42: COMPLETED
Sagas revisadas: 100
Problemas encontrados: 4

Order 1021 → OUTBOX_STILL_PENDING
Order 1022 → PAYMENT_REQUEST_MISSING
Order 1023 → PAYMENT_APPROVED_RESPONSE_MISSING
Order 1024 → MANUAL_INSPECTION_REQUIRED
```

La meta es reducir miles de Sagas detenidas a grupos diagnosticados y accionables, sin ejecutar compensaciones o cobros basados en suposiciones.

## Orden de implementación

1. Crear endpoints internos de estado en Inventory y Payment.
2. Crear `reconciliation_runs` y `reconciliation_issues` en Order.
3. Implementar reconciliación manual de una Saga.
4. Incorporar procesamiento programado por lotes.
5. Exponer métricas y alertas por diagnóstico.
6. Implementar replay manual usando los hallazgos.
7. Agregar límite de replay y parking lot.

Consulta también:

- [Saga de compra](08-saga-pattern.md)
- [Transactional Inbox y Outbox](09-transactional-inbox-outbox.md)
- [Reintentos y DLT](07-kafka-retries-dlt.md)
- [Estrategia de bases de datos](03-database-strategy.md)
