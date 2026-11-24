# Historial de la Saga y outbox

order-service guarda los eventos de una compra en dos tablas distintas. Esta guía explica qué va a cada una, por qué un comando como `RESERVE_STOCK` no aparece en el historial y cómo ver la línea de tiempo completa. Los estados y las estrategias de la Saga están en [08 · Saga de compra](08-saga-pattern.md).

## Los dos eventos al crear un pedido

Al crear un pedido se arman dos eventos, pero cada uno termina en una tabla distinta:

| #   | Evento          | Qué es                                | Para quién        | Dónde queda                                  |
|-----|-----------------|---------------------------------------|-------------------|----------------------------------------------|
| 1   | `ORDER_CREATED` | Un hecho: "se creó el pedido"         | order-service     | Solo su `eventId`, en la Saga y su historial |
| 2   | `RESERVE_STOCK` | Un comando: "inventory, reserva esto" | inventory-service | `outbox_events` → Kafka → inventory          |

`ORDER_CREATED` no se publica porque hoy nadie más lo necesita: es el inicio de la cadena (su `eventId` es el `previousEventId` de `RESERVE_STOCK`) y el primer paso de la Saga. Si algún día un servicio quiere enterarse (notificaciones, la tienda en vivo), se encola en `order-status-events-topic`, como `ORDER_CANCELLED`.

## La regla

| #   | Tabla                     | Qué guarda                                  | Pregunta que responde                   |
|-----|---------------------------|---------------------------------------------|-----------------------------------------|
| 1   | `purchase_saga_histories` | Cada vez que la Saga **cambia de estado**   | ¿Qué le pasó a la Saga y cómo quedó?    |
| 2   | `outbox_events`           | Cada comando o evento que la Saga **envía** | ¿Qué le pidió a otro servicio y cuándo? |

Enviar un comando no cambia el estado de la Saga: ocurre en la misma transacción que el paso que lo provoca. Por eso `RESERVE_STOCK` y `PAYMENT_REQUESTED` no aparecen en el historial. En cambio `STOCK_RESERVED` sí queda en el historial: la Saga lo **recibe** y pasa a `IN_PROGRESS`.

## Qué entra al historial

| #   | Momento                                                       | `event_type` en el historial                |
|-----|---------------------------------------------------------------|---------------------------------------------|
| 1   | Se crea el pedido                                             | `ORDER_CREATED`                             |
| 2   | Llega `STOCK_RESERVED`, `PAYMENT_APPROVED` o `PAYMENT_FAILED` | El evento recibido                          |
| 3   | Llega `STOCK_NOT_AVAILABLE` o `STOCK_RELEASED`                | `ORDER_CANCELLED`, que también va al outbox |
| 4   | Vence el timeout                                              | `SAGA_TIMED_OUT`                            |

## Ejemplo: el pedido 7

Así quedó en las dos tablas:

| #   | Tabla     | `event_type`        | Orden             | Saga          | `event_id` | `previousEventId` |
|-----|-----------|---------------------|-------------------|---------------|------------|-------------------|
| 1   | historial | `ORDER_CREATED`     | `CREATED`         | `STARTED`     | `e3b923c2` |                   |
| 2   | outbox    | `RESERVE_STOCK`     |                   |               | `519761c5` | `e3b923c2`        |
| 3   | historial | `STOCK_RESERVED`    | `PAYMENT_PENDING` | `IN_PROGRESS` | `72cc2f31` |                   |
| 4   | outbox    | `PAYMENT_REQUESTED` |                   |               | `c1f14558` | `72cc2f31`        |
| 5   | historial | `PAYMENT_APPROVED`  | `CONFIRMED`       | `COMPLETED`   | `d1d1f96d` |                   |

No se pierde nada: el `previousEventId` de cada comando del outbox apunta a la fila del historial que lo provocó.

## Ver la línea de tiempo completa

Se unen las dos tablas:

```sql
SELECT created_at, 'historial' AS tabla, event_type::text, event_id, NULL AS previous_event_id
FROM purchase_saga_histories
WHERE order_id = 7
UNION ALL
SELECT created_at, 'outbox', event_type, event_id, payload::jsonb->>'previousEventId'
FROM outbox_events
WHERE aggregate_id = '7'
ORDER BY created_at;
```

## Si se quisiera una sola tabla

| #   | Opción                                                   | A favor             | En contra                           |
|-----|----------------------------------------------------------|---------------------|-------------------------------------|
| 1   | Guardar también lo enviado, con columna `direction`      | Una sola consulta   | Duplica lo que ya está en el outbox |
| 2   | Que `/ops/purchase-sagas/{orderId}` combine ambas tablas | No cambia el modelo | La API hace la unión                |

Para la Ops Console se recomienda la opción 2.
