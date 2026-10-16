# Kafka: guía práctica de Kafbat UI

Kafbat UI permite inspeccionar Kafka desde el navegador. En este proyecto sirve para comprobar qué eventos se publicaron, revisar sus datos y observar el avance de los grupos de consumidores.

Abre [http://localhost:8090](http://localhost:8090) y selecciona el clúster `z4-event-commerce-local`.

## Qué muestra cada sección

| Sección       | Utilidad                                                                 |
|---------------|--------------------------------------------------------------------------|
| **Brokers**   | Estado del servidor Kafka, versión, particiones y almacenamiento.        |
| **Topics**    | Eventos almacenados, particiones, offsets y configuración de cada topic. |
| **Consumers** | Grupos de consumidores, miembros activos y mensajes pendientes (`lag`).  |

Para aprender el flujo de una orden se utilizarán principalmente **Topics > Messages** y **Consumers**.

## Topics del proyecto

| Topic                    | Publica                           | Consume                         | Eventos principales                                       |
|--------------------------|-----------------------------------|---------------------------------|-----------------------------------------------------------|
| `inventory-commands-topic` | Order Service | Inventory Service | `RESERVE_STOCK`, `RELEASE_STOCK` |
| `inventory-events-topic` | Inventory Service | Order Service | `STOCK_RESERVED`, `STOCK_NOT_AVAILABLE`, `STOCK_RELEASED` |
| `payments-commands-topic` | Order Service | Payment Service | `PAYMENT_REQUESTED` |
| `payments-events-topic` | Payment Service | Order Service | `PAYMENT_APPROVED`, `PAYMENT_FAILED` |
| `orders-events-topic` | Order Service | Consumidores futuros | `ORDER_CANCELLED` |

Cuando un consumer agota sus intentos, Spring Kafka conserva el mensaje en el DLT asociado:

| Topic original | Dead Letter Topic |
|---|---|
| `inventory-commands-topic` | `inventory-commands-topic-dlt` |
| `payments-commands-topic` | `payments-commands-topic-dlt` |
| `inventory-events-topic` | `inventory-events-topic-dlt` |
| `payments-events-topic` | `payments-events-topic-dlt` |

El flujo de reintentos, los headers de diagnóstico y las precauciones para reprocesar se detallan en [reintentos y DLT](07-kafka-retries-dlt.md).

Los topics de comandos representan acciones solicitadas a un servicio. Los topics de eventos representan resultados que ya ocurrieron. Los consumer groups registran el avance de cada suscriptor.

## Buscar mensajes de un topic

1. Entra en **Topics**.
2. Selecciona, por ejemplo, `inventory-commands-topic`.
3. Abre la pestaña **Messages**.
4. Selecciona **Oldest** para empezar por los eventos más antiguos o **Newest** para ver primero los recientes.
5. En **Select partitions**, elige todas las particiones o una partición concreta.
6. Deja vacío **Search** para una primera consulta.
7. Presiona **Refresh**.
8. Selecciona una fila para abrir el contenido completo del mensaje.

Si acabas de crear una orden, usa **Newest**, amplía el intervalo temporal si aparece esa opción y presiona **Refresh**.

> **Importante:** no presiones **Produce Message** para probar el flujo normal. La orden debe comenzar desde Postman contra Order Service; ese botón inserta manualmente un evento y puede crear datos incoherentes si el payload no cumple el contrato.

## Qué significa cada campo

| Campo         | Significado                                                                          |
|---------------|--------------------------------------------------------------------------------------|
| **Offset**    | Posición consecutiva del mensaje dentro de una partición.                            |
| **Partition** | División del topic donde Kafka almacenó el mensaje.                                  |
| **Timestamp** | Momento en que Kafka recibió o registró el mensaje.                                  |
| **Key**       | Clave usada para decidir la partición y conservar el orden de mensajes relacionados. |
| **Value**     | Evento serializado, normalmente como JSON.                                           |
| **Headers**   | Metadatos adicionales enviados con el mensaje.                                       |

El `offset` no identifica globalmente un evento. Solo es único dentro de la combinación `topic + partition`.

## Interpretar el evento del proyecto

Un mensaje puede tener una estructura similar a esta:

```json
{
  "eventId": "identificador-unico-del-evento",
  "eventType": "RESERVE_STOCK",
  "eventVersion": 1,
  "aggregateId": "15",
  "correlationId": "identificador-compartido-por-el-flujo",
  "causationId": null,
  "timestamp": "2026-08-30T15:15:00",
  "producer": "order-service",
  "payload": {}
}
```

| Propiedad       | Uso                                                                    |
|-----------------|------------------------------------------------------------------------|
| `eventId`       | Identifica un evento particular y permite controlar duplicados.        |
| `eventType`     | Indica lo que ocurrió o lo que se solicita ejecutar.                   |
| `aggregateId`   | Identifica la entidad principal; normalmente corresponde al `orderId`. |
| `correlationId` | Relaciona todos los eventos pertenecientes al mismo flujo de compra.   |
| `causationId`   | Indica qué evento anterior causó el evento actual.                     |
| `producer`      | Servicio que publicó el evento.                                        |
| `payload`       | Datos requeridos por el consumidor para procesar el evento.            |

Para seguir una compra completa, el valor más útil es `correlationId`. Para investigar un mensaje específico, utiliza `eventId`.

## Seguir una orden entre topics

1. Busca el `RESERVE_STOCK` en `inventory-commands-topic`.
2. Copia su `correlationId`.
3. Busca ese valor en `inventory-events-topic` para encontrar `STOCK_RESERVED` o `STOCK_NOT_AVAILABLE`.
4. Busca el mismo valor en `payments-commands-topic` para encontrar `PAYMENT_REQUESTED` y en `payments-events-topic` para encontrar `PAYMENT_APPROVED` o `PAYMENT_FAILED`.

Flujo exitoso esperado:

```text
inventory-commands-topic
  RESERVE_STOCK
        ↓
inventory-events-topic
  STOCK_RESERVED
        ↓
payments-commands-topic
  PAYMENT_REQUESTED
        ↓
payments-events-topic
  PAYMENT_APPROVED
        ↓
Order Service actualiza la orden a CONFIRMED
y la saga a COMPLETED
```

La confirmación final se persiste en la base de Order Service; no se publica otro evento final si ningún consumidor lo necesita.

## Filtros útiles

Dependiendo de la versión de Kafbat UI, **Search** puede buscar sobre la clave o el valor visible. Prueba con:

- Un `eventId` para encontrar un evento exacto.
- Un `correlationId` para seguir toda la compra.
- Un `orderId` o `aggregateId` para localizar eventos del pedido.
- Un tipo como `PAYMENT_FAILED` para revisar fallos funcionales.

Si el buscador no filtra el JSON como esperas, carga los mensajes y utiliza `Cmd + F` del navegador. Kafbat UI ayuda a inspeccionar Kafka, pero no reemplaza una plataforma de logs o trazas.

## Particiones y orden

Kafka garantiza el orden solamente dentro de una misma partición. Por eso, los eventos de una misma orden deberían publicarse con una key estable, normalmente el `orderId`. Así Kafka calcula la misma partición para los eventos relacionados.

Si el topic tiene varias particiones y no encuentras un evento, consulta **All partitions** o revisa cada partición.

## Consumer groups y lag

En **Consumers**, selecciona un grupo para revisar:

| Concepto           | Significado                                                                |
|--------------------|----------------------------------------------------------------------------|
| **Group ID**       | Identidad compartida por las instancias de un consumidor.                  |
| **Members**        | Instancias actualmente conectadas al grupo.                                |
| **Current offset** | Última posición confirmada por el grupo.                                   |
| **End offset**     | Última posición disponible en la partición.                                |
| **Lag**            | Cantidad aproximada de mensajes pendientes: `end offset - current offset`. |

- `lag = 0`: el consumidor está al día.
- `lag > 0`: todavía existen mensajes pendientes o el consumidor está detenido/fallando.
- estado **EMPTY**: no hay una instancia conectada en ese momento; el historial de offsets del grupo puede seguir existiendo.

Consumir un mensaje no lo elimina del topic. Kafka lo conserva según la política de retención y registra hasta qué offset avanzó cada consumer group.

## Si aparece “No messages found”

Comprueba en este orden:

1. Que seleccionaste el topic correcto.
2. Que la vista esté en **Newest** y hayas presionado **Refresh**.
3. Que no exista texto residual en **Search**.
4. Que estés consultando todas las particiones.
5. Que Order Service haya publicado el evento sin errores.
6. Que el backend apunte a `localhost:9092`.
7. Que Kafbat UI apunte internamente a `kafka:29092`.
8. Que Kafka y Kafbat UI estén activos:

```bash
docker compose -f infra/docker/docker-compose.yml ps kafka kafka-ui
```

También puedes revisar los logs del productor en Grafana y buscar el `eventId`, `correlationId` u `orderId` cuando esos valores formen parte del mensaje de log.

## Kafbat UI y Grafana no hacen lo mismo

| Herramienta        | Pregunta que responde                                                     |
|--------------------|---------------------------------------------------------------------------|
| **Kafbat UI**      | ¿El evento llegó a Kafka y en qué topic, partición y offset quedó?        |
| **Grafana + Loki** | ¿Qué hizo el código al producir o consumir el evento y qué error ocurrió? |
| **Base de datos** | ¿Cuál es el estado persistido actual de la orden, saga, inventario o pago? |

Una investigación completa normalmente comienza con `correlationId`: se comprueba el evento en Kafbat UI, se buscan los logs relacionados en Grafana y finalmente se valida el estado en la base de datos.
