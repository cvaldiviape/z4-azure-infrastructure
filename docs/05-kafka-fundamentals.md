# Kafka: fundamentos aplicados a Z4 Event Commerce

Esta guía explica las ideas mínimas necesarias para entender Kafka y leer el código del proyecto. Kafka no llama directamente a otro microservicio como lo haría una petición HTTP. Un servicio publica un mensaje, Kafka lo conserva y otro servicio lo consume cuando puede.

## La idea general

Piensa en Kafka como un sistema de buzones duraderos:

```text
Producer ── publica ──> Topic en Kafka ── entrega ──> Consumer
```

- El **producer** crea y publica el mensaje.
- El **topic** es el canal con nombre donde Kafka almacena mensajes.
- El **consumer** se suscribe al topic y procesa sus mensajes.
- El **broker** es el servidor Kafka que guarda los topics.

En este proyecto, por ejemplo:

```text
Order Service
    │ publica RESERVE_STOCK
    ▼
inventory-commands-topic
    │ consume
    ▼
Inventory Service
```

Order Service no necesita llamar directamente a Inventory Service ni esperar su respuesta. Inventory Service puede estar ocupado temporalmente y consumir el mensaje después.

## Broker y clúster

Un **broker** es una instancia de Kafka. Recibe mensajes, los escribe en disco y los entrega a los consumidores.

Un **clúster** es un conjunto de brokers que trabajan juntos. En el entorno local existe un solo broker porque es suficiente para aprender y desarrollar:

```text
Clúster local
└── Broker 1
```

En producción se suelen utilizar varios brokers para distribuir la carga y mantener copias de los datos. Si un broker falla, otro puede continuar atendiendo.

## Topic

Un **topic** es una secuencia de mensajes relacionados identificada por un nombre. No es una clase Java ni una tabla de base de datos.

Los topics principales del proyecto son:

| Topic | Publica | Consume | Propósito |
|---|---|---|---|
| `inventory-commands-topic` | Order Service | Inventory Service | Solicitar reserva o liberación de stock. |
| `inventory-events-topic` | Inventory Service | Order Service | Informar el resultado de inventario. |
| `payments-commands-topic` | Order Service | Payment Service | Solicitar un pago. |
| `payments-events-topic` | Payment Service | Order Service | Informar si el pago fue aprobado o rechazado. |
| `orders-events-topic` | Order Service | Consumidores futuros | Informar resultados del pedido. |

Kafka conserva los mensajes aunque ya hayan sido consumidos. La retención depende de su configuración; consumir no equivale a eliminar.

## Comando y evento

Kafka solo ve mensajes, pero el proyecto diferencia su intención:

- Un **comando** solicita que alguien haga algo: `RESERVE_STOCK` o `PAYMENT_REQUESTED`.
- Un **evento** informa algo que ya ocurrió: `STOCK_RESERVED` o `PAYMENT_APPROVED`.

Una forma práctica de distinguirlos:

```text
Comando: «reserva este stock»
Evento:  «el stock fue reservado»
```

Por eso los nombres terminan en `-commands-topic` o `-events-topic`.

## Mensaje: key, value y headers

Cada registro de Kafka contiene principalmente:

| Parte | Uso |
|---|---|
| `key` | Identifica mensajes relacionados y ayuda a elegir la partición. |
| `value` | Contiene el evento, en este proyecto serializado como JSON. |
| `headers` | Contiene metadatos adicionales; los DLT agregan información del error. |
| `timestamp` | Indica cuándo Kafka registró el mensaje. |

El `value` utiliza `EventEnvelopeDto`, que incluye datos como:

- `eventId`: identidad única del evento.
- `eventType`: tipo como `RESERVE_STOCK`.
- `aggregateId`: pedido relacionado.
- `correlationId`: conecta todos los mensajes de una misma Saga.
- `causationId`: identifica el evento que causó el actual.
- `payload`: datos específicos del evento.

El `correlationId` permite reconstruir el recorrido completo de una compra entre varios topics y servicios.

## Particiones

Un topic se divide en una o más **particiones**. Cada partición es un registro ordenado e independiente:

```text
Topic con dos particiones

Partición 0: [mensaje A] [mensaje B] [mensaje C]
Partición 1: [mensaje D] [mensaje E]
```

Las particiones sirven para procesar mensajes en paralelo. Dos consumers del mismo grupo pueden trabajar simultáneamente si reciben particiones diferentes.

Kafka garantiza el orden solamente dentro de una partición, no entre todo el topic. Por eso la key es importante: los mensajes con la misma key normalmente llegan a la misma partición y mantienen su orden relativo.

En el proyecto local los topics tienen una partición. Esto simplifica el aprendizaje y conserva un orden único, pero limita el paralelismo.

## Réplicas

Una **réplica** es una copia de una partición almacenada en otro broker:

```text
Partición 0
├── copia líder en Broker 1
├── réplica en Broker 2
└── réplica en Broker 3
```

El producer y los consumers trabajan con la copia líder. Las demás sirven para recuperación si el broker líder falla.

En local se usa `.replicas(1)` porque solo existe un broker. Configurar tres réplicas requeriría al menos tres brokers. Una réplica facilita el desarrollo, pero no ofrece tolerancia a la pérdida del único broker.

## Offset

El **offset** es la posición de un mensaje dentro de una partición:

```text
Partición 0
offset 0       offset 1       offset 2
[mensaje A]    [mensaje B]    [mensaje C]
```

Un offset solo tiene significado junto con su topic y partición:

```text
topic + partition + offset
```

Kafka usa esta posición para saber hasta dónde avanzó un grupo consumidor. El offset no es el identificador de negocio del evento; para eso existe `eventId`.

## Consumer group

Cada consumer pertenece a un **consumer group**. Kafka guarda el avance de cada grupo por separado.

```text
inventory-commands-topic
        │
        ├── Grupo inventory-service → procesa inventario
        └── Grupo auditoria         → podría guardar auditoría
```

Ambos grupos reciben todos los mensajes porque tienen avances independientes.

Dentro de un mismo grupo, Kafka reparte las particiones y cada mensaje es atendido por un solo miembro:

```text
Grupo inventory-service
├── Consumer A → partición 0
└── Consumer B → partición 1
```

Si hay más consumers que particiones, algunos quedan sin trabajo. Si un consumer falla, Kafka redistribuye sus particiones entre los miembros restantes; este proceso se llama **rebalance**.

## Confirmación o acknowledgment

Después de procesar un mensaje, el consumer confirma su offset. Esa confirmación comunica: «este grupo ya terminó hasta esta posición».

El proyecto usa:

```yaml
spring:
  kafka:
    listener:
      ack-mode: record
```

`record` confirma cada mensaje después de procesarlo correctamente o después de enviarlo al DLT. Si el proceso falla antes de confirmar, Kafka puede volver a entregarlo.

## Entrega al menos una vez e idempotencia

En la práctica, un mensaje puede ser entregado más de una vez. Por ejemplo:

1. El consumer actualiza la base de datos.
2. El proceso se detiene antes de confirmar el offset.
3. Kafka vuelve a entregar el mismo mensaje.

Esto se conoce como entrega **al menos una vez**. Por ello, los consumers deberían ser **idempotentes**: procesar dos veces el mismo `eventId` no debe duplicar un pago, una reserva o cualquier efecto de negocio.

Kafka no convierte automáticamente la base de datos y el offset en una sola transacción. La idempotencia y patrones como Transactional Outbox resuelven problemas que Kafka por sí solo no puede resolver.

## Reintentos

Si un listener lanza una excepción, `DefaultErrorHandler` reintenta el mismo mensaje. En la configuración actual:

```text
Intento inicial
  ↓ falla
Espera 2 segundos
  ↓
Reintento 2
  ↓ falla
Espera 2 segundos
  ↓
Reintento 3
```

Estos son reintentos bloqueantes: ocurren en el consumer sobre el mismo mensaje y el mismo topic. No se crean topics intermedios de retry.

## Dead Letter Topic

Si todos los intentos fallan, el mensaje se publica en un **Dead Letter Topic** o DLT:

```text
inventory-commands-topic
        │ tres intentos fallidos
        ▼
inventory-commands-topic-dlt
```

El DLT permite apartar el mensaje problemático para investigarlo sin bloquear permanentemente la partición original. No corrige el error ni reprocesa el mensaje automáticamente.

El proyecto conserva la partición original al publicar en el DLT. Por eso cada DLT debe tener al menos tantas particiones como su topic de origen.

## Producer y consumer en Spring Kafka

En el código, un producer utiliza `KafkaTemplate`:

```java
kafkaTemplate.send(topic, key, value);
```

La operación coloca el mensaje en Kafka; no ejecuta directamente el método del otro microservicio.

Un consumer declara su suscripción con `@KafkaListener`:

```java
@KafkaListener(topics = "inventory-commands-topic")
public void consume(String message) {
  // deserializar y procesar
}
```

Spring Kafka administra la conexión, consulta nuevos mensajes y ejecuta el método por cada registro recibido.

## Saga del proyecto paso a paso

El camino exitoso de una compra es:

```text
1. Cliente crea pedido por HTTP
2. Order publica RESERVE_STOCK       → inventory-commands-topic
3. Inventory publica STOCK_RESERVED  → inventory-events-topic
4. Order publica PAYMENT_REQUESTED   → payments-commands-topic
5. Payment publica PAYMENT_APPROVED  → payments-events-topic
6. Order confirma el pedido
```

Order Service coordina la Saga, pero no llama directamente a Inventory ni Payment. Reacciona a sus eventos y publica el siguiente comando.

Si el pago falla, Order solicita liberar el stock y termina cancelando el pedido. Todos los mensajes del mismo recorrido comparten un `correlationId`.

## Qué garantiza Kafka y qué no

Kafka ayuda a:

- Conservar mensajes durante un periodo configurable.
- Desacoplar producers y consumers.
- Procesar en paralelo mediante particiones.
- Mantener orden dentro de una partición.
- Guardar el avance independiente de cada consumer group.
- Reentregar mensajes no confirmados.

Kafka por sí solo no garantiza:

- Que un mensaje se procese exactamente una vez en la base de datos.
- Que el producer y Kafka confirmen atómicamente una transacción de negocio.
- Que el payload sea correcto para tu dominio.
- Que un mensaje en DLT sea corregido o reprocesado.
- Que mensajes de distintas particiones tengan un orden global.

## Vocabulario rápido

| Concepto | Definición corta |
|---|---|
| Broker | Servidor Kafka. |
| Clúster | Conjunto de brokers. |
| Topic | Canal duradero de mensajes. |
| Partición | División ordenada de un topic. |
| Réplica | Copia de una partición en otro broker. |
| Producer | Aplicación que publica mensajes. |
| Consumer | Aplicación que procesa mensajes. |
| Consumer group | Identidad compartida que registra avance y reparte particiones. |
| Offset | Posición de un mensaje dentro de una partición. |
| Key | Dato usado para agrupar y asignar mensajes a particiones. |
| Lag | Cantidad de mensajes que un grupo todavía no procesa. |
| Acknowledgment | Confirmación del offset procesado. |
| Rebalance | Redistribución de particiones entre consumers de un grupo. |
| DLT | Topic donde se apartan mensajes que agotaron sus reintentos. |
| Idempotencia | Capacidad de repetir una operación sin duplicar su efecto. |

## Orden recomendado para practicar

1. Levanta Kafka y Kafbat UI.
2. Crea una orden desde Postman.
3. Sigue el `correlationId` en los cuatro topics.
4. Observa el offset y la partición de cada mensaje.
5. Revisa el consumer group y su lag.
6. Detén temporalmente un consumer, publica mensajes y observa cómo aumenta el lag.
7. Inícialo nuevamente y observa cómo recupera el avance pendiente.
8. Después estudia [reintentos y DLT](07-kafka-retries-dlt.md).

La inspección paso a paso desde la interfaz está en la [guía de Kafbat UI](06-kafbat-ui-guide.md).
