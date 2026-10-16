# Kafka: reintentos y Dead Letter Topics

Esta primera estrategia de recuperación evita que una excepción no controlada deje un consumer reintentando indefinidamente el mismo mensaje. Cada backend que consume Kafka utiliza `DefaultErrorHandler` y `DeadLetterPublishingRecoverer` de Spring Kafka.

## Flujo configurado

```text
Mensaje recibido
      ↓
Intento 1 ── falla
      ↓ espera 2 segundos
Intento 2 ── falla
      ↓ espera 2 segundos
Intento 3 ── falla
      ↓
Publicación en <topic-original>-dlt
      ↓
Confirmación del offset original
```

`max-attempts: 3` incluye el intento inicial; por tanto, se realizan dos reintentos. Si cualquiera de los tres intentos funciona, el mensaje no llega al DLT.

La cantidad anterior se aplica a errores técnicos que podrían ser temporales, por ejemplo una caída de base de datos. Los errores permanentes no se reintentan porque procesar nuevamente el mismo mensaje produciría el mismo resultado:

| Categoría | Ejemplo | Tratamiento |
|---|---|---|
| Negocio esperado | Stock insuficiente o pago rechazado | Publicar el evento de negocio correspondiente; no usar el manejador de errores. |
| Técnico temporal | Timeout, conexión o base de datos no disponible | Aplicar los intentos configurados y, si no se recupera, enviar al DLT. |
| Permanente | JSON inválido, argumento inválido o regla que no puede cambiar al reintentar | Enviar directamente al DLT. |

La clasificación utiliza una lista positiva: por defecto ninguna excepción se reintenta. Solo `CustomRetryableKafkaException`, `TransientDataAccessException` y `RecoverableDataAccessException` reciben los intentos configurados. De esta manera, errores desconocidos como `NullPointerException`, errores de argumentos y bugs de programación van directamente al DLT.

`CustomNonRetryableKafkaException` expresa explícitamente que el mensaje es inválido o incompatible. `CustomBusinessException` no debe utilizarse para representar resultados normales como stock insuficiente; esos casos deben producir un evento de negocio. Si cualquiera de estas excepciones alcanza el listener, la política predeterminada la envía directamente al DLT.

El modo `ack-mode: record` confirma cada offset individualmente cuando el mensaje terminó correctamente o cuando ya fue recuperado mediante el DLT. Así, un mensaje fallido no bloquea permanentemente los mensajes posteriores de la partición.

## Configuración

Cada backend contiene `kafka/config/KafkaConsumerRetryConfig.java`. La clase:

1. Crea un `FixedBackOff` con el intervalo y el número de reintentos.
2. Registra un `DefaultErrorHandler` para los listeners del backend.
3. Registra un `DeadLetterPublishingRecoverer` que conserva el mensaje original.
4. Resuelve el destino agregando el sufijo `-dlt` al topic de origen.
5. Registra los logs `action=event_retry` y `action=event_sent_to_dlt`.
6. Declara explícitamente los topics DLT mediante `NewTopics`.
7. Cambia el valor predeterminado mediante `defaultFalse()` para no reintentar errores desconocidos.
8. Registra con `addRetryableExceptions` únicamente los errores temporales o recuperables.

Las propiedades están en el `application.yml` de cada backend:

```yaml
spring:
  kafka:
    listener:
      ack-mode: record

app:
  kafka:
    retry:
      interval-milliseconds: ${KAFKA_RETRY_INTERVAL_MILLISECONDS:2000}
      max-attempts: ${KAFKA_RETRY_MAX_ATTEMPTS:3}
```

Los valores se pueden sobrescribir con variables de entorno sin modificar el código. En producción conviene elegirlos según el tiempo máximo aceptable del flujo y la naturaleza de la dependencia que puede fallar.

## Topics DLT

| Consumer | Topic original | Topic de mensajes no recuperados |
|---|---|---|
| Inventory Service | `inventory-commands-topic` | `inventory-commands-topic-dlt` |
| Payment Service | `payments-commands-topic` | `payments-commands-topic-dlt` |
| Order Service | `inventory-events-topic` | `inventory-events-topic-dlt` |
| Order Service | `payments-events-topic` | `payments-events-topic-dlt` |

El DLT no es un log ni elimina el mensaje original. Es otro topic Kafka que conserva el key, el value y headers de diagnóstico como el topic, partición y offset originales, además de información sobre la excepción.

## Revisar un fallo

En Grafana, busca primero el `correlationId` o `eventId`. Los intentos ejecutados por la lógica del servicio aparecen como `action=event_processing_failed`; el manejador de Kafka agrega `action=event_retry` y finalmente `action=event_sent_to_dlt`.

En Kafbat UI:

1. Abre **Topics**.
2. Selecciona el topic terminado en `-dlt` correspondiente al servicio que falló.
3. Abre **Messages** y selecciona **Newest**.
4. Presiona **Refresh**.
5. Abre el mensaje y revisa `Value` para recuperar el evento original.
6. Revisa `Headers` para conocer el topic, partición, offset y excepción originales.

No se debe usar **Produce Message** para devolver manualmente el evento al topic original sin haber corregido la causa y comprobado la idempotencia. La reconciliación y el reprocesamiento seguro corresponden a una etapa posterior.

## Alcance actual

Esta implementación cubre excepciones lanzadas por los consumers. No garantiza que la publicación inicial y una transacción de base de datos sean atómicas; ese problema se resolverá posteriormente con Transactional Outbox.

Tampoco incluye todavía:

- Un consumer automático de los DLT.
- Reprocesamiento manual o automático.
- Alertas cuando aumenta la cantidad de mensajes en DLT.
- Detección por timeout de sagas detenidas.
