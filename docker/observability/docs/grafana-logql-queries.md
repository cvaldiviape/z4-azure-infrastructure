# Consultas de logs en Grafana con LogQL

Esta guía reúne consultas prácticas para explorar en Grafana los logs que Alloy envía a Loki.

## Ejecutar una consulta

1. Abre [http://localhost:3000](http://localhost:3000).
2. Ingresa a **Explore** y selecciona `Loki`.
3. Presiona **Code**.
4. Selecciona **Last 15 minutes** o **Last 1 hour**.
5. Escribe la consulta y presiona **Run query**.

Para seguir una compra de forma más visual, abre **Dashboards > Z4 Event Commerce > Flujo de compra por Correlation ID** y pega su `correlationId`. Ese dashboard muestra explícitamente el inicio, la línea temporal ascendente y el final de la Saga.

## Etiquetas disponibles

| Etiqueta         | Ejemplo                                       | Finalidad                        |
|------------------|-----------------------------------------------|----------------------------------|
| `environment`    | `local`                                       | Entorno donde se originó el log. |
| `service_name`.  | `order-service`                               | Backend que generó el log.       |
| `filename`       | `/var/log/z4/order-service/order-service.log` | Archivo leído por Alloy.         |
| `detected_level` | `info`, `warning`, `error`                    | Nivel detectado por Loki.        |

## Consultas por backend

Todos los backends:

```logql
{environment="local"}
```

Order Service:

```logql
{service_name="order-service"}
```

Inventory Service:

```logql
{service_name="inventory-service"}
```

Payment Service:

```logql
{service_name="payment-service"}
```

Auth Service:

```logql
{service_name="auth-service"}
```

## Combinar etiquetas

Las condiciones deben ir dentro de las mismas llaves y separadas por comas:

```logql
{environment="local", service_name="order-service"}
```

Esta forma es incorrecta:

```logql
{environment="local"}{service_name="order-service"}
```

## Filtrar errores y advertencias

Errores detectados de cualquier backend:

```logql
{environment="local", detected_level="error"}
```

Errores detectados de Order Service:

```logql
{service_name="order-service", detected_level="error"}
```

Advertencias:

```logql
{environment="local", detected_level="warning"}
```

Algunos formatos no permiten detectar correctamente el nivel. Una búsqueda por texto suele ser más amplia:

```logql
{environment="local"} |~ "(?i)error|exception|failed"
```

Errores de Order Service:

```logql
{service_name="order-service"} |~ "(?i)error|exception|failed"
```

## Operadores para filtrar contenido

| Operador         | Función                                                   |
|------------------|-----------------------------------------------------------|
| `|= "texto"`     | Incluye entradas que contienen el texto exacto.           |
| `!= "texto"`     | Excluye entradas que contienen el texto exacto.           |
| `|~ "expresión"` | Incluye entradas que coinciden con una expresión regular. |
| `!~ "expresión"` | Excluye entradas que coinciden con una expresión regular. |

Buscar una excepción específica:

```logql
{environment="local"} |= "NullPointerException"
```

Buscar errores o advertencias sin distinguir mayúsculas:

```logql
{environment="local"} |~ "(?i)error|warn"
```

Excluir logs informativos:

```logql
{service_name="order-service"} != "INFO"
```

Combinar filtros:

```logql
{service_name="order-service"} |~ "(?i)error|exception" != "Validation failed"
```

## Consultas para problemas frecuentes

Problemas de Kafka:

```logql
{environment="local"} |~ "(?i)kafka|broker|consumer|producer|timeout"
```

Problemas de PostgreSQL:

```logql
{environment="local"} |~ "(?i)postgres|jdbc|sqlstate|connection refused"
```

Errores de validación HTTP:

```logql
{environment="local"} |~ "MethodArgumentNotValidException|Validation failed"
```

Problemas de autenticación o JWT:

```logql
{environment="local"} |~ "(?i)unauthorized|forbidden|jwt|authentication"
```

Errores de conexión o disponibilidad:

```logql
{environment="local"} |~ "(?i)connection refused|timeout|unavailable"
```

## Buscar identificadores de la Saga

Si el identificador aparece en el mensaje del log, se puede buscar directamente.

`correlationId`:

```logql
{environment="local"} |= "purchase-identificador-a-buscar"
```

`eventId`:

```logql
{environment="local"} |= "uuid-del-evento"
```

`orderId`:

```logql
{environment="local"} |= "orderId=123"
```

Los productores y procesadores Kafka escriben estos identificadores con el formato `campo=valor`. El `correlationId` se conserva durante todo el flujo de compra y es el punto de partida recomendado para una investigación.

## Seguir el flujo completo de una compra

Busca el `correlationId` devuelto por Order Service:

```logql
{environment="local"} |= "correlationId=purchase-identificador-a-buscar"
```

El resultado permite ordenar cronológicamente las acciones de Order, Inventory y Payment Service. Las acciones principales son:

| Acción | Significado |
|---|---|
| `action=event_received` | Un servicio recibió el mensaje desde Kafka. |
| `action=event_published` | Kafka confirmó la publicación e informó partición y offset. |
| `action=event_processed` | El consumidor terminó de procesar y registrar el evento. |
| `action=event_ignored` | El mensaje era duplicado, no compatible o ya existía el resultado. |
| `action=payment_approved` | El pago fue aprobado. |
| `action=payment_rejected` | El pago fue rechazado de forma controlada e incluye `reason`. |
| `action=event_processing_failed` | Ocurrió una excepción inesperada e incluye su stack trace. |
| `action=event_publish_failed` | Kafka no confirmó la publicación del evento. |

Mostrar solamente publicaciones y recepciones de una compra:

```logql
{environment="local"} |= "correlationId=purchase-identificador-a-buscar" |~ "action=event_(published|received)"
```

Buscar un evento exacto:

```logql
{environment="local"} |= "eventId=uuid-del-evento"
```

Buscar todos los eventos de una orden:

```logql
{environment="local"} |= "orderId=123"
```

## Errores controlados y no controlados

Los resultados funcionales controlados se registran como advertencias. Por ejemplo, para encontrar pagos rechazados:

```logql
{service_name="payment-service"} |= "action=payment_rejected"
```

Para conocer el motivo de un pago concreto:

```logql
{service_name="payment-service"} |= "correlationId=purchase-identificador-a-buscar" |= "action=payment_rejected"
```

Las excepciones no controladas se registran como errores junto con el contexto del evento y el stack trace:

```logql
{environment="local"} |= "action=event_processing_failed"
```

Fallos de publicación o serialización:

```logql
{environment="local"} |~ "action=event_(publish|serialization)_failed"
```

Eventos duplicados o ignorados:

```logql
{environment="local"} |= "action=event_ignored"
```

Estos son logs estructurados como texto `campo=valor`, no etiquetas de Loki. Por ello se buscan con `|=` o `|~` y no se colocan dentro del selector `{...}`.

Esta etapa ofrece correlación de negocio mediante `correlationId`, pero todavía no genera un `traceId` distribuido con OpenTelemetry. El `correlationId` sigue una compra completa; un futuro `traceId` permitirá además visualizar spans y duraciones técnicas.

## Interpretar los resultados

- **Logs volume** muestra cuántas entradas se encontraron por periodo.
- **Logs** muestra las entradas individuales.
- `service_name` identifica el backend.
- `filename` identifica el archivo leído por Alloy.
- `detected_level` muestra el nivel detectado.
- Al expandir una excepción debe aparecer su stack trace agrupado.

## Cuando aparece `No data`

1. Ejecuta la consulta más amplia:

   ```logql
   {environment="local"}
   ```

2. Cambia el rango a **Last 1 hour**.
3. Confirma que los backends estén ejecutándose.
4. Confirma que existan archivos en `backend/*-service/logs`.
5. Verifica los contenedores:

   ```bash
   docker compose -f infra/docker/docker-compose.yml ps alloy loki grafana
   ```

6. Revisa Alloy:

   ```bash
   docker compose -f infra/docker/docker-compose.yml logs --tail=100 alloy
   ```

7. Ejecuta nuevamente una petición en Postman y presiona **Run query**.

## Consultas rápidas

```logql
{environment="local"}
```

```logql
{environment="local", service_name="order-service"}
```

```logql
{environment="local"} |~ "(?i)error|exception|failed"
```

```logql
{service_name="order-service"} |~ "(?i)error|exception|failed"
```

```logql
{environment="local"} |~ "(?i)kafka|broker|timeout"
```

```logql
{environment="local"} |~ "(?i)postgres|jdbc|sqlstate|connection refused"
```
