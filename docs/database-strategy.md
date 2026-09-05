# PostgreSQL: estrategia de bases de datos

Este documento explica las decisiones de persistencia de Z4 Event Commerce: propiedad de datos por microservicio, aislamiento local mediante contenedores PostgreSQL y criterios utilizados para elegir el motor de base de datos.

## Decisión principal

El proyecto aplica el patrón `database per service`: cada microservicio es propietario de sus datos y administra su esquema de manera independiente.

```text
auth-service      → auth-postgres      → auth_db
order-service     → orders-postgres    → orders_db
inventory-service → inventory-postgres → inventory_db
payment-service   → payments-postgres  → payments_db
```

Ser propietario de una base significa que solamente el microservicio responsable puede consultar o modificar directamente sus tablas.

Por ejemplo, `order-service` no debe ejecutar consultas SQL sobre `inventory_db`. Para conocer el resultado de una reserva utiliza eventos Kafka publicados por `inventory-service`.

## Motivos para separar los datos

### Autonomía

Cada microservicio puede evolucionar sus entidades, tablas y migraciones sin modificar directamente los esquemas de los demás servicios.

### Bajo acoplamiento

Los servicios intercambian información mediante contratos HTTP o eventos Kafka. No utilizan joins entre tablas pertenecientes a dominios diferentes.

```text
Incorrecto
order-service → SELECT directamente sobre inventory_db

Correcto
order-service → evento Kafka → inventory-service → inventory_db
```

### Despliegue independiente

Cada servicio ejecuta sus propias migraciones Flyway. Una migración de Payment no necesita desplegarse junto con Order si el contrato de eventos se mantiene compatible.

### Seguridad

En producción, cada servicio puede utilizar credenciales con acceso exclusivo a su base. Una vulnerabilidad en un servicio no debería conceder automáticamente acceso a los datos de los demás.

### Escalabilidad y operación

Una base con mayor carga puede escalarse, respaldarse o monitorearse de forma diferente. Por ejemplo, Inventory podría recibir más escrituras concurrentes que Auth.

## Por qué existen cuatro contenedores en local

Cada contenedor ejecuta una instancia PostgreSQL independiente que escucha internamente en el puerto `5432`. Docker publica un puerto distinto en el Mac porque un mismo puerto externo no puede asignarse simultáneamente a cuatro contenedores.

| Servicio Docker | Puerto del Mac | Puerto interno | Base |
|---|---:|---:|---|
| `auth-postgres` | 5433 | 5432 | `auth_db` |
| `orders-postgres` | 5434 | 5432 | `orders_db` |
| `inventory-postgres` | 5435 | 5432 | `inventory_db` |
| `payments-postgres` | 5436 | 5432 | `payments_db` |

Esta configuración prioriza el aprendizaje y hace visible el aislamiento:

- Cada PostgreSQL tiene su propio proceso.
- Cada base tiene su propio volumen.
- Cada servicio puede iniciarse y probarse de manera independiente.
- Una configuración incorrecta entre servicios es más fácil de detectar.

El costo es un mayor consumo de memoria, más contenedores y más puertos locales.

## Separación lógica y separación física

`Database per service` exige propiedad y aislamiento de datos, pero no obliga siempre a contratar una instancia física por microservicio.

### Instancia independiente por servicio

```text
Order service     → PostgreSQL A
Inventory service → PostgreSQL B
Payment service   → PostgreSQL C
```

Ofrece mayor aislamiento y permite escalar cada base por separado. También incrementa costos, backups, monitoreo y trabajo operativo.

### Clúster compartido con bases separadas

```text
Clúster PostgreSQL
├── auth_db      ← auth_user
├── orders_db    ← orders_user
├── inventory_db ← inventory_user
└── payments_db  ← payments_user
```

Es una alternativa frecuente cuando se busca reducir costos. Mantiene propiedad lógica mediante bases, usuarios y permisos separados, pero comparte capacidad y dominio de fallo.

### Base compartida por todos los servicios

```text
commerce_db
└── tablas utilizadas directamente por varios servicios
```

Esta alternativa no se eligió porque aumenta el acoplamiento: varios servicios dependen del mismo esquema, los cambios requieren mayor coordinación y se vuelve difícil identificar al propietario de cada dato.

## Por qué se eligió PostgreSQL

PostgreSQL se eligió por la combinación de capacidades relacionales y necesidades previstas del dominio:

- Transacciones ACID para operaciones locales de pedidos, inventario y pagos.
- Restricciones como claves foráneas, valores únicos y `CHECK` para proteger la integridad.
- Control de concurrencia y bloqueos de filas para futuras reservas simultáneas de inventario.
- SQL amplio para consultas, índices y operaciones de mantenimiento.
- Soporte adecuado de Spring Data JPA, Hibernate y Flyway.
- Disponibilidad en servicios administrados de los principales proveedores de nube.
- Experiencia práctica que se desea desarrollar con PostgreSQL.

Un ejemplo futuro para impedir cantidades negativas sería:

```sql
ALTER TABLE inventories
ADD CONSTRAINT chk_available_quantity
CHECK (available_quantity >= 0);
```

Para coordinar reservas concurrentes podría utilizarse una transacción con bloqueo de fila:

```sql
SELECT *
FROM inventories
WHERE product_id = :productId
FOR UPDATE;
```

Estas capacidades no son exclusivas de PostgreSQL. MySQL con InnoDB también ofrece transacciones, restricciones y `SELECT ... FOR UPDATE`.

## Por qué no se eligió MySQL

MySQL no fue descartado por una carencia imprescindible. También podría implementar correctamente este proyecto.

La elección de PostgreSQL se realizó porque satisface los requisitos, se integra con las herramientas seleccionadas y permite practicar sus capacidades transaccionales y SQL. Si una organización ya tuviera experiencia, monitoreo, backups y soporte consolidados alrededor de MySQL, elegir MySQL podría reducir el riesgo operativo.

La decisión general es:

```text
Requisitos técnicos
       +
Experiencia del equipo
       +
Infraestructura existente
       +
Costos y operación
       ↓
Motor de base de datos
```

La mejor opción no es necesariamente la que contiene más funciones, sino la que satisface los requisitos con un costo y riesgo razonables.

## Administración del esquema

Cada backend separa las responsabilidades entre Flyway e Hibernate:

```text
Flyway
└── crea y modifica el esquema mediante db/migration

Hibernate
└── valida que las entidades coincidan mediante ddl-auto: validate
```

La configuración utilizada es:

```yaml
spring:
  jpa:
    hibernate:
      ddl-auto: validate
```

Una modificación estructural se implementa creando una nueva migración SQL. No se modifica una migración que ya fue aplicada en un ambiente compartido.

## Persistencia local

Los contenedores PostgreSQL son reemplazables, pero sus datos se almacenan en volúmenes Docker:

```text
Contenedor eliminado + volumen conservado → datos conservados
Contenedor eliminado + volumen eliminado   → datos eliminados
```

`docker compose down` conserva los volúmenes. `docker compose down -v` también los elimina y provoca pérdida de los datos locales.

## Resumen de la decisión

| Decisión | Motivo |
|---|---|
| Una base propiedad de cada servicio | Autonomía y bajo acoplamiento |
| Cuatro PostgreSQL en local | Hacer explícito el aislamiento durante el aprendizaje |
| PostgreSQL | Integridad, transacciones, SQL amplio e integración con el stack |
| Flyway | Versionar y ejecutar cambios del esquema |
| Hibernate `validate` | Detectar diferencias sin modificar las tablas |
| Volúmenes Docker | Conservar los datos aunque se recreen los contenedores |

En producción, la separación puede implementarse con instancias independientes o con un clúster compartido y bases/usuarios separados, según carga, criticidad, seguridad, experiencia del equipo y presupuesto.
