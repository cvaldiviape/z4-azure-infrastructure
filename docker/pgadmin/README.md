# Configuración local de pgAdmin

pgAdmin es una interfaz web para consultar y administrar las mismas bases PostgreSQL utilizadas por los backends. No se conecta a los controladores ni a las aplicaciones Java; se conecta directamente a los contenedores PostgreSQL.

## Relación entre los componentes

```text
Order service ejecutado en el Mac
        │
        │ localhost:5434
        ▼
Docker publica el puerto 5434
        │
        ▼
orders-postgres:5432 ◄──────── pgAdmin
                               orders-postgres:5432
```

El backend y pgAdmin utilizan direcciones diferentes porque se ejecutan en lugares distintos:

- Los backends se ejecutan en el Mac desde IntelliJ y acceden mediante `localhost` y el puerto publicado por Docker.
- pgAdmin se ejecuta dentro de Docker y accede mediante el nombre del servicio y el puerto interno `5432`.

Ambos llegan a la misma base y observan los mismos registros.

| Backend   | Conexión desde Mac | Conexión desde pgAdmin | Base de datos  |
|-----------|--------------------|------------------------|----------------|
| Auth | `localhost:5433` | `auth-postgres:5432` | `auth_db` |
| Order | `localhost:5434` | `orders-postgres:5432` | `orders_db` |
| Inventory | `localhost:5435` | `inventory-postgres:5432` | `inventory_db` |
| Payment | `localhost:5436` | `payments-postgres:5432` | `payments_db` |

## Responsabilidad de cada archivo

### `docker-compose.yml`

**Contiene:** la definición de los contenedores, variables de entorno, puertos, dependencias, archivos montados y volúmenes persistentes de la infraestructura.

**Estructura utilizada por pgAdmin:**

```yaml
pgadmin:
  image: dpage/pgadmin4:9.17
  ports:
    - "5050:80"
  environment:
    PGADMIN_DEFAULT_EMAIL: admin@z4greed.com
    PGADMIN_DEFAULT_PASSWORD: admin
    PGADMIN_REPLACE_SERVERS_ON_STARTUP: "True"
    PGPASS_FILE: /pgadmin4/pgpass
  volumes:
    - pgadmin_data:/var/lib/pgadmin
    - ./pgadmin/servers.json:/pgadmin4/servers.json:ro
    - ./pgadmin/pgpass:/pgadmin4/pgpass:ro
```

| Campo   | Finalidad                                                                                                    |
|---------|--------------------------------------------------------------------------------------------------------------|
| `image` | Define la versión de pgAdmin ejecutada por Docker.                                                           |
| `ports` | Publica el puerto `80` del contenedor como `localhost:5050` en el Mac.                                       |
| `PGADMIN_DEFAULT_EMAIL` | Crea el usuario utilizado para iniciar sesión en la interfaz.                                |
| `PGADMIN_DEFAULT_PASSWORD` | Define la contraseña de acceso a la interfaz.                                             |
| `PGADMIN_REPLACE_SERVERS_ON_STARTUP` | Vuelve a importar las conexiones de `servers.json` cada vez que inicia pgAdmin. |
| `PGPASS_FILE` | Indica dentro del contenedor dónde debe buscar las contraseñas PostgreSQL.                             |
| `pgadmin_data` | Conserva usuarios y preferencias de pgAdmin entre reinicios.                                          |
| `servers.json` | Monta el catálogo de conexiones dentro del contenedor.                                                |
| `pgpass` | Monta las contraseñas de las conexiones dentro del contenedor.                                              |

**Usado por:** Docker Compose para crear y configurar el contenedor.

**Finalidad:** levantar pgAdmin de manera reproducible y vincularlo con las bases PostgreSQL. El sufijo `:ro` significa `read only`: el contenedor puede leer esos archivos, pero no modificarlos.

### `servers.json`

**Contiene:** el catálogo de servidores PostgreSQL que aparecerán automáticamente en el panel izquierdo de pgAdmin.

**Estructura:** el objeto raíz contiene `Servers`. Dentro de él, cada número representa el identificador de una conexión:

```json
{
  "Servers": {
    "1": {
      "Name": "Orders database",
      "Group": "Z4 Event Commerce Local",
      "Host": "orders-postgres",
      "Port": 5432,
      "MaintenanceDB": "orders_db",
      "Username": "ecommerce",
      "SSLMode": "prefer"
    }
  }
}
```

| Campo | Finalidad |
|------------------|------------------------------------------------------------------------|
| `Servers` | Agrupa todas las conexiones que se importarán. |
| `"1"`  | Identificador único de la conexión dentro del archivo. |
| `Name` | Nombre mostrado en el panel de pgAdmin. |
| `Group` | Carpeta visual que agrupa las conexiones del proyecto. |
| `Host` | Nombre del servicio PostgreSQL dentro de la red de Docker. |
| `Port` | Puerto interno en el que escucha PostgreSQL. |
| `MaintenanceDB` | Base a la que pgAdmin se conecta inicialmente. |
| `Username` | Usuario PostgreSQL con el que se realizará la conexión. |
| `SSLMode` | Política SSL de la conexión; `prefer` intenta usar SSL cuando está disponible. |

**Usado por:** pgAdmin durante su inicialización.

**Finalidad:** evitar registrar manualmente las conexiones Auth, Orders, Inventory y Payments. No contiene contraseñas porque pgAdmin no permite importarlas mediante este formato.

### `pgpass`

**Contiene:** las credenciales PostgreSQL utilizadas por pgAdmin para las conexiones declaradas en `servers.json`.

**Estructura:** es un archivo de texto sin encabezados. Cada conexión ocupa una línea con cinco valores separados por `:`:

```text
host:puerto:base_de_datos:usuario:contraseña
```

Contenido del proyecto:

```text
auth-postgres:5432:auth_db:ecommerce:ecommerce
orders-postgres:5432:orders_db:ecommerce:ecommerce
inventory-postgres:5432:inventory_db:ecommerce:ecommerce
payments-postgres:5432:payments_db:ecommerce:ecommerce
```

|   | Campo      | Ejemplo           | Finalidad                                     |
|---:|-----------|-------------------|-----------------------------------------------|
| 1 | Host       | `orders-postgres` | Identifica el servicio PostgreSQL de Docker.  |
| 2 | Puerto     | `5432`            | Identifica el puerto interno de PostgreSQL.   |
| 3 | Base       | `orders_db`       | Indica para qué base es válida la credencial. |
| 4 | Usuario    | `ecommerce`       | Indica qué usuario utilizará la contraseña.   |
| 5 | Contraseña | `ecommerce`       | Valor utilizado para autenticar al usuario.   |

**Usado por:** pgAdmin cuando abre una conexión PostgreSQL.

**Finalidad:** autenticar automáticamente las conexiones sin pedir la contraseña cada vez.

### `application-local.yml`

**Contiene:** la configuración que cada backend utiliza cuando está activo el perfil local. Entre otras propiedades, define la URL, el usuario y la contraseña de su base.

**Estructura en Order:**

```yaml
spring:
  datasource:
    url: ${ORDERS_DB_URL:jdbc:postgresql://localhost:5434/orders_db}
    username: ${ORDERS_DB_USERNAME:ecommerce}
    password: ${ORDERS_DB_PASSWORD:ecommerce}
```

La expresión `${VARIABLE:valor_predeterminado}` usa la variable de entorno cuando existe y, en caso contrario, utiliza el valor situado después de `:`.

**Usado por:** el backend Java correspondiente, no por pgAdmin.

**Finalidad:** permitir que el backend ejecutado desde IntelliJ se conecte a su PostgreSQL mediante el puerto publicado en el Mac.

### Resumen

| Archivo o configuración               | Contiene                                      | Usado por                  | Finalidad                                        |
|---------------------------------------|-----------------------------------------------|----------------------------|--------------------------------------------------|
| `docker-compose.yml`                  | Contenedores, puertos, variables y volúmenes  | Docker Compose             | Crear toda la infraestructura local              |
| `servers.json`                        | Host, puerto, base y usuario de cada conexión | pgAdmin                    | Registrar las conexiones automáticamente         |
| `pgpass`                              | Contraseña asociada a cada conexión | pgAdmin | Autenticarse en PostgreSQL |
| `application-local.yml`               | URL y credenciales de un backend              | Aplicación Spring Boot     | Conectar el backend a PostgreSQL                 |
| `POSTGRES_USER` y `POSTGRES_PASSWORD` | Usuario inicial de una base                   | Imagen PostgreSQL          | Crear el usuario cuando se inicializa el volumen |

pgAdmin combina la información de ambos archivos:

```text
servers.json
├── Host: orders-postgres
├── Port: 5432
├── MaintenanceDB: orders_db
└── Username: ecommerce
             │
             │ busca host + puerto + base + usuario
             ▼
pgpass
└── orders-postgres:5432:orders_db:ecommerce:ecommerce
                                               └── contraseña
```

Los valores de host, puerto, base, usuario y contraseña deben coincidir entre las configuraciones que participan en cada conexión.

## Uso

Desde la raíz del proyecto, inicia pgAdmin y las bases requeridas:

```bash
docker compose -f infra/docker/docker-compose.yml up -d pgadmin
```

Abre [http://localhost:5050](http://localhost:5050) e inicia sesión:

```text
Correo: admin@z4greed.com
Contraseña: admin
```

Las cuatro conexiones se encuentran en el grupo `Z4 Event Commerce Local`. Para consultar una tabla:

```text
Z4 Event Commerce Local
└── Orders database
    └── Databases
        └── orders_db
            └── Schemas
                └── public
                    └── Tables
```

Haz clic derecho sobre una tabla y selecciona `View/Edit Data → All Rows`.

## Seguridad

Las credenciales `ecommerce/ecommerce` y `admin` son únicamente para aprendizaje y desarrollo local. En un ambiente real, `pgpass` no debería almacenarse en Git; las contraseñas deben gestionarse mediante secrets o un servicio como Azure Key Vault, AWS Secrets Manager o HashiCorp Vault.
