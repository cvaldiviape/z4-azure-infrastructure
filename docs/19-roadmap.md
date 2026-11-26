# Plan a futuro

Decisiones tomadas y trabajo pendiente, para no perder el hilo entre sesiones. Cuando algo se termine, se marca aquí y se documenta en su guía.

## Visión del producto

Una plataforma de comercio **multi-tenant**: un solo backend donde cada cliente (vendedor) es un `tenant_id` con su catálogo, su dominio, su logo y su cuenta de pasarela. No se hace una web por cliente, se da de alta un tenant.

Al inicio hay **un solo tenant: yo**, vendiendo como si fuera el primer cliente. El objetivo hoy es aprender. El negocio se decide si aparece un cliente real.

La ventaja frente a Shopify o Tiendanube no es "tener una tienda", sino el inventario confiable, los pedidos que no se pierden y la trazabilidad: justo lo que dan Kafka, la Saga, el outbox y el DLT. Nichos posibles:

| #   | Nicho                                | Por qué encaja                           |
|-----|--------------------------------------|------------------------------------------|
| 1   | Negocios que hoy venden por WhatsApp | Pedido web con confirmación por WhatsApp |
| 2   | Restaurantes o delivery              | Stock en tiempo real                     |
| 3   | Colegios                             | Uniformes, útiles y eventos con pago     |
| 4   | Distribuidores B2B                   | Pedidos por mayor con stock y crédito    |

## Multi-tenant: estrategia elegida

| #   | Estrategia                  | Aislamiento | Complejidad | Estado                                      |
|-----|-----------------------------|-------------|-------------|---------------------------------------------|
| 1   | BD compartida + `tenant_id` | Bajo        | Baja        | **Elegida para empezar**                    |
| 2   | Un schema por tenant        | Medio       | Media       | Descartada por ahora                        |
| 3   | Una BD por tenant           | Alto        | Alta        | Posible más adelante, para clientes grandes |

Regla: diseñar desde ya pensando en el tenant, aunque solo exista uno. Agregar el concepto cuando todo está hecho cuesta mucho más.

| #   | Dónde va el tenant  | Cómo                                               |
|-----|---------------------|----------------------------------------------------|
| 1   | Tablas de negocio   | Columna `tenant_id`                                |
| 2   | JWT de auth-service | Claim `tenant_id`                                  |
| 3   | Eventos de Kafka    | Header o campo `tenantId` en `IntegrationEventDto` |
| 4   | Tienda              | Se resuelve por dominio (`tienda-juan.tuapp.com`)  |

## Los tres frontends

Separados por **quién los usa**:

| #   | Front           | Usuario          | Alcance                      | Stack sugerido        |
|-----|-----------------|------------------|------------------------------|-----------------------|
| 1   | **Tienda**      | Comprador        | Un tenant (según el dominio) | Next.js o Astro (SEO) |
| 2   | **Backoffice**  | Vendedor / admin | Solo su tenant               | Angular 22 + Material |
| 3   | **Ops Console** | Yo, como técnico | Todos los tenants            | Angular 22 + Material |

Nombres: igual que los servicios terminan en `-service`, los fronts terminan en `-web`. Primero va el dominio y al final el tipo, así en GitHub se agrupan por dominio.

| #   | Front       | Carpeta local              | Repo en GitHub             | Puerto local |
|-----|-------------|----------------------------|----------------------------|--------------|
| 1   | Ops Console | `frontend/ops-console-web` | `z4-azure-ops-console-web` | 4200         |
| 2   | Backoffice  | `frontend/backoffice-web`  | `z4-azure-backoffice-web`  | 4201         |
| 3   | Tienda      | `frontend/store-web`       | `z4-azure-store-web`       | 3001         |

La tienda usa 3001 y no 3000, porque ahí está Grafana (con Next.js se arranca con `next dev -p 3001`; con Astro sería 4321). CORS hoy solo permite 4200 y 5173: al crear el Backoffice y la tienda hay que agregar sus puertos a `app.security.cors-allowed-origins`.

### Base técnica de los fronts Angular

Tomada del proyecto propio MangaGodWEB (Angular 14), adaptada a Angular 22 (Node 26 LTS, instalado con nvm):

| #   | Tema        | Decisión                                                                              |
|-----|-------------|---------------------------------------------------------------------------------------|
| 1   | Componentes | Standalone, `@if`/`@for` en templates, signals, `*.routes.ts` y guards funcionales    |
| 2   | Librería UI | Angular Material: tabla, paginador, formularios, diálogos, snackbar y chips           |
| 3   | Estilos     | CSS propio en archivos aparte, sin Tailwind; clases con BEM (`sidebar__item--active`) |
| 4   | Responsive  | El `grid.css` de 12 columnas de MangaGodWEB (`row`, `col-6`, `md:col-3`)              |
| 5   | Variables   | Colores, espacios y breakpoints en `src/styles/variables.css`                         |
| 6   | Reutilizado | Sidebar con submenú, layout responsive, navbar, `loading-button`, `snack-bar.service` |
| 7   | Seguridad   | Carpeta `security/` con interceptores JWT y de error, `jwt.service` y guard de rol    |
| 8   | Estructura  | `modules/<x>/{components,services,static}`, `models/{request,response}`, `shared/`    |

Al sidebar de MangaGodWEB se le agrega un `path` opcional en `Item`, para ítems que navegan directo sin submenú. La línea de tiempo de la Saga es un componente propio, porque Material no trae uno.

Por qué la Ops Console va aparte del backoffice:

- **Usuarios distintos.** El vendedor piensa en pedidos y stock; el técnico, en eventos, offsets y excepciones.
- **Seguridad.** Puede reprocesar y descartar mensajes. Separada, puede quedar solo en red interna o detrás de VPN.
- **Multi-tenant.** El backoffice de cada cliente ve solo su tienda; la Ops Console ve todos. Es la razón más fuerte.
- **Ritmo propio.** Se cambia y despliega sin tocar lo que usan los clientes.

Las tres comparten el mismo login (auth-service) con roles distintos. Más adelante podría llegar un cuarto front, un **panel de plataforma** (alta de tenants, planes y cobros), pero solo cuando haya clientes reales.

### Tienda

| #   | Pantalla             | Qué hace                                                      | Backend           |
|-----|----------------------|---------------------------------------------------------------|-------------------|
| 1   | Inicio / catálogo    | Productos con foto, precio y disponibilidad; buscar y filtrar | inventory-service |
| 2   | Detalle de producto  | Fotos, descripción, stock y "Agregar al carrito"              | inventory-service |
| 3   | Carrito              | Cantidades y total; se guarda en el navegador                 | ninguno           |
| 4   | Registro / login     | Crear cuenta e iniciar sesión                                 | auth-service      |
| 5   | Checkout             | Dirección, resumen y "Pagar"                                  | order-service     |
| 6   | Resultado del pedido | "Procesando…" → "Confirmado" o "Rechazado"                    | order-service     |
| 7   | Mis pedidos          | Historial y estado de cada pedido                             | order-service     |

La pantalla 6 no confirma al instante: la Saga pasa por inventario y pago de forma asíncrona. El front muestra "procesando" y consulta el estado cada pocos segundos, o lo recibe por WebSocket o SSE.

### Backoffice

| #   | Sección       | Pantalla   | Qué hace                                                 | Backend           |
|-----|---------------|------------|----------------------------------------------------------|-------------------|
| 1   | Comercial     | Dashboard  | Ventas del día, pedidos pendientes, poco stock           | order + inventory |
| 2   | Comercial     | Productos  | Crear, editar y desactivar; precio, fotos, categoría     | inventory-service |
| 3   | Comercial     | Inventario | Stock actual, reservado y disponible; ajustes con motivo | inventory-service |
| 4   | Comercial     | Pedidos    | Lista con filtros por estado y detalle                   | order-service     |
| 5   | Comercial     | Pagos      | Aprobados y rechazados, con su motivo                    | payment-service   |
| 6   | Comercial     | Clientes   | Quién compró y cuánto                                    | auth + order      |
| 7   | Configuración | Tienda     | Nombre, logo y colores                                   | por definir       |
| 8   | Configuración | Usuarios   | Usuarios del backoffice y sus roles                      | auth-service      |
| 9   | Configuración | Pasarela   | Credenciales de la pasarela de pago                      | payment-service   |

### Ops Console

| #   | Pantalla        | Qué muestra                                       | Backend       | Estado                        |
|-----|-----------------|---------------------------------------------------|---------------|-------------------------------|
| 1   | Resumen         | DLT pendientes, sagas detenidas, outbox           | ops + order   | Parcial (`/stats`)            |
| 2   | Sagas           | Buscar por pedido; línea de tiempo de eventos     | order-service | Listo (`/ops/purchase-sagas`) |
| 3   | Sagas detenidas | En timeout o a medias, con acción de reconciliar  | order-service | Pendiente                     |
| 4   | Bandeja DLT     | Filtros, payload, headers; reprocesar o descartar | ops-service   | Listo (`/ops/dead-letters`)   |
| 5   | Outbox          | Eventos pendientes de publicar y antigüedad       | cada servicio | Pendiente                     |
| 6   | Inbox           | Mensajes ya procesados (verificar idempotencia)   | cada servicio | Pendiente                     |
| 7   | Topics          | Topics, particiones y lag por consumer group      | ops-service   | Pendiente                     |
| 8   | Auditoría       | Quién reprocesó o descartó qué y cuándo           | ops-service   | Datos listos                  |

El front nunca habla directo con Kafka ni con las bases de datos: todo pasa por ops-service u order-service. Para los logs completos, enlace a Grafana; no se rehace Grafana dentro del front.

Para la tienda, Angular con SSR también sirve si no se quiere aprender otro framework. Topics y lag se leerán con el `AdminClient` de Kafka; la auditoría ya tiene los datos (`resolved_by`, `resolution_note`).

Los orígenes que ya permite CORS (`app.security.cors-allowed-origins`) son `http://localhost:4200` (Angular) y `http://localhost:5173` (Vite).

## Pasarela de pago

| #   | Opción          | Nota                                                      |
|-----|-----------------|-----------------------------------------------------------|
| 1   | Stripe          | Mejor documentación y modo de prueba; ideal para aprender |
| 2   | Culqi           | Perú                                                      |
| 3   | Mercado Pago    | Perú y LATAM                                              |
| 4   | Izipay / Niubiz | Perú                                                      |

Encaja con payment-service: el webhook de la pasarela publica el resultado en Kafka y la Saga continúa sola. Hoy el pago es simulado.

## Orden de trabajo

| #   | Paso                                                                    | Estado    |
|-----|-------------------------------------------------------------------------|-----------|
| 1   | Librerías `z4-shared-*` y migración de los servicios                    | Hecho     |
| 2   | ops-service: guardar DLT, replay y descarte                             | Hecho     |
| 3   | order-service: API de lectura de Sagas                                  | Hecho     |
| 4   | Prueba de punta a punta con Docker (pedido ok, rechazado, DLT y replay) | Pendiente |
| 5   | Backoffice: login, productos e inventario (da el catálogo a la tienda)  | Pendiente |
| 6   | Tienda: catálogo, carrito, checkout y mis pedidos, con pago simulado    | Pendiente |
| 7   | Ops Console: Sagas y DLT primero                                        | Pendiente |
| 8   | Backoffice: pedidos, pagos y clientes                                   | Pendiente |
| 9   | Pasarela real en modo prueba, con webhook                               | Pendiente |
| 10  | Multi-tenant (`tenant_id` en tablas, JWT y eventos)                     | Pendiente |

La Ops Console puede adelantarse a la tienda si se quiere practicar Kafka primero; los pedidos se generan con Postman o Bruno.

## Pendientes técnicos del backend

| #   | Tema                     | Qué falta                                                                                   |
|-----|--------------------------|---------------------------------------------------------------------------------------------|
| 1   | Rol ADMIN                | Ningún usuario lo tiene por defecto; hay que insertarlo en `user_roles` para usar `/ops/**` |
| 2   | API de outbox e inbox    | Endpoints de lectura para la Ops Console                                                    |
| 3   | Eventos no soportados    | inventory y payment los ignoran en silencio; decidir si van al DLT                          |
| 4   | Retención del DLT        | Cuánto tiempo guardar mensajes en Kafka y en `dead_letter_messages`                         |
| 5   | `KAFKA_LOG_DIRS`         | Revisar la configuración del broker en docker-compose                                       |
| 6   | Dependencias redundantes | mapstruct en payment e inventory; security y oauth2 ya llegan por las librerías             |
| 7   | Endpoints de inventario  | Productos y stock para el backoffice (ya quedarán protegidos con JWT)                       |

## Despliegue y CI/CD (más adelante)

| #   | Tema                      | Idea                                                                                           |
|-----|---------------------------|------------------------------------------------------------------------------------------------|
| 1   | Repositorio de artefactos | Publicar las `z4-shared-*` (Nexus, CodeArtifact o GitHub Packages); reemplaza `install-all.sh` |
| 2   | Pipeline                  | Compilar, probar y publicar librerías y servicios en cada push                                 |
| 3   | Perfil Kubernetes         | Logs a stdout, probes de liveness/readiness, secretos fuera del yml                            |
| 4   | Varias réplicas           | El outbox ya lo soporta (`FOR UPDATE SKIP LOCKED`)                                             |
| 5   | Ops Console               | Solo en red interna o detrás de VPN                                                            |
