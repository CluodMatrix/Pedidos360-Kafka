# Pedidos360 + Kafka (DSY1107 · EA3)

Kafka en Docker aplicado a los eventos de Pedidos360: 1 productor, 3 grupos de
consumidores, 4 tópicos, DLT por consumidor y API de solo lectura.

```
bridge/producer.js ──► pedidos.eventos (3 particiones, key = pedidoId)
                            ├──► report/consumer.js   (groupId = report)
                            ├──► audit/consumer.js    (groupId = audit)
                            │        └──► audit.timeline (historial legible)
                            └──► report-v2           (grupo del experimento de replay)

bridge/desde-sqs.js ─► lee los eventos REALES del backend de AWS (SNS -> SQS)
                          y los publica en el mismo topico y con el mismo contrato

errores ──────────────► pedidos.eventos.report.dlt   (DLT por consumidor)
                          pedidos.eventos.audit.dlt
```

## Requisitos

- **Docker Desktop** corriendo (`docker --version`)
- **Node.js 18+**
- Para el puente real: credenciales de AWS del lab (las del CLI)

## Puesta en marcha

```bash
npm install
docker compose up -d        # o: npm run up
docker compose ps            # los 3 contenedores en "running"
npm run topics               # crea los 4 topicos + las DLT por consumidor
```

Abre **Kafka UI** en http://localhost:8080

Luego, cada uno en su propia terminal:

```bash
npm run report      # terminal 1: consumidor + API en :3001
npm run audit       # terminal 2: consumidor + API en :3002
npm run producer    # terminal 3: publica 12 eventos simulados
```

## Comandos

| Comando | Qué hace |
|---|---|
| `npm run up` / `down` | Levanta / apaga Kafka + ZooKeeper + Kafka UI |
| `npm run reset` | Apaga, levanta y recrea los tópicos desde cero |
| `npm run topics` | Crea los tópicos (idempotente) |
| `npm run admin` | API administradora en :3000 (tópicos, grupos, lag) |
| `npm run producer [n] [pausa]` | Publica n eventos simulados |
| `npm run puente` | Puente REAL: lee SQS y publica en Kafka |
| `npm run report` / `audit` | Consumidor + API de su servicio |
| `npm run report-api` / `audit-api` | Solo la API (para depurar) |
| `npm run lag -- --grupo=report` | Monitor del lag |
| `npm run carga -- --count=20000 --modo=lote` | Prueba de carga |
| `npm run particiones -- --numero=6` | Amplia las particiones de un tópico |
| `npm run retencion` | Muestra como Kafka borra los mensajes viejos |
| `npm run replicas -- --watch` | Lider, réplicas e ISR en vivo |

### Flags del productor
| Flag | Qué provoca |
|---|---|
| `npm run producer -- --salto` | Un pedido que salta de CREADO a DESPACHADO (transición inválida) |
| `npm run producer -- --malo` | JSON roto + evento sin pedidoId (ambos van a la DLT) |
| `npm run producer -- --duplicado` | El mismo evento dos veces (mismo `eventId`) |
| `npm run producer -- --total-malo` | `total` que no cuadra con los items |
| `npm run producer -- --key-evento` | Usa `key = eventId` (rompe el orden por pedido) |

## Dos detalles que hay que tener claros

**1. El consumidor valida la transición contra SU memoria, no contra el evento.**
Compara el último estado que él recuerda del pedido con el estado nuevo del evento.
Por eso, con `key = eventId` (los eventos de un pedido van a particiones distintas)
aparecen transiciones inválidas **falsas**: el productor sí los mandó en orden.

Si el consumidor **no conoce** el pedido (memoria vacía, típico tras un reinicio)
no valida nada: no hay contra qué comparar y no se rechaza un evento normal.

**2. `fromBeginning` solo aplica a un grupo SIN offsets confirmados.**
Si el grupo `report` ya tiene offsets, al reiniciarlo **no relee** el historial:
retoma donde lo dejó y sus KPIs quedan vacíos (la memoria es del proceso).
La forma de recuperar los datos es el **replay con otro grupo** (`report-v2`).

## Topología

| Tópico | Particiones | cleanup.policy | Retención | Key |
|---|---|---|---|---|
| `pedidos.eventos` | 3 | delete | 7 días | pedidoId |
| `pedidos.eventos.report.dlt` | 3 | delete | 14 días | pedidoId |
| `pedidos.eventos.audit.dlt` | 3 | delete | 14 días | pedidoId |
| `pedidos.eventos.report-v2.dlt` | 3 | delete | 14 días | pedidoId |
| `audit.timeline` | 3 | compact,delete | 30 días | pedidoId |

**Grupos**: `report` · `audit` · `report-v2` (el del replay)

## El evento (DTO v2)

```json
{
  "eventId": "uuid",
  "tipo": "PEDIDO_CREADO",
  "version": 2,
  "fecha": "2026-10-28T20:15:54.361Z",
  "pedidoId": "P-2001",
  "clienteId": "CLI-0397",
  "total": 19470,
  "actor":   { "usuario": "op-ana", "rol": "OPERADOR" },
  "origen":  "web",
  "items":   [ { "productoId": "PROD-1", "cantidad": 2, "precio": 8990 } ],
  "estadoAnterior": null,
  "estadoNuevo": "PEDIDO_CREADO"
}
```

Tipos: `PEDIDO_CREADO` · `PEDIDO_ACEPTADO` · `PEDIDO_EN_PREPARACION` · `PEDIDO_DESPACHADO` · `PEDIDO_ENTREGADO` · `PEDIDO_CANCELADO`

### Catálogo de prueba

| productoId | Producto | Precio (CLP) |
|---|---|---|
| PROD-1 | Pizza familiar | 8990 |
| PROD-2 | Bebida 1,5L | 1990 |
| PROD-3 | Postre | 3490 |

## Estructura

```
kafka/
├─ docker-compose.yml     ZooKeeper + broker + Kafka UI
├─ .env                   configuracion local (no se versiona)
├─ shared/
│  ├─ kafka.js            cliente (KAFKA_BROKERS)
│  ├─ topics.js           topicos y su configuracion
│  ├─ dto.js              evento v2 + validacion
│  ├─ estados.js          maquina de estados y transiciones validas
│  ├─ consumidor.js       parseo, validacion, reintentos y DLT
│  └─ consultas.js        listar topicos, grupos y lag (formas de la API)
├─ bridge/
│  ├─ producer.js         productor simulado (mock) del caso
│  └─ desde-sqs.js        puente REAL SQS -> Kafka
├─ report/
│  ├─ consumer.js         consumidor + validacion de transiciones
│  ├─ kpis.js             calculo de KPIs (aplicar / obtener)
│  └─ server.js           API de solo lectura (:3001)
├─ audit/
│  ├─ consumer.js         consumidor + publicador a audit.timeline
│  └─ server.js           API de solo lectura (:3002)
├─ admin/
│  ├─ topicos.js          definicion de topicos
│  ├─ setup.js            crea los topicos
│  └─ server.js           API administradora (:3000)
└─ util/                  lag · carga · particiones · retencion · replicas
```

## API de solo lectura

El frontend **nunca lee Kafka**: llama a estos endpoints, que responden con lo que
el consumidor ya calculo a partir de los eventos.

| Servicio | Puerto | Endpoint | Respuesta |
|---|---|---|---|
| report | 3001 | `GET /api/report/ventas-por-hora` | `[{hora, total}]` |
| report | 3001 | `GET /api/report/lead-time` | `{promedioSeg, pedidos:[{pedidoId, segundos}]}` |
| report | 3001 | `GET /api/report/top-productos` | `[{productoId, unidades}]` (los 3 primeros) |
| report | 3001 | `GET /api/report/estados` | `[{estado, cantidad}]` |
| audit | 3002 | `GET /api/audit/:pedidoId` | `[{fecha, tipo, quien, desdeDonde}]` |
| audit | 3002 | `GET /api/audit?usuario=&tipo=&desde=&hasta=` | eventos que cumplen los filtros |

Cualquier `POST`/`PUT`/`PATCH`/`DELETE` responde **405 Method Not Allowed**.

Administrador (`:3000`): `GET /admin/topics` · `POST /admin/topics` ·
`PUT /admin/topics/:t` · `DELETE /admin/topics/:t` · `GET /admin/groups` ·
`GET /admin/groups/:g/lag`

## El puente real (SQS -> Kafka)

El mock (`bridge/producer.js`) simula el servicio real. Cuando quieras el flujo
verdadero, apagas el mock y levantas el puente:

```bash
# .env
SQS_QUEUE_URL=https://sqs.us-east-1.amazonaws.com/<cuenta>/q-eventos-kafka
AWS_REGION=us-east-1

npm run puente
```

El puente lee de la cola `q-eventos-kafka` (a la que el backend publica los mismos
eventos) y los publica en Kafka con **el mismo contrato v2**. `report` y `audit` no
cambian ni una linea.

El mensaje solo se borra de SQS **despues** de publicarse en Kafka: si Kafka falla,
el evento sigue en la cola y no se pierde.

## Variables de entorno

| Variable | Para que | Default |
|---|---|---|
| `KAFKA_BROKERS` | Brokers de Kafka | `localhost:9092` |
| `PORT` | Puerto de la API de report / audit | 3001 / 3002 |
| `PORT_ADMIN` | Puerto de la API administradora | 3000 |
| `GRUPO_REPORT` / `GRUPO_AUDIT` | Grupo de consumidores | report / audit |
| `FROM_BEGINNING` | `false` = no releer el historial. **Por defecto `true`** (como el enunciado) | true |
| `REINTENTOS` | Intentos antes de la DLT | 3 |
| `SQS_QUEUE_URL` | Cola del puente real | (obligatorio para `--puente`) |
| `AWS_REGION` | Región de la cola | us-east-1 |

## Experimentos

| # | Haz esto | Deberías ver |
|---|---|---|
| 1 | `npm run producer -- 12` | Los 3 pedidoId siempre en la misma partición (Kafka UI › Topics › pedidos.eventos › Messages) |
| 2 | `npm run report` en **2 terminales** | Cada una con particiones distintas; al cerrar una, la otra toma todo (rebalanceo) |
| 3 | `npm run audit` | Recibe **TODOS** los eventos aunque report ya los haya leido (otro grupo, otro offset) |
| 4 | Detén report, `npm run producer -- 20`, mira el lag | Lag = 20; al volver a levantarlo baja a 0 |
| 5 | `npm run producer -- --salto` | El pedido inválido aparece **solo** en `pedidos.eventos.report.dlt` |
| 6 | `npm run producer -- --malo` | Los mensajes defectuosos aparecen en **las dos** DLT, cada una con su header `consumidor` |
| 7 | `npm run producer -- --duplicado` | El pedido se cuenta **una** vez en los KPIs (idempotencia) |

Ver el lag desde el broker:

```bash
docker exec -it kafka kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group report
```

## Volver a empezar

```bash
npm run reset     # apaga, levanta y recrea los topicos
```

## Notas

- `The group coordinator is not available`: solo la primera vez, justo después de
  levantar Kafka. Se resuelve solo.
- `Limiting retries for the idempotent producer`: aviso de KafkaJS por usar
  `idempotent: true` con `retries: 8`.
- Para la EC2 solo cambia la variable: `KAFKA_BROKERS=<ip-privada>:9092`.
- No usamos MSK: cobra por clúster encendido, aunque nadie lo use.
