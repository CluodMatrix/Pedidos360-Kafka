// Productor del caso Pedidos360 v2 con Kafka.
//
// DOS MODOS:
//
// 1) MOCK (por defecto). Simula el flujo de pedidos del cliente:
//      npm run producer                 -> 12 eventos, 3 pedidoId distintos
//      npm run producer -- 20           -> 20 eventos
//      npm run producer -- 12 1500      -> 12 eventos, uno cada 1,5 s (demo en vivo)
//
//    El mock publica EXACTAMENTE el mensaje que publicara el servicio real:
//    mismos campos y tipos, misma key (pedidoId) y mismos headers (correlationId).
//    Cuando el puente real entre, solo se apaga el mock: report y audit no cambian.
//
// 2) PUENTE REAL (--puente). Lee los eventos reales del backend de AWS
//    (SNS -> SQS) y los publica en Kafka con el mismo contrato.
//    Ver bridge/desde-sqs.js.
//
// Flags de error (para demostrar reintentos, DLT e idempotencia):
//      npm run producer -- --salto      -> un pedido que salta de CREADO a DESPACHADO
//      npm run producer -- --malo       -> JSON roto + evento sin pedidoId (-> DLT)
//      npm run producer -- --duplicado  -> el mismo evento dos veces (mismo eventId)
//      npm run producer -- --total-malo -> PEDIDO_CREADO con total que no cuadra
//      npm run producer -- --key-evento-> usa key = eventId (rompe el orden por pedido)
const crypto = require('crypto');
const { Partitioners } = require('kafkajs');
const { crearKafka } = require('../shared/kafka');
const { crearEvento, validar, itemsDeEjemplo, CATALOGO } = require('../shared/dto');
const { PEDIDOS } = require('../shared/topics');

const producer = crearKafka('bridge').producer({
  idempotent: true, // sin duplicados por red
  maxInFlightRequests: 1,
  createPartitioner: Partitioners.DefaultPartitioner,
});

/** Publica un evento ya construido. */
async function publicarEvento(evento, correlationId, key) {
  const [r] = await producer.send({
    topic: PEDIDOS,
    acks: -1, // all: lider + todas las replicas (lo seguro para pedidos)
    messages: [
      {
        key: key ?? evento.pedidoId, // mismo pedido -> misma particion -> su historia queda en orden
        value: JSON.stringify(evento),
        headers: { correlationId },
      },
    ],
  });
  console.log(`-> p${r.partition} off${r.baseOffset}  ${evento.pedidoId}  ${evento.tipo}`);
  return r;
}

/** Publica el mensaje TAL CUAL, sin validar: solo para provocar errores. */
async function publicarCrudo(key, value, correlationId) {
  await producer.send({
    topic: PEDIDOS,
    acks: -1,
    messages: [{ key, value, headers: { correlationId: correlationId || crypto.randomUUID() } }],
  });
  console.log(`-> (crudo) ${value}`);
}

// El flujo completo de un pedido: CREADO -> ACEPTADO -> EN_PREPARACION ->
// DESPACHADO -> ENTREGADO. El actor cambia en cada evento (y el timeline de la
// Parte 4 queda realista).
const FLUJO = [
  { tipo: 'PEDIDO_CREADO', rol: 'CLIENTE', usuario: 'cli-0397' },
  { tipo: 'PEDIDO_ACEPTADO', rol: 'OPERADOR', usuario: 'op-ana' },
  { tipo: 'PEDIDO_EN_PREPARACION', rol: 'OPERADOR', usuario: 'op-ana' },
  { tipo: 'PEDIDO_DESPACHADO', rol: 'DRIVER', usuario: 'drv-luis' },
  { tipo: 'PEDIDO_ENTREGADO', rol: 'CLIENTE', usuario: 'cli-0397' },
];

const ESTADO_ANTERIOR = {
  PEDIDO_CREADO: null,
  PEDIDO_ACEPTADO: 'PEDIDO_CREADO',
  PEDIDO_EN_PREPARACION: 'PEDIDO_ACEPTADO',
  PEDIDO_DESPACHADO: 'PEDIDO_EN_PREPARACION',
  PEDIDO_ENTREGADO: 'PEDIDO_DESPACHADO',
  PEDIDO_CANCELADO: 'PEDIDO_CREADO',
};

/** Construye el evento de un pedido en un punto del flujo. */
function eventoDe(paso, pedido, indice) {
  const { items, total } = itemsDeEjemplo(pedido.pedidoId, indice);
  return crearEvento({
    tipo: paso.tipo,
    pedidoId: pedido.pedidoId,
    clienteId: pedido.clienteId,
    total,
    items,
    actor: { usuario: paso.usuario, rol: paso.rol },
    origen: 'web',
    estadoAnterior: ESTADO_ANTERIOR[paso.tipo],
    estadoNuevo: paso.tipo,
  });
}

const PEDIDOS_DEMO = [
  { pedidoId: 'P-1001', clienteId: 'cli-0397' },
  { pedidoId: 'P-1002', clienteId: 'cli-0398' },
  { pedidoId: 'P-1003', clienteId: 'cli-0399' },
];

async function main() {
  const arg = process.argv[2];
  const flag = String(arg ?? '').startsWith('--') ? arg : null;

  await producer.connect();

  if (flag === '--malo') {
    // JSON roto + evento sin pedidoId: ambos van a la DLT del consumidor.
    await publicarCrudo('P-ROTO', '{ esto no es JSON');
    const sinId = crearEvento({
      tipo: 'PEDIDO_CREADO',
      pedidoId: '',
      clienteId: 'cli-9',
      total: 1000,
      items: [{ productoId: 'PROD-1', cantidad: 1, precio: 8990 }],
    });
    await publicarCrudo('SIN-ID', JSON.stringify(sinId));
  } else if (flag === '--total-malo') {
    // El total NO cuadra con la suma de cantidad x precio: el evento es invalido
    // y el consumidor lo manda a la DLT.
    const evento = eventoDe(FLUJO[0], PEDIDOS_DEMO[0], 0);
    evento.total = evento.total + 1; // desajuste deliberado
    await publicarEvento(evento, crypto.randomUUID());
  } else if (flag === '--duplicado') {
    // El mismo evento dos veces con el MISMO eventId: para probar idempotencia.
    const evento = eventoDe(FLUJO[0], PEDIDOS_DEMO[0], 0);
    await publicarEvento(evento, crypto.randomUUID());
    console.log('   (se publica OTRA vez el mismo evento, mismo eventId)');
    await publicarEvento(evento, crypto.randomUUID());
  } else if (flag === '--salto') {
    // Un pedido que salta de CREADO a DESPACHADO: transicion invalida.
    // Solo debe aparecer en la DLT de report, no en la de audit.
    const pedido = { pedidoId: 'P-2001', clienteId: 'cli-0397' };
    await publicarEvento(eventoDe(FLUJO[0], pedido, 0), crypto.randomUUID());
    const salto = eventoDe(FLUJO[3], pedido, 0);
    salto.estadoAnterior = 'PEDIDO_CREADO'; // se salta ACEPTADO y EN_PREPARACION
    await publicarEvento(salto, crypto.randomUUID());
  } else {
    const n = Number(arg) || 12;
    const pausa = Number(process.argv[3]) || 0; // ms entre eventos
    const usarEventId = process.argv.includes('--key-evento');

    // Cada pedido avanza por su flujo; se entrelazan varios pedidos.
    let i = 0;
    while (i < n) {
      const pedido = PEDIDOS_DEMO[i % PEDIDOS_DEMO.length];
      const paso = FLUJO[Math.floor(i / PEDIDOS_DEMO.length) % FLUJO.length];
      const evento = eventoDe(paso, pedido, i);

      const errores = validar(evento);
      if (errores.length) throw new Error(errores.join('; '));

      await publicarEvento(
        evento,
        crypto.randomUUID(),
        usarEventId ? evento.eventId : undefined,
      );
      i++;
      if (pausa) await new Promise((r) => setTimeout(r, pausa));
    }
  }

  await producer.disconnect();
}

main().catch((e) => {
  console.error(e.message);
  process.exit(1);
});
