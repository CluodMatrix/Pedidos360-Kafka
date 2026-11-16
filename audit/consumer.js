// Servicio audit (groupId = audit).
//
// A diferencia de report, audit NO valida transiciones: registra la historia
// completa de cada pedido, aunque venga desordenada. Por eso un evento con
// transicion invalida aparece en la DLT de report pero NO en la de audit.
//
// Despues de guardar el evento, publica un registro legible en el topico
// audit.timeline: "quien / que / cuando / desde donde".
const { Partitioners } = require('kafkajs');
const { iniciarConsumidor } = require('../shared/consumidor');
const { crearKafka } = require('../shared/kafka');
const { AUDIT_TIMELINE } = require('../shared/topics');
const { arrancar } = require('./server');

// --- estado del servicio --------------------------------------------------
// pedidoId -> [registros]  (el timeline de cada pedido)
const timelines = new Map();

// --- publicador hacia audit.timeline ---------------------------------------
const publisher = crearKafka('audit-service').producer({
  createPartitioner: Partitioners.DefaultPartitioner,
});
let publisherConectado = false;

async function asegurarPublisher() {
  if (publisherConectado) return;
  await publisher.connect();
  publisherConectado = true;
}

/** Publica el registro legible del evento en audit.timeline. */
async function publicarEnTimeline(evento) {
  await asegurarPublisher();
  const registro = {
    pedidoId: evento.pedidoId,
    quien: `${evento.actor?.usuario ?? 'desconocido'}(${evento.actor?.rol ?? '?'})`,
    que: evento.tipo,
    cuando: evento.fecha,
    desdeDonde: evento.origen,
    eventId: evento.eventId,
  };
  await publisher.send({
    topic: AUDIT_TIMELINE,
    acks: -1,
    messages: [
      {
        key: evento.pedidoId, // la key del topico de timeline
        value: JSON.stringify(registro),
        headers: { correlationId: evento.eventId },
      },
    ],
  });
  console.log(`   -> audit.timeline  ${registro.quien}  ${registro.que}`);
}

async function procesar(evento) {
  // 1. Guardamos el evento en el timeline del pedido (en memoria).
  const registro = {
    pedidoId: evento.pedidoId,
    fecha: evento.fecha,
    tipo: evento.tipo,
    quien: `${evento.actor?.usuario ?? 'desconocido'}(${evento.actor?.rol ?? '?'})`,
    desdeDonde: evento.origen,
    eventId: evento.eventId,
  };
  if (!timelines.has(evento.pedidoId)) timelines.set(evento.pedidoId, []);
  timelines.get(evento.pedidoId).push(registro);

  // 2. Y lo publicamos en audit.timeline (consumir -> transformar -> producir).
  await publicarEnTimeline(evento);
}

/** Timeline de un pedido, ordenado por fecha. */
function timelineDe(pedidoId) {
  return (timelines.get(pedidoId) || []).slice().sort((a, b) => a.fecha.localeCompare(b.fecha));
}

/** Todos los eventos, con filtros opcionales: usuario, tipo, desde, hasta. */
function eventosFiltrados({ usuario, tipo, desde, hasta } = {}) {
  const todos = [];
  for (const lista of timelines.values()) todos.push(...lista);
  return todos
    .filter((e) => (!usuario || e.quien.includes(usuario)) && (!tipo || e.tipo === tipo))
    .filter((e) => (!desde || e.fecha >= desde) && (!hasta || e.fecha <= hasta))
    .sort((a, b) => a.fecha.localeCompare(b.fecha));
}

// OJO: module.exports ANTES de arrancar(). server.js hace require('./consumer')
// (dependencia circular): si arrancamos antes de exportar, la API receberia un
// modulo vacio y timelineDe seria undefined.
module.exports = { timelineDe, eventosFiltrados };

// La API se levanta en el MISMO PROCESO que el consumidor: asi ambos
// comparten el timeline en memoria (el consumidor guarda, la API sirve).
arrancar();

iniciarConsumidor({
  clientId: 'audit-service',
  groupId: process.env.GRUPO_AUDIT || 'audit',
  procesar,
})
  .catch((e) => {
    console.error(e);
    process.exit(1);
  });
