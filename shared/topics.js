// Nombres y configuracion de los topicos (cada numero tiene una razon).
//
// Topologia del caso Pedidos360 v2 con Kafka:
//   1 productor (mock o puente SQS) . 3 grupos de consumidores . 4 topicos
//
// Todos los topicos nuevos se anaden aqui y se crean con `npm run topics`.
// Nunca a mano en la Kafka UI: la configuracion queda replicable en Git.

const PEDIDOS = 'pedidos.eventos';
const AUDIT_TIMELINE = 'audit.timeline';

// Un DLT (Dead Letter Topic) por consumidor: asi se sabe QUE proceso fallo.
//   pedidos.eventos.report.dlt
//   pedidos.eventos.audit.dlt
function topicoDLT(groupId) {
  return `${PEDIDOS}.${groupId}.dlt`;
}

const TOPICOS = [
  {
    // 3 carriles para escalar; la key (pedidoId) mantiene el orden de cada pedido.
    topic: PEDIDOS,
    particiones: 3,
    retencionDias: 7,
    cleanupPolicy: 'delete',
    key: 'pedidoId',
  },
  {
    // Registro legible de cada pedido (quien, que, cuando, desde donde).
    // compact: se queda solo el ultimo mensaje por key.
    topic: AUDIT_TIMELINE,
    particiones: 3,
    retencionDias: 30,
    cleanupPolicy: 'compact',
    key: 'pedidoId',
  },
  // Los DLT de los consumidores se crean al vuelo (admin/topicos.js los agrega).
];

const GRUPOS = ['report', 'audit', 'report-v2']; // report-v2 es el del experimento de replay

module.exports = { PEDIDOS, AUDIT_TIMELINE, DLT: topicoDLT, TOPICOS, GRUPOS };
