// PUENTE REAL: SQS -> Kafka.
//
// Lee los eventos reales que publica el backend de AWS (SNS -> SQS) y los
// publica en Kafka con el MISMO contrato v2 que el mock.
//
// Cuando esto esta apagado, el mock (bridge/producer.js) ocupa su lugar:
// report y audit no cambian ni una linea.
//
//   npm run puente
//
// Necesita credenciales de AWS en el equipo (las del lab) y la variable:
//   SQS_QUEUE_URL=https://sqs.us-east-1.amazonaws.com/<cuenta>/q-eventos-kafka
const { SQSClient, ReceiveMessageCommand, DeleteMessageCommand } = require('@aws-sdk/client-sqs');
const { Partitioners } = require('kafkajs');
require('dotenv').config();
const { crearKafka } = require('../shared/kafka');
const { crearEvento, validar } = require('../shared/dto');
const { PEDIDOS } = require('../shared/topics');

const REGION = process.env.AWS_REGION || 'us-east-1';
const QUEUE_URL = process.env.SQS_QUEUE_URL;
const ESPERA_MS = Number(process.env.PUENTE_INTERVALO_MS || 2000);
const MAX_MENSAJES = Number(process.env.PUENTE_MAX || 10);

// El evento del backend (SNS) trae estos tipos; el contrato v2 usa otros.
const TIPOS_SNS_A_V2 = {
  PedidoCreado: 'PEDIDO_CREADO',
  PedidoAceptado: 'PEDIDO_ACEPTADO',
  PedidoEnPreparacion: 'PEDIDO_EN_PREPARACION',
  PedidoDespachado: 'PEDIDO_DESPACHADO',
  PedidoEntregado: 'PEDIDO_ENTREGADO',
  PedidoCancelado: 'PEDIDO_CANCELADO',
};

// El backend guarda los estados en su propio idioma ('CREADO', 'ACEPTADO'...).
// El contrato v2 usa los nombres largos ('PEDIDO_CREADO'...). Sin esta tabla,
// el consumidor compararia 'ACEPTADO' contra 'PEDIDO_ACEPTADO' y TODAS las
// transiciones saldrian invalidas.
const ESTADOS_A_V2 = {
  CREADO: 'PEDIDO_CREADO',
  ACEPTADO: 'PEDIDO_ACEPTADO',
  EN_PREPARACION: 'PEDIDO_EN_PREPARACION',
  DESPACHADO: 'PEDIDO_DESPACHADO',
  ENTREGADO: 'PEDIDO_ENTREGADO',
  CANCELADO: 'PEDIDO_CANCELADO',
};

/**
 * Normaliza un estado al nombre del contrato v2.
 * Acepta los tres formatos que pueden llegar: el interno del backend
 * ('CREADO'), el nombre del evento de SNS ('PedidoCreado') y el del contrato
 * v2 ('PEDIDO_CREADO').
 */
function estadoV2(valor) {
  if (!valor) return null;
  if (ESTADOS_A_V2[valor]) return ESTADOS_A_V2[valor];
  if (TIPOS_SNS_A_V2[valor]) return TIPOS_SNS_A_V2[valor];
  return valor; // ya venia en v2
}

/** Traduce el evento del backend (SNS) al contrato v2 de Kafka. */
function traducir(bruto) {
  const ev = JSON.parse(bruto);
  const tipo = TIPOS_SNS_A_V2[ev.type] ?? ev.type;

  // items: el backend manda productos con {productoId, nombre, cantidad, precio}.
  const items = (ev.productos ?? []).map((p) => ({
    productoId: p.productoId,
    cantidad: p.cantidad,
    precio: p.precio,
  }));

  const evento = crearEvento({
    eventId: ev.eventId,
    tipo,
    fecha: ev.timestamp,
    pedidoId: ev.pedidoId,
    clienteId: ev.clienteId,
    total: ev.total,
    actor: ev.actor ?? { usuario: ev.clienteId ?? 'desconocido', rol: 'CLIENTE' },
    origen: ev.origen ?? 'web',
    items,
    estadoAnterior: estadoV2(ev.estadoAnterior),
    estadoNuevo: estadoV2(ev.estado) ?? tipo,
  });

  // Aviso en el origen: si el evento de AWS no cumple el contrato, se avisa
  // AQUI (no solo dentro de la DLT). Asi se ve de donde sale el problema.
  const errores = validar(evento);
  if (errores.length) {
    console.warn(
      `  ! el evento ${evento.pedidoId} (${evento.tipo}) no cumple el contrato v2: ${errores.join('; ')}`,
    );
  }

  return evento;
}

async function main() {
  if (!QUEUE_URL) {
    console.error('Falta SQS_QUEUE_URL (la cola q-eventos-kafka).');
    console.error('Ejemplo: SQS_QUEUE_URL=https://sqs.us-east-1.amazonaws.com/142780656952/q-eventos-kafka');
    process.exit(1);
  }

  const sqs = new SQSClient({ region: REGION });
  const producer = crearKafka('bridge-sqs').producer({
    idempotent: true,
    maxInFlightRequests: 1,
    createPartitioner: Partitioners.DefaultPartitioner,
  });
  await producer.connect();
  console.log(`Puente SQS -> Kafka activo. Escuchando ${QUEUE_URL}`);

  let publicados = 0;
  for (;;) {
    let recibidos;
    try {
      recibidos = await sqs.send(
        new ReceiveMessageCommand({
          QueueUrl: QUEUE_URL,
          MaxNumberOfMessages: MAX_MENSAJES,
          WaitTimeSeconds: 5, // long polling: no gasta peticiones si no hay nada
          MessageAttributeNames: ['All'],
        }),
      );
    } catch (err) {
      console.error(`Error leyendo SQS: ${err.message}`);
      await new Promise((r) => setTimeout(r, ESPERA_MS));
      continue;
    }

    for (const msg of recibidos.Messages ?? []) {
      try {
        const evento = traducir(msg.Body);
        const [r] = await producer.send({
          topic: PEDIDOS,
          acks: -1,
          messages: [
            {
              key: evento.pedidoId, // misma key que el mock: mismo pedido, misma particion
              value: JSON.stringify(evento),
              headers: { correlationId: evento.eventId },
            },
          ],
        });
        console.log(
          `-> p${r.partition} off${r.baseOffset}  ${evento.pedidoId}  ${evento.tipo}  (de SQS)`,
        );
        publicados += 1;
        // Solo borramos de SQS cuando ya esta en Kafka: si Kafka falla, el evento
        // sigue en la cola y no se pierde.
        await sqs.send(
          new DeleteMessageCommand({ QueueUrl: QUEUE_URL, ReceiptHandle: msg.ReceiptHandle }),
        );
      } catch (err) {
        console.error(`No se pudo publicar el evento de SQS: ${err.message}. Se deja en la cola.`);
      }
    }
  }
}

main().catch((e) => {
  console.error(e.message);
  process.exit(1);
});
