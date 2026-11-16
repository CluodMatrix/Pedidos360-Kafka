// Infraestructura del consumidor: parseo, validacion, reintentos y DLT.
//
// Cada servicio (report, audit) solo aporta su funcion procesar(evento):
// la LOGICA DE NEGOCIO queda separada de la MENSAJERIA.
//
// Camino de un mensaje:
//   1. VALIDAR    -> si el dato esta roto, va a la DLT directo (reintentar no lo arregla)
//   2. PROCESAR   -> logica del servicio
//   3. REINTENTAR -> si falla por algo temporal: 1 s, 2 s, 4 s (3 intentos)
//   4. DLT        -> si igual no se puede, queda en la DLT del consumidor CON EL MOTIVO
//
// Cada consumidor tiene SU PROPIA DLT (pedidos.eventos.<groupId>.dlt): asi se sabe
// que proceso fallo y se puede reprocesar solo ese.
const { Partitioners } = require('kafkajs');
const { crearKafka } = require('./kafka');
const { validar } = require('./dto');
const { PEDIDOS, DLT } = require('./topics');

/** Error de negocio que NO tiene sentido reintentar. */
class ErrorNoReintentable extends Error {
  constructor(mensaje) {
    super(mensaje);
    this.name = 'ErrorNoReintentable';
    this.esReintentable = false; // va directo a la DLT
  }
}

/**
 * Arranca un consumidor.
 * @param {object} opciones
 * @param {string} opciones.clientId    id del cliente Kafka (para trazas)
 * @param {string} opciones.groupId      grupo de consumidores (report | audit | report-v2)
 * @param {(evento) => Promise<void>} opciones.procesar  logica de negocio
 * @param {boolean} [opciones.fromBeginning]  true = replay (lee desde el inicio).
 *   Por defecto TRUE (como en el enunciado): si el consumidor arranca con el
 *   topico ya poblado, necesita el historial para validar las transiciones.
 * @param {number} [opiones.reintentos]  intentos antes de mandar a la DLT
 */
async function iniciarConsumidor({
  clientId,
  groupId,
  procesar,
  fromBeginning = process.env.FROM_BEGINNING !== 'false',
  reintentos = Number(process.env.REINTENTOS ?? 3),
}) {
  const kafka = crearKafka(clientId);
  const consumer = kafka.consumer({ groupId });
  const dlt = kafka.producer({ createPartitioner: Partitioners.DefaultPartitioner });
  const topicDLT = DLT(groupId);

  // Publica el mensaje original + el motivo (y la traza) en la DLT del consumidor.
  async function enviarADlt(message, partition, motivo) {
    const texto = Array.isArray(motivo) ? motivo.join(', ') : String(motivo);
    await dlt.send({
      topic: topicDLT,
      messages: [
        {
          key: message.key,
          value: message.value,
          headers: {
            ...message.headers,
            // Traza minima: quien fallo, cuando y por que.
            consumidor: groupId,
            fechaError: new Date().toISOString(),
            mensaje: texto.slice(0, 900),
            topicOrigen: PEDIDOS,
            particionOrigen: String(partition),
            offsetOrigen: message.offset,
          },
        },
      ],
    });
    console.error(`[${groupId}] -> DLT  p${partition} off${message.offset}  motivo: ${texto}`);
  }

  // Muestra que particiones le tocaron a este consumidor (util para ver el rebalanceo).
  consumer.on(consumer.events.GROUP_JOIN, ({ payload }) => {
    const asignadas = payload.memberAssignment[PEDIDOS] || [];
    console.log(`[${groupId}] particiones asignadas: [${asignadas.join(', ')}]`);
  });

  await consumer.connect();
  await dlt.connect();
  await consumer.subscribe({ topic: PEDIDOS, fromBeginning });

  await consumer.run({
    eachMessage: async ({ message, partition }) => {
      let evento;
      try {
        evento = JSON.parse(message.value.toString());
      } catch (e) {
        // JSON roto: reintentar no lo arregla -> DLT directo.
        return enviarADlt(message, partition, 'JSON invalido');
      }

      const errores = validar(evento);
      if (errores.length) return enviarADlt(message, partition, errores);

      console.log(
        `[${groupId}] p${partition} off${message.offset}  ${evento.pedidoId}  ${evento.tipo}`,
      );

      for (let i = 1; i <= reintentos; i++) {
        try {
          await procesar(evento);
          return; // sin excepcion: KafkaJS confirma (commit) el offset
        } catch (e) {
          // Un error NO reintentable va directo a la DLT: no gastamos los 3 intentos.
          if (e && e.esReintentable === false) {
            console.warn(`[${groupId}] no reintentable (intento ${i}) ${evento.eventId}: ${e.message}`);
            return enviarADlt(message, partition, e.message);
          }
          console.warn(`[${groupId}] intento ${i} ${evento.eventId} ${e.message}`);
          await new Promise((r) => setTimeout(r, 1000 * 2 ** (i - 1)));
        }
      }
      return enviarADlt(message, partition, 'agotados los reintentos');
    },
  });

  // Ctrl+C: salir del grupo de forma ordenada (provoca rebalanceo inmediato).
  for (const sig of ['SIGINT', 'SIGTERM']) {
    process.on(sig, async () => {
      await consumer.disconnect();
      await dlt.disconnect();
      process.exit(0);
    });
  }
}

module.exports = { iniciarConsumidor, ErrorNoReintentable };
