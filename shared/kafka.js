// Cliente de Kafka. Un solo lugar con la configuracion de conexion.
//
//   KAFKA_BROKERS=localhost:9092            -> local (Docker en tu computador)
//   KAFKA_BROKERS=<ip-privada>:9092         -> EC2 con Docker
//
// El mismo codigo funciona en los dos casos: solo cambia la variable.

// 1) El warning del particionador: KafkaJS v2 cambio el particionador por
//    defecto. A NOSOTROS nos conviene el nuevo (murmur2 sobre la key), porque
//    con key = pedidoId todos los eventos de un pedido van a la misma
//    particion y su historia queda en orden. El aviso solo confunde.
process.env.KAFKAJS_NO_PARTITIONER_WARNING = process.env.KAFKAJS_NO_PARTITIONER_WARNING ?? '1';

const { Kafka, logLevel } = require('kafkajs');

// 2) KafkaJS dispara internamente un TimeoutNegativeWarning (su planificador de
//    peticiones calcula un delay en el pasado). Es cosmetico: no afecta al
//    consumo ni a la entrega. Se filtra para que la consola de la demo no
//    ensucie con avisos que el usuario no puede actuar.
process.removeAllListeners('warning');
process.on('warning', (w) => {
  if (w.name === 'TimeoutNegativeWarning') return;
  console.warn(`${w.name}: ${w.message}`);
});

const brokers = (process.env.KAFKA_BROKERS || 'localhost:9092').split(',');

function crearKafka(clientId) {
  return new Kafka({
    clientId, // ej: 'report-service'
    brokers,
    logLevel: logLevel.INFO,
    retry: {
      initialRetryTime: 300,
      retries: 8,
    },
  });
}

module.exports = { crearKafka, brokers };
