// Deja el grupo "report-v2" al final del topico para que la demo muestre solo
// eventos nuevos. Hay que correrlo ANTES de abrir las terminales de la demo y
// con las instancias apagadas (Kafka no permite resetear un grupo con miembros).
//   npm run demo-reset
const { crearKafka } = require('../shared/kafka');
const { PEDIDOS } = require('../shared/topics');

(async () => {
  const admin = crearKafka('demo-reset').admin();
  await admin.connect();
  try {
    await admin.resetOffsets({ groupId: 'report-v2', topic: PEDIDOS, earliest: false });
    console.log('Grupo report-v2 posicionado al final de pedidos.eventos.');
  } catch (e) {
    console.error('No se pudo resetear: ' + e.message);
    console.error('Apaga las instancias de demo-consumidor y vuelve a correr npm run demo-reset.');
    process.exitCode = 1;
  } finally {
    await admin.disconnect();
  }
})();
