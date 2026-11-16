// Cambia el numero de particiones de un topico.
//   npm run particiones -- --topico=pedidos.eventos --numero=6
//
// OJO: las particiones SOLO se pueden AUMENTAR, nunca bajar.
// Si se bajan, los datos de las particiones eliminadas se pierden.
const { crearKafka } = require('../shared/kafka');
const { arg } = require('../shared/args.js');
const { listarTopicos } = require('../shared/consultas');

async function main() {
  const topico = arg('topico', 'pedidos.eventos');
  const numero = Number(arg('numero', 3));

  const admin = crearKafka('admin-particiones').admin();
  await admin.connect();

  // listarTopicos() devuelve [{topic, particiones, ...}] con la forma ya normalizada.
  const topics = await listarTopicos();
  const actual = topics.find((t) => t.topic === topico);
  if (!actual) {
    console.error(`El topico ${topico} no existe.`);
    await admin.disconnect();
    process.exit(1);
  }

  console.log(`${topico}: ${actual.particiones} particiones -> ${numero}`);
  if (numero < actual.particiones) {
    console.error('Las particiones NO se pueden bajar (se perderian datos).');
    await admin.disconnect();
    process.exit(1);
  }
  if (numero === actual.particiones) {
    console.log('No cambia nada.');
    await admin.disconnect();
    return;
  }

  await admin.createPartitions({ topicPartitions: [{ topic: topico, count: numero }] });
  console.log(`Listo: ${topico} ahora tiene ${numero} particiones (maximo de consumidores en paralelo).`);
  await admin.disconnect();
}

main().catch((e) => {
  console.error(e.message);
  process.exit(1);
});
