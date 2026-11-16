// Crea los topicos definidos en shared/topics.js (npm run topics).
// El mismo codigo que crea los topicos se versiona en Git: la configuracion
// queda replicable.
const { crearKafka } = require('../shared/kafka');
const { definicionesCompletas } = require('./topicos');
const { listarTopicos } = require('../shared/consultas');

async function main() {
  const admin = crearKafka('admin').admin();
  await admin.connect();

  const definiciones = definicionesCompletas();
  const creados = await admin.createTopics({ topics: definiciones, waitForLeaders: true });
  console.log(
    creados ? 'Topicos creados:' : 'Los topicos ya existian:',
    definiciones.map((d) => d.topic),
  );
  await admin.disconnect();

  console.log('\nTopicos en el broker:');
  for (const t of await listarTopicos()) {
    console.log(`  ${t.topic}  (${t.particiones} particiones, lider ${t.lideres.join(',')})`);
  }
}

main().catch((e) => {
  console.error(e.message);
  process.exit(1);
});
