// Muestra como Kafka borra los mensajes viejos por retencion.
//   npm run retencion -- --topico=pedidos.demo-retencion --segundos=30
//
// Al expirar la retencion, Kafka borra los segmentos viejos: el "low water mark"
// sube y los mensajes dejan de estar disponibles. Un consumidor nuevo NO puede
// leerlos. El "high" (ultimo offset escrito) no cambia: el hueco se queda vacio.
//
// QUE HACE FALTA PARA QUE SE VEA (y es la parte que hay que explicar):
//   1. retention.ms = retencion pedida.  (config del topico)
//   2. segment.ms  = cada cuanto se cierra un segmento. (config del topico)
//      Sin esto, los 50 mensajes quedan en el segmento ACTIVO, y Kafka nunca
//      borra el segmento activo: la retencion no se veria NUNCA.
//   3. log.retention.check.interval.ms = cada cuanto el broker revisa.
//      Es config del BROKER (no del topico) y por defecto son 5 MINUTOS:
//      si no se baja, en 30 segundos de demo no se borra nada.
//      Por eso docker-compose.yml lo deja en 5 segundos.
const { crearKafka } = require('../shared/kafka');
const { arg } = require('../shared/args.js');

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function asegurarTopico(admin, topico, segundos) {
  const configEntries = [
    { name: 'cleanup.policy', value: 'delete' },
    { name: 'retention.ms', value: String(segundos * 1000) },
    // Cierra un segmento cada 3s: asi los mensajes viejos quedan en segmentos
    // que YA NO son el activo (los unicos que Kafka puede borrar).
    { name: 'segment.ms', value: '3000' },
  ];

  // Si el topico existe de una corrida anterior, se borra y se recrea: asi el
  // script SIEMPRE aplica la retencion pedida (si no, conservaria la anterior).
  try {
    await admin.deleteTopics({ topics: [topico] });
    await sleep(1500);
  } catch {
    // No existia: se crea de una.
  }

  for (let intento = 1; intento <= 5; intento++) {
    try {
      await admin.createTopics({
        topics: [{ topic: topico, numPartitions: 1, replicationFactor: 1, configEntries }],
        waitForLeaders: true,
      });
      return;
    } catch (e) {
      // El borrado es asincrono: puede seguir existiendo un instante.
      if (intento === 5) throw e;
      await sleep(1000);
    }
  }
}

async function main() {
  const topico = arg('topico', 'pedidos.demo-retencion');
  const segundos = Number(arg('segundos', 30));
  const mensajes = Number(arg('mensajes', 60));

  const admin = crearKafka('admin-retencion').admin();
  await admin.connect();

  await asegurarTopico(admin, topico, segundos);

  const producer = crearKafka('retencion').producer();
  await producer.connect();

  // Se publican a lo largo del tiempo (no de golpe): asi cada tanda cae en un
  // segmento distinto y queda claro COMO se van borrando los viejos.
  const inicio = Date.now();
  const lotes = 6;
  const porLote = Math.max(1, Math.ceil(mensajes / lotes));
  let publicados = 0;

  console.log(`Publicado: ${mensajes} mensajes en "${topico}" (retencion ${segundos}s)`);
  console.log('  segundos |  low |  high |  disponibles');

  for (let lote = 1; lote <= lotes; lote++) {
    const loteMensajes = [];
    for (let i = 0; i < porLote && publicados < mensajes; i++) {
      loteMensajes.push({
        key: `pedido-${publicados}`,
        value: JSON.stringify({ n: publicados, ts: new Date().toISOString() }),
      });
      publicados++;
    }
    await producer.send({ topic: topico, acks: -1, messages: loteMensajes });

    const transcurrido = Math.floor((Date.now() - inicio) / 1000);
    const offsets = await admin.fetchTopicOffsets(topico);
    const p = offsets[0] ?? {};
    const low = Number(p.low ?? 0);
    const high = Number(p.offset ?? p.high ?? 0);
    console.log(
      `  ${String(transcurrido).padStart(8)} | ${String(low).padStart(4)} | ${String(high).padStart(4)} | ` +
        `${String(Math.max(0, high - low)).padStart(11)}`,
    );

    await sleep(Math.max(0, inicio + lote * ((segundos / lotes) * 1000) - Date.now()));
  }

  // Fase final: esperar a que el broker revise y borre lo vencido.
  console.log(`  (esperando a que expire la retencion de ${segundos}s...)`);
  const fin = Date.now() + (segundos + 10) * 1000;
  while (Date.now() < fin) {
    await sleep(5000);
    const offsets = await admin.fetchTopicOffsets(topico);
    const p = offsets[0] ?? {};
    const low = Number(p.low ?? 0);
    const high = Number(p.offset ?? p.high ?? 0);
    console.log(
      `  ${String(Math.floor((Date.now() - inicio) / 1000)).padStart(8)} | ${String(low).padStart(4)} | ` +
        `${String(high).padStart(4)} | ${String(Math.max(0, high - low)).padStart(11)}`,
    );
  }
  console.log('  (el "low" sube: Kafka boro los segmentos viejos. El "high" no cambia.)');
  console.log('  Un consumidor nuevo NO puede leer lo que ya expiro: arranca en el "low".');

  await producer.disconnect();
  await admin.disconnect();
}

main().catch((e) => {
  console.error(e.message);
  process.exit(1);
});
