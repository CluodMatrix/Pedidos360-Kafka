// Prueba de carga: mide el throughput del productor segun la configuracion.
//
//   npm run carga -- --count=20000 --modo=simple
//   npm run carga -- --count=20000 --modo=lote
//   npm run carga -- --count=20000 --modo=lote --lingerMs=0
//   npm run carga -- --count=20000 --modo=lote --acks=0
//   npm run carga -- --count=20000 --modo=lote --compresion=none
//
// Salida: msg/s, numero de llamadas al broker, bytes y tiempo total.
// Cada ajuste intercambia velocidad por latencia: es la evidencia del informe.
const { Partitioners } = require('kafkajs');
const { crearKafka } = require('../shared/kafka');
const { arg } = require('../shared/args.js');
const { crearEvento, itemsDeEjemplo } = require('../shared/dto');
const { PEDIDOS } = require('../shared/topics');

const COUNT = Number(arg('count', 5000));
const MODO = arg('modo', 'simple'); // simple | lote
const LINGER_MS = Number(arg('lingerMs', 50));
const ACKS = Number(arg('acks', -1)); // -1 = all, 0 = sin confirmacion, 1 = lider
const COMPRESION = arg('compresion', 'gzip'); // gzip | snappy | none
const BATCH = Number(arg('batch', 100));

async function main() {
  const producer = crearKafka('carga').producer({
    idempotent: false, // con acks=0 no se puede pedir idempotencia
    maxInFlightRequests: 5,
    createPartitioner: Partitioners.DefaultPartitioner,
    ...(COMPRESION === 'none' ? {} : { compression: { CompressionTypes: { [COMPRESION]: 1 } } }),
  });
  await producer.connect();

  const started = Date.now();
  let llamadas = 0;
  let bytes = 0;

  const mensajes = [];
  for (let i = 0; i < COUNT; i++) {
    const { items, total } = itemsDeEjemplo('P-CARGA', i);
    const evento = crearEvento({
      tipo: 'PEDIDO_CREADO',
      pedidoId: `P-CARGA-${i % 50}`,
      clienteId: 'cli-carga',
      total,
      items,
    });
    const valor = JSON.stringify(evento);
    bytes += valor.length;
    mensajes.push({ key: evento.pedidoId, value: valor });
  }

  if (MODO === 'lote') {
    // Un envio por lote: menos viajes al broker, mas throughput.
    for (let i = 0; i < mensajes.length; i += BATCH) {
      const lote = mensajes.slice(i, i + BATCH);
      await producer.send({ topic: PEDIDOS, acks: ACKS, messages: lote, lingerMs: LINGER_MS });
      llamadas += 1;
    }
  } else {
    // Un envio por mensaje: el broker responde a cada uno.
    for (const m of mensajes) {
      await producer.send({ topic: PEDIDOS, acks: ACKS, messages: [m], lingerMs: LINGER_MS });
      llamadas += 1;
    }
  }

  const ms = Date.now() - started;
  console.log('--- resultado de la prueba de carga ---');
  console.log(`  modo:        ${MODO}`);
  console.log(`  mensajes:    ${COUNT}`);
  console.log(`  llamadas:    ${llamadas}`);
  console.log(`  linger.ms:   ${LINGER_MS}`);
  console.log(`  acks:        ${ACKS}`);
  console.log(`  compresion:  ${COMPRESION}`);
  console.log(`  bytes:       ${bytes}`);
  console.log(`  tiempo:      ${ms} ms`);
  console.log(`  msg/s:       ${((COUNT / ms) * 1000).toFixed(0)}`);
  console.log(`  bytes/s:     ${((bytes / ms) * 1000).toFixed(0)}`);

  await producer.disconnect();
}

main().catch((e) => {
  console.error(e.message);
  process.exit(1);
});
