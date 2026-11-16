// Muestra lider, replicas e ISR de cada particion en vivo.
//   npm run replicas -- --watch
//
// Sirve para el experimento "se cae un broker": al apagarlo, sus replicas salen
// del ISR y, si era lider, otro broker toma el liderazgo.
const { PEDIDOS } = require('../shared/topics');
const { arg } = require('../shared/args.js');
const { infoReplicas } = require('../shared/consultas');

async function main() {
  const topico = arg('topico', PEDIDOS);
  console.log(`Lider y replicas de "${topico}" (Ctrl+C para salir)`);

  for (;;) {
    try {
      const filas = await infoReplicas(topico);
      console.log('\n  particion |  lider |  replicas |  ISR');
      for (const p of filas) {
        console.log(
          `  ${String(p.particion).padStart(9)} | ${String(p.lider).padStart(5)} | ` +
            `${String(p.replicas.join(',')).padStart(9)} | ${String(p.isr.join(',')).padStart(4)}`,
        );
      }
    } catch (err) {
      console.error(`Error: ${err.message}`);
    }
    await new Promise((r) => setTimeout(r, 3000));
  }
}

main().catch((e) => {
  console.error(e.message);
  process.exit(1);
});
