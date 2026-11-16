// Monitor del consumer lag de un grupo.
//   npm run lag -- --grupo=report
//   npm run lag -- --grupo=report --topico=pedidos.eventos
//
// El lag es el "termometro de salud" del consumidor: cuantos mensajes estan
// pendientes de leer.
const { PEDIDOS } = require('../shared/topics');
const { arg } = require('../shared/args.js');
const { lagDe } = require('../shared/consultas');

async function main() {
  const grupo = arg('grupo', 'report');
  const topico = arg('topico', PEDIDOS);

  console.log(`Lag del grupo "${grupo}" sobre "${topico}" (Ctrl+C para salir)`);
  console.log('  particion |  confirmado |   ultimo |     low |    lag');

  for (;;) {
    try {
      const info = await lagDe(grupo, topico);
      console.log(
        '\n' +
          info.particiones
            .map(
              (p) =>
                `  ${String(p.particion).padStart(9)} | ${String(p.confirmado ?? '-').padStart(10)} | ` +
                `${String(p.ultimo).padStart(7)} | ${String(p.low).padStart(7)} | ${String(p.lag).padStart(6)}`,
            )
            .join('\n'),
      );
      console.log(`  LAG TOTAL: ${info.lag}`);
    } catch (err) {
      console.error(`Error: ${err.message}`);
    }
    await new Promise((r) => setTimeout(r, 2000));
  }
}

main().catch((e) => {
  console.error(e.message);
  process.exit(1);
});
