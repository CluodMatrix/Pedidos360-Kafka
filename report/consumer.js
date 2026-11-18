// Servicio report (groupId = report).
//
// Calcula ventas por hora, lead time, pedidos activos por estado y top de
// productos. ANTES de tocar los KPIs revisa la transicion del pedido: si no es
// valida lanza un error NO REINTENTABLE y el mensaje va directo a la DLT de
// report (sin gastar los 3 reintentos).
const { iniciarConsumidor, ErrorNoReintentable } = require('../shared/consumidor');
const { esTransicionValida, explicarTransicion } = require('../shared/estados');
const kpis = require('./kpis');
const { arrancar } = require('./server');

async function procesar(evento) {
  // 0. Idempotencia: si el evento ya se aplico (Kafka entrega "al menos una vez"),
  //    se ignora ANTES de validar la transicion. Si no, un duplicado de CREADO
  //    llegaria con el pedido ya conocido y caeria en la DLT por error.
  if (kpis.yaProcesado(evento)) {
    console.log(`   duplicado ignorado ${evento.eventId} (${evento.pedidoId} ${evento.tipo})`);
    return;
  }

  // 1. Regla de transicion: el consumidor compara SU ultimo estado conocido con
  //    el estado nuevo del evento. Si el productor se salto un estado (o si el
  //    evento llego desordenado por usar key = eventId), la transicion no vale.
  //
  //    Si el consumidor NO conoce el pedido (memoria vacia, tipico tras un
  //    reinicio), no se valida nada: no hay con que comparar y no hay por que
  //    rechazar un evento que llega del todo normal.
  const anterior = kpis.estadoDe(evento.pedidoId);
  const nuevo = evento.estadoNuevo || evento.tipo;
  const conocido = anterior !== null && anterior !== undefined;
  if (conocido && !esTransicionValida(anterior, nuevo)) {
    throw new ErrorNoReintentable(explicarTransicion(anterior, nuevo));
  }

  // 2. Negocio: el KPI. (aplicar es idempotente: ignora los eventId repetidos.)
  // PROCESO_MS simula una consulta lenta a la base de datos (Parte 6 del enunciado).
  const pausa = Number(process.env.PROCESO_MS || 0);
  if (pausa > 0) await new Promise((r) => setTimeout(r, pausa));
  kpis.aplicar(evento);

  if (evento.tipo === 'PEDIDO_ENTREGADO') {
    const segundos = kpis.obtener().leadTime.pedidos.find((p) => p.pedidoId === evento.pedidoId);
    if (segundos) {
      console.log(`   lead time ${evento.pedidoId}: ${segundos.segundos} s`);
    }
  }
}

// La API se levanta en el MISMO PROCESO que el consumidor: asi ambos
// comparten los KPIs en memoria (el consumidor calcula, la API sirve).
// SOLO_LOGS=1 (demo de reparto): el proceso consume pero NO sirve la API, para
// que sus KPIs parciales nunca lleguen al dashboard.
if (!process.env.SOLO_LOGS) arrancar();

iniciarConsumidor({
  clientId: 'report-service',
  groupId: process.env.GRUPO_REPORT || 'report',
  procesar,
})
  .catch((e) => {
    console.error(e);
    process.exit(1);
  });
