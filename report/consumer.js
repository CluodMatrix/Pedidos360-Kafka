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
arrancar();

iniciarConsumidor({
  clientId: 'report-service',
  groupId: process.env.GRUPO_REPORT || 'report',
  procesar,
})
  .catch((e) => {
    console.error(e);
    process.exit(1);
  });
