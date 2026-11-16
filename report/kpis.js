// KPIs del servicio report. CALCULO PURO: sin Kafka y sin HTTP.
//
//   aplicar(evento)  -> mete el evento en los KPI (idempotente)
//   obtener()        -> devuelve los 4 KPI listos para graficar
//
// Asi el negocio queda separado de la mensajeria (el consumidor solo llama a
// aplicar) y separado de HTTP (los endpoints solo llaman a obtener).
//
// IDEMPOTENCIA: Kafka entrega "al menos una vez", asi que el mismo evento puede
// llegar dos veces. Guardamos los eventId ya procesados en un Set y los
// repetidos se ignoran: el pedido se cuenta UNA vez.
const { CATALOGO } = require('../shared/dto');
const { sigueActivo } = require('../shared/estados');

// --- estado interno -------------------------------------------------------
const ventasPorHora = {}; // '2026-10-27T16' -> total vendido
const creadoEn = {}; // pedidoId -> fecha del PEDIDO_CREADO
const entregadoEn = {}; // pedidoId -> fecha del PEDIDO_ENTREGADO
const estadoPorPedido = {}; // pedidoId -> ultimo estado visto
const unidadesPorProducto = {}; // productoId -> unidades vendidas
const procesados = new Set(); // eventId ya aplicados (idempotencia)

/** Devuelve true si el evento es NUEVO (no estaba procesado). */
function esNuevo(evento) {
  if (!evento.eventId) return true;
  if (procesados.has(evento.eventId)) return false;
  procesados.add(evento.eventId);
  return true;
}

/** Aplica un evento a los KPI. Los repetidos se ignoran. */
function aplicar(evento) {
  if (!esNuevo(evento)) return false; // duplicado: no cuenta dos veces

  const pedidoId = evento.pedidoId;
  if (!pedidoId) return false;

  estadoPorPedido[pedidoId] = evento.estadoNuevo || evento.tipo;

  if (evento.tipo === 'PEDIDO_CREADO') {
    const hora = String(evento.fecha).slice(0, 13); // 'YYYY-MM-DDTHH'
    ventasPorHora[hora] = (ventasPorHora[hora] || 0) + (evento.total || 0);
    creadoEn[pedidoId] = evento.fecha;

    // Top de productos mas vendidos: unidades de los items del pedido creado.
    for (const item of evento.items || []) {
      unidadesPorProducto[item.productoId] =
        (unidadesPorProducto[item.productoId] || 0) + (item.cantidad || 0);
    }
  }

  if (evento.tipo === 'PEDIDO_ENTREGADO') {
    entregadoEn[pedidoId] = evento.fecha;
  }

  return true;
}

/** Los 4 KPI, listos para graficar. */
function obtener() {
  // Ventas por hora
  const ventas = Object.entries(ventasPorHora)
    .map(([hora, total]) => ({ hora, total }))
    .sort((a, b) => a.hora.localeCompare(b.hora));

  // Lead time: promedio de (ENTREGADO - CREADO) de los pedidos entregados.
  const pedidos = [];
  let suma = 0;
  for (const [pedidoId, fin] of Object.entries(entregadoEn)) {
    if (!creadoEn[pedidoId]) continue;
    const segundos = (new Date(fin) - new Date(creadoEn[pedidoId])) / 1000;
    pedidos.push({ pedidoId, segundos: Math.round(segundos) });
    suma += segundos;
  }
  pedidos.sort((a, b) => b.segundos - a.segundos);
  const leadTime = {
    promedioSeg: pedidos.length ? Math.round(suma / pedidos.length) : 0,
    pedidos,
  };

  // Top productos: los 3 con mas unidades vendidas.
  const topProductos = Object.entries(unidadesPorProducto)
    .map(([productoId, unidades]) => ({
      productoId,
      unidades,
      nombre: (CATALOGO.find((p) => p.productoId === productoId) || {}).nombre || productoId,
    }))
    .sort((a, b) => b.unidades - a.unidades)
    .slice(0, 3);

  // Pedidos activos por estado: el ultimo estado de cada pedido que sigue vivo.
  const porEstado = {};
  for (const estado of Object.values(estadoPorPedido)) {
    if (!sigueActivo(estado)) continue;
    porEstado[estado] = (porEstado[estado] || 0) + 1;
  }
  const estados = Object.entries(porEstado)
    .map(([estado, cantidad]) => ({ estado, cantidad }))
    .sort((a, b) => a.estado.localeCompare(b.estado));

  return { ventas, leadTime, topProductos, estados };
}

/** Ultimo estado conocido de un pedido (para validar transiciones). */
function estadoDe(pedidoId) {
  return estadoPorPedido[pedidoId] ?? null;
}

/** Cantidad de eventos ya procesados (util para las evidencias). */
function totalProcesados() {
  return procesados.size;
}

/** Reinicia todo (para repetir un experimento desde cero). */
function reiniciar() {
  for (const k of Object.keys(ventasPorHora)) delete ventasPorHora[k];
  for (const k of Object.keys(creadoEn)) delete creadoEn[k];
  for (const k of Object.keys(entregadoEn)) delete entregadoEn[k];
  for (const k of Object.keys(estadoPorPedido)) delete estadoPorPedido[k];
  for (const k of Object.keys(unidadesPorProducto)) delete unidadesPorProducto[k];
  procesados.clear();
}

module.exports = { aplicar, obtener, estadoDe, totalProcesados, reiniciar };
