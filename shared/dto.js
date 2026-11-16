// Evento del caso (DTO v2) + catalogo de prueba + validacion.
//
// CONTRATO (v2). Todos los productores publican EXACTAMENTE esta forma:
//   - mismos campos y tipos
//   - misma key (pedidoId)
//   - mismos headers (correlationId)
//
// Asi, cuando el mock se apague y entre el servicio real, report y audit
// no cambian ni una linea.

const crypto = require('crypto');

const TIPOS = [
  'PEDIDO_CREADO',
  'PEDIDO_ACEPTADO',
  'PEDIDO_EN_PREPARACION',
  'PEDIDO_DESPACHADO',
  'PEDIDO_ENTREGADO',
  'PEDIDO_CANCELADO',
];

const ROLES = ['CLIENTE', 'OPERADOR', 'DRIVER'];

const VERSION = 2;

// Catalogo de prueba. En el proyecto final los productos vienen del catalogo.
const CATALOGO = [
  { productoId: 'PROD-1', nombre: 'Pizza familiar', precio: 8990 },
  { productoId: 'PROD-2', nombre: 'Bebida 1,5L', precio: 1990 },
  { productoId: 'PROD-3', nombre: 'Postre', precio: 3490 },
];

const PRECIOS = new Map(CATALOGO.map((p) => [p.productoId, p.precio]));

/**
 * Construye el evento v2.
 * @param {object} d datos del evento
 */
function crearEvento(d) {
  return {
    eventId: d.eventId || crypto.randomUUID(),
    tipo: d.tipo,
    version: VERSION,
    fecha: d.fecha || new Date().toISOString(),
    pedidoId: d.pedidoId,
    clienteId: d.clienteId,
    total: d.total,
    actor: d.actor || { usuario: 'sistema', rol: 'OPERADOR' },
    origen: d.origen || 'web',
    items: d.items || [],
    estadoAnterior: d.estadoAnterior ?? null,
    estadoNuevo: d.estadoNuevo ?? d.tipo ?? null,
  };
}

/**
 * Valida el payload. Devuelve un arreglo de errores; vacio = valido.
 * @param {object} e evento
 * @returns {string[]}
 */
function validar(e) {
  const err = [];
  if (!e.eventId) err.push('eventId requerido');
  if (!TIPOS.includes(e.tipo)) err.push('tipo invalido');
  if (!e.pedidoId) err.push('pedidoId requerido');
  if (typeof e.total !== 'number') err.push('total');
  if (e.version !== VERSION) err.push('version invalida');

  // actor: siempre presente, con un rol conocido.
  if (!e.actor || typeof e.actor.usuario !== 'string' || !e.actor.usuario) {
    err.push('actor.usuario requerido');
  } else if (!ROLES.includes(e.actor.rol)) {
    err.push('actor.rol invalido');
  }

  if (typeof e.origen !== 'string' || !e.origen) err.push('origen requerido');

  // items: no pueden venir vacios, cada cantidad > 0 y el total debe cuadrar.
  if (!Array.isArray(e.items) || e.items.length === 0) {
    err.push('items requerido');
  } else {
    let suma = 0;
    for (const it of e.items) {
      if (!it.productoId) err.push('item.productoId requerido');
      if (typeof it.cantidad !== 'number' || it.cantidad <= 0) {
        err.push(`item.cantidad invalida (${it.productoId})`);
      }
      if (typeof it.precio !== 'number' || it.precio <= 0) {
        err.push(`item.precio invalido (${it.productoId})`);
      }
      suma += (it.cantidad || 0) * (it.precio || 0);
    }
    // El total debe ser EXACTAMENTE la suma de cantidad x precio.
    if (typeof e.total === 'number' && suma !== e.total) {
      err.push(`total ${e.total} no cuadra con items (${suma})`);
    }
  }

  return err;
}

/** Items de ejemplo para un pedido del catalogo de prueba. */
function itemsDeEjemplo(pedidoId, indice = 0) {
  const p1 = CATALOGO[0];
  const p2 = CATALOGO[(indice + 1) % CATALOGO.length];
  const cantidad = 1 + (indice % 2);
  const items = [{ productoId: p1.productoId, cantidad, precio: p1.precio }];
  if (indice % 2 === 1) items.push({ productoId: p2.productoId, cantidad: 1, precio: p2.precio });
  const total = items.reduce((s, i) => s + i.cantidad * i.precio, 0);
  return { items, total };
}

module.exports = {
  TIPOS,
  ROLES,
  VERSION,
  CATALOGO,
  PRECIOS,
  crearEvento,
  validar,
  itemsDeEjemplo,
};
