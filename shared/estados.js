// Maquina de estados del pedido + regla de transiciones validas.
//
// Regla del caso: "NO SE PUEDE DESPACHAR SIN ACEPTAR".
//
// Un evento trae estadoAnterior y estadoNuevo. El consumidor de report
// revisa la transicion ANTES de actualizar sus KPIs: si no es valida, lanza
// un error NO reintentable y el mensaje va directo a la DLT.

const ESTADOS = [
  'PEDIDO_CREADO',
  'PEDIDO_ACEPTADO',
  'PEDIDO_EN_PREPARACION',
  'PEDIDO_DESPACHADO',
  'PEDIDO_ENTREGADO',
  'PEDIDO_CANCELADO',
];

// Estado -> estados a los que puede pasar.
const TRANSICIONES = {
  PEDIDO_CREADO: ['PEDIDO_ACEPTADO', 'PEDIDO_CANCELADO'],
  PEDIDO_ACEPTADO: ['PEDIDO_EN_PREPARACION', 'PEDIDO_CANCELADO'],
  PEDIDO_EN_PREPARACION: ['PEDIDO_DESPACHADO', 'PEDIDO_CANCELADO'],
  PEDIDO_DESPACHADO: ['PEDIDO_ENTREGADO'],
  PEDIDO_ENTREGADO: [],
  PEDIDO_CANCELADO: [],
};

// Estados finales: el pedido ya no esta activo.
const ESTADOS_FINALES = ['PEDIDO_ENTREGADO', 'PEDIDO_CANCELADO'];

/**
 * Indica si la transicion de un pedido es valida.
 * @param {string|null} anterior estado previo (null = pedido recien creado)
 * @param {string} nuevo estado nuevo
 * @returns {boolean}
 */
function esTransicionValida(anterior, nuevo) {
  // El primer evento de un pedido (CREADO) no tiene estado anterior.
  if (anterior === null || anterior === undefined) {
    return nuevo === 'PEDIDO_CREADO';
  }
  const permitidos = TRANSICIONES[anterior];
  if (!permitidos) return false;
  return permitidos.includes(nuevo);
}

/** Detalle legible del motivo de una transicion invalida. */
function explicarTransicion(anterior, nuevo) {
  if (anterior === null || anterior === undefined) {
    return `PEDIDO_CREADO esperado como primer estado, recibio ${nuevo}`;
  }
  const permitidos = TRANSICIONES[anterior] || [];
  return `transicion invalida ${anterior} -> ${nuevo} (permitidos: ${permitidos.join(', ') || 'ninguno'})`;
}

/** Un pedido sigue activo si su ultimo estado no es final. */
function sigueActivo(estado) {
  return !ESTADOS_FINALES.includes(estado);
}

module.exports = { ESTADOS, TRANSICIONES, ESTADOS_FINALES, esTransicionValida, explicarTransicion, sigueActivo };
