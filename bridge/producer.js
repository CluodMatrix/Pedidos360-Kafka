// Productor del caso Pedidos360 v2 con Kafka (mock del servicio de pedidos).
//
// MODOS:
//
// 1) MOCK (por defecto). Genera pedidos NUEVOS con su flujo completo:
//      npm run producer                  -> 12 eventos
//      npm run producer -- 10000         -> 10.000 eventos (~2.000 pedidos), en lotes
//      npm run producer -- 12 1500       -> 12 eventos, uno cada 1,5 s (demo en vivo)
//
//    Cada pedido recorre CREADO -> ACEPTADO -> EN_PREPARACION -> DESPACHADO -> ENTREGADO
//    y su id es nuevo en cada ejecucion, asi que no hay transiciones invalidas.
//    Los actores son personas de prueba con roles reales del caso (CLIENTE, OPERADOR,
//    ADMIN), con las mismas reglas que el backend: el cliente crea, el staff cambia estados.
//
//    El mock publica EXACTAMENTE el mensaje que publicara el servicio real:
//    mismos campos y tipos, misma key (pedidoId) y mismos headers (correlationId).
//
// 2) PUENTE REAL: ver bridge/desde-sqs.js (lee SQS y publica en Kafka).
//
// Flags de error (para demostrar reintentos, DLT e idempotencia):
//      npm run producer -- --salto       -> un pedido que salta de CREADO a DESPACHADO
//      npm run producer -- --malo        -> JSON roto + evento sin pedidoId (-> DLT)
//      npm run producer -- --duplicado   -> el mismo evento dos veces (mismo eventId)
//      npm run producer -- --total-malo  -> PEDIDO_CREADO con total que no cuadra
//      npm run producer -- 12 --key-evento -> usa key = eventId (rompe el orden por pedido)
const crypto = require('crypto');
const { crearKafka } = require('../shared/kafka');
const { crearEvento, validar, itemsDeEjemplo } = require('../shared/dto');
const { PEDIDOS } = require('../shared/topics');

const producer = crearKafka('bridge').producer({
  idempotent: true, // sin duplicados por red
  maxInFlightRequests: 1,
});

// Personas de prueba. Clientes: nombres aleatorios por pedido.
// Operadores: rotan entre dos (cada pedido queda con uno fijo).
// Admin: siempre el mismo.
const PERSONAS = {
  CLIENTE: [
    'Valentina Rojas', 'Matías Soto', 'Camila Muñoz', 'Diego Herrera',
    'Fernanda Lagos', 'Joaquín Pizarro', 'Antonia Vega', 'Sebastián Araya',
  ],
  OPERADOR: ['Camilo Leiva', 'Laura Fontecilla'],
  ADMIN: ['Ramiro Gómez'],
};

// Flujo de un pedido. El rol indica quien hace el cambio, igual que en el backend.
const FLUJO = [
  { tipo: 'PEDIDO_CREADO', rol: 'CLIENTE' },
  { tipo: 'PEDIDO_ACEPTADO', rol: 'OPERADOR' },
  { tipo: 'PEDIDO_EN_PREPARACION', rol: 'OPERADOR' },
  { tipo: 'PEDIDO_DESPACHADO', rol: 'OPERADOR' },
  { tipo: 'PEDIDO_ENTREGADO', rol: 'ADMIN' },
];

const ESTADO_ANTERIOR = {
  PEDIDO_CREADO: null,
  PEDIDO_ACEPTADO: 'PEDIDO_CREADO',
  PEDIDO_EN_PREPARACION: 'PEDIDO_ACEPTADO',
  PEDIDO_DESPACHADO: 'PEDIDO_EN_PREPARACION',
  PEDIDO_ENTREGADO: 'PEDIDO_DESPACHADO',
  PEDIDO_CANCELADO: 'PEDIDO_CREADO',
};

const PEDIDOS_POR_BLOQUE = 200; // pedidos que avanzan "a la vez": asi se intercalan en las particiones
const MENSAJES_POR_ENVIO = 500;

const aleatorioHex = (bytes) => crypto.randomBytes(bytes).toString('hex').toUpperCase();

/**
 * Persona de un rol. El cliente es aleatorio; el operador rota por indice
 * (el mismo indice = mismo operador en todo el flujo del pedido).
 */
function personaDe(rol, indice) {
  const lista = PERSONAS[rol];
  if (rol === 'CLIENTE') return lista[crypto.randomInt(lista.length)];
  return lista[indice % lista.length];
}

// Minutos desde que se crea el pedido hasta cada paso (promedios de una operacion real).
const MINUTOS_DESDE_CREACION = [0, 4, 19, 41, 65];

/**
 * Fechas simuladas para los 5 pasos de un pedido. Los minutos varian un poco
 * (jitter) pero siempre en orden. Asi el tiempo de entrega y las ventas por
 * hora salen realistas, sin esperar en tiempo real.
 */
function fechasDelPedido(inicioMs) {
  let t = inicioMs;
  return FLUJO.map((_, s) => {
    if (s > 0) {
      const gap = MINUTOS_DESDE_CREACION[s] - MINUTOS_DESDE_CREACION[s - 1];
      t += (gap + crypto.randomInt(-2, 3)) * 60 * 1000;
    }
    return new Date(t).toISOString();
  });
}

/** Construye el evento de un pedido en un paso de su flujo. */
function construirEvento(paso, pedido, indice, fecha) {
  const { items, total } = itemsDeEjemplo(pedido.pedidoId, indice);
  return crearEvento({
    tipo: paso.tipo,
    pedidoId: pedido.pedidoId,
    clienteId: pedido.clienteId,
    total,
    items,
    fecha,
    actor: { usuario: paso.rol === 'CLIENTE' ? pedido.clienteNombre : personaDe(paso.rol, indice), rol: paso.rol },
    origen: 'web',
    estadoAnterior: ESTADO_ANTERIOR[paso.tipo],
    estadoNuevo: paso.tipo,
  });
}

/**
 * Genera n eventos. Los pedidos se procesan en bloques: dentro de cada bloque,
 * todos avanzan un paso antes de que avance el siguiente. Asi los eventos de
 * distintos pedidos se mezclan, igual que en una operacion real.
 */
function generarEventos(n) {
  const eventos = [];
  const pedidosNecesarios = Math.ceil(n / FLUJO.length);

  for (let inicio = 0; inicio < pedidosNecesarios && eventos.length < n; inicio += PEDIDOS_POR_BLOQUE) {
    const fin = Math.min(inicio + PEDIDOS_POR_BLOQUE, pedidosNecesarios);
    const bloque = [];
    for (let k = inicio; k < fin; k++) {
      // Pedidos creados en las ultimas 3 horas, para que las ventas se repartan por hora.
      const inicioMs = Date.now() - crypto.randomInt(0, 3 * 60 * 60 * 1000);
      bloque.push({
        k,
        pedidoId: aleatorioHex(4),
        clienteId: `cli-${aleatorioHex(3).toLowerCase()}`,
        clienteNombre: personaDe('CLIENTE', k),
        fechas: fechasDelPedido(inicioMs),
      });
    }
    FLUJO.forEach((paso, s) => {
      for (const pedido of bloque) {
        if (eventos.length >= n) break;
        eventos.push(construirEvento(paso, pedido, pedido.k, pedido.fechas[s]));
      }
    });
  }
  return eventos.slice(0, n);
}

/** Publica un evento ya construido (una llamada por evento). */
async function publicarEvento(evento, correlationId, key) {
  const [r] = await producer.send({
    topic: PEDIDOS,
    acks: -1, // all: lider + todas las replicas (lo seguro para pedidos)
    messages: [
      {
        key: key ?? evento.pedidoId, // mismo pedido -> misma particion -> su historia queda en orden
        value: JSON.stringify(evento),
        headers: { correlationId },
      },
    ],
  });
  console.log(`-> p${r.partition} off${r.baseOffset}  ${evento.pedidoId}  ${evento.tipo}`);
  return r;
}

/** Publica el mensaje TAL CUAL, sin validar: solo para provocar errores. */
async function publicarCrudo(key, value, correlationId) {
  await producer.send({
    topic: PEDIDOS,
    acks: -1,
    messages: [{ key, value, headers: { correlationId: correlationId || crypto.randomUUID() } }],
  });
  console.log(`-> (crudo) ${value}`);
}

const PEDIDOS_DEMO = [
  { pedidoId: 'P-1001', clienteId: 'cli-valentina', clienteNombre: 'Valentina Rojas' },
  { pedidoId: 'P-1002', clienteId: 'cli-matias', clienteNombre: 'Matías Soto' },
  { pedidoId: 'P-1003', clienteId: 'cli-camila', clienteNombre: 'Camila Muñoz' },
];

async function publicarEnLotes(eventos, usarEventId) {
  const inicio = Date.now();
  let publicados = 0;
  for (let i = 0; i < eventos.length; i += MENSAJES_POR_ENVIO) {
    const lote = eventos.slice(i, i + MENSAJES_POR_ENVIO);
    await producer.send({
      topic: PEDIDOS,
      acks: -1,
      messages: lote.map((e) => ({
        key: usarEventId ? e.eventId : e.pedidoId,
        value: JSON.stringify(e),
        headers: { correlationId: crypto.randomUUID() },
      })),
    });
    publicados += lote.length;
    console.log(`  [mock] ${publicados}/${eventos.length} publicados`);
  }
  const segundos = (Date.now() - inicio) / 1000;
  console.log(`Listo: ${publicados} eventos en ${segundos.toFixed(1)} s (${Math.round(publicados / segundos)} msg/s).`);
}

async function main() {
  const arg = process.argv[2];
  const flag = String(arg ?? '').startsWith('--') ? arg : null;

  await producer.connect();

  if (flag === '--malo') {
    // JSON roto + evento sin pedidoId: ambos van a la DLT del consumidor.
    await publicarCrudo('P-ROTO', '{ esto no es JSON');
    const sinId = crearEvento({
      tipo: 'PEDIDO_CREADO',
      pedidoId: '',
      clienteId: 'cli-9',
      total: 1000,
      items: [{ productoId: 'PROD-1', cantidad: 1, precio: 8990 }],
    });
    await publicarCrudo('SIN-ID', JSON.stringify(sinId));
  } else if (flag === '--total-malo') {
    // El total NO cuadra con la suma de cantidad x precio: la DLT lo rechaza.
    const evento = construirEvento(FLUJO[0], PEDIDOS_DEMO[0], 0);
    evento.total = evento.total + 1; // desajuste deliberado
    await publicarEvento(evento, crypto.randomUUID());
  } else if (flag === '--duplicado') {
    // El mismo evento dos veces con el MISMO eventId: para probar idempotencia.
    // Usa un pedido NUEVO: asi el primer CREADO es valido, se aplica, y el
    // segundo (duplicado) se ignora sin ir a la DLT.
    const nuevo = { pedidoId: aleatorioHex(4), clienteId: 'cli-dup', clienteNombre: personaDe('CLIENTE', 0) };
    const evento = construirEvento(FLUJO[0], nuevo, 0);
    await publicarEvento(evento, crypto.randomUUID());
    console.log('   (se publica OTRA vez el mismo evento, mismo eventId)');
    await publicarEvento(evento, crypto.randomUUID());
  } else if (flag === '--salto') {
    // Un pedido que salta de CREADO a DESPACHADO: transicion invalida.
    // Solo debe aparecer en la DLT de report, no en la de audit.
    const pedido = { pedidoId: 'P-2001', clienteId: 'cli-valentina', clienteNombre: 'Valentina Rojas' };
    await publicarEvento(construirEvento(FLUJO[0], pedido, 0), crypto.randomUUID());
    const salto = construirEvento(FLUJO[3], pedido, 0);
    salto.estadoAnterior = 'PEDIDO_CREADO'; // se salta ACEPTADO y EN_PREPARACION
    await publicarEvento(salto, crypto.randomUUID());
  } else {
    const n = Number(arg) || 12;
    const pausa = Number(process.argv[3]) || 0; // ms entre eventos
    const usarEventId = process.argv.includes('--key-evento');

    const eventos = generarEventos(n);
    for (const evento of eventos) {
      const errores = validar(evento);
      if (errores.length) throw new Error(errores.join('; '));
    }

    if (pausa || n <= 50) {
      // Pocos eventos o demo en vivo: uno por uno, con su linea de particion y offset.
      for (const evento of eventos) {
        await publicarEvento(evento, crypto.randomUUID(), usarEventId ? evento.eventId : undefined);
        if (pausa) await new Promise((r) => setTimeout(r, pausa));
      }
    } else {
      await publicarEnLotes(eventos, usarEventId);
    }
  }

  await producer.disconnect();
}

main().catch((e) => {
  console.error(e.message);
  process.exit(1);
});
