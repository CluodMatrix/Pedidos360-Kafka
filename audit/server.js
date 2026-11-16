// API HTTP de SOLO LECTURA del servicio audit.
//
// Corre en el MISMO PROCESO que el consumidor (lo arranca audit/consumer.js),
// para compartir el timeline en memoria.
//
//   GET /api/audit/:pedidoId            -> [{fecha,tipo,quien,desdeDonde}] por fecha
//   GET /api/audit?usuario=&tipo=&desde=&hasta=  -> eventos que cumplen los filtros
//
// Cualquier POST/PUT/PATCH/DELETE responde 405 Method Not Allowed.
const express = require('express');

function arrancar() {
  const PUERTO = Number(process.env.PORT || 3002);
  // El timeline vive en el consumidor: se importa aqui (mismo proceso).
  const { timelineDe, eventosFiltrados } = require('./consumer');
  const app = express();

  const soloLectura = (req, res) => res.status(405).json({ error: 'Method Not Allowed' });
  app.post('/api/audit/*', soloLectura);
  app.put('/api/audit/*', soloLectura);
  app.patch('/api/audit/*', soloLectura);
  app.delete('/api/audit/*', soloLectura);

  app.use((req, res, next) => {
    res.set('Access-Control-Allow-Origin', '*');
    res.set('Access-Control-Allow-Methods', 'GET, OPTIONS');
    res.set('Access-Control-Allow-Headers', 'Content-Type');
    if (req.method === 'OPTIONS') return res.sendStatus(204);
    next();
  });

  // Timeline de un pedido.
  app.get('/api/audit/:pedidoId', (req, res) => {
    const items = timelineDe(req.params.pedidoId).map((e) => ({
      fecha: e.fecha,
      tipo: e.tipo,
      quien: e.quien,
      desdeDonde: e.desdeDonde,
    }));
    res.json(items);
  });

  // Todos los eventos, con filtros.
  app.get('/api/audit', (req, res) => {
    const { usuario, tipo, desde, hasta } = req.query;
    const items = eventosFiltrados({ usuario, tipo, desde, hasta }).map((e) => ({
      pedidoId: e.pedidoId,
      fecha: e.fecha,
      tipo: e.tipo,
      quien: e.quien,
      desdeDonde: e.desdeDonde,
    }));
    res.json(items);
  });

  app.listen(PUERTO, () => {
    console.log(`[audit] API en http://localhost:${PUERTO}`);
    console.log('  GET /api/audit/:pedidoId  |  GET /api/audit?usuario=&tipo=&desde=&hasta=');
  });
  return app;
}

module.exports = { arrancar };

// Arranque directo (sin el consumidor): util para depurar la API sola.
if (require.main === module) arrancar();
