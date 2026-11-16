// API HTTP de SOLO LECTURA del servicio report.
//
// Corre en el MISMO PROCESO que el consumidor (lo arranca report/consumer.js),
// para compartir los KPIs en memoria: el consumidor calcula, la API sirve.
//
//   GET /api/report/ventas-por-hora -> [{hora, total}]
//   GET /api/report/lead-time      -> {promedioSeg, pedidos:[{pedidoId,segundos}]}
//   GET /api/report/top-productos  -> [{productoId, unidades}]  (los 3 primeros)
//   GET /api/report/estados        -> [{estado, cantidad}]
//
// Cualquier POST/PUT/PATCH/DELETE responde 405 Method Not Allowed.
const express = require('express');
const kpis = require('./kpis');

function arrancar() {
  const PUERTO = Number(process.env.PORT || 3001);
  const app = express();

  // Solo lectura: cualquier otro metodo responde 405.
  const soloLectura = (req, res) => res.status(405).json({ error: 'Method Not Allowed' });
  app.post('/api/report/*', soloLectura);
  app.put('/api/report/*', soloLectura);
  app.patch('/api/report/*', soloLectura);
  app.delete('/api/report/*', soloLectura);

  // CORS para el frontend del alumno (Vite :5173) y el del profe (Angular :4200).
  app.use((req, res, next) => {
    res.set('Access-Control-Allow-Origin', '*');
    res.set('Access-Control-Allow-Methods', 'GET, OPTIONS');
    res.set('Access-Control-Allow-Headers', 'Content-Type');
    if (req.method === 'OPTIONS') return res.sendStatus(204);
    next();
  });

  // Reportes
  app.get('/api/report/ventas-por-hora', (req, res) => res.json(kpis.obtener().ventas));
  app.get('/api/report/lead-time', (req, res) => res.json(kpis.obtener().leadTime));
  app.get('/api/report/top-productos', (req, res) => res.json(kpis.obtener().topProductos));
  app.get('/api/report/estados', (req, res) => res.json(kpis.obtener().estados));

  // Utilidad para las evidencias
  app.get('/api/report/resumen', (req, res) =>
    res.json({ ...kpis.obtener(), eventosProcesados: kpis.totalProcesados() }),
  );

  app.listen(PUERTO, () => {
    console.log(`[report] API en http://localhost:${PUERTO}`);
    console.log('  GET /api/report/ventas-por-hora | /lead-time | /top-productos | /estados');
  });
  return app;
}

module.exports = { arrancar };

// Arranque directo (sin el consumidor): util para depurar la API sola.
if (require.main === module) arrancar();
