// Servicio administrador de Kafka: topicos, grupos y lag (puerto 3000).
//
//   GET    /admin/topics                  -> lista de topicos
//   POST   /admin/topics                  -> {topic, particiones, retencionDias, cleanupPolicy}
//   PUT    /admin/topics/:t               -> {retencionDias, cleanupPolicy, particiones}
//   DELETE /admin/topics/:t               -> elimina el topico
//   GET    /admin/groups                  -> grupos de consumidores
//   GET    /admin/groups/:g/lag           -> offsets y lag del grupo
//
// La idea clave: el mismo codigo que crea los topicos se versiona en Git.
const express = require('express');
const { crearKafka } = require('../shared/kafka');
const { PEDIDOS, DLT, GRUPOS } = require('../shared/topics');
const { listarTopicos, listarGrupos, lagDe } = require('../shared/consultas');

const PUERTO = Number(process.env.PORT_ADMIN || 3000);
const app = express();
app.use(express.json());

let admin = null;

async function conectar() {
  if (!admin) {
    admin = crearKafka('admin').admin();
    await admin.connect();
  }
  return admin;
}

// --- topicos ---------------------------------------------------------------
app.get('/admin/topics', async (req, res) => {
  try {
    res.json(await listarTopicos());
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

app.post('/admin/topics', async (req, res) => {
  const { topic, particiones = 3, retencionDias = 7, cleanupPolicy = 'delete' } = req.body || {};
  if (!topic) return res.status(400).json({ error: 'topic requerido' });
  try {
    const a = await conectar();
    const yaExiste = await a.listTopics();
    const nombres = Array.isArray(yaExiste) ? yaExiste : yaExiste.topics ?? [];
    if (nombres.includes(topic)) {
      return res.status(409).json({ error: 'el topic ya existe', topic });
    }
    await a.createTopics({
      topics: [
        {
          topic,
          numPartitions: particiones,
          replicationFactor: 1,
          configEntries: [
            { name: 'cleanup.policy', value: cleanupPolicy },
            { name: 'retention.ms', value: String(retencionDias * 86400000) },
          ],
        },
      ],
      waitForLeaders: true,
    });
    res.status(201).json({ ok: true, topic, particiones, retencionDias, cleanupPolicy });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

app.put('/admin/topics/:t', async (req, res) => {
  const { retencionDias, cleanupPolicy, particiones } = req.body || {};
  try {
    const a = await conectar();
    const cambios = [];
    if (retencionDias !== undefined) {
      cambios.push({ name: 'retention.ms', value: String(retencionDias * 86400000) });
    }
    if (cleanupPolicy !== undefined) {
      cambios.push({ name: 'cleanup.policy', value: cleanupPolicy });
    }
    if (cambios.length) {
      await a.alterConfigs({ resources: [{ name: req.params.t, configEntries: cambios }] });
    }
    // Las particiones SOLO se pueden AUMENTAR, nunca bajar.
    if (particiones !== undefined) {
      const topics = await listarTopicos();
      const actual = topics.find((t) => t.topic === req.params.t);
      if (!actual) return res.status(404).json({ error: 'topic no encontrado' });
      if (particiones > actual.particiones) {
        await a.createPartitions({ topicPartitions: [{ topic: req.params.t, count: particiones }] });
      } else if (particiones < actual.particiones) {
        return res.status(400).json({ error: 'las particiones no se pueden bajar' });
      }
    }
    res.json({ ok: true, topic: req.params.t, cambios: cambios.length, particiones });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

app.delete('/admin/topics/:t', async (req, res) => {
  try {
    const a = await conectar();
    await a.deleteTopics({ topics: [req.params.t] });
    res.json({ ok: true, eliminado: req.params.t });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// --- grupos y lag ----------------------------------------------------------
app.get('/admin/groups', async (req, res) => {
  try {
    res.json(await listarGrupos());
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

/** Lag = (ultimo offset del topico) - (offset confirmado por el grupo). */
app.get('/admin/groups/:g/lag', async (req, res) => {
  const grupo = req.params.g;
  const topico = req.query.topic || PEDIDOS;
  try {
    res.json(await lagDe(grupo, topico));
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

app.listen(PUERTO, () => {
  console.log(`admin escuchando en http://localhost:${PUERTO}`);
  console.log('  GET /admin/topics | /admin/groups | /admin/groups/:g/lag');
  console.log(`  topicos del caso: ${PEDIDOS} · ${DLT('report')} · ${DLT('audit')}`);
  console.log(`  grupos: ${GRUPOS.join(', ')}`);
});
