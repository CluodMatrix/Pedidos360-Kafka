// Creacion de topicos. El MISMO codigo que crea los topicos se versiona en
// Git: la configuracion queda replicable (nunca a mano en la Kafka UI).
const { DLT, TOPICOS, GRUPOS } = require('../shared/topics');

/**
 * Convierte la definicion de un topico en la que espera createTopics.
 * @param {object} t  definicion (topic, particiones, retencionDias, cleanupPolicy)
 */
function definicionTopico(t) {
  return {
    topic: t.topic,
    numPartitions: t.particiones,
    replicationFactor: 1, // 1 en local (un broker); 3 en produccion
    configEntries: [
      { name: 'cleanup.policy', value: t.cleanupPolicy || 'delete' },
      { name: 'retention.ms', value: String(t.retencionDias * 86400000) },
    ],
  };
}

/** Todos los topicos del caso: los de shared/topics.js + un DLT por consumidor. */
function definicionesCompletas() {
  const defs = TOPICOS.map(definicionTopico);
  for (const grupo of GRUPOS) {
    defs.push(
      definicionTopico({
        topic: DLT(grupo),
        particiones: 3,
        retencionDias: 14,
        cleanupPolicy: 'delete',
      }),
    );
  }
  return defs;
}

module.exports = { definicionTopico, definicionesCompletas };
