// Consultas al broker con las formas EXACTAS que devuelve la API de KafkaJS.
//
// Un solo lugar: si la version de kafkajs cambia la forma de una respuesta,
// se corrige aqui y no en los 5 scripts que la usan.
//
//   listTopics()                -> array de strings
//   fetchTopicOffsets(topico)    -> [{partition, offset, high, low}]
//   fetchTopicMetadata({topics}) -> {topics:[{name, partitions:[{partitionId, leader, replicas, isr}]}]}
//   fetchOffsets({groupId,topics}) -> [{topic, partitions:[{partition, offset}]}]
//   listGroups()                -> {groups:[...]}
const { crearKafka } = require('./kafka');

/** Lista de topicos con su numero de particiones. */
async function listarTopicos() {
  const admin = crearKafka('consultas').admin();
  await admin.connect();
  try {
    const nombres = await admin.listTopics();
    const lista = Array.isArray(nombres) ? nombres : nombres.topics ?? [];
    const { topics } = await admin.fetchTopicMetadata({ topics: lista });
    return (topics ?? []).map((t) => ({
      topic: t.name,
      particiones: (t.partitions ?? []).length,
      lideres: (t.partitions ?? []).map((p) => p.leader),
      replicas: (t.partitions ?? []).map((p) => p.replicas ?? []),
      isr: (t.partitions ?? []).map((p) => p.isr ?? []),
    }));
  } finally {
    await admin.disconnect();
  }
}

/** Grupos de consumidores conocidos del broker. */
async function listarGrupos() {
  const admin = crearKafka('consultas').admin();
  await admin.connect();
  try {
    const { groups } = await admin.listGroups();
    return (groups ?? []).map((g) => ({
      groupId: g.groupId,
      estado: g.state,
      miembros: g.members?.length ?? 0,
    }));
  } finally {
    await admin.disconnect();
  }
}

/**
 * Lag de un grupo sobre un topico.
 * lag = (ultimo offset escrito) - (offset confirmado por el grupo)
 */
async function lagDe(groupId, topico) {
  const admin = crearKafka('consultas').admin();
  await admin.connect();
  try {
    // Offset confirmado por el grupo (el "marcapaginas").
    const offsets = await admin.fetchOffsets({ groupId, topics: [topico] });
    const delGrupo = (Array.isArray(offsets) ? offsets : []).find((o) => o.topic === topico);
    const confirmadoPorParticion = new Map(
      (delGrupo?.partitions ?? [])
        .filter((p) => p.partition >= 0)
        .map((p) => [p.partition, Number(p.offset)]),
    );

    // Ultimo offset escrito en cada particion (y el "low": lo que Kafka ya borro).
    const porParticion = await admin.fetchTopicOffsets(topico);

    const particiones = porParticion.map((p) => {
      const confirmado = confirmadoPorParticion.get(p.partition);
      // p.offset ES el high watermark (el siguiente offset a escribir).
      // p.low es el low watermark (lo primero que Kafka todavia conserva).
      const ultimo = Number(p.offset ?? p.high ?? 0);
      const low = Number(p.low ?? 0);
      // Pending = high - low (todo lo que hay en la particion todavia esta vivo).
      // Pending = high - confirmado (lo que el grupo aun no leyo).
      // Si el grupo nunca confirmo (offset = -1), esta TODO pendiente.
      const pendiente = Math.max(0, ultimo - low);
      const lag =
        confirmado === undefined || confirmado < 0 ? pendiente : Math.max(0, ultimo - confirmado);
      return {
        particion: p.partition,
        confirmado: confirmado ?? null,
        ultimo,
        low,
        lag,
      };
    });

    return {
      grupo: groupId,
      topico,
      particiones,
      lag: particiones.reduce((s, p) => s + p.lag, 0),
    };
  } finally {
    await admin.disconnect();
  }
}

/** Lider y replicas (ISR) de cada particion. */
async function infoReplicas(topico) {
  const admin = crearKafka('consultas').admin();
  await admin.connect();
  try {
    const { topics } = await admin.fetchTopicMetadata({ topics: [topico] });
    const t = (topics ?? []).find((x) => x.name === topico);
    return (t?.partitions ?? []).map((p) => ({
      particion: p.partitionId,
      lider: p.leader,
      replicas: p.replicas ?? [],
      isr: p.isr ?? [],
      offline: p.offlineReplicas ?? [],
    }));
  } finally {
    await admin.disconnect();
  }
}

module.exports = { listarTopicos, listarGrupos, lagDe, infoReplicas };
