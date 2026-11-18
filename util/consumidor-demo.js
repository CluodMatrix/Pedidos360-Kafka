// Demo de reparto: consumidor de solo logs para mostrar cómo Kafka reparte las
// particiones entre instancias del mismo grupo. NO sirve API y NO toca el grupo
// "report" que usa el dashboard. Usa el grupo "report-v2" (experimento de replay).
//   npm run demo-consumidor      (una vez por terminal)
process.env.GRUPO_REPORT = 'report-v2';
process.env.SOLO_LOGS = '1';
process.env.FROM_BEGINNING = 'false'; // solo eventos nuevos, no el historial
require('../report/consumer');
