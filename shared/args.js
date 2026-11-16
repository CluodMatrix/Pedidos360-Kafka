// Lectura de argumentos de linea de comandos.
//
// Soporta LOS DOS formatos, porque en la guia aparecen ambos:
//   --count=20000    (con igual, como en el PPTX)
//   --count 20000    (con espacio)
//
// Un solo lugar para no tener el mismo bug en cada script.
function arg(nombre, porDefecto) {
  const prefijo = `--${nombre}=`;
  for (const a of process.argv) {
    if (a.startsWith(prefijo)) return a.slice(prefijo.length);
  }
  const i = process.argv.indexOf(`--${nombre}`);
  if (i >= 0 && process.argv[i + 1] && !process.argv[i + 1].startsWith('--')) {
    return process.argv[i + 1];
  }
  return porDefecto;
}

/** true si la bandera esta presente (--salto, --malo, --duplicado...). */
function bandera(nombre) {
  return process.argv.includes(`--${nombre}`);
}

/** Todos los flags presentes, sin los valores. */
function banderas() {
  return process.argv.filter((a) => a.startsWith('--') && !a.includes('=')).map((a) => a.slice(2));
}

module.exports = { arg, bandera, banderas };
