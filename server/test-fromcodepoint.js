const esbuild = require('./node_modules/wrangler/node_modules/esbuild');

const testCode = `
const emoji = String.fromCodePoint(0x1F39F);
const msg = emoji + " test";
console.log(msg);
`;

esbuild.transform(testCode, {loader: 'js'})
  .then(r => console.log('OK:', r.code))
  .catch(e => console.error('Error:', e.message));
