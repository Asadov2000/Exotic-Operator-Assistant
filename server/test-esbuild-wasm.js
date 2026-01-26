const esbuild = require('esbuild-wasm');
const fs = require('fs');

async function test() {
  // Initialize WASM for Node.js (no wasmURL needed)
  await esbuild.initialize({});
  
  try {
    const content = fs.readFileSync('worker.js', 'utf8');
    console.log('File size:', content.length, 'chars');
    
    const result = await esbuild.transform(content, {
      loader: 'js',
      format: 'esm',
    });
    console.log('Transform OK, output length:', result.code.length);
  } catch (e) {
    console.error('Error:', e.message);
    if (e.errors) {
      console.error('Details:', JSON.stringify(e.errors[0], null, 2));
    }
  }
}

test();
