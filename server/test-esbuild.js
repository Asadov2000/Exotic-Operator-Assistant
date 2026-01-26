const esbuild = require('./node_modules/wrangler/node_modules/esbuild');
const fs = require('fs');

async function test() {
  try {
    // Read file as buffer
    const content = fs.readFileSync('worker.js');
    console.log('File size:', content.length, 'bytes');
    
    // Check first bytes
    console.log('First 10 bytes:', content.slice(0, 10).toString('hex'));
    
    // Try transform with explicit encoding
    const result = await esbuild.transform(content, {
      loader: 'js',
      format: 'esm',
      charset: 'utf8'
    });
    console.log('Transform OK, output length:', result.code.length);
  } catch (e) {
    console.error('Error:', e.message);
    if (e.errors) {
      console.error('Details:', JSON.stringify(e.errors[0].location, null, 2));
    }
  }
}

test();
