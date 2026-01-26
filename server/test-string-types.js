const fs = require('fs');

let content = fs.readFileSync('worker.js', 'utf8');
console.log('Original size:', content.length);

// Remove BOM if present
if (content.charCodeAt(0) === 0xFEFF) {
  content = content.slice(1);
}

// This is a complex transformation. Let's just verify the issue first.
// Check if simple strings with emoji work:
const testSimple = "const x = '🎟 test';";
const testTemplate = "const x = `🎟 test`;";

console.log('Simple string test:', testSimple);
console.log('Template literal test:', testTemplate);

// Write test files
fs.writeFileSync('test-simple.js', testSimple);
fs.writeFileSync('test-template.js', testTemplate);

// Test both with esbuild
const esbuild = require('./node_modules/wrangler/node_modules/esbuild');

async function test() {
  try {
    await esbuild.transform(fs.readFileSync('test-simple.js', 'utf8'), {loader: 'js'});
    console.log('Simple string: OK');
  } catch (e) {
    console.log('Simple string ERROR:', e.message);
  }
  
  try {
    await esbuild.transform(fs.readFileSync('test-template.js', 'utf8'), {loader: 'js'});
    console.log('Template literal: OK');
  } catch (e) {
    console.log('Template literal ERROR:', e.errors[0].text);
  }
}

test();
