const fs = require('fs');
const acorn = require('acorn');

// Read file as buffer, then convert to string
const buffer = fs.readFileSync('worker.js');
console.log('File size:', buffer.length, 'bytes');

// Decode as UTF-8
const content = buffer.toString('utf8');
console.log('Content length:', content.length, 'chars');

// Check line 2958
const lines = content.split(/\r?\n/);
console.log('Line 2958 length:', lines[2957].length);
console.log('Line 2958:', lines[2957].substring(0, 80));

// Check char at position 26
const char26 = lines[2957][26];
console.log('Char at 26:', char26);
console.log('Char code at 26:', char26.codePointAt(0).toString(16));

// Try parsing with acorn
try {
  acorn.parse(content, { ecmaVersion: 2022, sourceType: 'module' });
  console.log('Acorn parse: OK');
} catch (e) {
  console.log('Acorn error:', e.message);
  console.log('At position:', e.pos);
  
  // What's at that position?
  console.log('Char at error pos:', content[e.pos]);
  console.log('Char code:', content.codePointAt(e.pos).toString(16));
  console.log('Surrounding:', content.substring(e.pos - 10, e.pos + 20));
}
