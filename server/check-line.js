const fs = require('fs');

// Read file and check line 2958
const content = fs.readFileSync('worker.js', 'utf8');
const lines = content.split(/\r?\n/);
const line2958 = lines[2957];

console.log('Line 2958 length:', line2958.length);
console.log('Line 2958:', line2958);

// Find all characters in the line
for (let i = 0; i < Math.min(line2958.length, 50); i++) {
  const char = line2958[i];
  const code = char.codePointAt(0);
  if (code > 127) {
    console.log(`Char at ${i}: U+${code.toString(16).toUpperCase()} = "${char}"`);
  }
}

// Check if backtick is at position 25 (0-indexed)
console.log('\nChar at position 25:', line2958[25], 'code:', line2958.charCodeAt(25).toString(16));
console.log('Char at position 26:', line2958[26], 'code:', line2958.codePointAt(26).toString(16));
