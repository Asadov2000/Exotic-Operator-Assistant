const fs = require('fs');

let content = fs.readFileSync('worker.js', 'utf8');
console.log('Original size:', content.length);

// First remove variation selectors that follow emojis
content = content.replace(/\ufe0f/g, '');

// Count replacements
let count = 0;

// Replace all 4-byte characters (emojis) with surrogate pair escapes
let result = '';
for (let i = 0; i < content.length; i++) {
  const code = content.codePointAt(i);
  if (code > 0xFFFF) {
    // 4-byte character - convert to surrogate pair
    const offset = code - 0x10000;
    const high = 0xD800 + (offset >> 10);
    const low = 0xDC00 + (offset & 0x3FF);
    result += '\\u' + high.toString(16).toUpperCase() + '\\u' + low.toString(16).toUpperCase();
    count++;
    i++; // Skip low surrogate
  } else {
    result += content[i];
  }
}

fs.writeFileSync('worker.js', result, 'utf8');
console.log('New size:', result.length);
console.log('Replaced', count, '4-byte characters with surrogate pairs');
