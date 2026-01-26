const fs = require('fs');

let content = fs.readFileSync('worker.js', 'utf8');
console.log('Original size:', content.length);

// Remove BOM
if (content.charCodeAt(0) === 0xFEFF) {
  content = content.slice(1);
  console.log('BOM removed');
}

// Convert CRLF to LF
content = content.replace(/\r\n/g, '\n');

// Replace all non-ASCII characters (except in comments and regular strings) 
// with unicode escapes ONLY inside template literals

// Actually, let's be more surgical - just escape ALL non-ASCII
let result = '';
let nonAsciiCount = 0;

for (let i = 0; i < content.length; i++) {
  const code = content.codePointAt(i);
  if (code > 127) {
    // Non-ASCII character
    if (code > 0xFFFF) {
      // Surrogate pair
      result += '\\u{' + code.toString(16) + '}';
      i++; // Skip low surrogate
    } else {
      result += '\\u' + code.toString(16).padStart(4, '0');
    }
    nonAsciiCount++;
  } else {
    result += content[i];
  }
}

fs.writeFileSync('worker-escaped.js', result, 'utf8');
console.log('Created worker-escaped.js');
console.log('Replaced', nonAsciiCount, 'non-ASCII characters');
console.log('New size:', result.length);
