const fs = require('fs');

// Read file
let content = fs.readFileSync('worker.js', 'utf8');
console.log('Original file size:', content.length);

// Remove BOM if present
if (content.charCodeAt(0) === 0xFEFF) {
  content = content.slice(1);
  console.log('BOM removed');
}

// Replace ALL emojis everywhere (not just template literals) 
// with unicode escapes to avoid esbuild issues
let emojisReplaced = 0;
let newContent = '';

for (let i = 0; i < content.length; i++) {
  const code = content.codePointAt(i);
  
  if (code > 0xFFFF) {
    // This is a surrogate pair (4-byte emoji)
    emojisReplaced++;
    newContent += '\\u{' + code.toString(16) + '}';
    i++; // Skip the low surrogate
  } else if (code === 0xFE0F) {
    // Variation selector - add as unicode escape with braces
    newContent += '\\u{fe0f}';
  } else {
    newContent += content[i];
  }
}

fs.writeFileSync('worker.js', newContent, 'utf8');
console.log('New file size:', newContent.length);
console.log('Replaced', emojisReplaced, 'emojis with unicode escapes');
