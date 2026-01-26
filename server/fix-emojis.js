const fs = require('fs');

// Read file
let content = fs.readFileSync('worker.js', 'utf8');

// Remove BOM if present
if (content.charCodeAt(0) === 0xFEFF) {
  content = content.slice(1);
}

// Replace ALL emojis (not just in template literals) with unicode escapes
// This regex matches any character outside BMP (surrogate pairs) plus variation selectors
let emojisReplaced = 0;

// First, let's replace emojis inside template literals only
// Match template literal content and replace emojis inside
content = content.replace(/`([^`]*)`/gs, (match, inner) => {
  // Replace all surrogate pairs (emojis) with unicode escapes
  let newInner = '';
  for (let i = 0; i < inner.length; i++) {
    const code = inner.codePointAt(i);
    if (code > 0xFFFF) {
      // This is a surrogate pair (4-byte emoji)
      emojisReplaced++;
      newInner += '\\u{' + code.toString(16) + '}';
      i++; // Skip the low surrogate
    } else if (code === 0xFE0F) {
      // Variation selector - skip it
      continue;
    } else {
      newInner += inner[i];
    }
  }
  return '`' + newInner + '`';
});

fs.writeFileSync('worker.js', content, 'utf8');
console.log('Replaced', emojisReplaced, 'emojis in template literals with unicode escapes');
