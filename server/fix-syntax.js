const fs = require('fs');

let c = fs.readFileSync('worker.js', 'utf8');
console.log('Original size:', c.length);

// Find the problematic pattern
const pattern = 'USDT/мес`;`';
const idx = c.indexOf(pattern);
console.log('Found pattern at:', idx);

if (idx >= 0) {
  console.log('Context:', c.substring(idx - 30, idx + 40));
  // Fix: replace `;` with just `;
  c = c.replace('`;`', '`;');
  fs.writeFileSync('worker.js', c);
  console.log('Fixed! New size:', c.length);
} else {
  console.log('Pattern not found');
}
