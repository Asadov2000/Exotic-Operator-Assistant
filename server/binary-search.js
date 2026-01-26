const fs = require('fs');
const acorn = require('acorn');

const content = fs.readFileSync('worker.js', 'utf8');
const lines = content.split(/\r?\n/);

console.log('Total lines:', lines.length);

// Binary search for the first problematic section
let low = 0;
let high = lines.length;

while (low < high) {
  const mid = Math.floor((low + high) / 2);
  const testContent = lines.slice(0, mid).join('\n');
  
  try {
    // Try to parse as module (will fail due to incomplete code, so use script mode)
    acorn.parse(testContent, { ecmaVersion: 2022, allowReturnOutsideFunction: true, allowAwaitOutsideFunction: true });
    // If no error, problem is after mid
    low = mid + 1;
  } catch (e) {
    // Check if error is due to incomplete code (expected) or actual syntax error
    if (e.message.includes('Unexpected token') || 
        e.message.includes('Unexpected character') ||
        e.message.includes('Invalid')) {
      // Actual syntax error - problem is before or at mid
      high = mid;
    } else {
      // Just incomplete code, try more
      low = mid + 1;
    }
  }
}

console.log('First problematic line around:', low);

// Show context
if (low > 0 && low < lines.length) {
  console.log('Lines around the problem:');
  for (let i = Math.max(0, low - 3); i < Math.min(lines.length, low + 3); i++) {
    console.log(`${i + 1}: ${lines[i].substring(0, 100)}`);
  }
}

// Try parsing up to that point
const testContent = lines.slice(0, low).join('\n');
try {
  acorn.parse(testContent, { ecmaVersion: 2022, allowReturnOutsideFunction: true, allowAwaitOutsideFunction: true });
  console.log('Parses OK up to line', low);
} catch (e) {
  console.log('Parse error:', e.message);
}
