const fs = require('fs');

// Test with emoji in template literal  
const testCode = `export default {};
const refMsg = \`🎟️ test\`;
console.log(refMsg);
`;

fs.writeFileSync('test5.js', testCode, 'utf8');
console.log('Created test5.js');
console.log('Content:', testCode);
