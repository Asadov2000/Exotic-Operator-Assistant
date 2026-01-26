const fs = require('fs');
const esbuild = require('./node_modules/wrangler/node_modules/esbuild');

const content = fs.readFileSync('worker.js', 'utf8');

console.log('Testing full file...');

async function test() {
  try {
    const result = await esbuild.transform(content, { loader: 'js' });
    console.log('Full file OK!');
  } catch (e) {
    console.log('Error:', e.errors[0].text);
    console.log('At line:', e.errors[0].location.line);
    console.log('At column:', e.errors[0].location.column);
    console.log('Line text:', e.errors[0].location.lineText.substring(0, 100));
    
    // Now let's check: if we remove ONLY this line, does it work?
    const lines = content.split(/\r?\n/);
    const errorLine = e.errors[0].location.line - 1;
    
    console.log('\nRemoving line', errorLine + 1, 'and testing...');
    const withoutLine = [...lines.slice(0, errorLine), ...lines.slice(errorLine + 1)].join('\n');
    
    try {
      await esbuild.transform(withoutLine, { loader: 'js' });
      console.log('Without line', errorLine + 1, ': OK');
    } catch (e2) {
      console.log('Without line', errorLine + 1, ': Still error at line', e2.errors[0].location.line);
    }
    
    // Check if just that line works
    const justLine = lines[errorLine];
    console.log('\nTesting just line', errorLine + 1, ':', justLine.substring(0, 80));
    try {
      await esbuild.transform(justLine.trim().replace(/ \+$/, ';'), { loader: 'js' });
      console.log('Just this line: OK');
    } catch (e3) {
      console.log('Just this line: Error -', e3.errors[0].text);
    }
  }
}

test();
