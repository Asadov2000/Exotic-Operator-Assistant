const esbuild = require('./node_modules/wrangler/node_modules/esbuild');
const fs = require('fs');

async function test() {
  // Create test file with emoji in template literal
  const testCode = 'const x = `\u{1F39F}\u{FE0F} test`;\nconsole.log(x);';
  fs.writeFileSync('emoji-test.js', testCode, 'utf8');
  
  console.log('Test file content:', testCode);
  console.log('Test file hex:', Buffer.from(testCode).toString('hex'));
  
  try {
    // Test with build API (file)
    const result = await esbuild.build({
      entryPoints: ['emoji-test.js'],
      bundle: true,
      write: false,
      format: 'esm'
    });
    console.log('Build from FILE OK:', result.outputFiles[0].text.substring(0, 100));
  } catch (e) {
    console.error('Build from FILE failed:', e.message);
  }
  
  try {
    // Test with transform API (string)
    const result = await esbuild.transform(testCode, { loader: 'js', format: 'esm' });
    console.log('Transform from STRING OK:', result.code.substring(0, 100));
  } catch (e) {
    console.error('Transform from STRING failed:', e.message);
  }
  
  // Now test with file read
  const fileContent = fs.readFileSync('emoji-test.js', 'utf8');
  console.log('\nRead file content:', fileContent);
  console.log('Read file hex:', Buffer.from(fileContent).toString('hex'));
  
  try {
    const result = await esbuild.transform(fileContent, { loader: 'js', format: 'esm' });
    console.log('Transform from READ FILE OK:', result.code.substring(0, 100));
  } catch (e) {
    console.error('Transform from READ FILE failed:', e.message);
  }
}

test();
