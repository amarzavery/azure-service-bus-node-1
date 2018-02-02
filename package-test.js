const child_process = require('child_process');
const mv = require('mv');

console.log('Generating package tarball...');
const output = child_process.execSync('npm pack', { encoding: 'utf8' });
console.log(output);
const tarball = output.trim().split('\n').pop().trim();
mv(tarball, 'package-test/azure-sb-messaging.tgz', err => {
    if (err) {
        console.error(err);
        process.exit(1);
    } else {
        process.exit(0);
    }
});
