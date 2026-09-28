'use strict';

const fs = require('node:fs');
const path = require('node:path');

if (process.platform !== 'win32') {
  fs.chmodSync(path.resolve(__dirname, '..', 'lib', 'index.js'), 0o755);
}
