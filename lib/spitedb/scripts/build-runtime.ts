/**
 * Builds the SpiteDB runtime as a single bundled file.
 *
 * This script uses Bun.build() to create a self-contained bundle that can be
 * vendored into generated .spitestack/ directories. All dependencies (like msgpackr)
 * are inlined so users don't need to install them separately.
 */

const result = await Bun.build({
  entrypoints: ['./src/index.ts'],
  outdir: './dist',
  naming: 'spitedb.js',
  target: 'bun',
  format: 'esm',
  minify: false, // Keep readable - production minification happens in `spite build`
  sourcemap: 'external',
  external: [], // Bundle everything including msgpackr
});

if (!result.success) {
  console.error('Build failed:');
  for (const log of result.logs) {
    console.error(log);
  }
  process.exit(1);
}

console.log('SpiteDB runtime built successfully:');
for (const output of result.outputs) {
  console.log(`  ${output.path}`);
}

// Generate type declarations
// For now, we'll copy the source types - a more sophisticated approach would use dts-bundle
console.log('\nNote: Type declarations should be generated separately with build-types.ts');
