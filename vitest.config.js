import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    environment: 'node',
    include: ['test/**/*.vitest.spec.js'],
    testTimeout: 180_000,
  },
});
