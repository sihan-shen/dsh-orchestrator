import { defineConfig } from 'vitest/config'

export default defineConfig({
  test: {
    include: ['tests/**/*.spec.ts'],
    exclude: [
      'tests/config.spec.ts',
      'tests/context.spec.ts',
      'tests/loader.spec.ts',
      'tests/package-entry.spec.ts',
      'tests/profile.spec.ts',
      'tests/scheduling.spec.ts',
      'tests/worker.spec.ts',
    ],
  },
})
