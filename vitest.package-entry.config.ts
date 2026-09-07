import { defineConfig } from 'vitest/config'

export default defineConfig({
  test: {
    include: ['tests/package-entry.spec.ts'],
  },
})
