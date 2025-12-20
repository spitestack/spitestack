/**
 * Test server for spitestack dev
 */

import { createCommandHandler } from "./.spitestack/generated/routes";

// Mock db for testing
const mockDb = {} as any;

const handler = createCommandHandler({ db: mockDb });

const server = Bun.serve({
  port: 3000,
  fetch: handler,
});

console.log(`Server running at http://localhost:${server.port}`);
