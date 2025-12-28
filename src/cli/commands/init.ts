/**
 * spite init - Scaffolds a new Spite project.
 */

import { resolve, join } from "path";
import { mkdir, writeFile, access } from "fs/promises";
import { logger } from "../utils/logger";

interface InitOptions {
  name: string;
  domain: string;
  port: number;
}

const PACKAGE_JSON = (name: string, domain: string, port: number) => `{
  "name": "${name}",
  "version": "0.1.0",
  "type": "module",
  "scripts": {
    "dev": "spite dev ${domain} -p ${port}",
    "build": "spite compile ${domain}",
    "check": "spite check ${domain}"
  },
  "dependencies": {
    "spitestack": "latest"
  }
}
`;

const TSCONFIG = (domain: string) => `{
  "compilerOptions": {
    "target": "ESNext",
    "module": "ESNext",
    "moduleResolution": "bundler",
    "strict": true,
    "skipLibCheck": true,
    "declaration": true,
    "outDir": "./dist",
    "rootDir": "."
  },
  "include": ["${domain}/**/*", ".spitestack/**/*"]
}
`;

const COUNTER_AGGREGATE = `/**
 * Counter aggregate - a simple example to get started.
 *
 * Aggregates are the core building blocks in event sourcing.
 * They encapsulate business logic and emit events.
 */

export type CounterState = {
  count: number;
};

export type CounterEvent =
  | { type: "Incremented"; amount: number }
  | { type: "Decremented"; amount: number }
  | { type: "Reset" };

export class Counter {
  static initialState: CounterState = { count: 0 };
  events: CounterEvent[] = [];
  state: CounterState = Counter.initialState;

  emit(event: CounterEvent): void {
    this.events.push(event);
  }

  apply(event: CounterEvent): void {
    switch (event.type) {
      case "Incremented":
        this.state.count += event.amount;
        break;
      case "Decremented":
        this.state.count -= event.amount;
        break;
      case "Reset":
        this.state.count = 0;
        break;
    }
  }

  increment(amount: number = 1): void {
    if (amount <= 0) {
      throw new Error("Amount must be positive");
    }
    this.emit({ type: "Incremented", amount });
  }

  decrement(amount: number = 1): void {
    if (amount <= 0) {
      throw new Error("Amount must be positive");
    }
    if (this.state.count - amount < 0) {
      throw new Error("Count cannot go negative");
    }
    this.emit({ type: "Decremented", amount });
  }

  reset(): void {
    this.emit({ type: "Reset" });
  }
}
`;

const INDEX_TS = `/**
 * Domain configuration.
 *
 * This file configures your app mode and exports aggregates.
 */

import { Counter } from "./counter";

// App mode determines access control defaults:
// - "greenfield": All endpoints public by default
// - "internal": Require authentication by default
// - "public": Read-only public, writes require auth
export const app = {
  mode: "greenfield" as const,
};

export { Counter };
`;

const GITIGNORE = `# Dependencies
node_modules/

# Build output
dist/
.spitestack/

# Environment
.env
.env.local

# IDE
.vscode/
.idea/

# OS
.DS_Store
`;

export async function init(options: InitOptions): Promise<void> {
  const startTime = performance.now();

  logger.header("spite init");
  logger.info(`Project:  ${options.name}`);
  logger.info(`Domain:   ${options.domain}`);
  logger.info(`Port:     ${options.port}`);
  logger.raw("");

  const projectPath = resolve(process.cwd(), options.name);
  const domainPath = join(projectPath, options.domain);

  try {
    // Check if directory already exists
    try {
      await access(projectPath);
      logger.error(`Directory "${options.name}" already exists`);
      process.exit(1);
    } catch {
      // Directory doesn't exist, good
    }

    // Create directories
    await mkdir(domainPath, { recursive: true });
    logger.step(`Created ${options.domain}/ directory`);

    // Write files
    await writeFile(
      join(projectPath, "package.json"),
      PACKAGE_JSON(options.name, options.domain, options.port)
    );
    logger.step("Created package.json");

    await writeFile(join(projectPath, "tsconfig.json"), TSCONFIG(options.domain));
    logger.step("Created tsconfig.json");

    await writeFile(join(projectPath, ".gitignore"), GITIGNORE);
    logger.step("Created .gitignore");

    await writeFile(join(domainPath, "counter.ts"), COUNTER_AGGREGATE);
    logger.step(`Created ${options.domain}/counter.ts`);

    await writeFile(join(domainPath, "index.ts"), INDEX_TS);
    logger.step(`Created ${options.domain}/index.ts`);

    const elapsed = ((performance.now() - startTime) / 1000).toFixed(2);
    logger.raw("");
    logger.success(`Project created in ${elapsed}s`);
    logger.raw("");
    logger.raw("Next steps:");
    logger.raw("");
    logger.raw(`  cd ${options.name}`);
    logger.raw("  bun install");
    logger.raw("  bun run dev");
    logger.raw("");
  } catch (error) {
    if (error instanceof Error) {
      logger.error(error.message);
    } else {
      logger.error(String(error));
    }
    process.exit(1);
  }
}
