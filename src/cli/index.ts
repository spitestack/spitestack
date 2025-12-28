#!/usr/bin/env bun
/**
 * Spite CLI - Code angry.
 */

import { resolve } from "path";
import { logger } from "./utils/logger";
import { parseArgs, getHelpText, getCommandHelp } from "./args";

const VERSION = "0.1.0";

async function main() {
  const command = parseArgs();

  switch (command.type) {
    case "help":
      logger.raw(getHelpText());
      break;

    case "version":
      logger.raw(`spite v${VERSION}`);
      break;

    case "compile": {
      const { compile } = await import("./commands/compile");
      await compile({
        domainDir: resolve(process.cwd(), command.options.domain),
        outDir: resolve(process.cwd(), command.options.output),
        skipPurityCheck: command.options.skipPurity,
      });
      break;
    }

    case "build": {
      const { build } = await import("./commands/build");
      await build({
        inputDir: resolve(process.cwd(), command.options.input),
        outputDir: resolve(process.cwd(), command.options.output),
      });
      break;
    }

    case "check": {
      const { check } = await import("./commands/check");
      await check({
        domainDir: resolve(process.cwd(), command.options.domain),
      });
      break;
    }

    case "dev": {
      const { dev } = await import("./commands/dev");
      await dev({
        domainDir: resolve(process.cwd(), command.options.domain),
        outDir: resolve(process.cwd(), command.options.output),
        skipPurityCheck: command.options.skipPurity,
        port: command.options.port,
      });
      break;
    }

    case "watch": {
      const { watchCommand } = await import("./commands/watch");
      await watchCommand(command.options);
      break;
    }

    case "init": {
      const { init } = await import("./commands/init");
      await init({
        name: command.options.name,
        domain: command.options.domain,
        port: command.options.port,
      });
      break;
    }

    case "schema": {
      const { schemaCommand } = await import("./commands/schema/index");
      await schemaCommand(command.action);
      break;
    }

    case "unknown":
      logger.error(`Unknown command: ${command.command}`);
      logger.raw("");
      logger.raw("Run 'spite --help' for usage.");
      process.exit(1);
  }
}

main().catch((error) => {
  logger.error(error instanceof Error ? error.message : String(error));
  process.exit(1);
});
