/**
 * spite schema status - Show current schema status.
 */

import { resolve, relative } from "path";
import { logger } from "../../utils/logger";

export async function statusCommand(domain: string): Promise<void> {
  const {
    Compiler,
    readLockFile,
    createLockFile,
    diffSchema,
  } = await import("../../../../lib/compiler/src/index");

  const domainDir = resolve(process.cwd(), domain);

  logger.header("spite schema status");
  logger.info(`Domain: ${relative(process.cwd(), domainDir) || "."}`);
  logger.raw("");

  try {
    // Parse current domain
    const compiler = new Compiler({ domainDir, outDir: ".spite-temp" });
    const domainIR = await compiler.parse();

    // Read existing lock file
    const lock = await readLockFile(domainDir);

    if (!lock) {
      logger.warn("No events.lock.json found");
      logger.raw("");
      logger.info("Run `spite schema sync` to create the initial lock file.");
      logger.raw("");

      // Show current schema summary
      const currentLock = createLockFile(domainIR);
      const aggregateCount = Object.keys(currentLock.aggregates).length;
      const eventCount = Object.values(currentLock.aggregates).reduce(
        (sum, agg) => sum + agg.events.length,
        0
      );

      logger.info(`Current schema:`);
      logger.step(`${aggregateCount} aggregate(s)`);
      logger.step(`${eventCount} event type(s)`);
      return;
    }

    // Diff current schema against lock
    const diff = diffSchema(domainIR, lock);

    if (diff.nonBreaking.length === 0 && diff.breaking.length === 0) {
      logger.success("Schema is up to date");
      logger.raw("");

      // Show schema summary
      const aggregateCount = Object.keys(lock.aggregates).length;
      const eventCount = Object.values(lock.aggregates).reduce(
        (sum, agg) => sum + agg.events.length,
        0
      );

      logger.info(`Locked schema (${lock.timestamp}):`);
      logger.step(`${aggregateCount} aggregate(s)`);
      logger.step(`${eventCount} event type(s)`);
      return;
    }

    // Show changes
    if (diff.breaking.length > 0) {
      logger.error(`${diff.breaking.length} breaking change(s) detected!`);
      logger.raw("");

      for (const change of diff.breaking) {
        printChange(change, true);
      }
    }

    if (diff.nonBreaking.length > 0) {
      logger.info(`${diff.nonBreaking.length} non-breaking change(s):`);
      logger.raw("");

      for (const change of diff.nonBreaking) {
        printChange(change, false);
      }
    }

    logger.raw("");

    if (diff.breaking.length > 0) {
      logger.warn("Breaking changes require --force to sync.");
      logger.info("Consider adding upcasters for backward compatibility.");
    } else {
      logger.info("Run `spite schema sync` to update the lock file.");
    }
  } catch (error) {
    if (error instanceof Error) {
      logger.error(error.message);
    } else {
      logger.error(String(error));
    }
    process.exit(1);
  }
}

type SchemaChange = import("../../../../lib/compiler/src/index").SchemaChange;

function printChange(change: SchemaChange, isBreaking: boolean): void {
  const prefix = isBreaking ? "  - " : "  + ";

  switch (change.kind) {
    case "eventAdded":
      logger.raw(`${prefix}Event added: ${change.aggregate}.${change.event}`);
      break;
    case "eventRemoved":
      logger.raw(`${prefix}Event removed: ${change.aggregate}.${change.event}`);
      break;
    case "fieldAdded":
      const optional = change.isOptional ? " (optional)" : " (required)";
      logger.raw(
        `${prefix}Field added: ${change.aggregate}.${change.event}.${change.field}${optional}`
      );
      break;
    case "fieldRemoved":
      logger.raw(
        `${prefix}Field removed: ${change.aggregate}.${change.event}.${change.field}`
      );
      break;
    case "fieldTypeChanged":
      logger.raw(
        `${prefix}Type changed: ${change.aggregate}.${change.event}.${change.field} (${change.from} -> ${change.to})`
      );
      break;
  }
}
