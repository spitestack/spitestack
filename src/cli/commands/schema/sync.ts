/**
 * spite schema sync - Generate or update the schema lock file.
 */

import { resolve, relative } from "path";
import { logger } from "../../utils/logger";

export async function syncCommand(domain: string, force: boolean): Promise<void> {
  const {
    Compiler,
    readLockFile,
    writeLockFile,
    createLockFile,
    diffSchema,
  } = await import("../../../../lib/compiler/src/index");

  const domainDir = resolve(process.cwd(), domain);

  logger.header("spite schema sync");
  logger.info(`Domain: ${relative(process.cwd(), domainDir) || "."}`);
  if (force) {
    logger.warn("Force mode enabled - breaking changes will be allowed");
  }
  logger.raw("");

  try {
    // Parse current domain
    const compiler = new Compiler({ domainDir, outDir: ".spite-temp" });
    const domainIR = await compiler.parse();

    // Read existing lock file
    const existingLock = await readLockFile(domainDir);

    // Create new lock from current schema
    const newLock = createLockFile(domainIR);

    if (!existingLock) {
      // No existing lock - create initial
      await writeLockFile(domainDir, newLock);

      const aggregateCount = Object.keys(newLock.aggregates).length;
      const eventCount = Object.values(newLock.aggregates).reduce(
        (sum, agg) => sum + agg.events.length,
        0
      );

      logger.success("Created events.lock.json");
      logger.step(`${aggregateCount} aggregate(s)`);
      logger.step(`${eventCount} event type(s)`);
      return;
    }

    // Diff against existing lock
    const diff = diffSchema(domainIR, existingLock);

    if (diff.nonBreaking.length === 0 && diff.breaking.length === 0) {
      logger.success("Schema is already up to date");
      return;
    }

    // Check for breaking changes
    if (diff.breaking.length > 0 && !force) {
      logger.error(`${diff.breaking.length} breaking change(s) detected:`);
      logger.raw("");

      for (const change of diff.breaking) {
        printBreakingChange(change);
      }

      logger.raw("");
      logger.error("Cannot sync with breaking changes.");
      logger.info("Use --force to override (dangerous in production!)");
      logger.info("Or add upcasters to handle schema migration.");
      process.exit(1);
    }

    // Write updated lock file
    await writeLockFile(domainDir, newLock);

    logger.success("Updated events.lock.json");

    if (diff.nonBreaking.length > 0) {
      logger.info(`${diff.nonBreaking.length} non-breaking change(s) applied:`);
      for (const change of diff.nonBreaking) {
        printNonBreakingChange(change);
      }
    }

    if (diff.breaking.length > 0) {
      logger.warn(`${diff.breaking.length} breaking change(s) force-applied:`);
      for (const change of diff.breaking) {
        printBreakingChange(change);
      }
      logger.raw("");
      logger.warn("Ensure all consumers can handle the new schema!");
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

function printBreakingChange(change: SchemaChange): void {
  switch (change.kind) {
    case "eventRemoved":
      logger.step(`Event removed: ${change.aggregate}.${change.event}`);
      break;
    case "fieldRemoved":
      logger.step(
        `Field removed: ${change.aggregate}.${change.event}.${change.field}`
      );
      break;
    case "fieldAdded":
      if (!change.isOptional) {
        logger.step(
          `Required field added: ${change.aggregate}.${change.event}.${change.field}`
        );
      }
      break;
    case "fieldTypeChanged":
      logger.step(
        `Type changed: ${change.aggregate}.${change.event}.${change.field} (${change.from} -> ${change.to})`
      );
      break;
  }
}

function printNonBreakingChange(change: SchemaChange): void {
  switch (change.kind) {
    case "eventAdded":
      logger.step(`Event added: ${change.aggregate}.${change.event}`);
      break;
    case "fieldAdded":
      if (change.isOptional) {
        logger.step(
          `Optional field added: ${change.aggregate}.${change.event}.${change.field}`
        );
      }
      break;
  }
}
