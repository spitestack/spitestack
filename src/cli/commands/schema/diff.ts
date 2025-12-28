/**
 * spite schema diff - Show diff between current code and lock file.
 */

import { resolve, relative } from "path";
import { logger } from "../../utils/logger";

// ANSI colors for diff output
const RED = "\x1b[31m";
const GREEN = "\x1b[32m";
const YELLOW = "\x1b[33m";
const CYAN = "\x1b[36m";
const DIM = "\x1b[2m";
const RESET = "\x1b[0m";

export async function diffCommand(domain: string): Promise<void> {
  const {
    Compiler,
    readLockFile,
    createLockFile,
    diffSchema,
  } = await import("../../../../lib/compiler/src/index");

  const domainDir = resolve(process.cwd(), domain);

  logger.header("spite schema diff");
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
      logger.info("Current schema (would be created):");
      logger.raw("");

      const newLock = createLockFile(domainIR);
      printFullSchema(newLock, true);
      return;
    }

    // Diff current schema against lock
    const diff = diffSchema(domainIR, lock);

    if (diff.nonBreaking.length === 0 && diff.breaking.length === 0) {
      logger.success("No changes detected");
      return;
    }

    // Print diff output
    logger.raw(`${DIM}--- events.lock.json (locked)${RESET}`);
    logger.raw(`${DIM}+++ current schema${RESET}`);
    logger.raw("");

    // Group changes by aggregate
    const changesByAggregate = groupChangesByAggregate([
      ...diff.breaking,
      ...diff.nonBreaking,
    ]);

    for (const [aggregate, changes] of changesByAggregate) {
      logger.raw(`${CYAN}@@ ${aggregate} @@${RESET}`);

      for (const change of changes) {
        printDiffLine(change, diff.breaking.includes(change));
      }

      logger.raw("");
    }

    // Summary
    const breakingCount = diff.breaking.length;
    const nonBreakingCount = diff.nonBreaking.length;

    if (breakingCount > 0) {
      logger.raw(
        `${RED}${breakingCount} breaking change(s)${RESET}, ${GREEN}${nonBreakingCount} addition(s)${RESET}`
      );
    } else {
      logger.raw(`${GREEN}${nonBreakingCount} addition(s)${RESET}`);
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
type SchemaLockFile = import("../../../../lib/compiler/src/index").SchemaLockFile;

function groupChangesByAggregate(
  changes: SchemaChange[]
): Map<string, SchemaChange[]> {
  const grouped = new Map<string, SchemaChange[]>();

  for (const change of changes) {
    const aggregate = change.aggregate;
    if (!grouped.has(aggregate)) {
      grouped.set(aggregate, []);
    }
    grouped.get(aggregate)!.push(change);
  }

  return grouped;
}

function printDiffLine(change: SchemaChange, isBreaking: boolean): void {
  const color = isBreaking ? RED : GREEN;
  const prefix = isBreaking ? "-" : "+";

  switch (change.kind) {
    case "eventAdded":
      logger.raw(`${color}${prefix} event ${change.event}${RESET}`);
      break;
    case "eventRemoved":
      logger.raw(`${color}${prefix} event ${change.event}${RESET}`);
      break;
    case "fieldAdded": {
      const marker = change.isOptional ? "?" : "";
      const warning = !change.isOptional ? ` ${YELLOW}(required!)${RESET}` : "";
      logger.raw(
        `${color}${prefix}   ${change.field}${marker}: ...${RESET}${warning}`
      );
      break;
    }
    case "fieldRemoved":
      logger.raw(`${color}${prefix}   ${change.field}: ...${RESET}`);
      break;
    case "fieldTypeChanged":
      logger.raw(
        `${RED}-   ${change.field}: ${change.from}${RESET}`
      );
      logger.raw(
        `${GREEN}+   ${change.field}: ${change.to}${RESET}`
      );
      break;
  }
}

function printFullSchema(lock: SchemaLockFile, isNew: boolean): void {
  const color = isNew ? GREEN : "";
  const prefix = isNew ? "+ " : "  ";

  for (const [aggName, aggregate] of Object.entries(lock.aggregates)) {
    logger.raw(`${color}${prefix}aggregate ${aggName} {${RESET}`);

    for (const event of aggregate.events) {
      logger.raw(`${color}${prefix}  event ${event.name} {${RESET}`);

      for (const field of event.fields) {
        logger.raw(`${color}${prefix}    ${field.name}: ${field.type}${RESET}`);
      }

      logger.raw(`${color}${prefix}  }${RESET}`);
    }

    logger.raw(`${color}${prefix}}${RESET}`);
    logger.raw("");
  }
}
