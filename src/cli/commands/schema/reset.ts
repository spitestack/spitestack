/**
 * spite schema reset - Reset the schema lock file.
 *
 * This is a dangerous operation that should only be used when you know what you're doing.
 */

import { resolve, relative, join } from "path";
import { rm } from "fs/promises";
import { existsSync } from "fs";
import { logger } from "../../utils/logger";

export async function resetCommand(domain: string, confirmed: boolean): Promise<void> {
  const domainDir = resolve(process.cwd(), domain);
  const lockPath = join(domainDir, "events.lock.json");

  logger.header("spite schema reset");
  logger.info(`Domain: ${relative(process.cwd(), domainDir) || "."}`);
  logger.raw("");

  if (!existsSync(lockPath)) {
    logger.warn("No events.lock.json found - nothing to reset");
    return;
  }

  if (!confirmed) {
    logger.error("This is a dangerous operation!");
    logger.raw("");
    logger.raw("Resetting the schema lock file will:");
    logger.raw("  - Delete your current events.lock.json");
    logger.raw("  - Remove all schema version history");
    logger.raw("  - Potentially break existing event consumers");
    logger.raw("  - Make it impossible to detect breaking changes");
    logger.raw("");
    logger.info("If you're sure, run with --i-know-what-im-doing");
    process.exit(1);
  }

  try {
    await rm(lockPath);
    logger.success("Deleted events.lock.json");
    logger.raw("");
    logger.warn("Schema history has been reset.");
    logger.info("Run `spite schema sync` to create a new lock file.");
  } catch (error) {
    if (error instanceof Error) {
      logger.error(`Failed to delete lock file: ${error.message}`);
    } else {
      logger.error(String(error));
    }
    process.exit(1);
  }
}
