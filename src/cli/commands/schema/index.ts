/**
 * spite schema - Schema evolution management subcommands.
 */

import { logger } from "../../utils/logger";
import type { SchemaAction } from "../../args";
import { statusCommand } from "./status";
import { syncCommand } from "./sync";
import { diffCommand } from "./diff";
import { resetCommand } from "./reset";

export async function schemaCommand(action: SchemaAction): Promise<void> {
  switch (action.type) {
    case "status":
      await statusCommand(action.domain);
      break;
    case "sync":
      await syncCommand(action.domain, action.force);
      break;
    case "diff":
      await diffCommand(action.domain);
      break;
    case "reset":
      await resetCommand(action.domain, action.confirmed);
      break;
    case "help":
      printSchemaHelp();
      break;
  }
}

function printSchemaHelp(): void {
  logger.header("spite schema");
  logger.raw(`
Manage event schema evolution for safe deployments.

Usage:
  spite schema <action> [options]

Actions:
  status          Show current schema status and pending changes
  sync            Generate or update events.lock.json
  diff            Show diff between current code and lock file
  reset           Reset lock file (dangerous!)

Options:
  -d, --domain    Domain source directory (default: src/domain)
  --force         Force sync even with breaking changes (sync only)
  --i-know-what-im-doing  Confirm reset (reset only)
  -h, --help      Show this help

Examples:
  spite schema status              Check for schema changes
  spite schema sync                Update lock file (safe changes only)
  spite schema sync --force        Update lock file (allow breaking changes)
  spite schema diff                Show what would change
  spite schema reset --i-know-what-im-doing   Reset lock file
`);
}
