import type { ApiLock, ApiCommandSchema, ApiParameterSchema } from "./lock";

// ============================================================================
// Diff Types
// ============================================================================

export type ApiChangeType = "additive" | "breaking";

export interface ApiParameterChange {
  kind: "added" | "removed" | "modified";
  parameterName: string;
  changeType: ApiChangeType;
  oldType?: string;
  newType?: string;
  wasOptional?: boolean;
  isOptional?: boolean;
  message: string;
}

export interface ApiCommandChange {
  kind: "added" | "removed" | "modified";
  commandName: string;
  changeType: ApiChangeType;
  parameterChanges: ApiParameterChange[];
  message: string;
}

export interface ApiDiff {
  hasChanges: boolean;
  hasBreakingChanges: boolean;
  commandChanges: ApiCommandChange[];
}

// ============================================================================
// Diff Logic
// ============================================================================

/**
 * Compare two parameter schemas
 */
function compareParameters(
  paramName: string,
  oldParam: ApiParameterSchema | undefined,
  newParam: ApiParameterSchema | undefined
): ApiParameterChange | null {
  // Parameter added
  if (!oldParam && newParam) {
    // New required parameter is breaking, new optional parameter is additive
    const isBreaking = !newParam.optional;
    return {
      kind: "added",
      parameterName: paramName,
      changeType: isBreaking ? "breaking" : "additive",
      newType: newParam.type,
      isOptional: newParam.optional,
      message: isBreaking
        ? `New required parameter '${paramName}' added - breaks existing API callers`
        : `New optional parameter '${paramName}' added`,
    };
  }

  // Parameter removed
  if (oldParam && !newParam) {
    return {
      kind: "removed",
      parameterName: paramName,
      changeType: "breaking",
      oldType: oldParam.type,
      wasOptional: oldParam.optional,
      message: `Parameter '${paramName}' removed - breaks existing API callers`,
    };
  }

  // Parameter modified
  if (oldParam && newParam) {
    // Type changed
    if (oldParam.type !== newParam.type) {
      return {
        kind: "modified",
        parameterName: paramName,
        changeType: "breaking",
        oldType: oldParam.type,
        newType: newParam.type,
        wasOptional: oldParam.optional,
        isOptional: newParam.optional,
        message: `Parameter '${paramName}' type changed from '${oldParam.type}' to '${newParam.type}'`,
      };
    }

    // Optional -> Required is breaking
    if (oldParam.optional && !newParam.optional) {
      return {
        kind: "modified",
        parameterName: paramName,
        changeType: "breaking",
        oldType: oldParam.type,
        newType: newParam.type,
        wasOptional: true,
        isOptional: false,
        message: `Parameter '${paramName}' changed from optional to required`,
      };
    }

    // Required -> Optional is additive (doesn't break existing callers)
    if (!oldParam.optional && newParam.optional) {
      return {
        kind: "modified",
        parameterName: paramName,
        changeType: "additive",
        oldType: oldParam.type,
        newType: newParam.type,
        wasOptional: false,
        isOptional: true,
        message: `Parameter '${paramName}' changed from required to optional`,
      };
    }
  }

  return null;
}

/**
 * Compare two command schemas
 */
function compareCommands(
  commandName: string,
  oldCommand: ApiCommandSchema | undefined,
  newCommand: ApiCommandSchema | undefined
): ApiCommandChange | null {
  // Command added
  if (!oldCommand && newCommand) {
    return {
      kind: "added",
      commandName,
      changeType: "additive",
      parameterChanges: [],
      message: `New command '${commandName}' added`,
    };
  }

  // Command removed
  if (oldCommand && !newCommand) {
    return {
      kind: "removed",
      commandName,
      changeType: "breaking",
      parameterChanges: [],
      message: `Command '${commandName}' removed - breaks existing API callers`,
    };
  }

  // Command modified
  if (oldCommand && newCommand) {
    const parameterChanges: ApiParameterChange[] = [];

    // Create maps for easier lookup
    const oldParamMap = new Map(oldCommand.parameters.map((p) => [p.name, p]));
    const newParamMap = new Map(newCommand.parameters.map((p) => [p.name, p]));

    const allParamNames = new Set([...oldParamMap.keys(), ...newParamMap.keys()]);

    for (const paramName of allParamNames) {
      const change = compareParameters(
        paramName,
        oldParamMap.get(paramName),
        newParamMap.get(paramName)
      );
      if (change) {
        parameterChanges.push(change);
      }
    }

    if (parameterChanges.length > 0) {
      const hasBreaking = parameterChanges.some((c) => c.changeType === "breaking");
      return {
        kind: "modified",
        commandName,
        changeType: hasBreaking ? "breaking" : "additive",
        parameterChanges,
        message: hasBreaking
          ? `Command '${commandName}' has breaking changes`
          : `Command '${commandName}' has additive changes`,
      };
    }
  }

  return null;
}

/**
 * Compare two API locks and return the differences
 */
export function compareApiLocks(oldLock: ApiLock, newLock: ApiLock): ApiDiff {
  const commandChanges: ApiCommandChange[] = [];
  const allCommands = new Set([
    ...Object.keys(oldLock.commands),
    ...Object.keys(newLock.commands),
  ]);

  for (const commandName of allCommands) {
    const change = compareCommands(
      commandName,
      oldLock.commands[commandName],
      newLock.commands[commandName]
    );
    if (change) {
      commandChanges.push(change);
    }
  }

  return {
    hasChanges: commandChanges.length > 0,
    hasBreakingChanges: commandChanges.some((c) => c.changeType === "breaking"),
    commandChanges,
  };
}

/**
 * Get all breaking changes from a diff
 */
export function getApiBreakingChanges(diff: ApiDiff): {
  command: string;
  changes: ApiParameterChange[];
}[] {
  const result: { command: string; changes: ApiParameterChange[] }[] = [];

  for (const cmdChange of diff.commandChanges) {
    if (cmdChange.changeType !== "breaking") continue;

    const breakingParams = cmdChange.parameterChanges.filter(
      (p) => p.changeType === "breaking"
    );

    if (breakingParams.length > 0 || cmdChange.kind === "removed") {
      result.push({
        command: cmdChange.commandName,
        changes: breakingParams,
      });
    }
  }

  return result;
}

/**
 * Format diff for display
 */
export function formatApiDiff(diff: ApiDiff): string {
  if (!diff.hasChanges) {
    return "No API changes detected.";
  }

  const lines: string[] = [];

  if (diff.hasBreakingChanges) {
    lines.push("BREAKING API CHANGES DETECTED:");
    lines.push("");
  }

  for (const cmdChange of diff.commandChanges) {
    const icon = cmdChange.changeType === "breaking" ? "!" : "+";
    lines.push(`[${icon}] ${cmdChange.message}`);

    for (const paramChange of cmdChange.parameterChanges) {
      const paramIcon = paramChange.changeType === "breaking" ? "!" : "+";
      lines.push(`    [${paramIcon}] ${paramChange.message}`);
    }
  }

  return lines.join("\n");
}
