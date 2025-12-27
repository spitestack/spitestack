/**
 * Value object representing a unique identifier for a command.
 *
 * CommandIds are used for idempotency - if the same command is
 * submitted multiple times, only the first one will be processed.
 *
 * @example
 * ```ts
 * const commandId = CommandId.generate();
 * const fromValue = CommandId.from('my-unique-command-id');
 * ```
 */
export class CommandId {
  private constructor(private readonly value: string) {}

  /**
   * Create a CommandId from a string value.
   * @throws {Error} if the value is invalid
   */
  static from(value: string): CommandId {
    if (!value) {
      throw new Error('CommandId cannot be empty');
    }
    if (value.length > 256) {
      throw new Error('CommandId cannot exceed 256 characters');
    }
    return new CommandId(value);
  }

  /**
   * Generate a new unique CommandId using UUID v4
   */
  static generate(): CommandId {
    return new CommandId(crypto.randomUUID());
  }

  /**
   * Get the string representation of this CommandId
   */
  toString(): string {
    return this.value;
  }

  /**
   * Check equality with another CommandId
   */
  equals(other: CommandId): boolean {
    return this.value === other.value;
  }
}
