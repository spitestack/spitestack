const MAX_TENANT_ID_LENGTH = 128;
const TENANT_ID_PATTERN = /^[a-zA-Z0-9_\-]+$/;

/**
 * Value object representing a tenant identifier for multi-tenant isolation.
 *
 * TenantIds are used to logically isolate streams and events between
 * different tenants in a multi-tenant application.
 *
 * @example
 * ```ts
 * const tenant = TenantId.from('acme-corp');
 * await store.append(streamId, events, { tenant });
 * ```
 */
export class TenantId {
  private constructor(private readonly value: string) {}

  /**
   * Create a TenantId from a string value.
   * @throws {Error} if the value is invalid
   */
  static from(value: string): TenantId {
    if (!value) {
      throw new Error('TenantId cannot be empty');
    }
    if (value.length > MAX_TENANT_ID_LENGTH) {
      throw new Error(`TenantId cannot exceed ${MAX_TENANT_ID_LENGTH} characters`);
    }
    if (!TENANT_ID_PATTERN.test(value)) {
      throw new Error(
        'TenantId can only contain alphanumeric characters, underscores, and hyphens',
      );
    }
    return new TenantId(value);
  }

  /**
   * The default tenant used when no tenant is specified
   */
  static readonly DEFAULT = new TenantId('default');

  /**
   * Get the string representation of this TenantId
   */
  toString(): string {
    return this.value;
  }

  /**
   * Check equality with another TenantId
   */
  equals(other: TenantId): boolean {
    return this.value === other.value;
  }

  /**
   * Check if this is the default tenant
   */
  isDefault(): boolean {
    return this.value === 'default';
  }
}
