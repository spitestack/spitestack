/**
 * SpiteStack Diagnostic Codes
 *
 * Code format: SS[category][number]
 * - SS0xx: Convention errors (missing properties/methods)
 * - SS1xx: Event type errors
 * - SS2xx: Command errors
 * - SS3xx: Configuration errors
 * - SSWxx: Warnings
 */

export const DiagnosticCode = {
  // Convention errors (SS001-SS099)
  MISSING_INITIAL_STATE: "SS002",
  MISSING_EVENTS_ARRAY: "SS003",
  MISSING_EMIT_METHOD: "SS004",
  MISSING_STATE_GETTER: "SS005",
  MISSING_APPLY_METHOD: "SS006",
  DUPLICATE_AGGREGATE_NAME: "SS007",
  INVALID_INITIAL_STATE_TYPE: "SS008",
  NO_AGGREGATES_FOUND: "SS009",

  // Event type errors (SS100-SS199)
  EVENT_NOT_UNION: "SS100",
  EVENT_MISSING_TYPE_DISCRIMINANT: "SS101",
  EVENT_VARIANT_NOT_OBJECT: "SS102",
  EVENT_TYPE_NOT_FOUND: "SS103",

  // Command errors (SS200-SS299)
  COMMAND_PARAM_NOT_SERIALIZABLE: "SS200",
  COMMAND_PARAM_UNKNOWN_TYPE: "SS201",
  COMMAND_NO_PARAMS: "SS202",

  // State type errors (SS250-SS299)
  STATE_TYPE_NOT_FOUND: "SS250",
  STATE_TYPE_NOT_OBJECT: "SS251",

  // Configuration errors (SS300-SS399)
  CONFIG_NOT_FOUND: "SS300",
  CONFIG_INVALID: "SS301",
  DOMAIN_DIR_NOT_FOUND: "SS302",
  OUTPUT_DIR_CREATE_FAILED: "SS303",

  // Warnings (SSW01-SSW99)
  UNUSED_EVENT_VARIANT: "SSW01",
  NO_COMMANDS_FOUND: "SSW02",
  EMPTY_DOMAIN_DIR: "SSW03",
} as const;

export type DiagnosticCodeType = (typeof DiagnosticCode)[keyof typeof DiagnosticCode];

/**
 * Human-readable messages for each diagnostic code
 */
export const DiagnosticMessages: Record<DiagnosticCodeType, string> = {
  // Convention errors
  [DiagnosticCode.MISSING_INITIAL_STATE]:
    "Missing required static property 'initialState'",
  [DiagnosticCode.MISSING_EVENTS_ARRAY]:
    "Missing required instance property 'events: TEvent[]'",
  [DiagnosticCode.MISSING_EMIT_METHOD]:
    "Missing required method 'emit(event: TEvent): void'",
  [DiagnosticCode.MISSING_STATE_GETTER]:
    "Missing required getter 'get currentState(): TState'",
  [DiagnosticCode.MISSING_APPLY_METHOD]:
    "Missing required method 'apply(event: TEvent): void'",
  [DiagnosticCode.DUPLICATE_AGGREGATE_NAME]:
    "Duplicate aggregate name - folder names must be unique",
  [DiagnosticCode.INVALID_INITIAL_STATE_TYPE]:
    "Property 'initialState' must match the state type",
  [DiagnosticCode.NO_AGGREGATES_FOUND]:
    "No aggregate classes found in domain directory",

  // Event type errors
  [DiagnosticCode.EVENT_NOT_UNION]:
    "Event type must be a discriminated union",
  [DiagnosticCode.EVENT_MISSING_TYPE_DISCRIMINANT]:
    "Each event variant must have a 'type' property as discriminant",
  [DiagnosticCode.EVENT_VARIANT_NOT_OBJECT]:
    "Event variant must be an object type",
  [DiagnosticCode.EVENT_TYPE_NOT_FOUND]:
    "Could not find event type definition",

  // Command errors
  [DiagnosticCode.COMMAND_PARAM_NOT_SERIALIZABLE]:
    "Command parameter type is not serializable to JSON",
  [DiagnosticCode.COMMAND_PARAM_UNKNOWN_TYPE]:
    "Could not determine command parameter type",
  [DiagnosticCode.COMMAND_NO_PARAMS]:
    "Command has no parameters (excluding 'id' which is auto-added)",

  // State type errors
  [DiagnosticCode.STATE_TYPE_NOT_FOUND]:
    "Could not find state type definition",
  [DiagnosticCode.STATE_TYPE_NOT_OBJECT]:
    "State type must be an object type",

  // Configuration errors
  [DiagnosticCode.CONFIG_NOT_FOUND]:
    "No configuration file found (spitestack.config.ts)",
  [DiagnosticCode.CONFIG_INVALID]:
    "Invalid configuration file",
  [DiagnosticCode.DOMAIN_DIR_NOT_FOUND]:
    "Domain directory does not exist",
  [DiagnosticCode.OUTPUT_DIR_CREATE_FAILED]:
    "Failed to create output directory",

  // Warnings
  [DiagnosticCode.UNUSED_EVENT_VARIANT]:
    "Event variant is never emitted by any command",
  [DiagnosticCode.NO_COMMANDS_FOUND]:
    "No command methods found on aggregate",
  [DiagnosticCode.EMPTY_DOMAIN_DIR]:
    "Domain directory is empty",
};

/**
 * Suggestions for fixing each diagnostic
 */
export const DiagnosticSuggestions: Partial<Record<DiagnosticCodeType, string>> = {
  [DiagnosticCode.MISSING_INITIAL_STATE]: `Add a static 'initialState' property:

  static readonly initialState: YourStateType = { /* initial values */ };`,

  [DiagnosticCode.MISSING_EVENTS_ARRAY]: `Add an 'events' array property:

  readonly events: YourEventType[] = [];`,

  [DiagnosticCode.MISSING_EMIT_METHOD]: `Add an 'emit' method:

  protected emit(event: YourEventType): void {
    this.events.push(event);
    this.apply(event);
  }`,

  [DiagnosticCode.MISSING_STATE_GETTER]: `Add a 'currentState' getter:

  get currentState(): YourStateType {
    return this.state;
  }`,

  [DiagnosticCode.MISSING_APPLY_METHOD]: `Add an 'apply' method to mutate state from events:

  apply(event: YourEventType): void {
    switch (event.type) {
      case 'YourEventType':
        this.state.property = event.value;
        break;
      // handle other event types...
    }
  }`,

  [DiagnosticCode.EVENT_NOT_UNION]: `Define your event type as a discriminated union:

  export type YourEvent =
    | { type: 'EventA'; data: string }
    | { type: 'EventB'; count: number };`,

  [DiagnosticCode.EVENT_MISSING_TYPE_DISCRIMINANT]: `Each event variant must have a 'type' property:

  { type: 'YourEventType', /* other properties */ }`,

  [DiagnosticCode.NO_COMMANDS_FOUND]: `Add public methods to your aggregate that call this.emit():

  create(title: string): void {
    this.emit({ type: 'Created', title });
  }`,
};
