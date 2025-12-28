/**
 * Test setup utilities for DST testing.
 *
 * @module test/setup
 */

// Seeded random
export {
  SeededRandom,
  getSeedFromEnv,
  withSeedReporting,
} from './seeded-random';

// Fault injection
export {
  FaultScheduler,
  createFaultScheduler,
  type IntermittentConfig,
} from './fault-scheduler';

// Test helpers
export {
  createTestEnvironment,
  createDSTContext,
  createTestEventStore,
  createTestEventStoreConfig,
  createTestCoordinator,
  runWithSeedReporting,
  advanceTime,
  advanceTimeAsync,
  simulateCrash,
  cleanupTestEnvironment,
  getFuzzIterations,
  generateTestDir,
  type TestEnvironment,
  type DSTContext,
} from './test-helpers';

// Mock projections
export {
  MockAggregatorProjection,
  MockViewProjection,
  TimingRecordingProjection,
  PausableProjection,
  createMockRegistration,
  createMockAggregatorRegistration,
  createMockViewRegistration,
} from './mock-projection';

// Test fixtures
export {
  EVENT_TYPES,
  type EventType,
  generateRandomEvent,
  generateRandomEvents,
  generateEventsAcrossStreams,
  generateRandomWorkload,
  createStoredEvent,
  generateStoredEvents,
  ProjectionTestScenarios,
  type WorkloadOperation,
} from './test-fixtures';

// DST scenarios
export {
  DSTScenarioRunner,
  CrashSimulatedError,
  runCrashScenario,
  runIOFailureScenario,
  checkInvariants,
  InvariantViolationError,
  EventStoreInvariants,
  ProjectionInvariants,
  type CrashScenario,
  type IOFailureScenario,
  type Invariant,
} from './dst-scenarios';
