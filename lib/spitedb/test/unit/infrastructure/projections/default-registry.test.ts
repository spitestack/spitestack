/**
 * Unit tests for DefaultProjectionRegistry.
 *
 * Tests projection registration and lookup with:
 * - Registration and duplicate detection
 * - Default option merging
 * - Query methods (get, has, count)
 * - Clear functionality
 */

import { describe, test, expect, beforeEach } from 'bun:test';
import { DefaultProjectionRegistry } from '../../../../src/infrastructure/projections/default-registry';
import type { ProjectionRegistration, ProjectionRuntimeOptions } from '../../../../src/ports/projections';
import type { StoredEvent } from '../../../../src/domain/events/stored-event';
import {
  MockAggregatorProjection,
  MockViewProjection,
  createMockAggregatorRegistration,
  createMockViewRegistration,
  createMockRegistration,
} from '../../../setup/mock-projection';

describe('DefaultProjectionRegistry', () => {
  let registry: DefaultProjectionRegistry;

  beforeEach(() => {
    registry = new DefaultProjectionRegistry();
  });

  describe('register', () => {
    test('should register a projection', () => {
      const registration = createMockAggregatorRegistration('TestProjection', ['*']);

      registry.register(registration);

      expect(registry.has('TestProjection')).toBe(true);
    });

    test('should throw on duplicate registration', () => {
      const registration = createMockAggregatorRegistration('DuplicateTest', ['*']);

      registry.register(registration);

      expect(() => registry.register(registration)).toThrow(
        "Projection 'DuplicateTest' is already registered"
      );
    });

    test('should register multiple projections', () => {
      registry.register(createMockAggregatorRegistration('Projection1', ['EventA']));
      registry.register(createMockAggregatorRegistration('Projection2', ['EventB']));
      registry.register(createMockViewRegistration('Projection3', ['EventC']));

      expect(registry.count()).toBe(3);
      expect(registry.has('Projection1')).toBe(true);
      expect(registry.has('Projection2')).toBe(true);
      expect(registry.has('Projection3')).toBe(true);
    });

    test('should use default options when not provided', () => {
      const registration = createMockAggregatorRegistration('DefaultOptions', ['*']);

      registry.register(registration);

      const resolved = registry.get('DefaultOptions');
      expect(resolved).toBeDefined();
      // Mock registration sets checkpointIntervalMs: 100 in metadata, which takes priority
      expect(resolved!.options.checkpointIntervalMs).toBe(100);
      expect(resolved!.options.memoryThresholdBytes).toBe(50 * 1024 * 1024);
      expect(resolved!.options.enabled).toBe(true);
    });

    test('should use metadata checkpoint interval when provided', () => {
      const registration = createMockRegistration<number>(
        'MetadataInterval',
        'aggregator',
        ['*'],
        () => new MockAggregatorProjection(['*']),
        []
      );
      // The mock registration has checkpointIntervalMs: 100

      registry.register(registration);

      const resolved = registry.get('MetadataInterval');
      expect(resolved!.options.checkpointIntervalMs).toBe(100);
    });

    test('should override options with provided values', () => {
      const registration = createMockAggregatorRegistration('OverrideOptions', ['*']);
      const options: ProjectionRuntimeOptions = {
        checkpointIntervalMs: 10000,
        memoryThresholdBytes: 100 * 1024 * 1024,
        enabled: false,
      };

      registry.register(registration, options);

      const resolved = registry.get('OverrideOptions');
      expect(resolved!.options.checkpointIntervalMs).toBe(10000);
      expect(resolved!.options.memoryThresholdBytes).toBe(100 * 1024 * 1024);
      expect(resolved!.options.enabled).toBe(false);
    });

    test('should preserve event filter option', () => {
      const registration = createMockAggregatorRegistration('FilteredProjection', ['*']);
      const eventFilter = (event: StoredEvent) => event.type.startsWith('Important');

      registry.register(registration, { eventFilter });

      const resolved = registry.get('FilteredProjection');
      expect(resolved!.options.eventFilter).toBeDefined();

      const importantEvent: StoredEvent = {
        streamId: 'test',
        type: 'ImportantEvent',
        data: {},
        revision: 0,
        globalPosition: 0,
        timestamp: 0,
        tenantId: 'default',
      };
      const otherEvent: StoredEvent = {
        streamId: 'test',
        type: 'OtherEvent',
        data: {},
        revision: 0,
        globalPosition: 0,
        timestamp: 0,
        tenantId: 'default',
      };

      expect(resolved!.options.eventFilter!(importantEvent)).toBe(true);
      expect(resolved!.options.eventFilter!(otherEvent)).toBe(false);
    });

    test('should register aggregator projections', () => {
      const registration = createMockAggregatorRegistration('AggregatorTest', ['OrderCompleted']);

      registry.register(registration);

      const resolved = registry.get('AggregatorTest');
      expect(resolved!.registration.metadata.kind).toBe('aggregator');
      expect(resolved!.registration.metadata.subscribedEvents).toEqual(['OrderCompleted']);
    });

    test('should register denormalized view projections', () => {
      const registration = createMockViewRegistration('ViewTest', ['UserCreated'], ['id']);

      registry.register(registration);

      const resolved = registry.get('ViewTest');
      expect(resolved!.registration.metadata.kind).toBe('denormalized_view');
    });
  });

  describe('get', () => {
    test('should return registered projection', () => {
      const registration = createMockAggregatorRegistration('GetTest', ['*']);
      registry.register(registration);

      const resolved = registry.get('GetTest');

      expect(resolved).toBeDefined();
      expect(resolved!.registration.metadata.name).toBe('GetTest');
    });

    test('should return undefined for non-existent projection', () => {
      const resolved = registry.get('NonExistent');
      expect(resolved).toBeUndefined();
    });
  });

  describe('has', () => {
    test('should return true for registered projection', () => {
      registry.register(createMockAggregatorRegistration('ExistsTest', ['*']));
      expect(registry.has('ExistsTest')).toBe(true);
    });

    test('should return false for non-existent projection', () => {
      expect(registry.has('NonExistent')).toBe(false);
    });
  });

  describe('count', () => {
    test('should return 0 for empty registry', () => {
      expect(registry.count()).toBe(0);
    });

    test('should return correct count', () => {
      registry.register(createMockAggregatorRegistration('P1', ['*']));
      expect(registry.count()).toBe(1);

      registry.register(createMockAggregatorRegistration('P2', ['*']));
      expect(registry.count()).toBe(2);

      registry.register(createMockAggregatorRegistration('P3', ['*']));
      expect(registry.count()).toBe(3);
    });
  });

  describe('getAll', () => {
    test('should return empty array for empty registry', () => {
      expect(registry.getAll()).toEqual([]);
    });

    test('should return all registered projections', () => {
      registry.register(createMockAggregatorRegistration('AllTest1', ['EventA']));
      registry.register(createMockAggregatorRegistration('AllTest2', ['EventB']));

      const all = registry.getAll();

      expect(all).toHaveLength(2);
      const names = all.map((r) => r.registration.metadata.name);
      expect(names).toContain('AllTest1');
      expect(names).toContain('AllTest2');
    });
  });

  describe('clear', () => {
    test('should remove all registrations', () => {
      registry.register(createMockAggregatorRegistration('ClearTest1', ['*']));
      registry.register(createMockAggregatorRegistration('ClearTest2', ['*']));
      registry.register(createMockAggregatorRegistration('ClearTest3', ['*']));

      expect(registry.count()).toBe(3);

      registry.clear();

      expect(registry.count()).toBe(0);
      expect(registry.has('ClearTest1')).toBe(false);
      expect(registry.has('ClearTest2')).toBe(false);
      expect(registry.has('ClearTest3')).toBe(false);
    });

    test('should allow re-registration after clear', () => {
      registry.register(createMockAggregatorRegistration('ReRegister', ['*']));
      registry.clear();
      registry.register(createMockAggregatorRegistration('ReRegister', ['*']));

      expect(registry.has('ReRegister')).toBe(true);
      expect(registry.count()).toBe(1);
    });
  });

  describe('factory and getInstance', () => {
    test('should create projection instance via factory', () => {
      const registration = createMockAggregatorRegistration('FactoryTest', ['*']);
      registry.register(registration);

      const resolved = registry.get('FactoryTest');
      const instance = resolved!.registration.factory();

      expect(instance).toBeDefined();
      expect(typeof instance.build).toBe('function');
      expect(typeof instance.getState).toBe('function');
      expect(typeof instance.setState).toBe('function');
    });

    test('should return same instance from getInstance after factory call', () => {
      const registration = createMockAggregatorRegistration('InstanceTest', ['*']);
      registry.register(registration);

      const resolved = registry.get('InstanceTest');
      expect(resolved!.registration.getInstance?.()).toBeNull();

      const instance = resolved!.registration.factory();
      expect(resolved!.registration.getInstance?.()).toBe(instance);
    });
  });

  describe('option priority', () => {
    test('should prioritize: options > metadata > defaults', () => {
      // Create a registration with metadata checkpoint interval
      const registration = createMockRegistration<number>(
        'PriorityTest',
        'aggregator',
        ['*'],
        () => new MockAggregatorProjection(['*']),
        []
      );
      // Mock registration sets checkpointIntervalMs to 100

      // Register without options - should use metadata (100)
      const registry1 = new DefaultProjectionRegistry();
      registry1.register(registration);
      expect(registry1.get('PriorityTest')!.options.checkpointIntervalMs).toBe(100);

      // Register with options - should override
      const registry2 = new DefaultProjectionRegistry();
      registry2.register(registration, { checkpointIntervalMs: 999 });
      expect(registry2.get('PriorityTest')!.options.checkpointIntervalMs).toBe(999);
    });
  });

  describe('edge cases', () => {
    test('should handle empty projection name', () => {
      const registration = createMockRegistration<number>(
        '',
        'aggregator',
        ['*'],
        () => new MockAggregatorProjection(['*']),
        []
      );

      registry.register(registration);

      expect(registry.has('')).toBe(true);
      expect(registry.get('')).toBeDefined();
    });

    test('should handle special characters in projection name', () => {
      const registration = createMockRegistration<number>(
        'Test/Projection:With*Special?Chars',
        'aggregator',
        ['*'],
        () => new MockAggregatorProjection(['*']),
        []
      );

      registry.register(registration);

      expect(registry.has('Test/Projection:With*Special?Chars')).toBe(true);
    });

    test('should handle unicode in projection name', () => {
      const registration = createMockRegistration<number>(
        '总收入投影',
        'aggregator',
        ['*'],
        () => new MockAggregatorProjection(['*']),
        []
      );

      registry.register(registration);

      expect(registry.has('总收入投影')).toBe(true);
      expect(registry.get('总收入投影')!.registration.metadata.name).toBe('总收入投影');
    });

    test('should handle many registrations', () => {
      for (let i = 0; i < 1000; i++) {
        registry.register(createMockAggregatorRegistration(`Projection${i}`, ['*']));
      }

      expect(registry.count()).toBe(1000);
      expect(registry.has('Projection0')).toBe(true);
      expect(registry.has('Projection999')).toBe(true);
    });
  });
});
