import { describe, test, expect, beforeEach } from 'bun:test';
import { SimulatedClock } from '../../../src/testing/simulated-clock';

describe('SimulatedClock', () => {
  let clock: SimulatedClock;

  beforeEach(() => {
    clock = new SimulatedClock();
  });

  describe('now()', () => {
    test('should start at 0', () => {
      expect(clock.now()).toBe(0);
    });

    test('should return current simulated time after tick', () => {
      clock.tick(1000);
      expect(clock.now()).toBe(1000);
    });
  });

  describe('sleep()', () => {
    test('should resolve when time advances', async () => {
      let resolved = false;
      const promise = clock.sleep(100).then(() => {
        resolved = true;
      });

      expect(resolved).toBe(false);
      clock.tick(100);
      await promise;
      expect(resolved).toBe(true);
    });

    test('should resolve immediately for 0 or negative duration', async () => {
      await clock.sleep(0);
      await clock.sleep(-10);
    });

    test('should resolve multiple sleeps in order', async () => {
      const order: number[] = [];

      clock.sleep(200).then(() => order.push(2));
      clock.sleep(100).then(() => order.push(1));
      clock.sleep(300).then(() => order.push(3));

      clock.tick(300);

      // Allow promises to resolve
      await Promise.resolve();
      await Promise.resolve();

      expect(order).toEqual([1, 2, 3]);
    });
  });

  describe('setTimeout()', () => {
    test('should execute callback after delay', () => {
      let called = false;
      clock.setTimeout(() => {
        called = true;
      }, 100);

      expect(called).toBe(false);
      clock.tick(100);
      expect(called).toBe(true);
    });

    test('should execute multiple callbacks in order', () => {
      const order: number[] = [];

      clock.setTimeout(() => order.push(2), 200);
      clock.setTimeout(() => order.push(1), 100);
      clock.setTimeout(() => order.push(3), 300);

      clock.tick(300);

      expect(order).toEqual([1, 2, 3]);
    });

    test('should allow cancellation', () => {
      let called = false;
      const timer = clock.setTimeout(() => {
        called = true;
      }, 100);

      timer.cancel();
      clock.tick(100);

      expect(called).toBe(false);
    });
  });

  describe('setInterval()', () => {
    test('should execute callback repeatedly', () => {
      let count = 0;
      clock.setInterval(() => {
        count++;
      }, 100);

      clock.tick(350);

      expect(count).toBe(3);
    });

    test('should allow cancellation', () => {
      let count = 0;
      const timer = clock.setInterval(() => {
        count++;
      }, 100);

      clock.tick(250);
      timer.cancel();
      clock.tick(200);

      expect(count).toBe(2);
    });

    test('should stop recurring after cancel', () => {
      let count = 0;
      const timer = clock.setInterval(() => {
        count++;
        if (count === 2) {
          timer.cancel();
        }
      }, 100);

      clock.tick(500);

      expect(count).toBe(2);
    });
  });

  describe('tick()', () => {
    test('should advance time by specified amount', () => {
      clock.tick(500);
      expect(clock.now()).toBe(500);
    });

    test('should throw for negative time', () => {
      expect(() => clock.tick(-100)).toThrow('Cannot tick negative time');
    });

    test('should execute timers at correct times', () => {
      const times: number[] = [];

      clock.setTimeout(() => times.push(clock.now()), 100);
      clock.setTimeout(() => times.push(clock.now()), 200);
      clock.setTimeout(() => times.push(clock.now()), 300);

      clock.tick(300);

      expect(times).toEqual([100, 200, 300]);
    });
  });

  describe('advanceTo()', () => {
    test('should advance to specific time', () => {
      clock.advanceTo(1000);
      expect(clock.now()).toBe(1000);
    });

    test('should throw when going back in time', () => {
      clock.tick(1000);
      expect(() => clock.advanceTo(500)).toThrow('Cannot go back in time');
    });
  });

  describe('runAllTimers()', () => {
    test('should run all pending timers', () => {
      const order: number[] = [];

      clock.setTimeout(() => order.push(1), 100);
      clock.setTimeout(() => order.push(2), 200);
      clock.setTimeout(() => order.push(3), 300);

      clock.runAllTimers();

      expect(order).toEqual([1, 2, 3]);
      expect(clock.now()).toBe(300);
    });

    test('should handle nested timers', () => {
      const order: number[] = [];

      clock.setTimeout(() => {
        order.push(1);
        clock.setTimeout(() => order.push(2), 100);
      }, 100);

      clock.runAllTimers();

      expect(order).toEqual([1, 2]);
    });

    test('should throw on infinite interval loop', () => {
      // Create interval that never gets cancelled
      clock.setInterval(() => {}, 1);

      expect(() => clock.runAllTimers()).toThrow('exceeded maximum iterations');
    });
  });

  describe('runPendingTimers()', () => {
    test('should run only pending non-interval timers', () => {
      let timeoutCalled = false;
      let intervalCount = 0;

      clock.setTimeout(() => {
        timeoutCalled = true;
      }, 100);
      clock.setInterval(() => {
        intervalCount++;
      }, 50);

      clock.runPendingTimers();

      expect(timeoutCalled).toBe(true);
      expect(intervalCount).toBe(2); // 50ms and 100ms
    });
  });

  describe('inspection methods', () => {
    test('getCurrentTime() should return current time', () => {
      clock.tick(500);
      expect(clock.getCurrentTime()).toBe(500);
    });

    test('getPendingTimerCount() should count active timers', () => {
      clock.setTimeout(() => {}, 100);
      clock.setTimeout(() => {}, 200);
      const timer = clock.setTimeout(() => {}, 300);
      timer.cancel();

      expect(clock.getPendingTimerCount()).toBe(2);
    });

    test('getPendingSleepCount() should count pending sleeps', () => {
      clock.sleep(100);
      clock.sleep(200);
      clock.sleep(300);

      expect(clock.getPendingSleepCount()).toBe(3);
    });
  });

  describe('reset()', () => {
    test('should reset clock to initial state', () => {
      clock.tick(1000);
      clock.setTimeout(() => {}, 100);
      clock.sleep(100);

      clock.reset();

      expect(clock.now()).toBe(0);
      expect(clock.getPendingTimerCount()).toBe(0);
      expect(clock.getPendingSleepCount()).toBe(0);
    });
  });

  describe('setTime()', () => {
    test('should set time without executing timers', () => {
      let called = false;
      clock.setTimeout(() => {
        called = true;
      }, 50);

      clock.setTime(100);

      expect(clock.now()).toBe(100);
      expect(called).toBe(false);
    });

    test('should throw for negative time', () => {
      expect(() => clock.setTime(-100)).toThrow('Time cannot be negative');
    });
  });
});
