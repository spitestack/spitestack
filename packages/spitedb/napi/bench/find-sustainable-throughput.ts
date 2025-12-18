/**
 * SpiteDB Sustainable Throughput Finder
 *
 * Binary searches for the maximum concurrency that maintains:
 * - p99 latency below target (default 60ms)
 * - Timeout rate below threshold (default 1%)
 *
 * This helps you find the optimal operating point for your hardware.
 *
 * Usage: bun run bench/find-sustainable-throughput.ts [p99_target_ms] [max_timeout_rate]
 *
 * Examples:
 *   bun run bench/find-sustainable-throughput.ts 60 0.01    # p99 < 60ms, timeouts < 1%
 *   bun run bench/find-sustainable-throughput.ts 50 0.005   # Stricter: p99 < 50ms, timeouts < 0.5%
 */

import { runLoadTest, type LoadTestResult } from './append-load-test.js';

// Configuration
const P99_TARGET_MS = parseInt(process.argv[2] || '60', 10);
const MAX_TIMEOUT_RATE = parseFloat(process.argv[3] || '0.01'); // 1% default
const TEST_DURATION_SECONDS = 10; // Short tests for binary search
const EVENTS_PER_APPEND = 1;

// Search bounds
const MIN_CONCURRENCY = 100;
const MAX_CONCURRENCY = 5000;
const CONVERGENCE_THRESHOLD = 100; // Stop when range is this small

interface SearchResult {
  concurrency: number;
  result: LoadTestResult;
}

async function testConcurrency(concurrency: number): Promise<SearchResult> {
  console.log(`\n>>> Testing concurrency: ${concurrency}`);
  console.log('─'.repeat(60));

  const result = await runLoadTest(
    TEST_DURATION_SECONDS,
    concurrency,
    EVENTS_PER_APPEND,
    P99_TARGET_MS,
    false // quiet mode
  );

  const passed = result.p99TargetMet && result.timeoutRate <= MAX_TIMEOUT_RATE;

  console.log(`    Events/sec:    ${result.eventsPerSec.toFixed(0)}`);
  console.log(`    p99 latency:   ${result.latency.p99.toFixed(2)}ms ${result.p99TargetMet ? '✓' : '✗'} (target: ${P99_TARGET_MS}ms)`);
  console.log(`    Timeout rate:  ${(result.timeoutRate * 100).toFixed(2)}% ${result.timeoutRate <= MAX_TIMEOUT_RATE ? '✓' : '✗'} (max: ${(MAX_TIMEOUT_RATE * 100).toFixed(1)}%)`);
  console.log(`    Result:        ${passed ? '✓ PASS' : '✗ FAIL'}`);

  return { concurrency, result };
}

function meetsTargets(result: LoadTestResult): boolean {
  return result.p99TargetMet && result.timeoutRate <= MAX_TIMEOUT_RATE;
}

async function findSustainableThroughput(): Promise<void> {
  console.log('=== SpiteDB Sustainable Throughput Finder ===');
  console.log(`p99 target:       ${P99_TARGET_MS}ms`);
  console.log(`Max timeout rate: ${(MAX_TIMEOUT_RATE * 100).toFixed(1)}%`);
  console.log(`Test duration:    ${TEST_DURATION_SECONDS}s per iteration`);
  console.log(`Search range:     ${MIN_CONCURRENCY} - ${MAX_CONCURRENCY}`);
  console.log('');

  let low = MIN_CONCURRENCY;
  let high = MAX_CONCURRENCY;
  let bestResult: SearchResult | null = null;

  // Binary search for optimal concurrency
  let iteration = 0;
  while (high - low > CONVERGENCE_THRESHOLD) {
    iteration++;
    const mid = Math.floor((low + high) / 2);

    console.log(`\n[Iteration ${iteration}] Range: ${low} - ${high}, Testing: ${mid}`);

    const searchResult = await testConcurrency(mid);

    if (meetsTargets(searchResult.result)) {
      // Can handle this load, try higher
      low = mid;
      bestResult = searchResult;
    } else {
      // Too much load, try lower
      high = mid;
    }
  }

  // Final verification at the found concurrency
  if (bestResult) {
    console.log('\n' + '='.repeat(60));
    console.log('=== Final Verification ===');
    console.log('Running longer test to verify stability...');

    const finalResult = await testConcurrency(bestResult.concurrency);

    console.log('\n' + '='.repeat(60));
    console.log('=== Results ===');
    console.log('');

    if (meetsTargets(finalResult.result)) {
      console.log(`✓ Sustainable concurrency: ${finalResult.concurrency}`);
      console.log(`  Events/sec:              ${finalResult.result.eventsPerSec.toFixed(0)}`);
      console.log(`  MB/sec:                  ${finalResult.result.mbPerSec.toFixed(2)}`);
      console.log(`  p99 latency:             ${finalResult.result.latency.p99.toFixed(2)}ms`);
      console.log(`  Timeout rate:            ${(finalResult.result.timeoutRate * 100).toFixed(2)}%`);
      console.log('');
      console.log('Recommendation:');
      console.log(`  Use concurrency ≤ ${finalResult.concurrency} for production`);
      console.log(`  Expected throughput: ~${finalResult.result.eventsPerSec.toFixed(0)} events/sec`);
    } else {
      console.log('⚠️  Final verification failed. Results may be unstable.');
      console.log(`   Best found: ${bestResult.concurrency} workers`);
      console.log('   Consider using a lower concurrency for safety margin.');
    }
  } else {
    console.log('\n' + '='.repeat(60));
    console.log('✗ Could not find sustainable concurrency');
    console.log(`  Even ${MIN_CONCURRENCY} workers exceeded targets.`);
    console.log('  Check your hardware or relax the targets.');
  }
}

// Run
findSustainableThroughput().catch(console.error);
