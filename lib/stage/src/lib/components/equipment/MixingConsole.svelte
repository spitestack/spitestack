<script lang="ts">
  /**
   * MixingConsole - The main mixing board
   * 
   * An array of channels. The command center.
   * Wood cheeks on the sides. Meter bridge up top.
   * This is where you feel the pulse of your system.
   */

  import ChannelStrip from './ChannelStrip.svelte';
  import { BarGraph } from '../primitives';

  import type { ProjectionStatus } from '$lib/types/telemetry';

  interface Props {
    projections: ProjectionStatus[];
    selectedId?: string;
    totalEventsPerSec?: number;
    avgLatencyMs?: number;
    onselect?: (id: string) => void;
  }

  let { 
    projections,
    selectedId,
    totalEventsPerSec = 0,
    avgLatencyMs = 0,
    onselect
  }: Props = $props();

  // Calculate throughput percentage (0-1000 events/s = 0-100%)
  const throughputPercent = $derived(Math.min(100, (totalEventsPerSec / 1000) * 100));
  
  // Calculate latency percentage (0-500ms = 0-100%)
  const latencyPercent = $derived(Math.min(100, (avgLatencyMs / 500) * 100));
</script>

<div class="mixing-console">
  <!-- Wood side panel (left) -->
  <div class="wood-cheek wood-cheek--left"></div>

  <div class="console-body">
    <!-- Meter bridge -->
    <div class="meter-bridge">
      <div class="meter-bridge-inner">
        <div class="bridge-meter">
          <BarGraph 
            value={throughputPercent} 
            variant="gradient"
            label="THR"
            showValue
          />
        </div>
        <div class="bridge-label">SYSTEM THROUGHPUT</div>
        <div class="bridge-meter">
          <BarGraph 
            value={latencyPercent}
            variant="gradient"
            label="LAT"
            showValue
            thresholds={{ warning: 50, error: 80 }}
          />
        </div>
      </div>
    </div>

    <!-- Channel strips -->
    <div class="channels">
      {#each projections as projection (projection.id)}
        <ChannelStrip
          id={projection.id}
          name={projection.name}
          health={projection.health}
          eventsPerSec={projection.eventsPerSec}
          latencyMs={projection.avgLatencyMs}
          selected={selectedId === projection.id}
          onclick={() => onselect?.(projection.id)}
        />
      {/each}

      <!-- Master section placeholder -->
      <div class="master-section">
        <div class="master-label">MST</div>
        <div class="master-meters">
          <div class="master-meter"></div>
          <div class="master-meter"></div>
        </div>
      </div>
    </div>
  </div>

  <!-- Wood side panel (right) -->
  <div class="wood-cheek wood-cheek--right"></div>
</div>

<style>
  .mixing-console {
    display: flex;
    background: linear-gradient(180deg, #2a2a2a 0%, #1a1a1a 100%);
    border: 2px solid var(--color-charcoal);
    border-radius: 4px;
    box-shadow: var(--shadow-equipment);
    overflow: hidden;
  }

  .wood-cheek {
    width: 24px;
    background: linear-gradient(
      90deg,
      #3d2314 0%,
      #5a3520 50%,
      #3d2314 100%
    );
    box-shadow: inset 0 0 10px rgba(0, 0, 0, 0.5);
  }

  .wood-cheek--left {
    border-right: 1px solid #2a1810;
  }

  .wood-cheek--right {
    border-left: 1px solid #2a1810;
  }

  .console-body {
    flex: 1;
    display: flex;
    flex-direction: column;
  }

  .meter-bridge {
    padding: var(--space-sm);
    background: linear-gradient(180deg, #1f1f1f 0%, #181818 100%);
    border-bottom: 1px solid #333;
  }

  .meter-bridge-inner {
    display: flex;
    align-items: center;
    gap: var(--space-lg);
    padding: var(--space-sm);
    background: var(--color-void);
    border: 1px solid #333;
    border-radius: 2px;
  }

  .bridge-meter {
    flex: 1;
    min-width: 120px;
  }

  .bridge-label {
    font-size: 10px;
    font-weight: 600;
    letter-spacing: 1px;
    color: var(--color-text-muted);
    white-space: nowrap;
  }

  .channels {
    display: flex;
    gap: 2px;
    padding: var(--space-sm);
    background: #1a1a1a;
    overflow-x: auto;
  }

  .master-section {
    display: flex;
    flex-direction: column;
    align-items: center;
    gap: var(--space-sm);
    padding: var(--space-sm);
    min-width: 80px;
    background: linear-gradient(180deg, #2a2520 0%, #1f1a18 100%);
    border: 1px solid #333;
    border-radius: 4px;
    margin-left: var(--space-sm);
  }

  .master-label {
    font-size: 10px;
    font-weight: 600;
    letter-spacing: 1px;
    color: var(--color-ember);
    padding: 2px 8px;
    background: rgba(204, 85, 0, 0.1);
    border: 1px solid rgba(204, 85, 0, 0.3);
    border-radius: 2px;
  }

  .master-meters {
    display: flex;
    gap: var(--space-xs);
    flex: 1;
  }

  .master-meter {
    width: 12px;
    flex: 1;
    background: var(--color-vu-background);
    border: 1px solid #333;
    border-radius: 2px;
  }
</style>
