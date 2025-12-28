<script lang="ts">
  /**
   * SystemOverview - Center panel system dashboard
   * 
   * The big picture. Large VU meters, health summary, totals.
   * The heartbeat of your event-sourced system.
   */

  import { VuMeter } from '$components/equipment';
  import { LED, Badge, SegmentDisplay, BarGraph } from '$components/primitives';
  import { telemetry, healthSummary, totalEventsPerSec } from '$lib/stores/telemetry';

  // Get store values
  const metrics = $derived($telemetry.eventsPerSec);
  const total = $derived($telemetry.totalEvents);
  const connected = $derived($telemetry.connected);
  const health = $derived($healthSummary);

  // Calculate VU meter values (log scale, max 1000/s = 100%)
  const readVu = $derived(Math.min(100, (Math.log10(metrics.read + 1) / 3) * 100));
  const writeVu = $derived(Math.min(100, (Math.log10(metrics.write + 1) / 3) * 100));

  // Format large numbers
  function formatNumber(n: number): string {
    if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(1)}M`;
    if (n >= 1_000) return `${(n / 1_000).toFixed(1)}K`;
    return n.toString();
  }
</script>

<div class="system-overview">
  <!-- Connection status -->
  <div class="status-row">
    <div class="connection">
      <LED color={connected ? 'green' : 'red'} on={true} size="lg" />
      <span class="connection-label">{connected ? 'CONNECTED' : 'DISCONNECTED'}</span>
    </div>
  </div>

  <!-- Main meters -->
  <div class="meters-section">
    <div class="meter-pair">
      <div class="meter-column">
        <div class="meter-label">READ</div>
        <div class="big-meter">
          <VuMeter value={readVu} segments={16} />
        </div>
        <div class="meter-value">{metrics.read}/s</div>
      </div>

      <div class="meter-divider"></div>

      <div class="meter-column">
        <div class="meter-label">WRITE</div>
        <div class="big-meter">
          <VuMeter value={writeVu} segments={16} />
        </div>
        <div class="meter-value">{metrics.write}/s</div>
      </div>
    </div>
  </div>

  <!-- Health summary -->
  <div class="health-section">
    <div class="health-title">PROJECTION HEALTH</div>
    <div class="health-badges">
      <div class="health-badge health-badge--healthy">
        <LED color="green" on={health.healthy > 0} size="sm" />
        <span class="health-count">{health.healthy}</span>
        <span class="health-label">HEALTHY</span>
      </div>
      <div class="health-badge health-badge--warning">
        <LED color="yellow" on={health.warning > 0} size="sm" blinking={health.warning > 0} />
        <span class="health-count">{health.warning}</span>
        <span class="health-label">WARNING</span>
      </div>
      <div class="health-badge health-badge--error">
        <LED color="red" on={health.error > 0} size="sm" blinking={health.error > 0} />
        <span class="health-count">{health.error}</span>
        <span class="health-label">ERROR</span>
      </div>
    </div>
  </div>

  <!-- Total counter -->
  <div class="total-section">
    <div class="total-label">TOTAL EVENTS</div>
    <div class="total-display">
      <SegmentDisplay value={formatNumber(total)} digits={8} color="amber" />
    </div>
  </div>
</div>

<style>
  .system-overview {
    display: flex;
    flex-direction: column;
    align-items: center;
    gap: var(--space-lg);
    padding: var(--space-md);
    height: 100%;
  }

  .status-row {
    width: 100%;
    display: flex;
    justify-content: center;
  }

  .connection {
    display: flex;
    align-items: center;
    gap: var(--space-sm);
    padding: var(--space-xs) var(--space-md);
    background: var(--color-void);
    border: 1px solid var(--color-charcoal);
    border-radius: 4px;
  }

  .connection-label {
    font-size: 10px;
    font-weight: 600;
    letter-spacing: 1px;
    color: var(--color-text);
  }

  .meters-section {
    flex: 1;
    display: flex;
    align-items: center;
    justify-content: center;
  }

  .meter-pair {
    display: flex;
    gap: var(--space-xl);
    padding: var(--space-lg);
    background: var(--color-void);
    border: 2px solid var(--color-charcoal);
    border-radius: 8px;
    box-shadow: var(--shadow-equipment);
  }

  .meter-column {
    display: flex;
    flex-direction: column;
    align-items: center;
    gap: var(--space-sm);
  }

  .meter-label {
    font-size: 11px;
    font-weight: 600;
    letter-spacing: 2px;
    color: var(--color-text-muted);
  }

  .big-meter {
    --vu-width: 32px;
    --vu-height: 120px;
  }

  .big-meter :global(.segments) {
    padding: 6px;
  }

  .big-meter :global(.segment) {
    height: 6px;
  }

  .meter-divider {
    width: 1px;
    background: var(--color-charcoal);
    margin: var(--space-lg) 0;
  }

  .meter-value {
    font-size: 14px;
    font-weight: 600;
    font-variant-numeric: tabular-nums;
    color: var(--color-text);
  }

  .health-section {
    width: 100%;
    max-width: 400px;
  }

  .health-title {
    font-size: 10px;
    font-weight: 600;
    letter-spacing: 1px;
    color: var(--color-text-muted);
    text-align: center;
    margin-bottom: var(--space-sm);
  }

  .health-badges {
    display: flex;
    justify-content: center;
    gap: var(--space-md);
  }

  .health-badge {
    display: flex;
    flex-direction: column;
    align-items: center;
    gap: var(--space-xs);
    padding: var(--space-sm) var(--space-md);
    background: var(--color-void);
    border: 1px solid var(--color-charcoal);
    border-radius: 4px;
    min-width: 80px;
  }

  .health-count {
    font-size: 24px;
    font-weight: 700;
    font-variant-numeric: tabular-nums;
  }

  .health-badge--healthy .health-count { color: var(--color-led-green); }
  .health-badge--warning .health-count { color: var(--color-led-yellow); }
  .health-badge--error .health-count { color: var(--color-led-red); }

  .health-label {
    font-size: 9px;
    letter-spacing: 0.5px;
    color: var(--color-text-muted);
  }

  .total-section {
    text-align: center;
  }

  .total-label {
    font-size: 10px;
    font-weight: 600;
    letter-spacing: 1px;
    color: var(--color-text-muted);
    margin-bottom: var(--space-xs);
  }

  .total-display {
    display: inline-block;
  }
</style>
