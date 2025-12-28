<script lang="ts">
  /**
   * StatusBar - Top navigation bar
   *
   * The command center header.
   * Spinning vinyl, aggregate metrics, panel hotkeys.
   */

  import { telemetry, healthSummary, totalEventsPerSec } from '$lib/stores/telemetry';
  import { LED } from '$components/primitives';

  interface Props {
    expandedPanelName?: string | null;
  }

  let { expandedPanelName = null }: Props = $props();

  let connected = $derived.by(() => {
    let value = false;
    telemetry.subscribe(state => { value = state.connected; })();
    return value;
  });

  let summary = $derived.by(() => {
    let value = { healthy: 0, warning: 0, error: 0, total: 0 };
    healthSummary.subscribe(state => { value = state; })();
    return value;
  });

  let eventsPerSec = $derived.by(() => {
    let value = 0;
    totalEventsPerSec.subscribe(state => { value = state; })();
    return value;
  });
</script>

<header class="status-bar">
  <!-- Brand section -->
  <div class="brand">
    <div class="vinyl" class:vinyl--spinning={connected}>
      <div class="vinyl-label"></div>
    </div>
    <span class="brand-name">SPITESTACK RECORDS</span>
    <LED color={connected ? 'green' : 'red'} on={true} size="sm" />
  </div>

  <!-- Metrics section -->
  <div class="metrics">
    {#if expandedPanelName}
      <div class="expanded-indicator">
        <span class="expanded-label">{expandedPanelName} EXPANDED</span>
        <span class="expanded-hint">[Esc] to close</span>
      </div>
    {:else}
      <div class="metric">
        <span class="metric-value">{eventsPerSec.toLocaleString()}</span>
        <span class="metric-unit">/s</span>
      </div>

      <span class="divider">|</span>

      <div class="metric">
        <span class="metric-value">{summary.total}</span>
        <span class="metric-label">proj</span>
      </div>

      {#if summary.error > 0}
        <span class="divider">|</span>
        <div class="metric metric--error">
          <span class="metric-value">{summary.error}</span>
          <span class="metric-label">err</span>
        </div>
      {/if}

      {#if summary.warning > 0}
        <span class="divider">|</span>
        <div class="metric metric--warning">
          <span class="metric-value">{summary.warning}</span>
          <span class="metric-label">warn</span>
        </div>
      {/if}
    {/if}
  </div>

  <!-- Panel hotkeys -->
  <div class="panel-nav">
    <span class="panel-key" class:panel-key--active={expandedPanelName === 'CONSOLE'}>[1]</span>
    <span class="panel-key" class:panel-key--active={expandedPanelName === 'RACKS'}>[2]</span>
    <span class="panel-key" class:panel-key--active={expandedPanelName === 'TIMELINE'}>[3]</span>
    <span class="panel-key" class:panel-key--active={expandedPanelName === 'LOGS'}>[4]</span>
  </div>
</header>

<style>
  .status-bar {
    display: flex;
    align-items: center;
    justify-content: space-between;
    padding: var(--space-sm) var(--space-md);
    background: var(--color-surface);
    border-bottom: 2px solid var(--color-charcoal);
    flex-shrink: 0;
    z-index: 100;
  }

  .brand {
    display: flex;
    align-items: center;
    gap: var(--space-sm);
  }

  .vinyl {
    position: relative;
    width: 20px;
    height: 20px;
    border-radius: 50%;
    background: radial-gradient(
      circle at center,
      var(--color-ember) 0%,
      var(--color-ember) 20%,
      #1a1a1a 20%,
      #1a1a1a 30%,
      #2a2a2a 30%,
      #1a1a1a 40%,
      #2a2a2a 40%,
      #1a1a1a 50%,
      #2a2a2a 50%,
      #1a1a1a 60%,
      #2a2a2a 60%,
      #1a1a1a 70%,
      #0a0a0a 100%
    );
    box-shadow: 0 0 8px rgba(204, 85, 0, 0.3);
  }

  .vinyl--spinning {
    animation: spin 2s linear infinite;
  }

  @keyframes spin {
    from { transform: rotate(0deg); }
    to { transform: rotate(360deg); }
  }

  .vinyl-label {
    position: absolute;
    top: 50%;
    left: 50%;
    transform: translate(-50%, -50%);
    width: 6px;
    height: 6px;
    border-radius: 50%;
    background: var(--color-ember);
  }

  .brand-name {
    font-weight: 600;
    letter-spacing: 1px;
    color: var(--color-bone);
    font-size: 12px;
  }

  .metrics {
    display: flex;
    align-items: center;
    gap: var(--space-sm);
  }

  .metric {
    display: flex;
    align-items: baseline;
    gap: 2px;
  }

  .metric-value {
    font-size: 13px;
    font-weight: 600;
    font-variant-numeric: tabular-nums;
    color: var(--color-text);
  }

  .metric-unit,
  .metric-label {
    font-size: 10px;
    color: var(--color-text-muted);
  }

  .metric--error .metric-value {
    color: var(--color-error);
  }

  .metric--warning .metric-value {
    color: var(--color-warning);
  }

  .divider {
    color: var(--color-charcoal);
    font-size: 12px;
  }

  .expanded-indicator {
    display: flex;
    align-items: center;
    gap: var(--space-md);
  }

  .expanded-label {
    font-size: 11px;
    font-weight: 600;
    letter-spacing: 1px;
    color: var(--color-ember);
  }

  .expanded-hint {
    font-size: 10px;
    color: var(--color-text-muted);
  }

  .panel-nav {
    display: flex;
    gap: var(--space-xs);
  }

  .panel-key {
    font-size: 11px;
    font-weight: 500;
    color: var(--color-text-muted);
    padding: 2px 4px;
    border-radius: 2px;
    transition: var(--transition-fast);
  }

  .panel-key--active {
    color: var(--color-ember);
    background: rgba(204, 85, 0, 0.15);
  }
</style>
