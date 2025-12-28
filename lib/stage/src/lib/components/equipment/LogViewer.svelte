<script lang="ts">
  /**
   * LogViewer - Scrolling log feed
   * 
   * Real-time logs scrolling like a tape counter.
   * Color-coded by severity. The pulse of what's happening.
   */

  import { telemetry } from '$lib/stores/telemetry';
  import { LED } from '$components/primitives';
  import type { LogEntry } from '$lib/types/telemetry';

  interface Props {
    maxVisible?: number;
    autoScroll?: boolean;
  }

  let { maxVisible = 100, autoScroll = true }: Props = $props();

  let logs = $state<LogEntry[]>([]);
  let containerEl: HTMLDivElement;
  let shouldAutoScroll = $state(autoScroll);

  // Subscribe to logs from telemetry store
  $effect(() => {
    const unsub = telemetry.subscribe(state => {
      logs = state.logs.slice(0, maxVisible);

      // Auto-scroll to top when new logs arrive
      if (shouldAutoScroll && containerEl) {
        containerEl.scrollTop = 0;
      }
    });
    return unsub;
  });

  function formatTime(ts: number): string {
    const d = new Date(ts);
    return d.toLocaleTimeString('en-US', { hour12: false });
  }

  function getSeverityColor(severity: string): string {
    switch (severity) {
      case 'debug': return 'var(--color-ash)';
      case 'info': return 'var(--color-bone)';
      case 'warn': return 'var(--color-led-yellow)';
      case 'error': return 'var(--color-led-red)';
      default: return 'var(--color-text)';
    }
  }

  function getSeverityLabel(severity: string): string {
    return severity.toUpperCase().slice(0, 3);
  }
</script>

<div class="log-viewer">
  <div class="log-header">
    <span class="log-title">LIVE FEED</span>
    <button 
      class="auto-scroll-toggle"
      class:active={shouldAutoScroll}
      onclick={() => { shouldAutoScroll = !shouldAutoScroll; }}
    >
      <LED color="green" on={shouldAutoScroll} size="sm" />
      <span>AUTO</span>
    </button>
  </div>

  <div class="log-entries" bind:this={containerEl}>
    {#each logs as log (log.id)}
      <div class="log-entry" style="--severity-color: {getSeverityColor(log.severity)}">
        <span class="log-time">{formatTime(log.timestamp)}</span>
        <span class="log-severity">[{getSeverityLabel(log.severity)}]</span>
        {#if log.service}
          <span class="log-service">{log.service}</span>
        {/if}
        <span class="log-message">{log.message}</span>
      </div>
    {/each}

    {#if logs.length === 0}
      <div class="log-empty">Waiting for events...</div>
    {/if}
  </div>
</div>

<style>
  .log-viewer {
    display: flex;
    flex-direction: column;
    height: 100%;
    background: var(--color-void);
    border: 1px solid var(--color-charcoal);
    border-radius: 4px;
    overflow: hidden;
  }

  .log-header {
    display: flex;
    align-items: center;
    justify-content: space-between;
    padding: var(--space-xs) var(--space-sm);
    background: var(--color-charcoal);
    border-bottom: 1px solid #333;
  }

  .log-title {
    font-size: 9px;
    font-weight: 600;
    letter-spacing: 1px;
    color: var(--color-text-muted);
  }

  .auto-scroll-toggle {
    display: flex;
    align-items: center;
    gap: 4px;
    padding: 2px 6px;
    font-size: 8px;
    color: var(--color-text-muted);
    background: var(--color-void);
    border: 1px solid var(--color-charcoal);
    border-radius: 2px;
    cursor: pointer;
    transition: var(--transition-fast);
  }

  .auto-scroll-toggle:hover,
  .auto-scroll-toggle.active {
    border-color: var(--color-ember);
  }

  .log-entries {
    flex: 1;
    overflow-y: auto;
    padding: var(--space-xs);
    font-family: var(--font-mono);
    font-size: 10px;
  }

  .log-entry {
    display: flex;
    gap: var(--space-xs);
    padding: 2px 4px;
    border-radius: 2px;
    transition: var(--transition-fast);
  }

  .log-entry:hover {
    background: rgba(255, 255, 255, 0.03);
  }

  .log-time {
    color: var(--color-ash);
    flex-shrink: 0;
  }

  .log-severity {
    color: var(--severity-color);
    font-weight: 600;
    flex-shrink: 0;
    min-width: 36px;
  }

  .log-service {
    color: var(--color-ember);
    flex-shrink: 0;
  }

  .log-service::after {
    content: ':';
    color: var(--color-ash);
  }

  .log-message {
    color: var(--color-text);
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }

  .log-empty {
    display: flex;
    align-items: center;
    justify-content: center;
    height: 100%;
    color: var(--color-ash);
    font-style: italic;
  }
</style>
