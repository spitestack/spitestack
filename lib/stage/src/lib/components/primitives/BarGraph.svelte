<script lang="ts">
  /**
   * BarGraph - Horizontal progress/meter bar
   * 
   * The visual pulse of throughput. Latency crawling.
   * Watch it fill up like the crowd at a basement show.
   */

  type BarVariant = 'default' | 'gradient' | 'segmented';

  interface Props {
    value: number;
    max?: number;
    variant?: BarVariant;
    showValue?: boolean;
    label?: string;
    segments?: number;
    thresholds?: { warning: number; error: number };
  }

  let { 
    value,
    max = 100,
    variant = 'default',
    showValue = false,
    label,
    segments = 10,
    thresholds = { warning: 70, error: 90 }
  }: Props = $props();

  const percentage = $derived(Math.min(100, (value / max) * 100));
  
  const barColor = $derived(() => {
    if (percentage >= thresholds.error) return 'var(--color-led-red)';
    if (percentage >= thresholds.warning) return 'var(--color-led-yellow)';
    return 'var(--color-led-green)';
  });

  const activeSegments = $derived(Math.ceil((percentage / 100) * segments));
</script>

<div class="bar-graph">
  {#if label}
    <span class="label">{label}</span>
  {/if}
  
  <div class="track">
    {#if variant === 'segmented'}
      <div class="segments">
        {#each Array(segments) as _, i}
          {@const segmentPercent = ((i + 1) / segments) * 100}
          {@const isActive = i < activeSegments}
          {@const segmentColor = segmentPercent >= thresholds.error 
            ? 'var(--color-led-red)' 
            : segmentPercent >= thresholds.warning 
              ? 'var(--color-led-yellow)' 
              : 'var(--color-led-green)'}
          <div 
            class="segment" 
            class:segment--active={isActive}
            style="--segment-color: {segmentColor}"
          ></div>
        {/each}
      </div>
    {:else}
      <div 
        class="fill"
        class:fill--gradient={variant === 'gradient'}
        style="
          width: {percentage}%;
          --bar-color: {barColor()};
        "
      ></div>
    {/if}
  </div>

  {#if showValue}
    <span class="value">{Math.round(percentage)}%</span>
  {/if}
</div>

<style>
  .bar-graph {
    display: flex;
    align-items: center;
    gap: var(--space-sm);
    width: 100%;
  }

  .label {
    font-size: 10px;
    color: var(--color-text-muted);
    min-width: 40px;
    text-transform: uppercase;
  }

  .track {
    flex: 1;
    height: 8px;
    background: var(--color-vu-background);
    border-radius: 1px;
    overflow: hidden;
    box-shadow: inset 0 1px 2px rgba(0, 0, 0, 0.5);
  }

  .fill {
    height: 100%;
    background: var(--bar-color);
    transition: width var(--transition-normal), background var(--transition-normal);
    box-shadow: 0 0 4px var(--bar-color);
  }

  .fill--gradient {
    background: linear-gradient(
      90deg,
      var(--color-led-green) 0%,
      var(--color-led-yellow) 70%,
      var(--color-led-red) 100%
    );
  }

  .segments {
    display: flex;
    gap: 2px;
    height: 100%;
    padding: 1px;
  }

  .segment {
    flex: 1;
    background: rgba(255, 255, 255, 0.05);
    border-radius: 1px;
    transition: var(--transition-fast);
  }

  .segment--active {
    background: var(--segment-color);
    box-shadow: 0 0 4px var(--segment-color);
  }

  .value {
    font-size: 10px;
    font-variant-numeric: tabular-nums;
    color: var(--color-text-muted);
    min-width: 32px;
    text-align: right;
  }
</style>
