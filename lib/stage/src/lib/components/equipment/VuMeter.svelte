<script lang="ts">
  /**
   * VuMeter - Vertical LED bar meter
   * 
   * The soul of every mixing board. Watch those LEDs climb.
   * Green is good. Yellow is hot. Red is clipping.
   * Based on the TUI's VU meter widget with jitter and decay.
   */

  import { spring } from 'svelte/motion';

  interface Props {
    value: number;        // 0-100
    peak?: number;        // Optional peak hold value
    segments?: number;    // Number of LED segments
    showPeak?: boolean;   // Show peak hold indicator
    decayRate?: number;   // Peak decay rate (ms per segment)
    label?: string;       // Label below meter
    compact?: boolean;    // Compact mode for tight spaces
  }

  let { 
    value,
    peak,
    segments = 12,
    showPeak = true,
    decayRate = 100,
    label,
    compact = false
  }: Props = $props();

  // Physics-based value
  const displayedValue = spring(0, {
    stiffness: 0.1,
    damping: 0.4
  });

  $effect(() => {
    displayedValue.set(value);
  });

  // Calculate which segments should be lit
  const activeSegments = $derived(Math.ceil(($displayedValue / 100) * segments));
  const peakSegment = $derived(peak ? Math.ceil((peak / 100) * segments) : activeSegments);

  // Determine segment color based on position
  function getSegmentColor(index: number, total: number): string {
    const position = (index + 1) / total;
    if (position > 0.85) return 'red';
    if (position > 0.7) return 'yellow';
    return 'green';
  }
</script>

<div class="vu-meter" class:vu-meter--compact={compact}>
  <div class="segments">
    {#each Array(segments) as _, i}
      {@const segmentIndex = segments - 1 - i}
      {@const isActive = segmentIndex < activeSegments}
      {@const isPeak = showPeak && segmentIndex === peakSegment - 1 && !isActive}
      {@const color = getSegmentColor(segmentIndex, segments)}
      <div 
        class="segment segment--{color}"
        class:segment--active={isActive}
        class:segment--peak={isPeak}
      />
    {/each}
  </div>
  
  {#if label}
    <span class="label">{label}</span>
  {/if}
</div>

<style>
  .vu-meter {
    display: flex;
    flex-direction: column;
    align-items: center;
    gap: var(--space-xs);
  }

  .segments {
    display: flex;
    flex-direction: column;
    gap: 2px;
    padding: 4px;
    background: var(--color-vu-background);
    border: 1px solid #333;
    border-radius: 2px;
    box-shadow: 
      inset 0 1px 3px rgba(0, 0, 0, 0.5),
      0 1px 0 rgba(255, 255, 255, 0.05);
  }

  .segment {
    width: var(--vu-width, 20px);
    height: 4px;
    border-radius: 1px;
    transition: background-color 50ms, box-shadow 50ms;
  }

  /* Off state - dim version of the color */
  .segment--green {
    background: #0a1a0a;
  }

  .segment--yellow {
    background: #1a1a0a;
  }

  .segment--red {
    background: #1a0a0a;
  }

  /* Active state - full brightness with glow */
  .segment--green.segment--active {
    background: var(--color-led-green);
    box-shadow: 0 0 4px var(--color-led-green);
  }

  .segment--yellow.segment--active {
    background: var(--color-led-yellow);
    box-shadow: 0 0 4px var(--color-led-yellow);
  }

  .segment--red.segment--active {
    background: var(--color-led-red);
    box-shadow: 0 0 6px var(--color-led-red);
  }

  /* Peak hold indicator */
  .segment--peak {
    animation: peak-pulse 200ms ease-out;
  }

  .segment--green.segment--peak {
    background: rgba(0, 255, 0, 0.5);
    box-shadow: 0 0 2px var(--color-led-green);
  }

  .segment--yellow.segment--peak {
    background: rgba(255, 215, 0, 0.5);
    box-shadow: 0 0 2px var(--color-led-yellow);
  }

  .segment--red.segment--peak {
    background: rgba(255, 0, 0, 0.5);
    box-shadow: 0 0 2px var(--color-led-red);
  }

  @keyframes peak-pulse {
    from { opacity: 1; }
    to { opacity: 0.5; }
  }

  .label {
    font-size: 9px;
    color: var(--color-text-muted);
    text-transform: uppercase;
    letter-spacing: 0.5px;
  }

  /* Compact mode */
  .vu-meter--compact .segments {
    padding: 2px;
  }

  .vu-meter--compact .segment {
    width: 12px;
    height: 3px;
  }
</style>
