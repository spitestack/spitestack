<script lang="ts">
  /**
   * SegmentDisplay - 7-segment LED numeric display
   * 
   * The red glow of a tape counter. Time code rolling.
   * Burning through the dark like a deadline.
   */

  type DisplayColor = 'red' | 'green' | 'amber' | 'blue';

  interface Props {
    value: string | number;
    digits?: number;
    color?: DisplayColor;
    padZero?: boolean;
    showColon?: boolean;
  }

  let { 
    value,
    digits = 6,
    color = 'red',
    padZero = true,
    showColon = false
  }: Props = $props();

  const displayValue = $derived(() => {
    const str = String(value);
    if (padZero) {
      return str.padStart(digits, '0');
    }
    return str.padStart(digits, ' ');
  });

  const colorMap = {
    red: '#ff3333',
    green: '#33ff33',
    amber: '#ffaa00',
    blue: '#33aaff'
  };
</script>

<div 
  class="segment-display"
  style="--display-color: {colorMap[color]};"
>
  {#each displayValue().split('') as char, i}
    {#if showColon && i > 0 && i % 2 === 0}
      <span class="colon">:</span>
    {/if}
    <span class="digit" class:digit--dim={char === ' '}>{char === ' ' ? '8' : char}</span>
  {/each}
</div>

<style>
  .segment-display {
    display: inline-flex;
    align-items: center;
    gap: 2px;
    padding: 4px 8px;
    background: #050505;
    border-radius: 2px;
    box-shadow: 
      inset 0 2px 4px rgba(0, 0, 0, 0.8),
      0 1px 0 rgba(255, 255, 255, 0.05);
  }

  .digit {
    font-family: 'Courier New', monospace;
    font-size: 18px;
    font-weight: bold;
    color: var(--display-color);
    text-shadow: 
      0 0 4px var(--display-color),
      0 0 8px var(--display-color);
    letter-spacing: 2px;
  }

  .digit--dim {
    opacity: 0.1;
    text-shadow: none;
  }

  .colon {
    font-family: 'Courier New', monospace;
    font-size: 18px;
    font-weight: bold;
    color: var(--display-color);
    text-shadow: 
      0 0 4px var(--display-color),
      0 0 8px var(--display-color);
    margin: 0 2px;
  }
</style>
