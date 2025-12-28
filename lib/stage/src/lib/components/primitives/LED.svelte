<script lang="ts">
  /**
   * LED - Status indicator light
   * 
   * The heartbeat of every rack. That satisfying glow.
   * Based on the TUI's tiered rendering approach.
   */

  type LEDColor = 'green' | 'yellow' | 'red' | 'amber' | 'blue' | 'pink';

  interface Props {
    color?: LEDColor;
    on?: boolean;
    blinking?: boolean;
    blinkSpeed?: number;
    size?: 'sm' | 'md' | 'lg';
  }

  let { 
    color = 'green',
    on = true,
    blinking = false,
    blinkSpeed = 500,
    size = 'md'
  }: Props = $props();

  const sizeMap = {
    sm: 6,
    md: 8,
    lg: 12
  };
</script>

<div 
  class="led led--{size}"
  class:led--on={on}
  class:led--off={!on}
  class:led--blinking={blinking && on}
  style="
    --led-color: var(--color-led-{color});
    --blink-speed: {blinkSpeed}ms;
    --led-size: {sizeMap[size]}px;
  "
  role="status"
  aria-label="{on ? 'Active' : 'Inactive'} {color} indicator"
/>

<style>
  .led {
    width: var(--led-size);
    height: var(--led-size);
    border-radius: 50%;
    transition: var(--transition-fast);
  }

  .led--on {
    background: var(--led-color);
    box-shadow: 
      0 0 4px var(--led-color),
      0 0 8px var(--led-color),
      inset 0 -2px 4px rgba(0, 0, 0, 0.3);
  }

  .led--off {
    background: color-mix(in srgb, var(--led-color) 20%, black);
    box-shadow: inset 0 1px 2px rgba(0, 0, 0, 0.5);
  }

  .led--blinking {
    animation: led-blink var(--blink-speed) ease-in-out infinite;
  }

  @keyframes led-blink {
    0%, 100% {
      opacity: 1;
      box-shadow: 
        0 0 4px var(--led-color),
        0 0 8px var(--led-color),
        inset 0 -2px 4px rgba(0, 0, 0, 0.3);
    }
    50% {
      opacity: 0.3;
      box-shadow: inset 0 1px 2px rgba(0, 0, 0, 0.5);
    }
  }

  /* Size variants */
  .led--sm {
    --led-size: 6px;
  }

  .led--md {
    --led-size: 8px;
  }

  .led--lg {
    --led-size: 12px;
  }
</style>
