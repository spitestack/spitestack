<script lang="ts">
  /**
   * Badge - Status indicator badge
   * 
   * Quick visual status. Healthy, warning, error.
   * The traffic lights of observability.
   */

  type BadgeVariant = 'healthy' | 'warning' | 'error' | 'info' | 'muted';

  interface Props {
    variant?: BadgeVariant;
    children: import('svelte').Snippet;
    withDot?: boolean;
    pulsing?: boolean;
  }

  let { 
    variant = 'muted',
    children,
    withDot = false,
    pulsing = false
  }: Props = $props();
</script>

<span 
  class="badge badge--{variant}"
  class:badge--pulsing={pulsing}
>
  {#if withDot}
    <span class="dot" class:dot--pulsing={pulsing}></span>
  {/if}
  {@render children()}
</span>

<style>
  .badge {
    display: inline-flex;
    align-items: center;
    gap: 4px;
    padding: 2px 6px;
    font-size: 10px;
    font-weight: 500;
    letter-spacing: 0.5px;
    text-transform: uppercase;
    border-radius: 2px;
    transition: var(--transition-fast);
  }

  .badge--healthy {
    background: rgba(0, 255, 0, 0.1);
    color: var(--color-led-green);
    border: 1px solid rgba(0, 255, 0, 0.3);
  }

  .badge--warning {
    background: rgba(204, 85, 0, 0.1);
    color: var(--color-ember);
    border: 1px solid rgba(204, 85, 0, 0.3);
  }

  .badge--error {
    background: rgba(139, 0, 0, 0.1);
    color: var(--color-blood);
    border: 1px solid rgba(139, 0, 0, 0.3);
  }

  .badge--info {
    background: rgba(76, 201, 240, 0.1);
    color: var(--color-neon-blue);
    border: 1px solid rgba(76, 201, 240, 0.3);
  }

  .badge--muted {
    background: rgba(105, 105, 105, 0.1);
    color: var(--color-ash);
    border: 1px solid rgba(105, 105, 105, 0.3);
  }

  .badge--pulsing {
    animation: badge-pulse 2s ease-in-out infinite;
  }

  @keyframes badge-pulse {
    0%, 100% { opacity: 1; }
    50% { opacity: 0.6; }
  }

  .dot {
    width: 6px;
    height: 6px;
    border-radius: 50%;
    background: currentColor;
  }

  .dot--pulsing {
    animation: dot-pulse 1s ease-in-out infinite;
  }

  @keyframes dot-pulse {
    0%, 100% { 
      opacity: 1;
      box-shadow: 0 0 4px currentColor;
    }
    50% { 
      opacity: 0.5;
      box-shadow: none;
    }
  }
</style>
