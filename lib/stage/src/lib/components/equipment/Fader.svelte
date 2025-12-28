<script lang="ts">
  /**
   * Fader - Vertical slider control
   * 
   * The satisfying slide of a channel fader.
   * Unity at 0dB. Pushing it hot.
   * Interactive for controlling time ranges, log levels, etc.
   */

  interface Props {
    value: number;        // 0-100
    min?: number;
    max?: number;
    step?: number;
    disabled?: boolean;
    showValue?: boolean;
    label?: string;
    onchange?: (value: number) => void;
  }

  let { 
    value = $bindable(50),
    min = 0,
    max = 100,
    step = 1,
    disabled = false,
    showValue = true,
    label,
    onchange
  }: Props = $props();

  let isDragging = $state(false);
  let trackElement: HTMLDivElement;

  // Calculate percentage for display
  const percentage = $derived(((value - min) / (max - min)) * 100);

  function handlePointerDown(e: PointerEvent) {
    if (disabled) return;
    isDragging = true;
    trackElement.setPointerCapture(e.pointerId);
    updateValue(e);
  }

  function handlePointerMove(e: PointerEvent) {
    if (!isDragging) return;
    updateValue(e);
  }

  function handlePointerUp(e: PointerEvent) {
    isDragging = false;
    trackElement.releasePointerCapture(e.pointerId);
  }

  function updateValue(e: PointerEvent) {
    const rect = trackElement.getBoundingClientRect();
    // Inverted because fader goes from bottom (0) to top (100)
    const y = 1 - (e.clientY - rect.top) / rect.height;
    const clampedY = Math.max(0, Math.min(1, y));
    const newValue = min + clampedY * (max - min);
    const steppedValue = Math.round(newValue / step) * step;
    value = Math.max(min, Math.min(max, steppedValue));
    onchange?.(value);
  }
</script>

<div class="fader" class:fader--disabled={disabled} class:fader--dragging={isDragging}>
  {#if label}
    <span class="label">{label}</span>
  {/if}

  <div 
    class="track"
    bind:this={trackElement}
    onpointerdown={handlePointerDown}
    onpointermove={handlePointerMove}
    onpointerup={handlePointerUp}
    onpointercancel={handlePointerUp}
    role="slider"
    aria-valuenow={value}
    aria-valuemin={min}
    aria-valuemax={max}
    tabindex={disabled ? -1 : 0}
  >
    <!-- Track markers -->
    <div class="markers">
      {#each [0, 25, 50, 75, 100] as mark}
        <div class="marker" style="bottom: {mark}%"></div>
      {/each}
    </div>

    <!-- Fill/active area -->
    <div class="fill" style="height: {percentage}%"></div>

    <!-- Fader cap/knob -->
    <div class="cap" style="bottom: {percentage}%">
      <div class="cap-grip"></div>
    </div>
  </div>

  {#if showValue}
    <span class="value">{Math.round(value)}</span>
  {/if}
</div>

<style>
  .fader {
    display: flex;
    flex-direction: column;
    align-items: center;
    gap: var(--space-xs);
    user-select: none;
  }

  .label {
    font-size: 9px;
    color: var(--color-text-muted);
    text-transform: uppercase;
  }

  .track {
    position: relative;
    width: 24px;
    height: var(--fader-height, 120px);
    background: linear-gradient(180deg, #1a1a1a 0%, #0a0a0a 100%);
    border: 1px solid #333;
    border-radius: 2px;
    cursor: pointer;
    touch-action: none;
    box-shadow: 
      inset 0 2px 4px rgba(0, 0, 0, 0.5),
      0 1px 0 rgba(255, 255, 255, 0.05);
  }

  .markers {
    position: absolute;
    inset: 0;
    pointer-events: none;
  }

  .marker {
    position: absolute;
    left: 2px;
    right: 2px;
    height: 1px;
    background: rgba(255, 255, 255, 0.1);
  }

  .fill {
    position: absolute;
    bottom: 0;
    left: 4px;
    right: 4px;
    background: linear-gradient(
      0deg,
      var(--color-ember) 0%,
      var(--color-led-amber) 100%
    );
    opacity: 0.3;
    border-radius: 1px;
    transition: height 50ms;
  }

  .cap {
    position: absolute;
    left: 50%;
    transform: translateX(-50%) translateY(50%);
    width: 20px;
    height: 28px;
    background: linear-gradient(
      180deg,
      #4a4a4a 0%,
      #2a2a2a 50%,
      #3a3a3a 100%
    );
    border: 1px solid #555;
    border-radius: 2px;
    cursor: grab;
    box-shadow: 
      0 2px 4px rgba(0, 0, 0, 0.5),
      inset 0 1px 0 rgba(255, 255, 255, 0.1);
    transition: transform 50ms;
  }

  .cap-grip {
    position: absolute;
    top: 50%;
    left: 50%;
    transform: translate(-50%, -50%);
    width: 12px;
    height: 4px;
    background: #222;
    border-radius: 1px;
    box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.1);
  }

  .fader--dragging .cap {
    cursor: grabbing;
    background: linear-gradient(
      180deg,
      #5a5a5a 0%,
      #3a3a3a 50%,
      #4a4a4a 100%
    );
  }

  .fader--disabled {
    opacity: 0.5;
    pointer-events: none;
  }

  .value {
    font-size: 10px;
    font-variant-numeric: tabular-nums;
    color: var(--color-text-muted);
  }

  /* Hover state */
  .track:hover .cap {
    border-color: var(--color-ember);
  }

  /* Focus state */
  .track:focus-visible {
    outline: 2px solid var(--color-ember);
    outline-offset: 2px;
  }
</style>
