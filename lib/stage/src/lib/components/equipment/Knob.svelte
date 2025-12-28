<script lang="ts">
  /**
   * Knob - Rotary control
   * 
   * Twist it like a gain knob. That satisfying click.
   * Interactive for controlling refresh rates, filter ranges, etc.
   */

  interface Props {
    value: number;        // 0-100
    min?: number;
    max?: number;
    step?: number;
    disabled?: boolean;
    size?: 'sm' | 'md' | 'lg';
    label?: string;
    onchange?: (value: number) => void;
  }

  let { 
    value = $bindable(50),
    min = 0,
    max = 100,
    step = 1,
    disabled = false,
    size = 'md',
    label,
    onchange
  }: Props = $props();

  let isDragging = $state(false);
  let startY = 0;
  let startValue = 0;

  const sizeMap = {
    sm: 24,
    md: 32,
    lg: 40
  };

  // Calculate rotation angle (270° sweep from -135° to 135°)
  const rotation = $derived(() => {
    const percentage = (value - min) / (max - min);
    return -135 + percentage * 270;
  });

  function handlePointerDown(e: PointerEvent) {
    if (disabled) return;
    isDragging = true;
    startY = e.clientY;
    startValue = value;
    (e.target as HTMLElement).setPointerCapture(e.pointerId);
  }

  function handlePointerMove(e: PointerEvent) {
    if (!isDragging) return;
    
    // Calculate delta (moving up increases, down decreases)
    const deltaY = startY - e.clientY;
    const sensitivity = (max - min) / 100; // Full range over 100px
    const newValue = startValue + deltaY * sensitivity;
    const steppedValue = Math.round(newValue / step) * step;
    value = Math.max(min, Math.min(max, steppedValue));
    onchange?.(value);
  }

  function handlePointerUp(e: PointerEvent) {
    isDragging = false;
    (e.target as HTMLElement).releasePointerCapture(e.pointerId);
  }

  function handleWheel(e: WheelEvent) {
    if (disabled) return;
    e.preventDefault();
    const delta = e.deltaY > 0 ? -step : step;
    value = Math.max(min, Math.min(max, value + delta));
    onchange?.(value);
  }
</script>

<div class="knob knob--{size}" class:knob--disabled={disabled}>
  {#if label}
    <span class="label">{label}</span>
  {/if}

  <div 
    class="knob-body"
    style="
      --knob-size: {sizeMap[size]}px;
      --rotation: {rotation()}deg;
    "
    onpointerdown={handlePointerDown}
    onpointermove={handlePointerMove}
    onpointerup={handlePointerUp}
    onpointercancel={handlePointerUp}
    onwheel={handleWheel}
    role="slider"
    aria-valuenow={value}
    aria-valuemin={min}
    aria-valuemax={max}
    tabindex={disabled ? -1 : 0}
  >
    <!-- Outer ring with notches -->
    <div class="ring">
      {#each Array(11) as _, i}
        <div 
          class="notch" 
          style="transform: rotate({-135 + i * 27}deg)"
        ></div>
      {/each}
    </div>

    <!-- Knob cap -->
    <div class="cap">
      <div class="indicator"></div>
    </div>
  </div>
</div>

<style>
  .knob {
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

  .knob-body {
    position: relative;
    width: var(--knob-size);
    height: var(--knob-size);
    cursor: pointer;
    touch-action: none;
  }

  .ring {
    position: absolute;
    inset: 0;
    border-radius: 50%;
    background: #1a1a1a;
    box-shadow: 
      inset 0 2px 4px rgba(0, 0, 0, 0.5),
      0 1px 0 rgba(255, 255, 255, 0.05);
  }

  .notch {
    position: absolute;
    top: 2px;
    left: 50%;
    width: 1px;
    height: 4px;
    background: rgba(255, 255, 255, 0.2);
    transform-origin: center calc(var(--knob-size) / 2 - 2px);
  }

  .cap {
    position: absolute;
    inset: 4px;
    border-radius: 50%;
    background: linear-gradient(
      135deg,
      #4a4a4a 0%,
      #2a2a2a 50%,
      #3a3a3a 100%
    );
    border: 1px solid #555;
    box-shadow: 
      0 2px 4px rgba(0, 0, 0, 0.5),
      inset 0 1px 0 rgba(255, 255, 255, 0.1);
    transform: rotate(var(--rotation));
    transition: transform 50ms;
  }

  .indicator {
    position: absolute;
    top: 4px;
    left: 50%;
    transform: translateX(-50%);
    width: 2px;
    height: 6px;
    background: var(--color-ember);
    border-radius: 1px;
    box-shadow: 0 0 4px var(--color-ember);
  }

  .knob--disabled {
    opacity: 0.5;
    pointer-events: none;
  }

  /* Hover state */
  .knob-body:hover .cap {
    border-color: var(--color-ember);
  }

  /* Focus state */
  .knob-body:focus-visible {
    outline: 2px solid var(--color-ember);
    outline-offset: 4px;
    border-radius: 50%;
  }

  /* Size variants */
  .knob--sm .label { font-size: 8px; }
  .knob--lg .label { font-size: 10px; }
</style>
