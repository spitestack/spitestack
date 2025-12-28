<script lang="ts">
  /**
   * ChannelStrip - A complete mixer channel
   * 
   * ScribbleStrip + LED + VuMeter + Fader.
   * Each one is a projection, a service, a heartbeat.
   * The vertical slice of your system's soul.
   */

  import LED from '../primitives/LED.svelte';
  import ScribbleStrip from '../primitives/ScribbleStrip.svelte';
  import VuMeter from './VuMeter.svelte';
  import Fader from './Fader.svelte';
  import Knob from './Knob.svelte';
  
  import { focus } from '$lib/stores/focus';
  import type { ProjectionHealth } from '$lib/types/telemetry';

  interface Props {
    id: string;
    name: string;
    health: ProjectionHealth;
    eventsPerSec: number;
    latencyMs: number;
    selected?: boolean;
    faderValue?: number;
    knobValue?: number;
    onclick?: () => void;
    onfaderchange?: (value: number) => void;
    onknobchange?: (value: number) => void;
  }

  let { 
    id,
    name,
    health,
    eventsPerSec,
    latencyMs,
    selected = false,
    faderValue = $bindable(75),
    knobValue = $bindable(50),
    onclick,
    onfaderchange,
    onknobchange
  }: Props = $props();

  // Focus Logic
  const isDimmed = $derived($focus.active && $focus.activeId !== id);
  const isFocused = $derived($focus.activeId === id);

  // Map health to LED color

  // Map health to LED color
  const ledColor = $derived(() => {
    if (muted) return 'off'; // New 'off' state handled in LED component? Or just 'grey'
    switch (health) {
      case 'healthy': return 'green';
      case 'warning': return 'yellow';
      case 'error': return 'red';
    }
  });

  // Threshold logic
  // Fader at 100 = High threshold (lazy)
  // Fader at 0 = Low threshold (sensitive)
  // Let's say threshold range is 0 to 1000 events/sec
  const threshold = $derived(faderValue * 10); // 0-100 -> 0-1000
  const isThresholdExceeded = $derived(eventsPerSec > threshold);

  // VU Meter Value
  const vuValue = $derived(() => {
    if (eventsPerSec === 0) return 0;
    const logValue = Math.log10(eventsPerSec + 1) / 3 * 100;
    return Math.min(100, logValue);
  });

  const displayName = $derived(name.slice(0, 8).toUpperCase());

  // State
  let muted = $state(false);
  let soloed = $state(false);

  function toggleMute(e: MouseEvent) {
    e.stopPropagation();
    muted = !muted;
  }

  function toggleSolo(e: MouseEvent) {
    e.stopPropagation();
    soloed = !soloed;
    focus.toggle(id);
  }

</script>

<div 
  class="channel-strip"
  class:channel-strip--selected={selected}
  class:channel-strip--error={health === 'error'}
  class:channel-strip--dimmed={isDimmed}
  onclick={onclick}
  role="button"
  tabindex="0"
  aria-label="{name} channel - {health}"
>
  <!-- Scribble strip / name label -->
  <div class="header">
    <ScribbleStrip 
      text={displayName} 
      backlight={selected ? 'amber' : 'cyan'} 
    />
  </div>

  <!-- Status LED -->
  <div class="status">
    <LED 
      color={muted ? 'red' : ledColor() === 'off' ? 'green' : ledColor()} 
      on={!muted} 
      blinking={(health === 'error' || isThresholdExceeded) && !muted}
    />
  </div>

  <!-- Mute / Solo Buttons -->

  <!-- Mute / Solo Buttons -->
  <div class="channel-controls">
    <button 
      class="control-btn control-btn--mute" 
      class:active={muted}
      onclick={toggleMute}
      title="Mute"
    >M</button>
    <button 
      class="control-btn control-btn--solo" 
      class:active={soloed}
      onclick={toggleSolo}
      title="Solo"
    >S</button>
  </div>

  <!-- Knob (now Threshold Trim?) -->
  <div class="knob-section">
    <Knob 
      bind:value={knobValue} 
      size="sm" 
      onchange={onknobchange}
    />
  </div>

  <!-- VU Meter -->
  <div class="meter" class:threshold-alert={isThresholdExceeded && !muted}>
    <VuMeter 
      value={vuValue()} 
      segments={10}
      compact
    />
  </div>

  <!-- Fader -->
  <div class="fader">
    <Fader 
      bind:value={faderValue}
      showValue={false}
      onchange={onfaderchange}
    />
  </div>

  <!-- Events per second display -->
  <div class="events">
    <span class="events-value">{eventsPerSec}</span>
    <span class="events-unit">/s</span>
  </div>
</div>

<style>
  .channel-strip {
    display: flex;
    flex-direction: column;
    align-items: center;
    gap: var(--space-xs);
    padding: var(--space-sm);
    width: var(--channel-width, 64px);
    background: linear-gradient(180deg, #252525 0%, #1a1a1a 100%);
    border: 1px solid #333;
    border-radius: 4px;
    cursor: pointer;
    transition: var(--transition-fast);
  }

  .channel-strip:hover {
    border-color: var(--color-ash);
    background: linear-gradient(180deg, #2a2a2a 0%, #1f1f1f 100%);
  }

  .channel-strip--selected {
    border-color: var(--color-ember);
    background: linear-gradient(180deg, #2d2520 0%, #1f1a18 100%);
    box-shadow: 0 0 8px rgba(204, 85, 0, 0.2);
  }

  .channel-strip--error {
    border-color: var(--color-blood);
    animation: error-pulse 2s ease-in-out infinite;
  }

  @keyframes error-pulse {
    0%, 100% { box-shadow: none; }
    50% { box-shadow: 0 0 8px rgba(139, 0, 0, 0.3); }
  }

  .header {
    width: 100%;
  }

  .status {
    padding: var(--space-xs) 0;
  }

  .knob-section {
    padding: var(--space-xs) 0;
  }

  .meter {
    padding: var(--space-xs) 0;
    width: 100%;
    display: flex;
    justify-content: center;
    border-radius: 2px;
  }

  .threshold-alert {
    background: rgba(139, 0, 0, 0.2);
    box-shadow: 0 0 10px rgba(139, 0, 0, 0.3);
    animation: threshold-pulse 0.5s ease-in-out infinite alternate;
  }

  @keyframes threshold-pulse {
    from { box-shadow: 0 0 5px rgba(255, 0, 0, 0.2); }
    to { box-shadow: 0 0 15px rgba(255, 0, 0, 0.5); }
  }

  .fader {
    padding: var(--space-xs) 0;
  }

  .events {
    display: flex;
    align-items: baseline;
    gap: 1px;
    padding-top: var(--space-xs);
  }

  .events-value {
    font-size: 11px;
    font-weight: 600;
    font-variant-numeric: tabular-nums;
    color: var(--color-text);
  }

  .events-unit {
    font-size: 8px;
    color: var(--color-text-muted);
  }

  /* Controls */
  .channel-controls {
    display: flex;
    gap: 4px;
    width: 100%;
    padding: 2px;
  }

  .control-btn {
    flex: 1;
    height: 20px;
    font-size: 10px;
    font-weight: 700;
    color: #333;
    background: #555;
    border: 1px solid #222;
    border-radius: 2px;
    display: flex;
    align-items: center;
    justify-content: center;
    box-shadow: 0 1px 2px rgba(0,0,0,0.5);
  }

  .control-btn:hover {
    filter: brightness(1.2);
  }

  /* Mute Active: Red/Yellow */
  .control-btn--mute.active {
    background: var(--color-led-red);
    color: #fff;
    box-shadow: 0 0 5px var(--color-led-red);
  }

  /* Solo Active: Amber */
  .control-btn--solo.active {
    background: var(--color-led-amber);
    color: #fff;
    box-shadow: 0 0 5px var(--color-led-amber);
  }

  .channel-strip--dimmed {
    opacity: 0.2;
    filter: blur(2px) grayscale(0.8);
    transform: scale(0.95);
    pointer-events: none; /* Prevent clicking whilst dimmed? Maybe allow to steal focus */
  }

  /* Focus state */
  .channel-strip:focus-visible {
    outline: 2px solid var(--color-ember);
    outline-offset: 2px;
  }
</style>
