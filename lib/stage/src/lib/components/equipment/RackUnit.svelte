<script lang="ts">
  /**
   * RackUnit - Single rack-mounted device
   * 
   * One U of server rack goodness.
   * Green LEDs blinking. Activity light flickering.
   * The infrastructure that keeps everything running.
   */

  import LED from '../primitives/LED.svelte';
  import { BarGraph } from '../primitives';

  type RackUnitHealth = 'healthy' | 'warning' | 'error' | 'offline';

  interface Props {
    name: string;
    health?: RackUnitHealth;
    activity?: number;      // 0-100 activity percentage
    load?: number;          // 0-100 load percentage
    blinking?: boolean;
    height?: 1 | 2;         // 1U or 2U
  }

  let { 
    name,
    health = 'healthy',
    activity = 0,
    load = 0,
    blinking = false,
    height = 1
  }: Props = $props();

  const healthColor = $derived(() => {
    switch (health) {
      case 'healthy': return 'green';
      case 'warning': return 'yellow';
      case 'error': return 'red';
      case 'offline': return 'red';
    }
  });

  const isOnline = $derived(health !== 'offline');
</script>

<div 
  class="rack-unit rack-unit--{height}u"
  class:rack-unit--offline={!isOnline}
>
  <!-- Rack ears with screw holes -->
  <div class="rack-ear rack-ear--left">
    <div class="screw-hole"></div>
    <div class="screw-hole"></div>
  </div>

  <!-- Main unit body -->
  <div class="unit-body">
    <!-- Front panel -->
    <div class="front-panel">
      <!-- Name display -->
      <div class="name-display">
        <span class="name">{name}</span>
      </div>

      <!-- Status LEDs -->
      <div class="led-cluster">
        <LED color={healthColor()} on={isOnline} blinking={health === 'error'} size="sm" />
        <LED color="amber" on={isOnline && activity > 0} blinking={blinking} size="sm" />
      </div>

      <!-- Activity/Load bar -->
      <div class="activity-bar">
        <BarGraph value={load} variant="segmented" segments={8} />
      </div>
    </div>
  </div>

  <!-- Rack ears -->
  <div class="rack-ear rack-ear--right">
    <div class="screw-hole"></div>
    <div class="screw-hole"></div>
  </div>
</div>

<style>
  .rack-unit {
    display: flex;
    background: var(--color-charcoal);
    border-radius: 2px;
    overflow: hidden;
  }

  .rack-unit--1u {
    height: var(--rack-unit-height, 44px);
  }

  .rack-unit--2u {
    height: calc(var(--rack-unit-height, 44px) * 2 + 2px);
  }

  .rack-unit--offline {
    opacity: 0.5;
  }

  .rack-ear {
    width: 16px;
    display: flex;
    flex-direction: column;
    justify-content: space-around;
    align-items: center;
    padding: 4px 0;
    background: linear-gradient(
      90deg,
      #2a2a2a 0%,
      #3a3a3a 50%,
      #2a2a2a 100%
    );
    border: 1px solid #444;
  }

  .rack-ear--left {
    border-right: none;
    border-radius: 2px 0 0 2px;
  }

  .rack-ear--right {
    border-left: none;
    border-radius: 0 2px 2px 0;
  }

  .screw-hole {
    width: 6px;
    height: 6px;
    border-radius: 50%;
    background: #1a1a1a;
    border: 1px solid #333;
    box-shadow: inset 0 1px 2px rgba(0, 0, 0, 0.5);
  }

  .unit-body {
    flex: 1;
    display: flex;
    background: linear-gradient(180deg, #252525 0%, #1a1a1a 100%);
    border-top: 1px solid #444;
    border-bottom: 1px solid #444;
  }

  .front-panel {
    flex: 1;
    display: flex;
    align-items: center;
    gap: var(--space-md);
    padding: 0 var(--space-md);
  }

  .name-display {
    min-width: 100px;
    padding: 4px 8px;
    background: #0a1a0a;
    border: 1px solid #333;
    border-radius: 2px;
  }

  .name {
    font-size: 11px;
    font-weight: 600;
    letter-spacing: 0.5px;
    color: var(--color-led-green);
    text-shadow: 0 0 4px var(--color-led-green);
    text-transform: uppercase;
  }

  .led-cluster {
    display: flex;
    gap: var(--space-xs);
  }

  .activity-bar {
    flex: 1;
    max-width: 150px;
  }

  /* Offline state */
  .rack-unit--offline .name {
    color: var(--color-ash);
    text-shadow: none;
  }
</style>
