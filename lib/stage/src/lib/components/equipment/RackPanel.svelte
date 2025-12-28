<script lang="ts">
  /**
   * RackPanel - Server rack cabinet
   * 
   * A vertical stack of rack units.
   * The infrastructure backbone. The machine room.
   * Glass door optional. Blinking lights required.
   */

  import RackUnit from './RackUnit.svelte';

  interface RackService {
    id: string;
    name: string;
    health: 'healthy' | 'warning' | 'error' | 'offline';
    activity?: number;
    load?: number;
  }

  interface Props {
    title?: string;
    services: RackService[];
    maxUnits?: number;
  }

  let { 
    title = 'INFRASTRUCTURE',
    services,
    maxUnits = 8
  }: Props = $props();

  // Calculate empty slots
  const emptySlots = $derived(Math.max(0, maxUnits - services.length));
</script>

<div class="rack-panel">
  <!-- Header -->
  <div class="rack-header">
    <span class="header-text">{title}</span>
  </div>

  <!-- Cabinet body -->
  <div class="cabinet">
    <!-- Glass door effect -->
    <div class="glass-door">
      <!-- Rack rails -->
      <div class="rails">
        <div class="rail rail--left"></div>
        <div class="rail rail--right"></div>
      </div>

      <!-- Rack units -->
      <div class="units">
        {#each services as service (service.id)}
          <RackUnit
            name={service.name}
            health={service.health}
            activity={service.activity ?? 50}
            load={service.load ?? 30}
            blinking={service.health === 'healthy'}
          />
        {/each}

        <!-- Empty slots -->
        {#each Array(emptySlots) as _, i}
          <div class="empty-slot">
            <div class="slot-label">EMPTY</div>
          </div>
        {/each}
      </div>
    </div>

    <!-- Ventilation at bottom -->
    <div class="ventilation">
      {#each Array(8) as _}
        <div class="vent-slot"></div>
      {/each}
    </div>
  </div>

  <!-- Base/feet -->
  <div class="rack-base">
    <div class="foot"></div>
    <div class="foot"></div>
  </div>
</div>

<style>
  .rack-panel {
    display: flex;
    flex-direction: column;
    width: 320px;
    background: var(--color-charcoal);
    border: 2px solid #333;
    border-radius: 4px;
    box-shadow: var(--shadow-equipment);
  }

  .rack-header {
    display: flex;
    justify-content: center;
    padding: var(--space-sm);
    background: linear-gradient(180deg, #2a2a2a 0%, #1f1f1f 100%);
    border-bottom: 1px solid #333;
  }

  .header-text {
    font-size: 11px;
    font-weight: 600;
    letter-spacing: 2px;
    color: var(--color-text-muted);
  }

  .cabinet {
    position: relative;
    padding: var(--space-sm);
    background: linear-gradient(180deg, #1a1a1a 0%, #0f0f0f 100%);
    min-height: 300px;
  }

  .glass-door {
    position: relative;
    padding: var(--space-xs);
    background: rgba(0, 0, 0, 0.3);
    border: 1px solid #444;
    border-radius: 2px;
  }

  .glass-door::before {
    content: '';
    position: absolute;
    inset: 0;
    background: linear-gradient(
      135deg,
      rgba(255, 255, 255, 0.02) 0%,
      transparent 50%,
      rgba(255, 255, 255, 0.01) 100%
    );
    pointer-events: none;
    border-radius: 2px;
  }

  .rails {
    position: absolute;
    inset: 0;
    pointer-events: none;
  }

  .rail {
    position: absolute;
    top: 0;
    bottom: 0;
    width: 4px;
    background: linear-gradient(
      90deg,
      #2a2a2a 0%,
      #3a3a3a 50%,
      #2a2a2a 100%
    );
  }

  .rail--left { left: 8px; }
  .rail--right { right: 8px; }

  .units {
    display: flex;
    flex-direction: column;
    gap: 2px;
    padding: 0 16px;
  }

  .empty-slot {
    height: var(--rack-unit-height);
    display: flex;
    align-items: center;
    justify-content: center;
    background: #0a0a0a;
    border: 1px dashed #333;
    border-radius: 2px;
  }

  .slot-label {
    font-size: 10px;
    color: #333;
    letter-spacing: 1px;
  }

  .ventilation {
    display: flex;
    justify-content: center;
    gap: 4px;
    margin-top: var(--space-md);
    padding: var(--space-sm);
  }

  .vent-slot {
    width: 20px;
    height: 4px;
    background: #0a0a0a;
    border-radius: 1px;
  }

  .rack-base {
    display: flex;
    justify-content: space-between;
    padding: var(--space-xs) var(--space-lg);
    background: linear-gradient(180deg, #1f1f1f 0%, #181818 100%);
    border-top: 1px solid #333;
  }

  .foot {
    width: 40px;
    height: 8px;
    background: #0a0a0a;
    border-radius: 0 0 4px 4px;
    box-shadow: 0 2px 4px rgba(0, 0, 0, 0.5);
  }
</style>
