<script lang="ts">
  /**
   * ZoneNav - Zone navigation buttons
   * 
   * Quick access to studio zones.
   * Click or use keyboard shortcuts.
   */

  import { viewport, zones } from '$lib/stores/viewport';

  let currentZone = $derived.by(() => {
    let zone: string | null = null;
    viewport.subscribe(state => {
      zone = state.currentZone;
    })();
    return zone;
  });
</script>

<nav class="zone-nav" aria-label="Zone navigation">
  {#each zones as zone (zone.id)}
    <button
      class="zone-btn"
      class:zone-btn--active={currentZone === zone.id}
      onclick={() => viewport.goToZone(zone.id)}
      title="{zone.name} (Press {zone.shortcut})"
    >
      <span class="shortcut">[{zone.shortcut}]</span>
      <span class="name">{zone.name.slice(0, 4).toUpperCase()}</span>
    </button>
  {/each}
</nav>

<style>
  .zone-nav {
    display: flex;
    gap: var(--space-xs);
  }

  .zone-btn {
    display: flex;
    align-items: center;
    gap: 2px;
    padding: var(--space-xs) var(--space-sm);
    font-size: 10px;
    color: var(--color-text-muted);
    background: transparent;
    border: 1px solid var(--color-charcoal);
    border-radius: 2px;
    transition: var(--transition-fast);
  }

  .zone-btn:hover {
    color: var(--color-text);
    border-color: var(--color-ember);
    background: rgba(204, 85, 0, 0.05);
  }

  .zone-btn--active {
    color: var(--color-ember);
    border-color: var(--color-ember);
    background: rgba(204, 85, 0, 0.1);
  }

  .shortcut {
    font-weight: 500;
    opacity: 0.7;
  }

  .name {
    font-weight: 600;
    letter-spacing: 0.5px;
  }
</style>
