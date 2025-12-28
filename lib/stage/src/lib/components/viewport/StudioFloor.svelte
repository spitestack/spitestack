<script lang="ts">
  /**
   * StudioFloor - The studio environment
   * 
   * A large canvas with equipment zones.
   * Mixing console, racks, tape deck, patch bay.
   * The floor plan of your observability command center.
   */

  import { MixingConsole, RackPanel } from '$components/equipment';
  import type { ProjectionStatus } from '$lib/types/telemetry';

  interface Props {
    projections: ProjectionStatus[];
    rackServices?: Array<{
      id: string;
      name: string;
      health: 'healthy' | 'warning' | 'error' | 'offline';
      activity?: number;
      load?: number;
    }>;
    selectedProjection?: string;
    onSelectProjection?: (id: string) => void;
  }

  let { 
    projections,
    rackServices = [],
    selectedProjection,
    onSelectProjection
  }: Props = $props();

  // Calculate aggregate metrics
  const totalEventsPerSec = $derived(
    projections.reduce((sum, p) => sum + p.eventsPerSec, 0)
  );
  
  const avgLatencyMs = $derived(
    projections.length > 0
      ? projections.reduce((sum, p) => sum + p.avgLatencyMs, 0) / projections.length
      : 0
  );
</script>

<div class="studio-floor">
  <!-- Floor grid pattern -->
  <div class="floor-grid"></div>

  <!-- Zone: Mixing Console -->
  <div class="zone zone--console" data-zone="console">
    <div class="zone-label">MIXING CONSOLE</div>
    <MixingConsole
      {projections}
      selectedId={selectedProjection}
      totalEventsPerSec={totalEventsPerSec}
      avgLatencyMs={avgLatencyMs}
      onselect={onSelectProjection}
    />
  </div>

  <!-- Zone: Racks -->
  <div class="zone zone--racks" data-zone="racks">
    <div class="zone-label">INFRASTRUCTURE</div>
    <RackPanel
      title="INFRASTRUCTURE"
      services={rackServices}
    />
  </div>

  <!-- Zone: Tape Deck (placeholder) -->
  <div class="zone zone--tape" data-zone="tape">
    <div class="zone-label">TAPE DECK</div>
    <div class="placeholder-equipment">
      <div class="tape-deck-placeholder">
        <div class="reel"></div>
        <div class="display">00:00:00:00</div>
        <div class="reel"></div>
        <div class="transport">
          <button class="transport-btn">&#9664;&#9664;</button>
          <button class="transport-btn">&#9654;</button>
          <button class="transport-btn">&#9632;</button>
          <button class="transport-btn">&#9654;&#9654;</button>
        </div>
      </div>
    </div>
  </div>

  <!-- Zone: Patch Bay (placeholder) -->
  <div class="zone zone--patch" data-zone="patch">
    <div class="zone-label">PATCH BAY</div>
    <div class="placeholder-equipment">
      <div class="patch-bay-placeholder">
        {#each Array(4) as _, row}
          <div class="jack-row">
            {#each Array(8) as _, col}
              <div class="jack"></div>
            {/each}
          </div>
        {/each}
      </div>
    </div>
  </div>
</div>

<style>
  .studio-floor {
    position: relative;
    width: 1600px;
    height: 1000px;
    background: var(--color-void);
  }

  /* Subtle grid pattern on the floor */
  .floor-grid {
    position: absolute;
    inset: 0;
    background-image: 
      linear-gradient(rgba(255, 255, 255, 0.02) 1px, transparent 1px),
      linear-gradient(90deg, rgba(255, 255, 255, 0.02) 1px, transparent 1px);
    background-size: 50px 50px;
    pointer-events: none;
  }

  .zone {
    position: absolute;
    display: flex;
    flex-direction: column;
    gap: var(--space-sm);
  }

  .zone-label {
    font-size: 10px;
    font-weight: 600;
    letter-spacing: 2px;
    color: var(--color-ash);
    text-transform: uppercase;
    padding: var(--space-xs) var(--space-sm);
    background: rgba(0, 0, 0, 0.3);
    border-radius: 2px;
    align-self: flex-start;
  }

  /* Zone positions */
  .zone--console {
    top: 100px;
    left: 100px;
  }

  .zone--racks {
    top: 100px;
    left: 750px;
  }

  .zone--tape {
    top: 550px;
    left: 200px;
  }

  .zone--patch {
    top: 100px;
    left: 1100px;
  }

  /* Placeholder equipment styling */
  .placeholder-equipment {
    padding: var(--space-md);
    background: var(--color-charcoal);
    border: 2px solid #333;
    border-radius: 4px;
    box-shadow: var(--shadow-equipment);
  }

  /* Tape deck placeholder */
  .tape-deck-placeholder {
    display: flex;
    flex-direction: column;
    align-items: center;
    gap: var(--space-md);
    padding: var(--space-lg);
    min-width: 400px;
  }

  .tape-deck-placeholder > div:first-child {
    display: flex;
    justify-content: space-between;
    width: 100%;
  }

  .reel {
    width: 80px;
    height: 80px;
    border-radius: 50%;
    background: linear-gradient(135deg, #2a2a2a 0%, #1a1a1a 50%, #2a2a2a 100%);
    border: 3px solid #333;
    box-shadow: inset 0 0 20px rgba(0, 0, 0, 0.5);
    display: inline-block;
  }

  .display {
    font-family: 'Courier New', monospace;
    font-size: 24px;
    font-weight: bold;
    color: var(--color-led-amber);
    text-shadow: 0 0 8px var(--color-led-amber);
    background: #050505;
    padding: var(--space-sm) var(--space-md);
    border-radius: 2px;
  }

  .transport {
    display: flex;
    gap: var(--space-sm);
  }

  .transport-btn {
    padding: var(--space-sm) var(--space-md);
    font-size: 14px;
    color: var(--color-text-muted);
    background: linear-gradient(180deg, #3a3a3a 0%, #2a2a2a 100%);
    border: 1px solid #444;
    border-radius: 4px;
    cursor: pointer;
    transition: var(--transition-fast);
  }

  .transport-btn:hover {
    color: var(--color-text);
    border-color: var(--color-ember);
  }

  /* Patch bay placeholder */
  .patch-bay-placeholder {
    display: flex;
    flex-direction: column;
    gap: var(--space-sm);
    padding: var(--space-md);
    background: #1a1a1a;
    border-radius: 4px;
  }

  .jack-row {
    display: flex;
    gap: var(--space-sm);
  }

  .jack {
    width: 16px;
    height: 16px;
    border-radius: 50%;
    background: #0a0a0a;
    border: 2px solid #333;
    box-shadow: inset 0 1px 2px rgba(0, 0, 0, 0.5);
  }
</style>
