<script lang="ts">
  /**
   * PatchBay - Visual Dependency Graph
   * 
   * Top Row: Events (Outputs/Sources)
   * Bottom Row: Projections (Inputs/Destinations)
   * 
   * Cables connect them.
   * Hovering a cable shows the flow.
   */

  import { projectionsList } from '$lib/stores/telemetry';
  import { onMount } from 'svelte';
  
  // Data prep
  // We need a list of all unique Event Types
  // Data prep
  // We need a list of all unique Event Types
  const eventTypes = $derived(
    Array.from(new Set($projectionsList.flatMap(p => p.eventSubscriptions))).sort()
  );

  const projections = $derived($projectionsList);

  // Layout calculation
  const JACK_WIDTH = 40;
  const GAP = 10;
  
  // Interaction
  let hoveredCable: string | null = $state(null);
  let hoveredItem: string | null = $state(null); // specific jack (event type or projection id)

  function getJackX(index: number) {
    return index * (JACK_WIDTH + GAP) + JACK_WIDTH/2;
  }

  // ... (getCablePath and getCableColor remain same) ...

  // Check if a cable is related to the hovered item
  function isRelated(sub: string, projId: string) {
    if (!hoveredItem && !hoveredCable) return true; // Show all if nothing hovered? Or dim all?
    // Let's dim all by default to reduce noise? No, show all faintly.
    
    if (hoveredCable === `${sub}-${projId}`) return true;
    if (hoveredItem === sub) return true;
    if (hoveredItem === projId) return true;
    return false;
  }
  
  // Is anything focused at all?
  const hasFocus = $derived(hoveredItem !== null || hoveredCable !== null);

</script>

<div class="patch-bay">
  <div class="panel-header">
    <div class="label">PATCH BAY (SUBSCRIPTIONS)</div>
    <!-- Debug info -->
    <div style="font-size: 9px; color: #666; margin-left: auto;">
      Proj: {projections.length} | Events: {eventTypes.length}
    </div>
  </div>

  <div class="bay-body">
    <!-- Top Row: Events (Outputs) -->
    <div class="jack-row top-row">
      {#each eventTypes as type, i}
        <div 
          class="jack-unit"
          onmouseenter={() => hoveredItem = type}
          onmouseleave={() => hoveredItem = null}
          class:dimmed={hasFocus && hoveredItem !== type && !eventTypes.includes(hoveredItem ?? '') /* Logic simplification needed */}
          role="group"
        >
          <div class="jack-label" class:highlight={hoveredItem === type}>{type}</div>
          <div class="jack jack--output" class:highlight={hoveredItem === type}></div>
        </div>
      {/each}
    </div>

    <!-- Cables Layer -->
    <svg class="cables-layer">
      {#each projections as proj, projIndex}
        {#each proj.eventSubscriptions as sub}
          {@const eventIndex = eventTypes.indexOf(sub)}
          {@const x1 = getJackX(eventIndex) + 20}
          {@const y1 = 40}
          {@const x2 = getJackX(projIndex) + 20}
          {@const y2 = 260}
          {@const id = `${sub}-${proj.id}`}
          {@const color = getCableColor(sub)}
          {@const related = isRelated(sub, proj.id)}
          
          <path 
            d={getCablePath(x1, y1, x2, y2)}
            stroke={color}
            stroke-width={related && hasFocus ? 3 : 1}
            fill="none"
            class="cable"
            class:faint={hasFocus && !related}
            class:highlight={related && hasFocus}
            onmouseenter={() => hoveredCable = id}
            onmouseleave={() => hoveredCable = null}
          />
        {/each}
      {/each}
    </svg>

    <!-- Bottom Row: Projections (Inputs) -->
    <div class="jack-row bottom-row">
      {#each projections as proj, i}
        <div 
          class="jack-unit"
          onmouseenter={() => hoveredItem = proj.id}
          onmouseleave={() => hoveredItem = null}
          role="group"
        >
          <div class="jack jack--input" class:highlight={hoveredItem === proj.id}></div>
          <div class="jack-label bottom" class:highlight={hoveredItem === proj.id}>{proj.name}</div>
        </div>
      {/each}
    </div>
  </div>
</div>

<style>
  .patch-bay {
    background: #1a1a1a;
    border: 1px solid #333;
    border-radius: 4px;
    padding: 10px;
    display: flex;
    flex-direction: column;
    gap: 10px;
    width: 100%;
    overflow-x: auto;
  }

  .panel-header .label {
    font-size: 10px;
    font-weight: 700;
    color: #666;
    letter-spacing: 1px;
  }

  .bay-body {
    position: relative;
    height: 300px;
    background: #111;
    border-radius: 4px;
    padding: 20px;
    min-width: max-content;
  }

  .jack-row {
    display: flex;
    gap: 10px;
  }

  .top-row {
    position: absolute;
    top: 20px;
    left: 20px;
  }

  .bottom-row {
    position: absolute;
    bottom: 20px;
    left: 20px;
  }

  .jack-unit {
    width: 40px;
    display: flex;
    flex-direction: column;
    align-items: center;
    gap: 5px;
  }

  .jack {
    width: 16px;
    height: 16px;
    border-radius: 50%;
    background: #000;
    border: 2px solid #444;
    box-shadow: inset 0 1px 3px rgba(0,0,0,0.8);
  }

  .jack--output { border-color: #666; }
  .jack--input { border-color: #888; }

  .jack-label {
    font-size: 9px;
    color: #888;
    text-align: center;
    writing-mode: vertical-rl;
    transform: rotate(180deg);
    white-space: nowrap;
    max-height: 60px;
    overflow: hidden;
  }

  .jack-label.bottom {
    writing-mode: vertical-rl;
    transform: none;
  }

  .cables-layer {
    position: absolute;
    inset: 0;
    width: 100%;
    height: 100%;
    pointer-events: none; /* Let clicks pass through, but we need pointer events for hover? */
    border: 1px dotted rgba(255,255,255,0.1); /* DEBUG BOUNDS */
    overflow: visible;
  }

  /* We need pointer events on paths */
  .cable {
    pointer-events: stroke;
    cursor: crosshair;
    transition: all 0.2s ease;
    opacity: 0.5; /* Increased visibility */
  }

  .cable.faint {
    opacity: 0.05;
  }

  .cable.highlight {
    opacity: 1;
    filter: drop-shadow(0 0 5px currentColor);
    z-index: 10;
  }

  .cable:hover {
    opacity: 1;
    stroke-width: 4px;
    z-index: 20;
  }

  .jack-unit:hover .jack {
    border-color: #fff;
    box-shadow: 0 0 8px rgba(255, 255, 255, 0.5);
  }

  .jack-label.highlight {
    color: #fff;
    font-weight: 700;
  }
  
  .jack.highlight {
    background: #444;
    border-color: #fff;
  }
</style>
