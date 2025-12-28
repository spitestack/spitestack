<script lang="ts">
  /**
   * Viewport - Pannable/zoomable container
   * 
   * The window into the studio floor.
   * Drag to pan, scroll to zoom, fly to zones.
   */

  import { viewport, viewportTransform, zones } from '$lib/stores/viewport';

  interface Props {
    children: import('svelte').Snippet;
  }

  let { children }: Props = $props();

  let containerEl: HTMLDivElement;
  let isDragging = $state(false);
  let lastPointer = $state({ x: 0, y: 0 });

  function handlePointerDown(e: PointerEvent) {
    // Only respond to primary button
    if (e.button !== 0) return;
    
    isDragging = true;
    lastPointer = { x: e.clientX, y: e.clientY };
    containerEl.setPointerCapture(e.pointerId);
    viewport.setDragging(true);
  }

  function handlePointerMove(e: PointerEvent) {
    if (!isDragging) return;

    const dx = e.clientX - lastPointer.x;
    const dy = e.clientY - lastPointer.y;
    lastPointer = { x: e.clientX, y: e.clientY };

    viewport.pan(dx, dy);
  }

  function handlePointerUp(e: PointerEvent) {
    isDragging = false;
    containerEl.releasePointerCapture(e.pointerId);
    viewport.setDragging(false);
  }

  function handleWheel(e: WheelEvent) {
    e.preventDefault();
    
    // Get the position relative to viewport center
    const rect = containerEl.getBoundingClientRect();
    const centerX = rect.width / 2;
    const centerY = rect.height / 2;

    // Zoom towards cursor position
    const delta = e.deltaY > 0 ? -0.1 : 0.1;
    viewport.zoomAt(delta, centerX, centerY);
  }

  function handleKeyDown(e: KeyboardEvent) {
    // Zone shortcuts
    const zone = zones.find(z => z.shortcut === e.key);
    if (zone) {
      e.preventDefault();
      viewport.goToZone(zone.id);
      return;
    }

    // Escape to overview
    if (e.key === 'Escape') {
      e.preventDefault();
      viewport.goToZone('overview');
    }

    // Arrow keys to pan
    const panAmount = 50;
    switch (e.key) {
      case 'ArrowUp':
        e.preventDefault();
        viewport.pan(0, -panAmount);
        break;
      case 'ArrowDown':
        e.preventDefault();
        viewport.pan(0, panAmount);
        break;
      case 'ArrowLeft':
        e.preventDefault();
        viewport.pan(-panAmount, 0);
        break;
      case 'ArrowRight':
        e.preventDefault();
        viewport.pan(panAmount, 0);
        break;
      case '+':
      case '=':
        e.preventDefault();
        viewport.zoomIn();
        break;
      case '-':
        e.preventDefault();
        viewport.zoomOut();
        break;
    }
  }

  // Double-click to zoom in
  function handleDoubleClick(e: MouseEvent) {
    const rect = containerEl.getBoundingClientRect();
    const centerX = rect.width / 2;
    const centerY = rect.height / 2;
    viewport.zoomAt(0.3, centerX, centerY);
  }
</script>

<svelte:window onkeydown={handleKeyDown} />

<div 
  class="viewport"
  class:viewport--dragging={isDragging}
  bind:this={containerEl}
  onpointerdown={handlePointerDown}
  onpointermove={handlePointerMove}
  onpointerup={handlePointerUp}
  onpointercancel={handlePointerUp}
  onwheel={handleWheel}
  ondblclick={handleDoubleClick}
  role="application"
  aria-label="Studio floor viewport - drag to pan, scroll to zoom"
  tabindex="0"
>
  <div 
    class="viewport-content"
    style="
      transform: {$viewportTransform.transform};
      transition: {$viewportTransform.transition};
    "
  >
    {@render children()}
  </div>
</div>

<style>
  .viewport {
    position: relative;
    width: 100%;
    height: 100%;
    overflow: hidden;
    background: var(--color-void);
    cursor: grab;
    touch-action: none;
    outline: none;
  }

  .viewport--dragging {
    cursor: grabbing;
  }

  .viewport-content {
    position: absolute;
    top: 50%;
    left: 50%;
    transform-origin: center center;
    will-change: transform;
  }

  /* Focus indicator */
  .viewport:focus-visible {
    box-shadow: inset 0 0 0 2px var(--color-ember);
  }
</style>
