/**
 * Viewport Store
 * 
 * Manages the pan/zoom state and zone navigation.
 * Like camera controls for a 2D studio floor.
 */

import { writable, derived } from 'svelte/store';

export interface ViewportPosition {
  x: number;
  y: number;
  zoom: number;
}

export interface Zone {
  id: string;
  name: string;
  shortcut: string;
  x: number;
  y: number;
  zoom: number;
}

interface ViewportState {
  position: ViewportPosition;
  isDragging: boolean;
  isAnimating: boolean;
  currentZone: string | null;
}

// Predefined zones for the studio floor
export const zones: Zone[] = [
  { id: 'console', name: 'Mixing Console', shortcut: '1', x: 400, y: 300, zoom: 1 },
  { id: 'racks', name: 'Racks', shortcut: '2', x: 1000, y: 300, zoom: 1 },
  { id: 'tape', name: 'Tape Deck', shortcut: '3', x: 700, y: 600, zoom: 1.2 },
  { id: 'patch', name: 'Patch Bay', shortcut: '4', x: 1300, y: 300, zoom: 1 },
  { id: 'overview', name: 'Overview', shortcut: '0', x: 700, y: 400, zoom: 0.6 },
];

// Viewport constraints
const ZOOM_MIN = 0.3;
const ZOOM_MAX = 2;
const ZOOM_STEP = 0.1;

function createViewportStore() {
  const { subscribe, set, update } = writable<ViewportState>({
    position: { x: 400, y: 300, zoom: 1 },
    isDragging: false,
    isAnimating: false,
    currentZone: 'console',
  });

  return {
    subscribe,

    // Set position directly (no animation)
    setPosition: (pos: Partial<ViewportPosition>) => {
      update(state => ({
        ...state,
        position: {
          ...state.position,
          ...pos,
          zoom: pos.zoom ? Math.max(ZOOM_MIN, Math.min(ZOOM_MAX, pos.zoom)) : state.position.zoom,
        },
        currentZone: null,
      }));
    },

    // Pan by delta
    pan: (dx: number, dy: number) => {
      update(state => ({
        ...state,
        position: {
          ...state.position,
          x: state.position.x - dx / state.position.zoom,
          y: state.position.y - dy / state.position.zoom,
        },
        currentZone: null,
      }));
    },

    // Zoom at point
    zoomAt: (delta: number, centerX: number, centerY: number) => {
      update(state => {
        const newZoom = Math.max(ZOOM_MIN, Math.min(ZOOM_MAX, state.position.zoom + delta));
        const zoomRatio = newZoom / state.position.zoom;
        
        // Adjust position to zoom towards the center point
        const newX = centerX - (centerX - state.position.x) * zoomRatio;
        const newY = centerY - (centerY - state.position.y) * zoomRatio;

        return {
          ...state,
          position: { x: newX, y: newY, zoom: newZoom },
          currentZone: null,
        };
      });
    },

    // Zoom in/out
    zoomIn: () => {
      update(state => ({
        ...state,
        position: {
          ...state.position,
          zoom: Math.min(ZOOM_MAX, state.position.zoom + ZOOM_STEP),
        },
      }));
    },

    zoomOut: () => {
      update(state => ({
        ...state,
        position: {
          ...state.position,
          zoom: Math.max(ZOOM_MIN, state.position.zoom - ZOOM_STEP),
        },
      }));
    },

    // Navigate to zone with animation
    goToZone: (zoneId: string) => {
      const zone = zones.find(z => z.id === zoneId);
      if (!zone) return;

      update(state => ({
        ...state,
        position: { x: zone.x, y: zone.y, zoom: zone.zoom },
        isAnimating: true,
        currentZone: zoneId,
      }));

      // Clear animation flag after transition
      setTimeout(() => {
        update(state => ({ ...state, isAnimating: false }));
      }, 500);
    },

    // Set dragging state
    setDragging: (isDragging: boolean) => {
      update(state => ({ ...state, isDragging }));
    },

    // Reset to default view
    reset: () => {
      update(state => ({
        ...state,
        position: { x: 400, y: 300, zoom: 1 },
        currentZone: 'console',
        isAnimating: true,
      }));

      setTimeout(() => {
        update(state => ({ ...state, isAnimating: false }));
      }, 500);
    },
  };
}

export const viewport = createViewportStore();

// Derived store for transform CSS
export const viewportTransform = derived(viewport, $viewport => {
  const { x, y, zoom } = $viewport.position;
  return {
    transform: `scale(${zoom}) translate(${-x}px, ${-y}px)`,
    transition: $viewport.isAnimating ? 'transform 0.5s cubic-bezier(0.34, 1.56, 0.64, 1)' : 'none',
  };
});
