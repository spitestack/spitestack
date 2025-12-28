/**
 * UI Store
 * 
 * Manages UI state like panel expansion, overlays, selection.
 */

import { writable, derived } from 'svelte/store';

export type PanelId = 'console' | 'racks' | 'timeline' | 'logs';
export type OverlayId = 'help' | 'search' | null;

interface UIState {
  expandedPanel: PanelId | null;
  activeOverlay: OverlayId;
  selectedProjection: string | null;
}

function createUIStore() {
  const { subscribe, set, update } = writable<UIState>({
    expandedPanel: null,
    activeOverlay: null,
    selectedProjection: null,
  });

  return {
    subscribe,

    // Toggle panel expansion
    togglePanel: (panelId: PanelId) => {
      update(state => ({
        ...state,
        expandedPanel: state.expandedPanel === panelId ? null : panelId,
      }));
    },

    // Expand specific panel
    expandPanel: (panelId: PanelId) => {
      update(state => ({ ...state, expandedPanel: panelId }));
    },

    // Collapse all panels
    collapsePanel: () => {
      update(state => ({ ...state, expandedPanel: null }));
    },

    // Toggle overlay
    toggleOverlay: (overlayId: OverlayId) => {
      update(state => ({
        ...state,
        activeOverlay: state.activeOverlay === overlayId ? null : overlayId,
      }));
    },

    // Close overlay
    closeOverlay: () => {
      update(state => ({ ...state, activeOverlay: null }));
    },

    // Select projection
    selectProjection: (id: string | null) => {
      update(state => ({ ...state, selectedProjection: id }));
    },
  };
}

export const ui = createUIStore();

// Derived stores
export const isAnyPanelExpanded = derived(ui, $ui => $ui.expandedPanel !== null);
export const expandedPanelName = derived(ui, $ui => {
  switch ($ui.expandedPanel) {
    case 'console': return 'CONSOLE';
    case 'racks': return 'RACKS';
    case 'timeline': return 'TIMELINE';
    case 'logs': return 'LOGS';
    default: return null;
  }
});
