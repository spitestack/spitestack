<script lang="ts">
  /**
   * DashboardShell - Main application layout
   *
   * Fixed grid layout with expandable panels.
   * Center = System Overview, corners = Console, Racks, Timeline, Logs
   */

  import StatusBar from './StatusBar.svelte';
  import HelpBar from './HelpBar.svelte';
  import Panel from './Panel.svelte';
  import { MixingConsole, RackPanel, SystemOverview, LogViewer, TapeDeck, PatchBay } from '$components/equipment';
  import GlobalAmbience from '$components/scene/GlobalAmbience.svelte';
  import { telemetry, projectionsList } from '$lib/stores/telemetry';
  import { ui, type PanelId } from '$lib/stores/ui';
  import { onMount } from 'svelte';

  // State for overlays (separate from panel expansion)
  let activeOverlay = $state<'help' | 'search' | null>(null);
  let selectedProjection = $state<string | undefined>(undefined);
  let showPatchBay = $state(false);

  // Subscribe to UI store for panel expansion
  let expandedPanel = $state<PanelId | null>(null);
  $effect(() => {
    const unsub = ui.subscribe(state => {
      expandedPanel = state.expandedPanel;
    });
    return unsub;
  });

  // Get projections from store
  const projections = $derived($projectionsList);

  // Map projections to rack services format for display
  const rackServices = $derived(projections.map(p => ({
    id: p.id,
    name: p.name.toUpperCase(),
    health: p.health as 'healthy' | 'warning' | 'error' | 'offline',
    activity: Math.min(100, p.eventsPerSec), // Cap at 100 for display
    load: Math.min(100, Math.round(p.avgLatencyMs / 10)), // Normalize latency to 0-100
  })));

  // Panel hotkey mapping
  const panelKeys: Record<string, PanelId> = {
    '1': 'console',
    '2': 'racks',
    '3': 'timeline',
    '4': 'logs',
  };

  // Keyboard shortcuts
  function handleKeyDown(e: KeyboardEvent) {
    // Don't capture if in an input
    if (e.target instanceof HTMLInputElement || e.target instanceof HTMLTextAreaElement) {
      return;
    }

    // Panel hotkeys 1-4
    if (panelKeys[e.key]) {
      e.preventDefault();
      ui.togglePanel(panelKeys[e.key]);
      return;
    }

    switch (e.key) {
      case '?':
        e.preventDefault();
        toggleOverlay('help');
        break;
      case '/':
        e.preventDefault();
        toggleOverlay('search');
        break;
      case 'p':
      case 'P':
        e.preventDefault();
        showPatchBay = !showPatchBay;
        // Auto-expand if showing patch bay
        if (showPatchBay && expandedPanel !== 'racks') {
          ui.togglePanel('racks');
        }
        break;
      case 'Escape':
        e.preventDefault();
        // First collapse expanded panel, then close overlay
        if (expandedPanel) {
          ui.collapsePanel();
        } else if (activeOverlay) {
          activeOverlay = null;
        }
        break;
    }
  }

  function toggleOverlay(overlay: typeof activeOverlay) {
    activeOverlay = activeOverlay === overlay ? null : overlay;
  }

  // Connect to backend API and WebSocket
  onMount(() => {
    telemetry.connect().catch(err => {
      telemetry.setError(err.message);
    });

    return () => telemetry.disconnect();
  });
</script>

<svelte:window onkeydown={handleKeyDown} />

<div class="dashboard-shell">
  <GlobalAmbience />
  <div style="position: relative; z-index: 1;">
    <StatusBar expandedPanelName={expandedPanel ? expandedPanel.toUpperCase() : null} />
  </div>

  <main class="grid-content">
    <!-- Upper Left: Console (Mixing Console) -->
    <Panel
      title="CONSOLE"
      hotkey="1"
      position="ul"
      expanded={expandedPanel === 'console'}
      onToggle={() => ui.togglePanel('console')}
    >
      {#snippet children()}
        <MixingConsole
          {projections}
          selectedId={selectedProjection}
          onselect={(id: string) => { selectedProjection = id; }}
        />
      {/snippet}
    </Panel>

    <!-- Center: System Overview -->
    <Panel title="SYSTEM OVERVIEW" position="center">
      {#snippet children()}
        <SystemOverview />
      {/snippet}
    </Panel>

    <!-- Upper Right: Racks (Infrastructure) / Patch Bay -->
    <Panel
      title={showPatchBay ? "PATCH BAY" : "RACKS"}
      hotkey="2"
      position="ur"
      expanded={expandedPanel === 'racks'}
      onToggle={() => ui.togglePanel('racks')}
    >
      {#snippet children()}
        {#if showPatchBay}
          <PatchBay />
        {:else}
          <RackPanel title="PROJECTIONS" services={rackServices} />
        {/if}
      {/snippet}
    </Panel>

    <!-- Lower Left: Timeline (TapeDeck) -->
    <Panel
      title="TIMELINE"
      hotkey="3"
      position="ll"
      expanded={expandedPanel === 'timeline'}
      onToggle={() => ui.togglePanel('timeline')}
    >
      {#snippet children()}
        <TapeDeck />
      {/snippet}
    </Panel>

    <!-- Lower Right: Logs -->
    <Panel
      title="LOGS"
      hotkey="4"
      position="lr"
      expanded={expandedPanel === 'logs'}
      onToggle={() => ui.togglePanel('logs')}
    >
      {#snippet children()}
        <LogViewer />
      {/snippet}
    </Panel>
  </main>

  <HelpBar
    onShowHelp={() => toggleOverlay('help')}
    onShowSearch={() => toggleOverlay('search')}
  />

  <!-- Overlay container -->
  {#if activeOverlay}
    <div class="overlay-backdrop" onclick={() => { activeOverlay = null; }}>
      <div class="overlay-content" onclick={(e) => e.stopPropagation()}>
        {#if activeOverlay === 'help'}
          <div class="overlay-panel">
            <h2 class="overlay-title">Keyboard Shortcuts</h2>
            <div class="shortcut-list">
              <div class="shortcut-group">
                <h3>Panels</h3>
                <div class="shortcut-item"><span class="key">1</span> Console</div>
                <div class="shortcut-item"><span class="key">2</span> Racks</div>
                <div class="shortcut-item"><span class="key">3</span> Timeline</div>
                <div class="shortcut-item"><span class="key">4</span> Logs</div>
                <div class="shortcut-item"><span class="key">Esc</span> Collapse</div>
              </div>
              <div class="shortcut-group">
                <h3>Overlays</h3>
                <div class="shortcut-item"><span class="key">?</span> Help</div>
                <div class="shortcut-item"><span class="key">/</span> Search</div>
                <div class="shortcut-item"><span class="key">P</span> Patch Bay</div>
                <div class="shortcut-item"><span class="key">Esc</span> Close</div>
              </div>
            </div>
          </div>
        {:else if activeOverlay === 'search'}
          <div class="overlay-panel">
            <h2 class="overlay-title">Search</h2>
            <input
              type="text"
              class="search-input"
              placeholder="Search projections, events, logs..."
              autofocus
            />
          </div>
        {/if}
      </div>
    </div>
  {/if}
</div>

<style>
  .dashboard-shell {
    display: flex;
    flex-direction: column;
    height: 100vh;
    /* background: var(--color-bg); Removed to let Ambience show through */
  }

  .grid-content {
    position: relative;
    z-index: 1;
    flex: 1;
    display: grid;
    grid-template-columns: 1fr 2fr 1fr;
    grid-template-rows: 1fr 1fr;
    grid-template-areas:
      "ul center ur"
      "ll center lr";
    gap: 2px;
    padding: 2px;
    min-height: 0;
    /* background: var(--color-void);  Removed to let Ambience show through */
  }

  /* Overlay styles */
  .overlay-backdrop {
    position: fixed;
    inset: 0;
    background: rgba(0, 0, 0, 0.7);
    display: flex;
    align-items: center;
    justify-content: center;
    z-index: 1000;
    backdrop-filter: blur(4px);
  }

  .overlay-content {
    max-width: 90vw;
    max-height: 90vh;
    overflow: auto;
  }

  .overlay-panel {
    background: var(--color-surface);
    border: 2px solid var(--color-charcoal);
    border-radius: 8px;
    padding: var(--space-lg);
    min-width: 300px;
    box-shadow: 0 20px 40px rgba(0, 0, 0, 0.5);
  }

  .overlay-title {
    font-size: 14px;
    font-weight: 600;
    letter-spacing: 1px;
    color: var(--color-text);
    margin-bottom: var(--space-lg);
    padding-bottom: var(--space-sm);
    border-bottom: 1px solid var(--color-charcoal);
  }

  .shortcut-list {
    display: flex;
    gap: var(--space-xl);
  }

  .shortcut-group h3 {
    font-size: 11px;
    font-weight: 600;
    color: var(--color-text-muted);
    margin-bottom: var(--space-sm);
    text-transform: uppercase;
    letter-spacing: 1px;
  }

  .shortcut-item {
    display: flex;
    align-items: center;
    gap: var(--space-sm);
    padding: var(--space-xs) 0;
    font-size: 12px;
    color: var(--color-text-muted);
  }

  .shortcut-item .key {
    display: inline-block;
    min-width: 50px;
    padding: 2px 6px;
    background: var(--color-void);
    border: 1px solid var(--color-charcoal);
    border-radius: 3px;
    font-size: 10px;
    font-weight: 500;
    color: var(--color-ember);
    text-align: center;
  }

  .search-input {
    width: 100%;
    padding: var(--space-sm) var(--space-md);
    font-family: var(--font-mono);
    font-size: 14px;
    color: var(--color-text);
    background: var(--color-void);
    border: 1px solid var(--color-charcoal);
    border-radius: 4px;
    outline: none;
  }

  .search-input:focus {
    border-color: var(--color-ember);
    box-shadow: 0 0 0 2px rgba(204, 85, 0, 0.2);
  }

  .search-input::placeholder {
    color: var(--color-text-muted);
  }
</style>
