<script lang="ts">
  /**
   * TapeDeck - Event timeline and replay controls
   * 
   * Like a reel-to-reel tape machine for your events.
   * Transport controls. Time code display.
   * Rewind to any point in history.
   */

  import { SegmentDisplay, LED } from '$components/primitives';
  import { telemetry } from '$lib/stores/telemetry';

  interface Props {
    isPlaying?: boolean;
    position?: number;
    onPlay?: () => void;
    onPause?: () => void;
    onStop?: () => void;
    onRewind?: () => void;
    onFastForward?: () => void;
  }

  let {
    isPlaying = $bindable(true),
    position = 0,
    onPlay,
    onPause,
    onStop,
    onRewind,
    onFastForward
  }: Props = $props();

  let totalEvents = $derived.by(() => {
    let value = 0;
    telemetry.subscribe(state => { value = state.totalEvents; })();
    return value;
  });

  // Format position as timecode HH:MM:SS:FF
  function formatTimecode(events: number): string {
    // Pretend each event takes ~1ms, convert to timecode
    const totalMs = events;
    const hours = Math.floor(totalMs / 3600000);
    const minutes = Math.floor((totalMs % 3600000) / 60000);
    const seconds = Math.floor((totalMs % 60000) / 1000);
    const frames = Math.floor((totalMs % 1000) / 40); // 25fps
    
    return `${hours.toString().padStart(2, '0')}:${minutes.toString().padStart(2, '0')}:${seconds.toString().padStart(2, '0')}:${frames.toString().padStart(2, '0')}`;
  }
</script>

<div class="tape-deck">
  <!-- Reels -->
  <div class="reels">
    <div class="reel reel--left" class:spinning={isPlaying}>
      <div class="reel-hub"></div>
      <div class="reel-spokes">
        <div class="spoke"></div>
        <div class="spoke"></div>
        <div class="spoke"></div>
      </div>
    </div>

    <div class="tape-path">
      <div class="tape-guide"></div>
      <div class="tape-head">
        <LED color="amber" on={isPlaying} size="sm" blinking={isPlaying} />
      </div>
      <div class="tape-guide"></div>
    </div>

    <div class="reel reel--right" class:spinning={isPlaying}>
      <div class="reel-hub"></div>
      <div class="reel-spokes">
        <div class="spoke"></div>
        <div class="spoke"></div>
        <div class="spoke"></div>
      </div>
    </div>
  </div>

  <!-- Counter display -->
  <div class="counter-section">
    <div class="counter-label">POSITION</div>
    <div class="counter-display">
      <SegmentDisplay value={formatTimecode(totalEvents)} digits={11} color="amber" />
    </div>
  </div>

  <!-- Transport controls -->
  <div class="transport">
    <button 
      class="transport-btn" 
      onclick={onRewind}
      title="Rewind"
    >
      ⏪
    </button>
    
    {#if isPlaying}
      <button 
        class="transport-btn transport-btn--active" 
        onclick={() => { isPlaying = false; onPause?.(); }}
        title="Pause"
      >
        ⏸
      </button>
    {:else}
      <button 
        class="transport-btn" 
        onclick={() => { isPlaying = true; onPlay?.(); }}
        title="Play"
      >
        ▶
      </button>
    {/if}
    
    <button 
      class="transport-btn" 
      onclick={() => { isPlaying = false; onStop?.(); }}
      title="Stop"
    >
      ⏹
    </button>
    
    <button 
      class="transport-btn" 
      onclick={onFastForward}
      title="Fast Forward"
    >
      ⏩
    </button>
  </div>

  <!-- Status line -->
  <div class="status-line">
    <span class="status-item">
      <LED color={isPlaying ? 'green' : 'red'} on={true} size="sm" />
      <span>{isPlaying ? 'LIVE' : 'PAUSED'}</span>
    </span>
    <span class="status-divider">|</span>
    <span class="status-item">
      EVENTS: {totalEvents.toLocaleString()}
    </span>
  </div>
</div>

<style>
  .tape-deck {
    display: flex;
    flex-direction: column;
    align-items: center;
    gap: var(--space-md);
    padding: var(--space-md);
    background: linear-gradient(180deg, #252525 0%, #1a1a1a 100%);
    border: 2px solid var(--color-charcoal);
    border-radius: 8px;
    box-shadow: var(--shadow-equipment);
  }

  /* Reels */
  .reels {
    display: flex;
    align-items: center;
    gap: var(--space-lg);
  }

  .reel {
    position: relative;
    width: 80px;
    height: 80px;
    border-radius: 50%;
    background: linear-gradient(135deg, #2a2a2a 0%, #1a1a1a 50%, #2a2a2a 100%);
    border: 3px solid #333;
    box-shadow: 
      inset 0 0 20px rgba(0, 0, 0, 0.5),
      0 4px 8px rgba(0, 0, 0, 0.3);
  }

  .reel.spinning {
    animation: spin 1.5s linear infinite;
  }

  @keyframes spin {
    from { transform: rotate(0deg); }
    to { transform: rotate(360deg); }
  }

  .reel-hub {
    position: absolute;
    top: 50%;
    left: 50%;
    transform: translate(-50%, -50%);
    width: 20px;
    height: 20px;
    border-radius: 50%;
    background: var(--color-charcoal);
    border: 2px solid #444;
  }

  .reel-spokes {
    position: absolute;
    inset: 10px;
  }

  .spoke {
    position: absolute;
    top: 50%;
    left: 50%;
    width: 100%;
    height: 2px;
    background: #333;
    transform-origin: center;
  }

  .spoke:nth-child(1) { transform: translate(-50%, -50%) rotate(0deg); }
  .spoke:nth-child(2) { transform: translate(-50%, -50%) rotate(60deg); }
  .spoke:nth-child(3) { transform: translate(-50%, -50%) rotate(120deg); }

  /* Tape path */
  .tape-path {
    display: flex;
    flex-direction: column;
    align-items: center;
    gap: var(--space-xs);
  }

  .tape-guide {
    width: 40px;
    height: 4px;
    background: #333;
    border-radius: 2px;
  }

  .tape-head {
    padding: var(--space-xs);
    background: var(--color-charcoal);
    border: 1px solid #444;
    border-radius: 2px;
  }

  /* Counter */
  .counter-section {
    text-align: center;
  }

  .counter-label {
    font-size: 9px;
    font-weight: 600;
    letter-spacing: 1px;
    color: var(--color-text-muted);
    margin-bottom: var(--space-xs);
  }

  /* Transport controls */
  .transport {
    display: flex;
    gap: var(--space-sm);
  }

  .transport-btn {
    width: 40px;
    height: 32px;
    font-size: 14px;
    color: var(--color-text-muted);
    background: linear-gradient(180deg, #3a3a3a 0%, #2a2a2a 100%);
    border: 1px solid #444;
    border-radius: 4px;
    cursor: pointer;
    transition: var(--transition-fast);
    display: flex;
    align-items: center;
    justify-content: center;
  }

  .transport-btn:hover {
    color: var(--color-text);
    border-color: var(--color-ember);
    background: linear-gradient(180deg, #4a4a4a 0%, #3a3a3a 100%);
  }

  .transport-btn--active {
    color: var(--color-ember);
    border-color: var(--color-ember);
    box-shadow: 0 0 8px rgba(204, 85, 0, 0.3);
  }

  /* Status line */
  .status-line {
    display: flex;
    align-items: center;
    gap: var(--space-sm);
    font-size: 10px;
    color: var(--color-text-muted);
  }

  .status-item {
    display: flex;
    align-items: center;
    gap: var(--space-xs);
  }

  .status-divider {
    opacity: 0.3;
  }
</style>
