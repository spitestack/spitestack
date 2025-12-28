<script lang="ts">
  /**
   * HelpBar - Bottom shortcut hints
   *
   * Quick reference for keyboard shortcuts.
   * Always visible at the bottom.
   */

  interface Props {
    onShowHelp?: () => void;
    onShowSearch?: () => void;
  }

  let { onShowHelp, onShowSearch }: Props = $props();
</script>

<footer class="help-bar">
  <div class="shortcut-group">
    <button class="shortcut" onclick={onShowHelp}>
      <span class="key">[?]</span>
      <span class="label">Help</span>
    </button>

    <button class="shortcut" onclick={onShowSearch}>
      <span class="key">[/]</span>
      <span class="label">Search</span>
    </button>
  </div>

  <div class="panel-hints">
    <span class="hint"><span class="key">[1-4]</span> Expand panels</span>
    <span class="hint"><span class="key">[Esc]</span> Collapse</span>
  </div>
</footer>

<style>
  .help-bar {
    display: flex;
    justify-content: space-between;
    align-items: center;
    padding: var(--space-sm) var(--space-md);
    background: var(--color-surface);
    border-top: 2px solid var(--color-charcoal);
    flex-shrink: 0;
    z-index: 100;
  }

  .shortcut-group {
    display: flex;
    gap: var(--space-md);
  }

  .shortcut {
    display: flex;
    align-items: center;
    gap: var(--space-xs);
    padding: var(--space-xs) var(--space-sm);
    color: var(--color-text-muted);
    background: transparent;
    border: none;
    border-radius: 2px;
    cursor: pointer;
    transition: var(--transition-fast);
  }

  .shortcut:hover {
    color: var(--color-text);
    background: rgba(255, 255, 255, 0.05);
  }

  .panel-hints {
    display: flex;
    gap: var(--space-lg);
  }

  .hint {
    display: flex;
    align-items: center;
    gap: var(--space-xs);
    font-size: 10px;
    color: var(--color-text-muted);
  }

  .key {
    font-size: 10px;
    font-weight: 500;
    color: var(--color-ember);
  }

  .label {
    font-size: 10px;
  }
</style>
