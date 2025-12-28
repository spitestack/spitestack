<script lang="ts">
  /**
   * Panel - Expandable panel wrapper
   * 
   * Click the header to expand to full screen.
   * Each panel is a window into a different part of the system.
   */

  interface Props {
    title: string;
    hotkey?: string;
    expanded?: boolean;
    position: 'ul' | 'ur' | 'll' | 'lr' | 'center';
    onToggle?: () => void;
    children: import('svelte').Snippet;
  }

  let { 
    title,
    hotkey,
    expanded = false,
    position,
    onToggle,
    children
  }: Props = $props();

  const isExpandable = position !== 'center';
</script>

<article 
  class="panel panel--{position}"
  class:panel--expanded={expanded}
  role="region"
  aria-label={title}
  aria-expanded={expanded}
>
  {#if isExpandable}
    <header 
      class="panel-header"
      onclick={onToggle}
      onkeydown={(e) => e.key === 'Enter' && onToggle?.()}
      role="button"
      tabindex="0"
      aria-label="{expanded ? 'Collapse' : 'Expand'} {title}"
    >
      <span class="panel-title">{title}</span>
      {#if hotkey}
        <span class="panel-hotkey">[{hotkey}]</span>
      {/if}
      <span class="panel-expand-icon">{expanded ? '×' : '⤢'}</span>
    </header>
  {/if}

  <div class="panel-content" class:panel-content--scrollable={isExpandable}>
    {@render children()}
  </div>
</article>

<style>
  .panel {
    display: flex;
    flex-direction: column;
    background: var(--color-surface);
    border: 1px solid var(--color-charcoal);
    border-radius: 4px;
    overflow: hidden;
    transition: all 0.3s cubic-bezier(0.34, 1.56, 0.64, 1);
  }

  /* Grid positions */
  .panel--ul { grid-area: ul; }
  .panel--ur { grid-area: ur; }
  .panel--ll { grid-area: ll; }
  .panel--lr { grid-area: lr; }
  .panel--center { 
    grid-area: center;
    border-color: var(--color-maroon);
  }

  /* Expanded state */
  .panel--expanded {
    position: fixed;
    inset: 48px 0 40px 0;
    z-index: 100;
    border-radius: 0;
    border-left: none;
    border-right: none;
  }

  /* Panel header */
  .panel-header {
    display: flex;
    align-items: center;
    gap: var(--space-sm);
    padding: var(--space-xs) var(--space-sm);
    background: linear-gradient(180deg, var(--color-charcoal) 0%, #181818 100%);
    border-bottom: 1px solid #333;
    cursor: pointer;
    user-select: none;
    transition: var(--transition-fast);
  }

  .panel-header:hover {
    background: linear-gradient(180deg, #2a2a2a 0%, #1f1f1f 100%);
  }

  .panel-header:focus-visible {
    outline: 2px solid var(--color-ember);
    outline-offset: -2px;
  }

  .panel-title {
    flex: 1;
    font-size: 10px;
    font-weight: 600;
    letter-spacing: 1px;
    color: var(--color-bone);
    text-transform: uppercase;
  }

  .panel-hotkey {
    font-size: 9px;
    font-weight: 500;
    color: var(--color-ember);
    opacity: 0.8;
  }

  .panel-expand-icon {
    font-size: 12px;
    color: var(--color-ash);
    transition: var(--transition-fast);
  }

  .panel-header:hover .panel-expand-icon {
    color: var(--color-text);
  }

  /* Panel content */
  .panel-content {
    flex: 1;
    padding: var(--space-sm);
    min-height: 0;
  }

  .panel-content--scrollable {
    overflow: auto;
  }

  /* Expanded content gets more padding */
  .panel--expanded .panel-content {
    padding: var(--space-md);
  }
</style>
