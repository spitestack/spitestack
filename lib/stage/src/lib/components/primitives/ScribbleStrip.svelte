<script lang="ts">
  /**
   * ScribbleStrip - LCD label display
   * 
   * That backlit strip above each channel fader.
   * Usually has the engineer's handwriting in sharpie.
   * Ours glows with digital precision.
   */

  type BacklightColor = 'cyan' | 'green' | 'amber' | 'white';

  interface Props {
    text: string;
    backlight?: BacklightColor;
    inverted?: boolean;
    maxChars?: number;
  }

  let { 
    text,
    backlight = 'cyan',
    inverted = false,
    maxChars = 8
  }: Props = $props();

  const truncatedText = $derived(
    text.length > maxChars ? text.slice(0, maxChars) : text.padEnd(maxChars, ' ')
  );

  const backlightColors = {
    cyan: { bg: '#e0f7fa', text: '#004d40' },
    green: { bg: '#e8f5e9', text: '#1b5e20' },
    amber: { bg: '#fff8e1', text: '#e65100' },
    white: { bg: '#fafafa', text: '#212121' }
  };
</script>

<div 
  class="scribble-strip"
  class:scribble-strip--inverted={inverted}
  style="
    --strip-bg: {backlightColors[backlight].bg};
    --strip-text: {backlightColors[backlight].text};
  "
>
  <span class="text">{truncatedText.toUpperCase()}</span>
</div>

<style>
  .scribble-strip {
    display: inline-flex;
    align-items: center;
    justify-content: center;
    padding: 2px 4px;
    background: var(--strip-bg);
    color: var(--strip-text);
    font-family: var(--font-mono);
    font-size: 9px;
    font-weight: 600;
    letter-spacing: 0.5px;
    text-transform: uppercase;
    border-radius: 2px;
    box-shadow: 
      inset 0 1px 2px rgba(0, 0, 0, 0.1),
      0 1px 0 rgba(255, 255, 255, 0.1);
    min-width: 48px;
    text-align: center;
  }

  .scribble-strip--inverted {
    background: var(--strip-text);
    color: var(--strip-bg);
  }

  .text {
    white-space: pre;
    font-variant-numeric: tabular-nums;
  }
</style>
