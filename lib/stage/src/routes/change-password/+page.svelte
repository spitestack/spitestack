<script lang="ts">
  /**
   * Change Password Page
   *
   * Required when mustChangePassword flag is set.
   * Same equipment aesthetic as login.
   */

  import { goto } from '$app/navigation';
  import { base } from '$app/paths';
  import { auth, needsPasswordChange, isAuthLoading, authError } from '$lib/stores/auth';
  import { onMount } from 'svelte';

  // Form state
  let newPassword = $state('');
  let confirmPassword = $state('');
  let showPassword = $state(false);
  let submitted = $state(false);
  let localError = $state<string | null>(null);

  // Canvas for ambient background
  let canvas: HTMLCanvasElement;
  let rafId: number;

  // Redirect if no password change token
  onMount(() => {
    const unsubscribe = needsPasswordChange.subscribe(needs => {
      if (!needs) {
        goto(`${base}/login`);
      }
    });

    // Start ambient animation
    rafId = requestAnimationFrame(animateAmbience);

    return () => {
      unsubscribe();
      cancelAnimationFrame(rafId);
    };
  });

  // Ambient background animation
  let time = 0;
  function animateAmbience() {
    if (!canvas) {
      rafId = requestAnimationFrame(animateAmbience);
      return;
    }

    const ctx = canvas.getContext('2d');
    if (!ctx) return;

    const width = canvas.width = window.innerWidth;
    const height = canvas.height = window.innerHeight;

    ctx.fillStyle = '#0A0A0A';
    ctx.fillRect(0, 0, width, height);

    time += 0.003;

    const drawOrb = (xOff: number, yOff: number, size: number, alpha: number, speed: number) => {
      const t = time * speed;
      const x = width * (0.5 + Math.sin(t + xOff) * 0.3);
      const y = height * (0.5 + Math.cos(t * 1.3 + yOff) * 0.3);
      const r = Math.min(width, height) * size;

      const gradient = ctx.createRadialGradient(x, y, 0, x, y, r);
      gradient.addColorStop(0, `rgba(204, 85, 0, ${alpha})`);
      gradient.addColorStop(1, 'rgba(10, 10, 10, 0)');

      ctx.fillStyle = gradient;
      ctx.fillRect(0, 0, width, height);
    };

    drawOrb(0, 0, 0.6, 0.03, 0.5);
    drawOrb(2, 2, 0.5, 0.02, 0.7);

    rafId = requestAnimationFrame(animateAmbience);
  }

  async function handleSubmit(e: Event) {
    e.preventDefault();
    submitted = true;
    localError = null;
    auth.clearError();

    if (!newPassword || !confirmPassword) {
      return;
    }

    if (newPassword !== confirmPassword) {
      localError = 'Passwords do not match';
      return;
    }

    if (newPassword.length < 12) {
      localError = 'Password must be at least 12 characters';
      return;
    }

    const success = await auth.changePassword(newPassword);
    if (success) {
      // Password changed - user is now logged in, redirect to dashboard
      goto(`${base}/`);
    }
  }

  function togglePassword() {
    showPassword = !showPassword;
  }

  function cancel() {
    auth.clearPasswordChangeToken();
    goto(`${base}/login`);
  }

  // Combine local and store errors
  const displayError = $derived(localError || $authError);
</script>

<svelte:head>
  <title>Change Password | Spitestack</title>
</svelte:head>

<div class="change-password-page">
  <canvas bind:this={canvas} class="ambience-canvas"></canvas>

  <div class="panel-container">
    <div class="control-panel">
      <!-- Panel Header -->
      <div class="panel-header">
        <div class="led-indicator" class:active={$isAuthLoading}></div>
        <h1 class="panel-title">PASSWORD RESET</h1>
        <div class="led-indicator right" class:error={displayError}></div>
      </div>

      <!-- Equipment Label -->
      <div class="equipment-label">
        <span class="model">SPITE-STAGE</span>
        <span class="divider">|</span>
        <span class="type">AUTH-02</span>
      </div>

      <!-- Info Message -->
      <div class="info-message">
        Your password must be changed before accessing the system.
      </div>

      <!-- Form -->
      <form class="password-form" onsubmit={handleSubmit}>
        <div class="input-group">
          <label class="input-label" for="newPassword">NEW PASSPHRASE</label>
          <div class="input-wrapper">
            <input
              type={showPassword ? 'text' : 'password'}
              id="newPassword"
              bind:value={newPassword}
              class="equipment-input"
              class:error={submitted && !newPassword}
              placeholder="Min 12 characters"
              autocomplete="new-password"
            />
            <div class="input-led" class:active={newPassword.length >= 12}></div>
          </div>
        </div>

        <div class="input-group">
          <label class="input-label" for="confirmPassword">CONFIRM PASSPHRASE</label>
          <div class="input-wrapper">
            <input
              type={showPassword ? 'text' : 'password'}
              id="confirmPassword"
              bind:value={confirmPassword}
              class="equipment-input"
              class:error={submitted && (!confirmPassword || confirmPassword !== newPassword)}
              placeholder="Re-enter passphrase"
              autocomplete="new-password"
            />
            <button
              type="button"
              class="toggle-visibility"
              onclick={togglePassword}
              aria-label={showPassword ? 'Hide password' : 'Show password'}
            >
              {showPassword ? 'HIDE' : 'SHOW'}
            </button>
            <div class="input-led" class:active={confirmPassword && confirmPassword === newPassword}></div>
          </div>
        </div>

        <!-- Error Display -->
        {#if displayError}
          <div class="error-display">
            <span class="error-prefix">ERR:</span>
            <span class="error-message">{displayError}</span>
          </div>
        {/if}

        <!-- Buttons -->
        <div class="button-group">
          <button type="button" class="cancel-button" onclick={cancel}>
            CANCEL
          </button>
          <button type="submit" class="submit-button" disabled={$isAuthLoading}>
            <span class="button-led" class:active={$isAuthLoading}></span>
            <span class="button-text">
              {#if $isAuthLoading}
                UPDATING...
              {:else}
                UPDATE PASSWORD
              {/if}
            </span>
          </button>
        </div>
      </form>

      <!-- Panel Footer -->
      <div class="panel-footer">
        <span class="footer-text">REQUIRED ACTION</span>
        <span class="footer-version">v1.0</span>
      </div>
    </div>

    <!-- Decorative Elements -->
    <div class="panel-screws">
      <div class="screw top-left"></div>
      <div class="screw top-right"></div>
      <div class="screw bottom-left"></div>
      <div class="screw bottom-right"></div>
    </div>
  </div>
</div>

<style>
  .change-password-page {
    position: fixed;
    inset: 0;
    display: flex;
    align-items: center;
    justify-content: center;
    background: var(--color-void);
    font-family: var(--font-mono);
  }

  .ambience-canvas {
    position: absolute;
    top: 0;
    left: 0;
    width: 100%;
    height: 100%;
    z-index: 0;
    pointer-events: none;
  }

  .panel-container {
    position: relative;
    z-index: 1;
  }

  .control-panel {
    width: 380px;
    padding: 32px;
    background: linear-gradient(180deg, #2a2a2a 0%, #1a1a1a 100%);
    border: 2px solid var(--color-charcoal);
    border-radius: 4px;
    box-shadow:
      inset 0 1px 0 rgba(255, 255, 255, 0.05),
      0 20px 60px rgba(0, 0, 0, 0.6),
      0 4px 20px rgba(0, 0, 0, 0.4);
  }

  .panel-header {
    display: flex;
    align-items: center;
    justify-content: center;
    gap: 12px;
    margin-bottom: 8px;
  }

  .panel-title {
    font-size: 16px;
    font-weight: 600;
    letter-spacing: 3px;
    color: var(--color-bone);
    text-transform: uppercase;
  }

  .led-indicator {
    width: 8px;
    height: 8px;
    border-radius: 50%;
    background: #333;
    border: 1px solid #222;
    box-shadow: inset 0 1px 2px rgba(0, 0, 0, 0.5);
    transition: all 200ms ease;
  }

  .led-indicator.active {
    background: var(--color-led-amber);
    box-shadow:
      inset 0 1px 2px rgba(0, 0, 0, 0.3),
      0 0 8px var(--color-led-amber),
      0 0 16px rgba(255, 140, 0, 0.3);
  }

  .led-indicator.error {
    background: var(--color-led-red);
    box-shadow:
      inset 0 1px 2px rgba(0, 0, 0, 0.3),
      0 0 8px var(--color-led-red),
      0 0 16px rgba(255, 0, 0, 0.3);
  }

  .equipment-label {
    display: flex;
    align-items: center;
    justify-content: center;
    gap: 8px;
    margin-bottom: 16px;
    padding: 6px 0;
    font-size: 10px;
    color: var(--color-ash);
    letter-spacing: 1px;
    text-transform: uppercase;
    border-top: 1px solid #333;
    border-bottom: 1px solid #333;
  }

  .equipment-label .divider {
    color: #444;
  }

  .info-message {
    margin-bottom: 20px;
    padding: 12px;
    font-size: 11px;
    color: var(--color-bone);
    background: rgba(204, 85, 0, 0.1);
    border: 1px solid var(--color-ember);
    border-radius: 2px;
    text-align: center;
  }

  .password-form {
    display: flex;
    flex-direction: column;
    gap: 20px;
  }

  .input-group {
    display: flex;
    flex-direction: column;
    gap: 6px;
  }

  .input-label {
    font-size: 10px;
    font-weight: 500;
    letter-spacing: 2px;
    color: var(--color-ash);
    text-transform: uppercase;
  }

  .input-wrapper {
    position: relative;
    display: flex;
    align-items: center;
  }

  .equipment-input {
    flex: 1;
    padding: 12px 16px;
    padding-right: 40px;
    font-family: var(--font-mono);
    font-size: 14px;
    color: var(--color-bone);
    background: var(--color-void);
    border: 1px solid #333;
    border-radius: 2px;
    outline: none;
    box-shadow: inset 0 2px 4px rgba(0, 0, 0, 0.5);
    transition: all 200ms ease;
  }

  .equipment-input::placeholder {
    color: #444;
  }

  .equipment-input:focus {
    border-color: var(--color-ember);
    box-shadow:
      inset 0 2px 4px rgba(0, 0, 0, 0.5),
      0 0 0 2px rgba(204, 85, 0, 0.2);
  }

  .equipment-input.error {
    border-color: var(--color-blood);
    box-shadow:
      inset 0 2px 4px rgba(0, 0, 0, 0.5),
      0 0 0 2px rgba(139, 0, 0, 0.2);
  }

  .input-led {
    position: absolute;
    right: 12px;
    width: 6px;
    height: 6px;
    border-radius: 50%;
    background: #333;
    border: 1px solid #222;
    transition: all 200ms ease;
  }

  .input-led.active {
    background: var(--color-led-green);
    box-shadow: 0 0 6px var(--color-led-green);
  }

  .toggle-visibility {
    position: absolute;
    right: 28px;
    padding: 4px 6px;
    font-size: 8px;
    font-weight: 600;
    letter-spacing: 1px;
    color: var(--color-ash);
    background: transparent;
    border: 1px solid #444;
    border-radius: 2px;
    cursor: pointer;
    transition: all 150ms ease;
  }

  .toggle-visibility:hover {
    color: var(--color-bone);
    border-color: var(--color-ember);
    background: rgba(204, 85, 0, 0.1);
  }

  .error-display {
    padding: 12px;
    background: rgba(139, 0, 0, 0.15);
    border: 1px solid var(--color-blood);
    border-radius: 2px;
    font-size: 11px;
  }

  .error-prefix {
    color: var(--color-blood);
    font-weight: 600;
    margin-right: 6px;
  }

  .error-message {
    color: var(--color-bone);
  }

  .button-group {
    display: flex;
    gap: 12px;
    margin-top: 8px;
  }

  .cancel-button {
    flex: 1;
    padding: 14px 24px;
    font-family: var(--font-mono);
    font-size: 12px;
    font-weight: 600;
    letter-spacing: 2px;
    color: var(--color-ash);
    background: transparent;
    border: 1px solid #444;
    border-radius: 2px;
    cursor: pointer;
    transition: all 150ms ease;
  }

  .cancel-button:hover {
    color: var(--color-bone);
    border-color: #666;
  }

  .submit-button {
    flex: 2;
    display: flex;
    align-items: center;
    justify-content: center;
    gap: 12px;
    padding: 14px 24px;
    font-family: var(--font-mono);
    font-size: 12px;
    font-weight: 600;
    letter-spacing: 2px;
    color: var(--color-bone);
    background: linear-gradient(180deg, #333 0%, #222 100%);
    border: 1px solid #444;
    border-radius: 2px;
    cursor: pointer;
    box-shadow:
      inset 0 1px 0 rgba(255, 255, 255, 0.05),
      0 2px 8px rgba(0, 0, 0, 0.3);
    transition: all 150ms ease;
  }

  .submit-button:hover:not(:disabled) {
    background: linear-gradient(180deg, #3d3d3d 0%, #2a2a2a 100%);
    border-color: var(--color-ember);
  }

  .submit-button:disabled {
    cursor: not-allowed;
    opacity: 0.7;
  }

  .button-led {
    width: 8px;
    height: 8px;
    border-radius: 50%;
    background: #333;
    border: 1px solid #222;
    transition: all 200ms ease;
  }

  .button-led.active {
    background: var(--color-led-amber);
    box-shadow: 0 0 8px var(--color-led-amber);
    animation: pulse-loading 1s ease-in-out infinite;
  }

  @keyframes pulse-loading {
    0%, 100% { opacity: 1; }
    50% { opacity: 0.4; }
  }

  .button-text {
    text-transform: uppercase;
  }

  .panel-footer {
    display: flex;
    justify-content: space-between;
    margin-top: 24px;
    padding-top: 12px;
    border-top: 1px solid #333;
    font-size: 9px;
    color: #444;
    letter-spacing: 1px;
    text-transform: uppercase;
  }

  .panel-screws {
    position: absolute;
    inset: 6px;
    pointer-events: none;
  }

  .screw {
    position: absolute;
    width: 8px;
    height: 8px;
    background: linear-gradient(135deg, #555 0%, #333 50%, #444 100%);
    border-radius: 50%;
    box-shadow:
      inset 0 1px 1px rgba(255, 255, 255, 0.1),
      inset 0 -1px 1px rgba(0, 0, 0, 0.3);
  }

  .screw::after {
    content: '';
    position: absolute;
    top: 50%;
    left: 50%;
    width: 5px;
    height: 1px;
    background: #333;
    transform: translate(-50%, -50%);
  }

  .screw.top-left { top: 0; left: 0; }
  .screw.top-right { top: 0; right: 0; }
  .screw.bottom-left { bottom: 0; left: 0; }
  .screw.bottom-right { bottom: 0; right: 0; }
</style>
