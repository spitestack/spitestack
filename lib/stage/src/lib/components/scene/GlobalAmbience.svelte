<script lang="ts">
  /**
   * GlobalAmbience - The living room tone
   * 
   * A reactive background layer that gives emotional feedback 
   * on the system's state.
   * 
   * - Low traffic: Deep, slow, calm (Void/Blue)
   * - High traffic: Bright, fast, energetic (Ember/Orange)
   * - Error: Unsettled, pulsing (Red)
   */

  import { onMount } from 'svelte';
  import { totalEventsPerSec, healthSummary } from '$lib/stores/telemetry';
  import { spring, tweened } from 'svelte/motion';
  import { cubicOut } from 'svelte/easing';

  // State
  let canvas: HTMLCanvasElement;
  let rafId: number;
  let time = 0;

  // Reactivity to stores
  const activityLevel = spring(0, { stiffness: 0.05, damping: 0.8 });
  const errorLevel = tweened(0, { duration: 1000, easing: cubicOut });

  $effect(() => {
    // Normalize 0-2000 events to 0-1
    activityLevel.set(Math.min(1, $totalEventsPerSec / 2000));
    
    // Error presence (0 or 1)
    errorLevel.set($healthSummary.error > 0 ? 1 : 0);
  });

  // Animation loop
  function animate() {
    if (!canvas) return;
    const ctx = canvas.getContext('2d');
    if (!ctx) return;

    const width = canvas.width = window.innerWidth;
    const height = canvas.height = window.innerHeight;
    
    // Clear
    ctx.fillStyle = '#0A0A0A'; // var(--color-void)
    ctx.fillRect(0, 0, width, height);

    time += 0.005 + ($activityLevel * 0.02); // Speed up with activity

    // Colors
    // Base: Deep Blue/Grey
    // Active: Ember/Orange
    // Error: Blood/Red
    
    const intensity = $activityLevel;
    const error = $errorLevel;

    // Draw Nebula Clouds
    // We'll use a few large radial gradients moving around
    
    const drawOrb = (
      xOffset: number, 
      yOffset: number, 
      sizeBase: number, 
      colorBase: string,
      colorActive: string,
      colorError: string,
      speed: number
    ) => {
      const t = time * speed;
      const x = width * (0.5 + Math.sin(t + xOffset) * 0.3);
      const y = height * (0.5 + Math.cos(t * 1.3 + yOffset) * 0.3);
      const size = Math.min(width, height) * sizeBase * (1 + Math.sin(t * 2) * 0.1);

      const gradient = ctx.createRadialGradient(x, y, 0, x, y, size);
      
      // Interpolate colors (simplified for performance)
      // This is a "feeling" implementation, not exact RGB blending
      let r, g, b;
      
      // Base (Blue-ish Grey to Ember)
      // Very rough approximation of color shifting
      if (error > 0.1) {
        // Red shift
        gradient.addColorStop(0, `rgba(139, 0, 0, ${0.1 + error * 0.1})`); // Blood
        gradient.addColorStop(1, 'rgba(10, 10, 10, 0)');
      } else {
        // Normal to Active
        // 0: #1a1a2e (Deep Blue) -> #cc5500 (Ember)
        const alpha = 0.05 + (intensity * 0.1);
        const r = 26 + (204 - 26) * intensity;
        const g = 26 + (85 - 26) * intensity;
        const b = 46 + (0 - 46) * intensity;
        
        gradient.addColorStop(0, `rgba(${r}, ${g}, ${b}, ${alpha})`);
        gradient.addColorStop(1, 'rgba(10, 10, 10, 0)');
      }

      ctx.fillStyle = gradient;
      ctx.fillRect(0, 0, width, height);
    };

    // Orb 1: The Deep Foundation
    drawOrb(0, 0, 0.8, '', '', '', 0.7);
    
    // Orb 2: The High Energy
    drawOrb(2, 2, 0.6, '', '', '', 1.1);

    // Orb 3: The Chaos (more movement)
    drawOrb(4, 1, 0.5, '', '', '', 1.4);

    // Noise Overlay (simulated via grain in pure CSS usually, but here we just do light effect)
    
    rafId = requestAnimationFrame(animate);
  }

  onMount(() => {
    rafId = requestAnimationFrame(animate);
    return () => cancelAnimationFrame(rafId);
  });
</script>

<canvas bind:this={canvas} class="ambience-canvas"></canvas>

<style>
  .ambience-canvas {
    position: fixed;
    top: 0;
    left: 0;
    width: 100vw;
    height: 100vh;
    z-index: 0; /* Behind everything */
    pointer-events: none;
    opacity: 1;
    transition: opacity 1s ease;
  }
</style>
