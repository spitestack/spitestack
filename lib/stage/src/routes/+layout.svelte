<script lang="ts">
  import '../app.css';
  import { onMount } from 'svelte';
  import { goto } from '$app/navigation';
  import { page } from '$app/stores';
  import { base } from '$app/paths';
  import { auth, isAuthenticated } from '$lib/stores/auth';

  let { children } = $props();
  let authChecked = $state(false);

  // Check authentication state and handle routing
  onMount(() => {
    // Check stored auth on mount
    auth.checkAuth().then(() => {
      authChecked = true;
    }).catch(() => {
      authChecked = true;
    });
  });

  // React to auth state and route changes
  $effect(() => {
    if (!authChecked) return;

    const currentPath = $page.url.pathname;
    const isLoginPage = currentPath === `${base}/login`;
    const isChangePasswordPage = currentPath === `${base}/change-password`;
    const authenticated = $isAuthenticated;

    if (authenticated && isLoginPage) {
      // Authenticated user on login page -> redirect to dashboard
      goto(`${base}/`);
    } else if (!authenticated && !isLoginPage && !isChangePasswordPage) {
      // Unauthenticated user on protected page -> redirect to login
      // (allow change-password page for users with passwordChangeToken)
      goto(`${base}/login`);
    }
  });
</script>

<svelte:head>
  <title>Spitestack Stage</title>
  <meta name="description" content="2D command center for Spitestack - mixing board observability" />
</svelte:head>

{#if authChecked}
  {@render children()}
{:else}
  <!-- Loading state while checking auth -->
  <div class="auth-loading">
    <div class="auth-loading-indicator"></div>
  </div>
{/if}

<style>
  .auth-loading {
    position: fixed;
    inset: 0;
    display: flex;
    align-items: center;
    justify-content: center;
    background: var(--color-void, #0A0A0A);
  }

  .auth-loading-indicator {
    width: 8px;
    height: 8px;
    border-radius: 50%;
    background: var(--color-ember, #CC5500);
    animation: pulse 1s ease-in-out infinite;
  }

  @keyframes pulse {
    0%, 100% { opacity: 1; transform: scale(1); }
    50% { opacity: 0.5; transform: scale(1.2); }
  }
</style>
