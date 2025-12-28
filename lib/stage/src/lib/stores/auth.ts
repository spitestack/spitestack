/**
 * Auth Store
 *
 * Session management for spite-stage admin dashboard.
 * Like a keycard to the control room. No entry without credentials.
 */

import { writable, derived, get } from 'svelte/store';
import { browser } from '$app/environment';

// =============================================================================
// Types
// =============================================================================

export interface AuthUser {
  sub: string;
  email: string;
  orgs?: Record<string, { roles: string[] }>;
}

export interface AuthState {
  token: string | null;
  user: AuthUser | null;
  isLoading: boolean;
  error: string | null;
  /** Token for password change flow (when mustChangePassword is true) */
  passwordChangeToken: string | null;
}

interface LoginResponse {
  status?: 'success' | 'mfa_required' | 'password_change_required';
  userId?: string;
  user?: AuthUser;
  error?: string;
  message?: string;
  mfaToken?: string;
  authenticators?: Array<{ id: string; type: string; name: string }>;
}

// =============================================================================
// Storage Keys
// =============================================================================

// Note: Tokens are stored in HttpOnly cookies by the backend (more secure)
// We only store user info in localStorage for UI purposes
const USER_KEY = 'spite_admin_user';
const AUTH_FLAG_KEY = 'spite_admin_authenticated';

// =============================================================================
// Store
// =============================================================================

function createAuthStore() {
  // Initialize from localStorage (browser only)
  // Note: actual auth is via HttpOnly cookies, localStorage just caches user info
  const initialState: AuthState = {
    token: null, // Not used with cookie auth, kept for interface compatibility
    user: null,
    isLoading: false,
    error: null,
    passwordChangeToken: null,
  };

  if (browser) {
    try {
      const storedUser = localStorage.getItem(USER_KEY);
      const isAuthenticated = localStorage.getItem(AUTH_FLAG_KEY) === 'true';
      if (storedUser && isAuthenticated) {
        initialState.user = JSON.parse(storedUser);
        initialState.token = 'cookie'; // Placeholder to indicate authenticated
      }
    } catch {
      // Storage access failed, continue with empty state
    }
  }

  const { subscribe, set, update } = writable<AuthState>(initialState);

  // Persist to localStorage when state changes
  function persistState(state: AuthState) {
    if (!browser) return;

    try {
      if (state.user) {
        localStorage.setItem(USER_KEY, JSON.stringify(state.user));
        localStorage.setItem(AUTH_FLAG_KEY, 'true');
      } else {
        localStorage.removeItem(USER_KEY);
        localStorage.removeItem(AUTH_FLAG_KEY);
      }
    } catch {
      // Storage access failed, continue silently
    }
  }

  return {
    subscribe,

    /**
     * Attempt login with email and password.
     * Tokens are stored in HttpOnly cookies by the backend.
     */
    async login(email: string, password: string): Promise<boolean> {
      update(s => ({ ...s, isLoading: true, error: null }));

      try {
        const response = await fetch('/auth/login', {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
          },
          credentials: 'include', // Important: send/receive cookies
          body: JSON.stringify({ email, password }),
        });

        const data: LoginResponse = await response.json();

        if (!response.ok) {
          const errorMsg = data.error || data.message || `Login failed: ${response.status}`;
          update(s => ({ ...s, isLoading: false, error: errorMsg }));
          return false;
        }

        // Handle MFA required
        if (data.status === 'mfa_required') {
          update(s => ({ ...s, isLoading: false, error: 'MFA required (not yet supported in admin)' }));
          return false;
        }

        // Handle password change required - store the token for the change password flow
        if (data.status === 'password_change_required') {
          update(s => ({
            ...s,
            isLoading: false,
            error: null,
            passwordChangeToken: (data as any).passwordChangeToken || null,
          }));
          return 'password_change_required' as any; // Special return value
        }

        // Check for successful login
        if (data.status !== 'success') {
          update(s => ({ ...s, isLoading: false, error: data.error || 'Login failed' }));
          return false;
        }

        // Get user info from response
        const user: AuthUser = data.user || { sub: data.userId || '', email };

        const newState: AuthState = {
          token: 'cookie', // Placeholder - actual token is in HttpOnly cookie
          user,
          isLoading: false,
          error: null,
        };

        set(newState);
        persistState(newState);
        return true;

      } catch (err) {
        const message = err instanceof Error ? err.message : 'Login failed';
        update(s => ({ ...s, isLoading: false, error: message }));
        return false;
      }
    },

    /**
     * Clear authentication state.
     */
    logout() {
      const newState: AuthState = {
        token: null,
        user: null,
        isLoading: false,
        error: null,
        passwordChangeToken: null,
      };
      set(newState);
      persistState(newState);
    },

    /**
     * Check if current session is still valid.
     * Used on app load to verify cookies are still valid.
     */
    async checkAuth(): Promise<boolean> {
      const state = get({ subscribe });

      // If no stored user info, not authenticated
      if (!state.user) {
        return false;
      }

      // Verify session by hitting a protected endpoint (cookies sent automatically)
      try {
        const response = await fetch('/admin/api/status', {
          credentials: 'include',
        });

        if (!response.ok) {
          this.logout();
          return false;
        }

        return true;

      } catch {
        // Network error, but don't logout - might be offline
        return false;
      }
    },

    /**
     * Clear error state.
     */
    clearError() {
      update(s => ({ ...s, error: null }));
    },

    /**
     * Clear password change token (after successful change or cancel).
     */
    clearPasswordChangeToken() {
      update(s => ({ ...s, passwordChangeToken: null }));
    },

    /**
     * Change password using the password change token.
     */
    async changePassword(newPassword: string): Promise<boolean> {
      const state = get({ subscribe });
      const token = state.passwordChangeToken;

      if (!token) {
        update(s => ({ ...s, error: 'No password change token' }));
        return false;
      }

      update(s => ({ ...s, isLoading: true, error: null }));

      try {
        const response = await fetch('/auth/change-password', {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
          },
          credentials: 'include',
          body: JSON.stringify({ passwordChangeToken: token, newPassword }),
        });

        const data = await response.json();

        if (!response.ok) {
          update(s => ({ ...s, isLoading: false, error: data.error || 'Password change failed' }));
          return false;
        }

        // Password changed successfully - user is now logged in via cookies
        // Update state with user info from response
        const user: AuthUser = data.user || { sub: data.userId || '', email: '' };
        const newState: AuthState = {
          token: 'cookie', // Placeholder - actual token is in HttpOnly cookie
          user,
          isLoading: false,
          error: null,
          passwordChangeToken: null,
        };
        set(newState);
        persistState(newState);
        return true;

      } catch (err) {
        const message = err instanceof Error ? err.message : 'Password change failed';
        update(s => ({ ...s, isLoading: false, error: message }));
        return false;
      }
    },

    /**
     * Get current token (for API calls).
     * Note: With cookie-based auth, this returns null.
     * The API client should use credentials: 'include' to send cookies.
     */
    getToken(): string | null {
      // Tokens are in HttpOnly cookies, not accessible to JS
      return null;
    },
  };
}


// =============================================================================
// Exports
// =============================================================================

export const auth = createAuthStore();

// Derived store for authentication status
export const isAuthenticated = derived(auth, $auth => !!$auth.token && !!$auth.user);

// Derived store for loading state
export const isAuthLoading = derived(auth, $auth => $auth.isLoading);

// Derived store for auth error
export const authError = derived(auth, $auth => $auth.error);

// Derived store for password change required
export const needsPasswordChange = derived(auth, $auth => !!$auth.passwordChangeToken);
