import { writable } from 'svelte/store';

/**
 * Focus Store
 * 
 * Manages the "Cinematic Focus" state of the dashboard.
 * When an element is focused, everything else dims.
 */

interface FocusState {
    activeId: string | null;
    active: boolean;
}

function createFocusStore() {
    const { subscribe, set, update } = writable<FocusState>({
        activeId: null,
        active: false
    });

    return {
        subscribe,
        focus: (id: string) => set({ activeId: id, active: true }),
        blur: () => set({ activeId: null, active: false }),
        toggle: (id: string) => update(s =>
            s.activeId === id ? { activeId: null, active: false } : { activeId: id, active: true }
        )
    };
}

export const focus = createFocusStore();
