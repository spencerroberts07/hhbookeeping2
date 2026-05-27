import { create } from 'zustand';
import type { AssistantMessageItem } from '@/components/assistant/assistant-message';

/**
 * Conversation state for the global AssistantWidget.
 *
 * Lifted out of component-local useState so the chat survives route
 * navigation — the widget mounts once in (app)/layout.tsx and remains
 * mounted across every authenticated page. Without this store the
 * messages array would reset every time React re-renders the widget
 * after a route change.
 *
 * Not persisted to localStorage on purpose: conversations are a
 * within-session affair. Closing the panel (X) clears explicitly.
 */
interface AssistantState {
  isOpen: boolean;
  conversationId: string | null;
  messages: AssistantMessageItem[];

  open: () => void;
  minimize: () => void;
  closeAndReset: () => void;
  setConversationId: (id: string | null) => void;
  setMessages: (m: AssistantMessageItem[]) => void;
  pushMessage: (m: AssistantMessageItem) => void;
  patchMessage: (id: string, patch: Partial<AssistantMessageItem>) => void;
  clearMessages: () => void;
}

export const useAssistantStore = create<AssistantState>()((set) => ({
  isOpen: false,
  conversationId: null,
  messages: [],

  open: () => set({ isOpen: true }),
  // Minimize = close the panel UI but keep conversationId + messages
  // so reopening continues the same thread.
  minimize: () => set({ isOpen: false }),
  closeAndReset: () => set({ isOpen: false, conversationId: null, messages: [] }),
  setConversationId: (id) => set({ conversationId: id }),
  setMessages: (m) => set({ messages: m }),
  pushMessage: (m) => set((s) => ({ messages: [...s.messages, m] })),
  patchMessage: (id, patch) =>
    set((s) => ({
      messages: s.messages.map((mm) => (mm.id === id ? { ...mm, ...patch } : mm)),
    })),
  clearMessages: () => set({ messages: [] }),
}));
