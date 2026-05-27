import { ClerkTokenBridge } from '@/components/providers/clerk-token-bridge';
import { Sidebar } from '@/components/layout/sidebar';
import { DemoBanner } from '@/components/layout/demo-banner';
import { ErrorBoundary } from '@/components/shared/ErrorBoundary';
import { AssistantWidget } from '@/components/assistant/assistant-widget';

export default function AppLayout({ children }: { children: React.ReactNode }) {
  return (
    <ClerkTokenBridge>
      <ErrorBoundary label="app">
        <div className="flex min-h-screen bg-cloud">
          <Sidebar />
          <div className="flex-1 min-w-0 flex flex-col">
            <DemoBanner />
            <ErrorBoundary label="page">
              <div className="flex-1 min-w-0">{children}</div>
            </ErrorBoundary>
          </div>
        </div>
      </ErrorBoundary>
      {/* Global assistant — mounted once here so conversation state in
          useAssistantStore survives every route navigation. */}
      <AssistantWidget />
    </ClerkTokenBridge>
  );
}
