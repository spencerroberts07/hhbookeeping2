'use client';

import { Topbar } from '@/components/layout/topbar';
import { Card } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Printer, Download } from 'lucide-react';
import { useEntityStore } from '@/lib/store/entity';

interface ReportShellProps {
  title: string;
  subtitle?: string;
  children: React.ReactNode;
  onExportCsv?: () => void;
}

export function ReportShell({
  title,
  subtitle,
  children,
  onExportCsv,
}: ReportShellProps) {
  const entityName = useEntityStore((s) => s.activeEntityName);
  const entityCode = useEntityStore((s) => s.activeEntityCode);

  return (
    <>
      <Topbar title={title} periodLabel={subtitle} />
      <main className="p-6">
        <div className="flex items-center justify-between gap-4 mb-4 no-print">
          <div>
            <h2 className="text-h2 text-deep-navy">{title}</h2>
            {subtitle && <p className="text-sm text-slate">{subtitle}</p>}
          </div>
          <div className="flex items-center gap-2">
            {onExportCsv && (
              <Button variant="secondary" size="sm" onClick={onExportCsv}>
                <Download className="h-4 w-4" strokeWidth={1.5} />
                Export CSV
              </Button>
            )}
            <Button
              variant="secondary"
              size="sm"
              onClick={() => window.print()}
            >
              <Printer className="h-4 w-4" strokeWidth={1.5} />
              Print
            </Button>
          </div>
        </div>
        <div className="print-only mb-6">
          <h1 className="text-2xl font-bold text-deep-navy">{title}</h1>
          <p className="text-sm text-slate">
            {entityName} ({entityCode}) — {subtitle}
          </p>
        </div>
        <Card className="p-6">{children}</Card>
      </main>
    </>
  );
}
