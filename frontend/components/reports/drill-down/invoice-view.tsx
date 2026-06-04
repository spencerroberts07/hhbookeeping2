'use client';

import { useQuery } from '@tanstack/react-query';
import { ExternalLink, FileWarning } from 'lucide-react';
import { Skeleton } from '@/components/ui/skeleton';
import { useEntityStore } from '@/lib/store/entity';
import { getHHAPInvoiceDrill } from '@/lib/api/hh_ap';
import { formatMoney } from '@/lib/utils';
import { EntryView } from './entry-view';

function isPdf(name: string | null): boolean {
  return !!name && name.toLowerCase().endsWith('.pdf');
}

/**
 * HH AP invoice drill root. Resolves the invoice to its journal batch (if
 * linked) or its source document. Because `invoice_journal_links` is empty
 * today, this renders the document (PDF); once posting links invoices it
 * will render the full journal entry instead.
 */
export function InvoiceView({ hhApInvoiceId }: { hhApInvoiceId: string }) {
  const entityCode = useEntityStore((s) => s.activeEntityCode);

  const q = useQuery({
    queryKey: ['hh-ap-invoice-drill', hhApInvoiceId, entityCode],
    enabled: !!entityCode,
    queryFn: () =>
      getHHAPInvoiceDrill({ entity_code: entityCode!, invoice_id: hhApInvoiceId }),
  });

  if (q.isLoading) return <Skeleton className="h-80" />;
  if (q.isError) return <p className="text-red-700 text-sm">Could not load the invoice.</p>;
  if (!q.data) return <p className="text-slate text-sm">No data.</p>;

  // Linked to a posted batch → show the full journal entry.
  if (q.data.journal_batch_id) {
    return <EntryView journalBatchId={q.data.journal_batch_id} />;
  }

  const doc = q.data.document;
  if (!doc) {
    return (
      <div className="flex flex-col items-center justify-center gap-2 rounded-lg border border-dashed border-border py-12 text-center">
        <FileWarning className="h-8 w-8 text-slate" strokeWidth={1.25} />
        <p className="text-sm text-slate">
          This invoice has no journal entry or source document yet.
        </p>
      </div>
    );
  }

  return (
    <div className="rounded-lg border border-border">
      <div className="flex items-center justify-between gap-3 border-b border-border px-3 py-2">
        <div className="min-w-0">
          <div className="truncate text-sm font-medium text-deep-navy">
            {doc.file_name ?? 'Invoice document'}
          </div>
          <div className="text-[11px] text-slate">
            {[doc.vendor_name, doc.invoice_number, doc.invoice_type]
              .filter(Boolean)
              .join(' · ')}
            {doc.amount != null && ` · ${formatMoney(doc.amount)}`}
          </div>
        </div>
        {doc.presigned_url && (
          <a
            href={doc.presigned_url}
            target="_blank"
            rel="noopener noreferrer"
            className="inline-flex shrink-0 items-center gap-1.5 rounded-md border border-ledger-blue px-2.5 py-1 text-xs font-medium text-ledger-blue hover:bg-ledger-blue/5"
          >
            <ExternalLink className="h-3.5 w-3.5" strokeWidth={1.5} />
            Open
          </a>
        )}
      </div>
      {doc.presigned_url ? (
        isPdf(doc.file_name) ? (
          <iframe
            src={doc.presigned_url}
            title={doc.file_name ?? 'invoice'}
            className="h-[60vh] w-full rounded-b-lg"
          />
        ) : (
          // eslint-disable-next-line @next/next/no-img-element
          <img
            src={doc.presigned_url}
            alt={doc.file_name ?? 'invoice'}
            className="max-h-[60vh] w-full rounded-b-lg object-contain"
          />
        )
      ) : (
        <p className="px-3 py-4 text-xs text-slate">
          The file link is unavailable (storage may be offline).
        </p>
      )}
    </div>
  );
}
