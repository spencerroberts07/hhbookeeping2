'use client';

import { useQuery } from '@tanstack/react-query';
import { ExternalLink, FileWarning } from 'lucide-react';
import { Skeleton } from '@/components/ui/skeleton';
import { useEntityStore } from '@/lib/store/entity';
import { getJournalEntryDocuments } from '@/lib/api/reports';
import { formatMoney } from '@/lib/utils';

function isPdf(name: string | null): boolean {
  return !!name && name.toLowerCase().endsWith('.pdf');
}

export function DocumentView({
  journalBatchId,
  journalLineId,
}: {
  journalBatchId: string;
  journalLineId?: string;
}) {
  const entityCode = useEntityStore((s) => s.activeEntityCode);

  const q = useQuery({
    queryKey: ['journal-entry-docs', journalBatchId, journalLineId, entityCode],
    enabled: !!entityCode,
    queryFn: () =>
      getJournalEntryDocuments({
        entity_code: entityCode!,
        journal_batch_id: journalBatchId,
        journal_line_id: journalLineId,
      }),
  });

  if (q.isLoading) return <Skeleton className="h-80" />;
  if (q.isError) return <p className="text-red-700 text-sm">Could not load documents.</p>;

  const docs = q.data?.documents ?? [];

  if (docs.length === 0) {
    return (
      <div className="flex flex-col items-center justify-center gap-2 rounded-lg border border-dashed border-border py-12 text-center">
        <FileWarning className="h-8 w-8 text-slate" strokeWidth={1.25} />
        <p className="text-sm text-slate">No document attached to this entry.</p>
      </div>
    );
  }

  return (
    <div className="space-y-5">
      {docs.map((d) => (
        <div key={d.link_id} className="rounded-lg border border-border">
          <div className="flex items-center justify-between gap-3 border-b border-border px-3 py-2">
            <div className="min-w-0">
              <div className="truncate text-sm font-medium text-deep-navy">
                {d.file_name ?? 'Document'}
              </div>
              <div className="text-[11px] text-slate">
                {[d.vendor_name, d.invoice_number, d.invoice_type]
                  .filter(Boolean)
                  .join(' · ')}
                {d.amount != null && ` · ${formatMoney(d.amount)}`}
              </div>
            </div>
            {d.presigned_url && (
              <a
                href={d.presigned_url}
                target="_blank"
                rel="noopener noreferrer"
                className="inline-flex shrink-0 items-center gap-1.5 rounded-md border border-ledger-blue px-2.5 py-1 text-xs font-medium text-ledger-blue hover:bg-ledger-blue/5"
              >
                <ExternalLink className="h-3.5 w-3.5" strokeWidth={1.5} />
                Open
              </a>
            )}
          </div>
          {d.presigned_url ? (
            isPdf(d.file_name) ? (
              <iframe
                src={d.presigned_url}
                title={d.file_name ?? 'document'}
                className="h-[60vh] w-full rounded-b-lg"
              />
            ) : (
              // eslint-disable-next-line @next/next/no-img-element
              <img
                src={d.presigned_url}
                alt={d.file_name ?? 'document'}
                className="max-h-[60vh] w-full rounded-b-lg object-contain"
              />
            )
          ) : (
            <p className="px-3 py-4 text-xs text-slate">
              The file link is unavailable (storage may be offline).
            </p>
          )}
        </div>
      ))}
    </div>
  );
}
