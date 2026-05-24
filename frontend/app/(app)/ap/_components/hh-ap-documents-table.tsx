'use client';

import { useEffect, useMemo, useRef } from 'react';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { Badge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Skeleton } from '@/components/ui/skeleton';
import { Eye, Loader2, RotateCw } from 'lucide-react';
import { useEntityStore } from '@/lib/store/entity';
import {
  listHHAPDocuments,
  reprocessHHAPDocument,
  type HHAPDocumentRow,
} from '@/lib/api/hh_ap';
import { formatDate } from '@/lib/utils';
import { toast } from 'sonner';

// "Recently Uploaded Documents" table for the AP module HH tab.
// Auto-refreshes every 10s while any row is pending; stops once
// everything is parsed or errored.
export function HHAPDocumentsTable() {
  const entityCode = useEntityStore((s) => s.activeEntityCode);
  const qc = useQueryClient();

  const q = useQuery({
    queryKey: ['hh-ap-documents', entityCode],
    enabled: !!entityCode,
    queryFn: () => listHHAPDocuments({ entity_code: entityCode!, limit: 50 }),
    refetchInterval: (query) => {
      // Stop polling once nothing is in-flight.
      const data = query.state.data;
      const hasPending =
        !!data &&
        (data.summary.pending > 0 ||
          data.documents.some((d) =>
            d.processing_status === 'uploaded_pending_parse' ||
            d.processing_status === 'parsing',
          ));
      return hasPending ? 10_000 : false;
    },
  });

  const reprocess = useMutation({
    mutationFn: reprocessHHAPDocument,
    onSuccess: (res) => {
      toast.success(res.message);
      qc.invalidateQueries({ queryKey: ['hh-ap-documents', entityCode] });
    },
    onError: () => toast.error('Re-process failed'),
  });

  return (
    <Card>
      <CardHeader>
        <CardTitle className="flex items-center justify-between text-base">
          <span>Recently Uploaded Documents</span>
          {q.data && (
            <span className="text-xs font-normal text-slate">
              {q.data.summary.total} uploaded · {q.data.summary.parsed} parsed
              {q.data.summary.pending > 0 && (
                <>
                  {' · '}
                  <span className="text-amber-700">
                    {q.data.summary.pending} pending
                  </span>
                </>
              )}
              {q.data.summary.errors > 0 && (
                <>
                  {' · '}
                  <span className="text-red-600">
                    {q.data.summary.errors} errors
                  </span>
                </>
              )}
            </span>
          )}
        </CardTitle>
      </CardHeader>
      <CardContent className="p-0">
        {q.isLoading ? (
          <div className="p-4 space-y-2">
            <Skeleton className="h-8 w-full" />
            <Skeleton className="h-8 w-full" />
            <Skeleton className="h-8 w-full" />
          </div>
        ) : !q.data || q.data.documents.length === 0 ? (
          <p className="p-6 text-sm text-slate">
            No HH AP documents uploaded yet.
          </p>
        ) : (
          <div className="overflow-x-auto">
            <table className="min-w-full text-sm">
              <thead className="bg-cloud">
                <tr>
                  <th className="text-left px-4 py-2 font-semibold text-deep-navy">Filename</th>
                  <th className="text-left px-4 py-2 font-semibold text-deep-navy">Type</th>
                  <th className="text-left px-4 py-2 font-semibold text-deep-navy">Period</th>
                  <th className="text-left px-4 py-2 font-semibold text-deep-navy">Status</th>
                  <th className="text-right px-4 py-2 font-semibold text-deep-navy">Records</th>
                  <th className="text-left px-4 py-2 font-semibold text-deep-navy">Uploaded</th>
                  <th className="text-right px-4 py-2 font-semibold text-deep-navy">Actions</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-border">
                {q.data.documents.map((d) => (
                  <DocumentRow
                    key={d.id}
                    doc={d}
                    onReprocess={() =>
                      reprocess.mutate({
                        document_id: d.id,
                        entity_code: entityCode!,
                      })
                    }
                    isRepostingThis={
                      reprocess.isPending &&
                      reprocess.variables?.document_id === d.id
                    }
                  />
                ))}
              </tbody>
            </table>
          </div>
        )}
      </CardContent>
    </Card>
  );
}

function DocumentRow({
  doc,
  onReprocess,
  isRepostingThis,
}: {
  doc: HHAPDocumentRow;
  onReprocess: () => void;
  isRepostingThis: boolean;
}) {
  const status = statusFromProcessing(doc.processing_status);
  return (
    <tr className="hover:bg-cloud">
      <td className="px-4 py-2 text-ink max-w-[240px] truncate" title={doc.source_filename}>
        {doc.source_filename}
      </td>
      <td className="px-4 py-2 text-slate text-xs uppercase">{doc.document_type}</td>
      <td className="px-4 py-2 text-slate">{doc.period ?? '—'}</td>
      <td className="px-4 py-2">
        <StatusBadge label={status.label} variant={status.variant} spin={status.spin} title={doc.error_message ?? undefined} />
      </td>
      <td className="px-4 py-2 tabular-nums text-right text-slate">
        {doc.records_parsed ?? '—'}
      </td>
      <td className="px-4 py-2 text-slate text-xs">
        {doc.created_at ? formatDate(doc.created_at) : '—'}
      </td>
      <td className="px-4 py-2 text-right">
        <div className="inline-flex gap-1.5">
          {doc.file_url && (
            <a
              href={doc.file_url}
              target="_blank"
              rel="noreferrer"
              className="inline-flex items-center gap-1 text-xs text-ledger-blue hover:underline"
            >
              <Eye className="h-3 w-3" /> View
            </a>
          )}
          {doc.processing_status === 'parse_error' && (
            <Button
              size="sm"
              variant="outline"
              onClick={onReprocess}
              disabled={isRepostingThis}
            >
              {isRepostingThis ? (
                <Loader2 className="h-3 w-3 animate-spin" />
              ) : (
                <RotateCw className="h-3 w-3" />
              )}
              Re-process
            </Button>
          )}
        </div>
      </td>
    </tr>
  );
}

function statusFromProcessing(s: string): {
  label: string;
  variant: 'complete' | 'warning' | 'error' | 'pending';
  spin: boolean;
} {
  if (s === 'uploaded_pending_parse')
    return { label: 'Pending', variant: 'warning', spin: true };
  if (s === 'parsing') return { label: 'Parsing…', variant: 'pending', spin: true };
  if (s === 'parse_error') return { label: 'Error', variant: 'error', spin: false };
  if (s.startsWith('parsed_')) return { label: 'Parsed', variant: 'complete', spin: false };
  return { label: s, variant: 'warning', spin: false };
}

function StatusBadge({
  label,
  variant,
  spin,
  title,
}: {
  label: string;
  variant: 'complete' | 'warning' | 'error' | 'pending';
  spin: boolean;
  title?: string;
}) {
  return (
    <Badge variant={variant} title={title} className="inline-flex items-center gap-1">
      {spin && <Loader2 className="h-3 w-3 animate-spin" />}
      {label}
      {variant === 'complete' && ' ✓'}
    </Badge>
  );
}
