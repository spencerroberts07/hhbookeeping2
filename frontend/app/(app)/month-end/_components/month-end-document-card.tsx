'use client';

import { useState } from 'react';
import { useQuery, useQueryClient } from '@tanstack/react-query';
import { useUser } from '@clerk/nextjs';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';
import { Skeleton } from '@/components/ui/skeleton';
import { Download, FileText, Loader2, Mail, RefreshCw } from 'lucide-react';
import { toast } from 'sonner';
import {
  generateMonthEndDocument,
  getMonthEndDocument,
  resendMonthEndDocument,
} from '@/lib/api/month-end-documents';
import { formatDate } from '@/lib/utils';

interface Props {
  entityCode: string;
  periodEnd: string;
}

const STATUS_VARIANT: Record<string, 'complete' | 'warning' | 'error'> = {
  ready: 'complete',
  generating: 'warning',
  failed: 'error',
  not_generated: 'warning',
};

export function MonthEndDocumentCard({ entityCode, periodEnd }: Props) {
  const { user } = useUser();
  const actorEmail = user?.primaryEmailAddress?.emailAddress ?? '';
  const qc = useQueryClient();
  const [busy, setBusy] = useState<'gen' | 'resend' | null>(null);

  const doc = useQuery({
    queryKey: ['month-end-document', entityCode, periodEnd],
    queryFn: () => getMonthEndDocument(entityCode, periodEnd),
  });

  const invalidate = () =>
    qc.invalidateQueries({ queryKey: ['month-end-document', entityCode, periodEnd] });

  const onGenerate = async () => {
    setBusy('gen');
    try {
      const res = await generateMonthEndDocument({
        entity_code: entityCode,
        period_end: periodEnd,
        actor_email: actorEmail,
      });
      const degraded = res.sections.filter((s) => s.state !== 'ready').length;
      toast.success(
        `Document generated — ${res.sections.length - degraded}/${res.sections.length} sections ready` +
          (res.email?.sent ? ' · emailed' : res.email?.skipped ? ' · email skipped' : ''),
      );
      invalidate();
    } catch {
      toast.error('Generation failed');
    } finally {
      setBusy(null);
    }
  };

  const onResend = async () => {
    setBusy('resend');
    try {
      const res = await resendMonthEndDocument({
        entity_code: entityCode,
        period_end: periodEnd,
        actor_email: actorEmail,
      });
      toast.success(res.email?.sent ? 'Email sent' : 'Email skipped (not configured)');
      invalidate();
    } catch (e: unknown) {
      const msg =
        (e as { response?: { data?: { detail?: string } } })?.response?.data?.detail ??
        'Resend failed';
      toast.error(msg);
    } finally {
      setBusy(null);
    }
  };

  const d = doc.data;
  const status = d?.status ?? 'not_generated';

  return (
    <Card>
      <CardHeader>
        <CardTitle className="flex items-center justify-between text-base">
          <span className="flex items-center gap-2">
            <FileText className="h-4 w-4 text-bw-teal" /> Month-end document
          </span>
          <Badge variant={STATUS_VARIANT[status] ?? 'warning'}>{status.replace(/_/g, ' ')}</Badge>
        </CardTitle>
      </CardHeader>
      <CardContent>
        {doc.isLoading ? (
          <Skeleton className="h-16" />
        ) : (
          <div className="space-y-3">
            {d?.generated_at && (
              <p className="text-xs text-slate">
                Generated {formatDate(d.generated_at)}
                {d.generated_by ? ` by ${d.generated_by}` : ''}
                {d.email_sent_at ? ` · emailed ${formatDate(d.email_sent_at)}` : ''}
              </p>
            )}
            {status === 'failed' && d?.error_msg && (
              <p className="text-xs text-error">{d.error_msg}</p>
            )}
            <div className="flex flex-wrap gap-2">
              {d?.presigned_url && (
                <Button variant="secondary" asChild>
                  <a href={d.presigned_url} target="_blank" rel="noopener noreferrer">
                    <Download className="mr-2 h-4 w-4" /> Download PDF
                  </a>
                </Button>
              )}
              <Button onClick={onGenerate} disabled={busy !== null}>
                {busy === 'gen' ? (
                  <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                ) : (
                  <RefreshCw className="mr-2 h-4 w-4" />
                )}
                {status === 'ready' ? 'Re-generate' : 'Generate'}
              </Button>
              <Button variant="ghost" onClick={onResend} disabled={busy !== null || status !== 'ready'}>
                {busy === 'resend' ? (
                  <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                ) : (
                  <Mail className="mr-2 h-4 w-4" />
                )}
                Re-send email
              </Button>
            </div>
          </div>
        )}
      </CardContent>
    </Card>
  );
}
