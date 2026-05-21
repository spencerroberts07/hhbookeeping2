'use client';

import { useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import { Topbar } from '@/components/layout/topbar';
import { Card, CardContent } from '@/components/ui/card';
import { Skeleton } from '@/components/ui/skeleton';
import { Badge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select';
import { useEntityStore } from '@/lib/store/entity';
import { listDocuments } from '@/lib/api/documents';
import { formatDate } from '@/lib/utils';
import { File, FileText, FileSpreadsheet, Grid3x3, List, Eye } from 'lucide-react';

const DOC_TYPE_LABEL: Record<string, string> = {
  invoice: 'Invoice',
  bank_pdf: 'Bank PDF',
  bank_csv: 'Bank CSV',
  hh_ap_statement: 'HH AP Statement',
  pos_import: 'POS Import',
  gl_import: 'GL Export',
  payroll: 'Payroll',
};

export default function DocumentsPage() {
  const entityCode = useEntityStore((s) => s.activeEntityCode);
  const [view, setView] = useState<'grid' | 'list'>('grid');
  const [typeFilter, setTypeFilter] = useState<string>('all');

  const docs = useQuery({
    queryKey: ['documents', entityCode, typeFilter],
    enabled: !!entityCode,
    queryFn: () =>
      listDocuments({
        entity_code: entityCode!,
        type: typeFilter === 'all' ? undefined : typeFilter,
        limit: 200,
      }),
  });

  return (
    <>
      <Topbar title="Documents" />
      <main className="p-6 space-y-4">
        <div className="flex items-center justify-between gap-3 flex-wrap">
          <div className="flex items-center gap-2">
            <Select value={typeFilter} onValueChange={setTypeFilter}>
              <SelectTrigger className="w-56">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="all">All types</SelectItem>
                {Object.entries(DOC_TYPE_LABEL).map(([k, v]) => (
                  <SelectItem key={k} value={k}>
                    {v}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>
          <div className="flex gap-1">
            <Button
              variant={view === 'grid' ? 'secondary' : 'ghost'}
              size="icon"
              onClick={() => setView('grid')}
              aria-label="Grid view"
            >
              <Grid3x3 className="h-4 w-4" strokeWidth={1.5} />
            </Button>
            <Button
              variant={view === 'list' ? 'secondary' : 'ghost'}
              size="icon"
              onClick={() => setView('list')}
              aria-label="List view"
            >
              <List className="h-4 w-4" strokeWidth={1.5} />
            </Button>
          </div>
        </div>

        {docs.isLoading ? (
          <Skeleton className="h-64" />
        ) : !docs.data || docs.data.documents.length === 0 ? (
          <Card className="p-8 text-center">
            <p className="text-slate">
              No documents uploaded yet. Start by uploading your bank statement.
            </p>
          </Card>
        ) : view === 'grid' ? (
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4 gap-3">
            {docs.data.documents.map((d) => (
              <Card key={d.id} className="p-4">
                <div className="flex items-start gap-3">
                  <div className="grid h-10 w-10 place-items-center rounded-md bg-cloud text-ledger-blue shrink-0">
                    <DocIcon type={d.document_type} />
                  </div>
                  <div className="min-w-0 flex-1">
                    <div className="text-xs">
                      <Badge variant="info">
                        {DOC_TYPE_LABEL[d.document_type] ?? d.document_type}
                      </Badge>
                    </div>
                    <div
                      className="font-semibold text-deep-navy text-sm mt-1 truncate"
                      title={d.filename}
                    >
                      {d.filename}
                    </div>
                    <div className="text-xs text-slate">
                      {d.upload_date ? formatDate(d.upload_date) : '—'}
                      {d.parsed_record_count > 0 && ` · ${d.parsed_record_count} rows`}
                    </div>
                    {d.file_url && (
                      <a
                        href={d.file_url}
                        target="_blank"
                        rel="noreferrer"
                        className="inline-flex items-center gap-1 text-xs text-ledger-blue hover:underline mt-2"
                      >
                        <Eye className="h-3 w-3" /> View
                      </a>
                    )}
                  </div>
                </div>
              </Card>
            ))}
          </div>
        ) : (
          <Card>
            <CardContent className="p-0">
              <table className="min-w-full text-sm">
                <thead className="bg-cloud">
                  <tr>
                    <th className="text-left px-4 py-2 font-semibold text-deep-navy">Type</th>
                    <th className="text-left px-4 py-2 font-semibold text-deep-navy">File</th>
                    <th className="text-left px-4 py-2 font-semibold text-deep-navy">Uploaded</th>
                    <th className="text-right px-4 py-2 font-semibold text-deep-navy">Records</th>
                    <th className="text-right px-4 py-2 font-semibold text-deep-navy">View</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-border">
                  {docs.data.documents.map((d) => (
                    <tr key={d.id} className="hover:bg-cloud">
                      <td className="px-4 py-2">
                        <Badge variant="info">
                          {DOC_TYPE_LABEL[d.document_type] ?? d.document_type}
                        </Badge>
                      </td>
                      <td className="px-4 py-2 text-ink">{d.filename}</td>
                      <td className="px-4 py-2 text-slate">
                        {d.upload_date ? formatDate(d.upload_date) : '—'}
                      </td>
                      <td className="px-4 py-2 tabular-nums text-right text-slate">
                        {d.parsed_record_count || '—'}
                      </td>
                      <td className="px-4 py-2 text-right">
                        {d.file_url ? (
                          <a
                            href={d.file_url}
                            target="_blank"
                            rel="noreferrer"
                            className="text-xs text-ledger-blue hover:underline"
                          >
                            Open ↗
                          </a>
                        ) : (
                          <span className="text-xs text-slate">—</span>
                        )}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </CardContent>
          </Card>
        )}

        <p className="text-xs text-slate">
          Files uploaded before R2 archival was enabled won't have a View
          link — only metadata is preserved.
        </p>
      </main>
    </>
  );
}

function DocIcon({ type }: { type: string }) {
  if (type === 'gl_import' || type === 'pos_import' || type === 'bank_csv') {
    return <FileSpreadsheet className="h-5 w-5" strokeWidth={1.5} />;
  }
  if (type === 'payroll' || type === 'hh_ap_statement') {
    return <FileText className="h-5 w-5" strokeWidth={1.5} />;
  }
  return <File className="h-5 w-5" strokeWidth={1.5} />;
}
