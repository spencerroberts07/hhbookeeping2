'use client';

import { useCallback, useMemo, useRef, useState } from 'react';
import {
  Upload,
  X,
  CheckCircle2,
  AlertCircle,
  FileText,
  Loader2,
  Trash2,
} from 'lucide-react';
import { Card } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';
import { api } from '@/lib/api/client';
import { cn } from '@/lib/utils';
import { toast } from 'sonner';
import type { AxiosError } from 'axios';

export interface UploadResult {
  fileName: string;
  status: 'success' | 'error';
  recordCount?: number;
  errorMessage?: string;
  /** Raw response body — call sites can read it for extra detail. */
  data?: unknown;
}

interface FileItem {
  id: string;
  file: File;
  status: 'queued' | 'uploading' | 'success' | 'error';
  recordCount?: number;
  errorMessage?: string;
  data?: unknown;
}

export interface MultiFileUploadProps {
  /** API path, e.g. `/api/bank-pdf/upload` */
  endpoint: string;
  /** 'file' = one request per file (sequential). 'files' = all files in one request. */
  fileKey: 'file' | 'files';
  /** HTML accept attribute, e.g. ".pdf,.xlsx". */
  accept: string;
  /** Form fields posted with every request (entity_code, actor_email, plus
   *  endpoint-specific things like document_type, snapshot_date, etc.). */
  extraFields: Record<string, string | undefined>;
  /** Called once after the queue finishes, with per-file results. */
  onComplete?: (results: UploadResult[]) => void;
  label: string;
  description: string;
  /** Prominent variant — bigger drop zone for the marquee endpoints. */
  variant?: 'default' | 'prominent';
  /** Optional note rendered below the title (e.g. "Upload all PDFs for this period"). */
  note?: string;
  /** Hook the call site can use to override the record-count extraction. */
  extractRecordCount?: (data: unknown) => number | undefined;
  /** Disable the uploader (e.g. while a required extraField is missing). */
  disabled?: boolean;
  /** Optional callback fired whenever an individual file transitions to success.
   *  Useful for "uploaded for this period" badges that should appear before the
   *  whole queue finishes. */
  onFileSuccess?: (result: UploadResult) => void;
}

const DEFAULT_RECORD_KEYS = [
  'record_count',
  'inserted',
  'parsed_rows',
  'transaction_count',
  'total_inserted',
  'total_parsed',
  'rows_inserted',
  'rows_parsed',
  'invoice_count',
];

function _defaultExtractRecordCount(data: unknown): number | undefined {
  if (!data || typeof data !== 'object') return undefined;
  const obj = data as Record<string, unknown>;
  for (const k of DEFAULT_RECORD_KEYS) {
    const v = obj[k];
    if (typeof v === 'number') return v;
  }
  // hh-ap upload-documents returns inserted_documents[] / updated[] / duplicates[]
  const inserted = obj['inserted_documents'];
  if (Array.isArray(inserted)) return inserted.length;
  return undefined;
}

function _formatBytes(n: number): string {
  if (n < 1024) return `${n} B`;
  if (n < 1024 * 1024) return `${(n / 1024).toFixed(1)} KB`;
  return `${(n / (1024 * 1024)).toFixed(1)} MB`;
}

function _humanizeAxiosError(err: unknown): string {
  const axiosErr = err as AxiosError<{ detail?: string }>;
  return (
    axiosErr.response?.data?.detail ??
    axiosErr.message ??
    'Upload failed'
  );
}

export function MultiFileUpload({
  endpoint,
  fileKey,
  accept,
  extraFields,
  onComplete,
  label,
  description,
  variant = 'default',
  note,
  extractRecordCount = _defaultExtractRecordCount,
  disabled = false,
  onFileSuccess,
}: MultiFileUploadProps) {
  const [items, setItems] = useState<FileItem[]>([]);
  const [dragActive, setDragActive] = useState(false);
  const [uploading, setUploading] = useState(false);
  const [summary, setSummary] = useState<{
    filesProcessed: number;
    totalRecords: number;
    errors: number;
  } | null>(null);
  const inputRef = useRef<HTMLInputElement>(null);

  const isProminent = variant === 'prominent';

  // Missing required fields — keep the button disabled and surface why so
  // the dealer isn't confused.
  const missingFields = useMemo(() => {
    return Object.entries(extraFields)
      .filter(([, v]) => v === undefined || v === '')
      .map(([k]) => k);
  }, [extraFields]);

  const queueEmpty = items.length === 0;
  const allDone =
    !queueEmpty && items.every((i) => i.status === 'success' || i.status === 'error');

  const addFiles = useCallback((picked: FileList | File[]) => {
    const arr = Array.from(picked);
    if (arr.length === 0) return;
    setItems((prev) => [
      ...prev,
      ...arr.map((f) => ({
        id: `${f.name}-${f.size}-${f.lastModified}-${Math.random()}`,
        file: f,
        status: 'queued' as const,
      })),
    ]);
    setSummary(null);
  }, []);

  const removeItem = (id: string) => {
    setItems((prev) => prev.filter((i) => i.id !== id));
  };

  const clearAll = () => {
    setItems([]);
    setSummary(null);
  };

  const onDragOver = (e: React.DragEvent) => {
    e.preventDefault();
    if (disabled || uploading) return;
    setDragActive(true);
  };
  const onDragLeave = (e: React.DragEvent) => {
    e.preventDefault();
    setDragActive(false);
  };
  const onDrop = (e: React.DragEvent) => {
    e.preventDefault();
    setDragActive(false);
    if (disabled || uploading) return;
    if (e.dataTransfer.files?.length) {
      addFiles(e.dataTransfer.files);
    }
  };

  const updateItem = (id: string, patch: Partial<FileItem>) => {
    setItems((prev) => prev.map((i) => (i.id === id ? { ...i, ...patch } : i)));
  };

  const buildFormData = (files: File[]): FormData => {
    const fd = new FormData();
    for (const [k, v] of Object.entries(extraFields)) {
      if (v !== undefined && v !== '') fd.append(k, v);
    }
    for (const f of files) fd.append(fileKey, f);
    return fd;
  };

  const startUpload = async () => {
    if (queueEmpty || uploading || missingFields.length > 0) return;
    setUploading(true);
    setSummary(null);

    const queued = items.filter((i) => i.status === 'queued' || i.status === 'error');
    const results: UploadResult[] = [];

    if (fileKey === 'files') {
      // Batch: one POST with every file in the queue.
      const batchIds = queued.map((q) => q.id);
      batchIds.forEach((id) => updateItem(id, { status: 'uploading' }));
      try {
        const res = await api.post(endpoint, buildFormData(queued.map((q) => q.file)));
        // Each file in a batch endpoint shares one response; we report per-file
        // success and attribute the aggregate record count to each entry so
        // the UI sums work.
        const totalRecords = extractRecordCount(res.data);
        const perFileCount =
          totalRecords !== undefined && batchIds.length > 0
            ? Math.round(totalRecords / batchIds.length)
            : undefined;
        for (const q of queued) {
          const result: UploadResult = {
            fileName: q.file.name,
            status: 'success',
            recordCount: perFileCount,
            data: res.data,
          };
          updateItem(q.id, {
            status: 'success',
            recordCount: perFileCount,
            data: res.data,
          });
          results.push(result);
          onFileSuccess?.(result);
        }
      } catch (err) {
        const msg = _humanizeAxiosError(err);
        for (const q of queued) {
          updateItem(q.id, { status: 'error', errorMessage: msg });
          results.push({
            fileName: q.file.name,
            status: 'error',
            errorMessage: msg,
          });
        }
      }
    } else {
      // Single: one POST per file, sequentially, so the dealer can see
      // progress and so a single bad file doesn't kill the batch.
      for (const q of queued) {
        updateItem(q.id, { status: 'uploading' });
        try {
          const res = await api.post(endpoint, buildFormData([q.file]));
          const count = extractRecordCount(res.data);
          const result: UploadResult = {
            fileName: q.file.name,
            status: 'success',
            recordCount: count,
            data: res.data,
          };
          updateItem(q.id, {
            status: 'success',
            recordCount: count,
            data: res.data,
          });
          results.push(result);
          onFileSuccess?.(result);
        } catch (err) {
          const msg = _humanizeAxiosError(err);
          updateItem(q.id, { status: 'error', errorMessage: msg });
          results.push({
            fileName: q.file.name,
            status: 'error',
            errorMessage: msg,
          });
        }
      }
    }

    const successCount = results.filter((r) => r.status === 'success').length;
    const totalRecords = results.reduce(
      (s, r) => s + (r.recordCount ?? 0),
      0,
    );
    const errors = results.filter((r) => r.status === 'error').length;
    setSummary({ filesProcessed: successCount, totalRecords, errors });
    setUploading(false);

    if (errors === 0 && successCount > 0) {
      toast.success(`${label}: ${successCount} file${successCount === 1 ? '' : 's'} uploaded`);
    } else if (errors > 0 && successCount > 0) {
      toast.warning(`${label}: ${successCount} succeeded, ${errors} failed`);
    } else if (errors > 0) {
      toast.error(`${label}: all ${errors} uploads failed`);
    }
    onComplete?.(results);
  };

  return (
    <Card className={cn('p-4', isProminent && 'p-6')}>
      <div className="flex items-start justify-between gap-3 mb-3">
        <div>
          <div className={cn('font-semibold text-deep-navy', isProminent && 'text-h2')}>
            {label}
          </div>
          <p className="text-sm text-slate">{description}</p>
          {note && <p className="text-xs text-bw-teal mt-1 font-semibold">{note}</p>}
        </div>
        {summary && summary.errors === 0 && summary.filesProcessed > 0 && (
          <Badge variant="complete">
            {summary.filesProcessed} uploaded
          </Badge>
        )}
      </div>

      {missingFields.length > 0 && (
        <div className="mb-3 rounded-lg border border-amber-200 bg-amber-50 px-3 py-2 text-xs text-amber-800">
          Missing required fields: {missingFields.join(', ')}
        </div>
      )}

      {/* Drop zone */}
      <button
        type="button"
        onClick={() => inputRef.current?.click()}
        onDragOver={onDragOver}
        onDragLeave={onDragLeave}
        onDrop={onDrop}
        disabled={disabled || uploading}
        className={cn(
          'w-full rounded-xl border-2 border-dashed transition flex flex-col items-center justify-center gap-2 text-sm',
          isProminent ? 'py-12 px-6' : 'py-6 px-4',
          dragActive
            ? 'border-bw-teal bg-bw-teal/10'
            : 'border-input bg-cloud hover:bg-white',
          (disabled || uploading) && 'cursor-not-allowed opacity-60',
        )}
        aria-label={`Choose files for ${label}`}
      >
        <Upload
          className={cn('text-slate', isProminent ? 'h-8 w-8' : 'h-5 w-5')}
          strokeWidth={1.5}
        />
        <span className="font-semibold text-deep-navy">
          {dragActive ? 'Drop files here' : 'Drag-drop or click to browse'}
        </span>
        <span className="text-xs text-slate">Accepts {accept}</span>
        <input
          ref={inputRef}
          type="file"
          accept={accept}
          multiple
          className="hidden"
          onChange={(e) => {
            if (e.target.files) addFiles(e.target.files);
            // Reset so re-selecting the same file fires a change event.
            e.target.value = '';
          }}
        />
      </button>

      {/* Queue */}
      {!queueEmpty && (
        <div className="mt-4">
          <div className="flex items-center justify-between mb-2">
            <div className="text-sm font-semibold text-deep-navy">
              {items.length} file{items.length === 1 ? '' : 's'} queued
            </div>
            <div className="flex gap-2">
              <Button
                size="sm"
                variant="ghost"
                onClick={clearAll}
                disabled={uploading}
              >
                <Trash2 className="h-4 w-4" strokeWidth={1.5} />
                Clear all
              </Button>
              <Button
                size="sm"
                variant="accent"
                onClick={startUpload}
                disabled={
                  uploading || missingFields.length > 0 || allDone
                }
              >
                {uploading
                  ? 'Uploading…'
                  : allDone
                    ? 'Done'
                    : `Upload ${items.filter((i) => i.status !== 'success').length}`}
              </Button>
            </div>
          </div>
          <ul className="divide-y divide-border rounded-lg border border-border">
            {items.map((i) => (
              <li
                key={i.id}
                className="flex items-center justify-between gap-3 px-3 py-2 text-sm"
              >
                <FileText
                  className="h-4 w-4 text-slate shrink-0"
                  strokeWidth={1.5}
                />
                <div className="flex-1 min-w-0">
                  <div className="truncate text-ink" title={i.file.name}>
                    {i.file.name}
                  </div>
                  <div className="text-xs text-slate">
                    {_formatBytes(i.file.size)}
                    {i.recordCount !== undefined &&
                      ` · ${i.recordCount} record${i.recordCount === 1 ? '' : 's'}`}
                    {i.errorMessage && (
                      <span className="text-red-700"> · {i.errorMessage}</span>
                    )}
                  </div>
                </div>
                <StatusBadge status={i.status} />
                {!uploading && i.status !== 'success' && (
                  <button
                    onClick={() => removeItem(i.id)}
                    aria-label={`Remove ${i.file.name}`}
                    className="text-slate hover:text-red-700 p-1"
                  >
                    <X className="h-4 w-4" strokeWidth={1.5} />
                  </button>
                )}
              </li>
            ))}
          </ul>
        </div>
      )}

      {/* Summary */}
      {summary && (
        <div className="mt-4 rounded-lg border border-border bg-cloud p-3 text-sm">
          <div className="flex items-center gap-3 flex-wrap">
            <CheckCircle2
              className="h-5 w-5 text-bw-teal"
              strokeWidth={1.5}
            />
            <span className="font-semibold text-deep-navy">
              {summary.filesProcessed} file
              {summary.filesProcessed === 1 ? '' : 's'} processed
            </span>
            {summary.totalRecords > 0 && (
              <span className="text-slate">
                · {summary.totalRecords} record
                {summary.totalRecords === 1 ? '' : 's'} parsed
              </span>
            )}
            {summary.errors > 0 && (
              <span className="text-red-700 font-semibold">
                · {summary.errors} error{summary.errors === 1 ? '' : 's'}
              </span>
            )}
          </div>
          {summary.errors > 0 && (
            <ul className="mt-2 text-xs text-red-700 space-y-1">
              {items
                .filter((i) => i.status === 'error')
                .map((i) => (
                  <li key={i.id}>
                    <span className="font-semibold">{i.file.name}</span>:{' '}
                    {i.errorMessage}
                  </li>
                ))}
            </ul>
          )}
        </div>
      )}
    </Card>
  );
}

function StatusBadge({ status }: { status: FileItem['status'] }) {
  if (status === 'queued')
    return <Badge variant="info">Queued</Badge>;
  if (status === 'uploading')
    return (
      <Badge variant="pending" className="gap-1">
        <Loader2 className="h-3 w-3 animate-spin" strokeWidth={2} />
        Uploading
      </Badge>
    );
  if (status === 'success')
    return (
      <Badge variant="complete" className="gap-1">
        <CheckCircle2 className="h-3 w-3" strokeWidth={2} />
        Uploaded
      </Badge>
    );
  return (
    <Badge variant="error" className="gap-1">
      <AlertCircle className="h-3 w-3" strokeWidth={2} />
      Error
    </Badge>
  );
}
