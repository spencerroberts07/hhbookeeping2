'use client';

import { useMemo } from 'react';
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select';
import { formatMonthLabel } from '@/lib/utils';

interface Props {
  value: string;
  onChange: (periodEnd: string) => void;
}

export function PeriodSelector({ value, onChange }: Props) {
  // Last 12 month-ends, ending with the most recent closed month.
  const options = useMemo(() => {
    const today = new Date();
    const out: string[] = [];
    for (let i = 0; i < 12; i++) {
      const d = new Date(today.getFullYear(), today.getMonth() - i, 0);
      out.push(d.toISOString().slice(0, 10));
    }
    return out;
  }, []);

  return (
    <div className="max-w-sm">
      <Select value={value} onValueChange={onChange}>
        <SelectTrigger>
          <SelectValue />
        </SelectTrigger>
        <SelectContent>
          {options.map((d) => (
            <SelectItem key={d} value={d}>
              {formatMonthLabel(d)} ({d})
            </SelectItem>
          ))}
        </SelectContent>
      </Select>
    </div>
  );
}
