'use client';

import Link from 'next/link';
import { usePathname } from 'next/navigation';

const TABS = [
  { href: '/dashboard/sales', label: 'Sales' },
  { href: '/dashboard/cash', label: 'Cash' },
  { href: '/dashboard/inventory', label: 'Inventory' },
  { href: '/dashboard/margin', label: 'Margin' },
  { href: '/dashboard/ap-ar', label: 'AP / AR' },
];

export function AnalyticsNav() {
  const pathname = usePathname();
  return (
    <div className="flex flex-wrap gap-1.5">
      {TABS.map((t) => {
        const active = pathname === t.href;
        return (
          <Link
            key={t.href}
            href={t.href}
            className={
              'rounded-md px-3 py-1.5 text-sm font-medium ' +
              (active ? 'bg-deep-navy text-white' : 'text-slate hover:bg-cloud')
            }
          >
            {t.label}
          </Link>
        );
      })}
    </div>
  );
}
