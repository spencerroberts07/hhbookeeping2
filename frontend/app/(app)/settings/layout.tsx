'use client';

import Link from 'next/link';
import { usePathname } from 'next/navigation';
import { Topbar } from '@/components/layout/topbar';
import { Card } from '@/components/ui/card';
import { cn } from '@/lib/utils';

const TABS = [
  { href: '/settings/store', label: 'Store' },
  { href: '/settings/team', label: 'Team' },
  { href: '/settings/billing', label: 'Billing' },
  { href: '/settings/accounts', label: 'Chart of accounts' },
  { href: '/settings/integrations', label: 'Integrations' },
  { href: '/settings/data-import', label: 'Data import' },
  { href: '/settings/notifications', label: 'Notifications' },
];

export default function SettingsLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  const pathname = usePathname();
  return (
    <>
      <Topbar title="Settings" />
      <main className="p-6 grid grid-cols-1 lg:grid-cols-[220px_1fr] gap-4">
        <aside>
          <Card className="p-2">
            <nav className="flex flex-col gap-1">
              {TABS.map((t) => {
                const active = pathname === t.href || pathname.startsWith(`${t.href}/`);
                return (
                  <Link
                    key={t.href}
                    href={t.href}
                    className={cn(
                      'rounded-md px-3 py-2 text-sm',
                      active
                        ? 'bg-deep-navy text-white font-semibold'
                        : 'text-ink hover:bg-cloud',
                    )}
                  >
                    {t.label}
                  </Link>
                );
              })}
            </nav>
          </Card>
        </aside>
        <section>{children}</section>
      </main>
    </>
  );
}
