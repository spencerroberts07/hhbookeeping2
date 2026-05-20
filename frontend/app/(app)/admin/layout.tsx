'use client';

import { redirect } from 'next/navigation';
import { useIsBookwizeAdmin } from '@/lib/store/user';

export default function AdminLayout({ children }: { children: React.ReactNode }) {
  const isBookwizeAdmin = useIsBookwizeAdmin();
  if (!isBookwizeAdmin) {
    redirect('/dashboard');
  }
  return <>{children}</>;
}
