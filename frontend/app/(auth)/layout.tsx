import Image from 'next/image';
import Link from 'next/link';

export default function AuthLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <div className="min-h-screen flex">
      <aside className="hidden lg:flex flex-col w-1/2 bg-gradient-primary text-white p-12">
        <Link href="/" aria-label="BookWize home">
          <Image
            src="/brand/bookwize-logo-reversed.svg"
            alt="BookWize"
            width={200}
            height={48}
          />
        </Link>
        <div className="flex-1 flex items-center">
          <div className="max-w-md">
            <h2 className="text-h2 font-bold mb-4">
              Month-end close, automated for Home Hardware dealers.
            </h2>
            <p className="text-white/80">
              Smart bookkeeping. Clearer business. Built for the way dealers
              actually work — not generic SaaS workflows bolted onto a P&amp;L.
            </p>
          </div>
        </div>
        <p className="text-xs text-white/60">
          © {new Date().getFullYear()} BookWize. Built for Home Hardware dealers
          across Canada.
        </p>
      </aside>
      <main className="flex-1 flex items-center justify-center p-6 bg-cloud">
        <div className="w-full max-w-md">{children}</div>
      </main>
    </div>
  );
}
