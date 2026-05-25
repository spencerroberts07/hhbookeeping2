import Image from 'next/image';
import Link from 'next/link';

export default function MarketingLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <div className="min-h-screen flex flex-col bg-white">
      <header className="border-b border-border bg-white sticky top-0 z-30">
        <div className="container mx-auto flex items-center justify-between py-3 px-4">
          <Link href="/" aria-label="BookWize home">
            <Image
              src="/brand/bookwize-logo-primary.svg"
              alt="BookWize"
              width={140}
              height={36}
              priority
            />
          </Link>
          <nav className="hidden md:flex items-center gap-6 text-sm font-semibold text-deep-navy">
            <Link href="/pricing" className="hover:text-ledger-blue">
              Pricing
            </Link>
            <Link href="/about" className="hover:text-ledger-blue">
              About
            </Link>
            <Link
              href="/sign-in"
              className="text-deep-navy hover:text-ledger-blue"
            >
              Sign in
            </Link>
            <Link
              href="/sign-up"
              className="rounded-xl bg-bw-teal text-white px-4 py-2 hover:bg-aqua transition"
            >
              Start free trial
            </Link>
          </nav>
        </div>
      </header>
      <main className="flex-1">{children}</main>
      <footer className="bg-deep-navy text-white/70 py-8 px-4 mt-12">
        <div className="container mx-auto flex flex-col md:flex-row items-start md:items-center justify-between gap-4">
          <Image
            src="/brand/bookwize-logo-reversed.svg"
            alt="BookWize"
            width={140}
            height={36}
          />
          <div className="text-sm">
            <Link href="/pricing" className="hover:text-bw-teal mr-4">
              Pricing
            </Link>
            <Link href="/terms" className="hover:text-bw-teal mr-4">
              Terms
            </Link>
            <Link href="/privacy" className="hover:text-bw-teal">
              Privacy
            </Link>
          </div>
          <div className="text-xs">
            © {new Date().getFullYear()} BookWize · Built for Home Hardware
            dealers across Canada
          </div>
        </div>
      </footer>
    </div>
  );
}
