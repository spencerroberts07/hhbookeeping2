import Link from 'next/link';

export const metadata = {
  title: 'About BookWize — Built by a dealer, for dealers',
  description:
    "BookWize was built by a Home Hardware dealer who needed better tools. A letter from Spencer Roberts, dealer/owner of Bridlewood Home Hardware and Lyndhurst Home Building Centre.",
};

export default function AboutPage() {
  return (
    <>
      {/* Hero — same gradient + type treatment as the landing page so
          marketing pages feel like one product, not a stitched-together
          microsite. */}
      <section className="bg-gradient-primary text-white">
        <div className="container mx-auto px-4 py-20 text-center max-w-3xl">
          <h1 className="text-h1 lg:text-display font-extrabold leading-tight">
            Built by a dealer,{' '}
            <span className="text-bw-teal">for dealers.</span>
          </h1>
          <p className="text-lg text-white/80 mt-6 max-w-2xl mx-auto">
            BookWize exists because the tools available to Home Hardware
            dealers weren't good enough. So we built something better.
          </p>
        </div>
      </section>

      {/* Founder letter — narrow column for readability, serif body for
          a warmer, more personal feel than the rest of the marketing
          site, subtle left accent to set the letter apart from prose. */}
      <section className="bg-white py-16 px-4">
        <article className="container mx-auto max-w-2xl">
          <p className="text-xs font-semibold tracking-widest text-bw-teal uppercase mb-6">
            From the desk of Spencer Roberts
          </p>

          <div className="border-l-2 border-bw-teal/40 pl-6 md:pl-8 font-serif text-lg leading-relaxed text-ink space-y-5">
            <p>
              I became a Home Hardware dealer in 2023. Like most dealers, I
              inherited a stack of tools that were never really built for us —
              QuickBooks for the books, NETEmployer for payroll, a spreadsheet
              for HH AP reconciliation, and a bookkeeper to hold it all
              together. Every month-end felt like an exercise in damage
              control. Manual entries between systems meant mistakes. Mistakes
              meant corrections. Corrections meant more time, more fees, and
              less confidence in the numbers sitting in front of me.
            </p>

            <p>
              The frustrating part wasn't any one tool. It was the sum of
              them. I was paying for four platforms to do the job that one
              should do — and none of them understood what it actually meant
              to run a Home Hardware store.
            </p>

            <p className="text-deep-navy font-semibold not-italic font-sans text-xl">
              So I built BookWize.
            </p>

            <p>
              Not as a software project. As a solution to my own problem.
              Every feature in BookWize exists because I needed it at
              Bridlewood Home Hardware or Lyndhurst Home Building Centre. The
              HH AP reconciliation, the payroll module, the GL import, the
              period closes — I built each one because I was doing it manually
              and it was costing me time and money I didn't have to waste.
            </p>

            <p>
              That's the difference with BookWize. This isn't software built
              by developers who've read about retail. It's built by a dealer,
              running live on my own stores, updated every time I hit a
              problem you're probably hitting too.
            </p>

            <p>
              My goal is simple: give every Home Hardware dealer the financial
              clarity and operational efficiency that used to require a full
              accounting team. Eliminate the redundant costs. Remove the
              manual work. Close your books with confidence every month.
            </p>

            <p className="text-deep-navy font-semibold">
              Dealer to dealer — I built what I wish I'd had on day one.
            </p>
          </div>

          {/* Signature block */}
          <div className="mt-10 pt-6 border-t border-border">
            <div className="text-xl font-bold text-deep-navy">
              Spencer Roberts
            </div>
            <div className="text-sm text-slate mt-1">
              Dealer / Owner, Bridlewood Home Hardware
              <br className="hidden md:inline" />
              <span className="md:hidden"> · </span>Lyndhurst Home Building
              Centre
            </div>
          </div>
        </article>
      </section>

      {/* CTA — mirrors the landing-page closer so the conversion path is
          consistent across marketing pages. */}
      <section className="bg-cloud py-16 px-4 border-t border-border">
        <div className="container mx-auto text-center max-w-xl">
          <h2 className="text-h2 text-deep-navy mb-3">
            Ready to close your books with confidence?
          </h2>
          <p className="text-slate mb-8">
            Start a free 30-day trial. No credit card up front. Cancel any
            time.
          </p>
          <div className="flex flex-wrap items-center justify-center gap-3">
            <Link
              href="/sign-up"
              className="rounded-xl bg-bw-teal text-white font-semibold px-8 py-4 hover:bg-aqua transition"
            >
              Get started
            </Link>
            <Link
              href="/pricing"
              className="rounded-xl border border-deep-navy text-deep-navy font-semibold px-6 py-4 hover:bg-white transition"
            >
              See pricing
            </Link>
          </div>
        </div>
      </section>
    </>
  );
}
