import Link from 'next/link';
import type { Metadata } from 'next';
import { ArrowLeft } from 'lucide-react';

export const metadata: Metadata = {
  title: 'Privacy Policy — BookWize',
  description:
    'How BookWize collects, uses, stores, and shares data when you use our bookkeeping automation service for Home Hardware dealers.',
};

const LAST_UPDATED = 'May 25, 2026';

export default function PrivacyPage() {
  return (
    <article className="container mx-auto max-w-3xl px-4 py-12">
      <Link
        href="/"
        className="inline-flex items-center gap-1 text-sm text-slate hover:text-ledger-blue mb-6"
      >
        <ArrowLeft className="h-4 w-4" /> Back to home
      </Link>

      <h1 className="text-h1 font-extrabold text-deep-navy">Privacy Policy</h1>
      <p className="text-sm text-slate mt-2">Last updated {LAST_UPDATED}</p>

      <Section title="Introduction">
        <p>
          BookWize ("we," "our," or "us") provides a bookkeeping automation
          service ("the Service") designed for Canadian Home Hardware dealers.
          This Privacy Policy explains what information we collect when you
          use the Service, how we use it, who we share it with, and the
          choices you have. By using BookWize you consent to the practices
          described here.
        </p>
      </Section>

      <Section title="Information we collect">
        <p>We collect the following categories of information:</p>
        <ul className="list-disc pl-6 space-y-2 mt-2">
          <li>
            <strong>Account information.</strong> Your name, email address,
            business name (store / entity), Home Hardware dealer code, and
            role within your organization. This is provided when you sign
            up via Clerk, our authentication provider.
          </li>
          <li>
            <strong>Financial and bookkeeping data.</strong> Chart of
            accounts, journal entries, bank transactions, accounts payable
            records, payroll details, fixed-asset registers, and any other
            data you upload or that we import on your behalf from
            connected systems. This is the operational data the Service is
            built to manage.
          </li>
          <li>
            <strong>QuickBooks Online OAuth tokens.</strong> When you
            connect QuickBooks, we receive and store an access token and a
            refresh token issued by Intuit. These tokens are scoped to
            your QBO realm and are used only to read your accounting data
            on your behalf. You can disconnect at any time from the
            settings page.
          </li>
          <li>
            <strong>Source documents.</strong> PDFs, CSVs, and other files
            you upload (bank statements, HH AP statements, invoices). We
            archive these so the journal entries we build remain
            auditable.
          </li>
          <li>
            <strong>Usage and audit data.</strong> Authentication events,
            journal-batch approval history, period-close events, and
            similar logs needed to maintain an audit trail.
          </li>
        </ul>
      </Section>

      <Section title="How we use your data">
        <ul className="list-disc pl-6 space-y-2">
          <li>
            <strong>To provide the Service.</strong> Build journals, run
            month-end close workflows, generate reports, match bank
            transactions, and otherwise carry out the core bookkeeping
            tasks you ask the Service to perform.
          </li>
          <li>
            <strong>AI-assisted bookkeeping.</strong> We send anonymized or
            transactional data to large-language-model providers (currently
            Anthropic) to power features such as bank-transaction
            classification, vendor recognition, and conversational
            assistance. We do not use your data to train third-party AI
            models.
          </li>
          <li>
            <strong>Billing.</strong> Plan information, store count, and
            invoice records are processed through Stripe to manage your
            subscription.
          </li>
          <li>
            <strong>Support.</strong> If you contact us, we may review the
            relevant data in your account to resolve your issue.
          </li>
          <li>
            <strong>Security and compliance.</strong> Detect abuse, satisfy
            audit-trail requirements, and meet legal obligations.
          </li>
        </ul>
      </Section>

      <Section title="Where your data is stored">
        <p>
          BookWize is hosted on Render. Application data lives in a
          PostgreSQL database managed by Render and hosted in North
          America (United States or Canadian regions, depending on the
          plan). Uploaded files are stored in Cloudflare R2 object
          storage. Both providers use encryption at rest and in transit.
        </p>
      </Section>

      <Section title="Third-party services">
        <p>The Service integrates with the following providers:</p>
        <ul className="list-disc pl-6 space-y-2 mt-2">
          <li>
            <strong>Intuit QuickBooks Online.</strong> OAuth-based read
            access to your QBO company file. Used to import chart of
            accounts, GL transactions, and opening balances.
          </li>
          <li>
            <strong>Clerk.</strong> Authentication and organization /
            membership management.
          </li>
          <li>
            <strong>Stripe.</strong> Subscription billing. Payment card
            data is handled directly by Stripe and never touches
            BookWize's servers.
          </li>
          <li>
            <strong>Anthropic.</strong> AI inference for classification and
            assistant features.
          </li>
          <li>
            <strong>Cloudflare R2.</strong> Document storage.
          </li>
          <li>
            <strong>Render.</strong> Application hosting and database.
          </li>
        </ul>
        <p className="mt-3">
          Each provider's handling of data is governed by their own
          privacy policy. We only share what is necessary for the Service
          to function.
        </p>
      </Section>

      <Section title="Data retention and deletion">
        <p>
          We retain your data for as long as your account is active. You
          may request deletion of your account and associated data at any
          time by emailing{' '}
          <a
            href="mailto:support@bookwize.ca"
            className="text-ledger-blue hover:underline"
          >
            support@bookwize.ca
          </a>
          . We will delete or anonymize personal data within 30 days of
          your request, except where retention is required by Canadian
          tax law or to resolve a legitimate dispute.
        </p>
      </Section>

      <Section title="Your rights">
        <p>
          You can access, correct, export, or delete the data in your
          BookWize account at any time. For data we hold beyond what the
          in-app tools expose, contact us at the email below.
        </p>
      </Section>

      <Section title="Cookies and similar technologies">
        <p>
          We use first-party cookies and local storage to keep you signed
          in and remember your active store. We do not use third-party
          advertising or tracking cookies.
        </p>
      </Section>

      <Section title="Changes to this policy">
        <p>
          We may update this Privacy Policy from time to time. When we do,
          we'll update the "Last updated" date at the top of this page and,
          for material changes, notify you in the app or by email.
        </p>
      </Section>

      <Section title="Contact us">
        <p>
          Questions about this policy or about how we handle your data?
          Email{' '}
          <a
            href="mailto:support@bookwize.ca"
            className="text-ledger-blue hover:underline"
          >
            support@bookwize.ca
          </a>
          .
        </p>
      </Section>
    </article>
  );
}

function Section({
  title,
  children,
}: {
  title: string;
  children: React.ReactNode;
}) {
  return (
    <section className="mt-8 text-ink leading-relaxed">
      <h2 className="text-h3 font-bold text-deep-navy mb-2">{title}</h2>
      <div className="space-y-2">{children}</div>
    </section>
  );
}
