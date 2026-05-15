import React, { type ReactNode, useState, useRef, useEffect } from 'react';
import clsx from 'clsx';
import { useWindowSize } from '@docusaurus/theme-common';
import { useDoc } from '@docusaurus/plugin-content-docs/client';
import DocItemPaginator from '@theme/DocItem/Paginator';
import DocVersionBanner from '@theme/DocVersionBanner';
import DocVersionBadge from '@theme/DocVersionBadge';
import DocItemFooter from '@theme/DocItem/Footer';
import DocItemTOCMobile from '@theme/DocItem/TOC/Mobile';
import DocItemTOCDesktop from '@theme/DocItem/TOC/Desktop';
import DocItemContent from '@theme/DocItem/Content';
import DocBreadcrumbs from '@theme/DocBreadcrumbs';
import ContentVisibility from '@theme/ContentVisibility';
import type { Props } from '@theme/DocItem/Layout';
import { FileText, Copy, ArrowSquareOut, Check } from '@phosphor-icons/react';

import styles from './styles.module.css';

function useDocTOC() {
  const { frontMatter, toc } = useDoc();
  const windowSize = useWindowSize();
  const hidden = frontMatter.hide_table_of_contents;
  const canRender = !hidden && toc.length > 0;
  const mobile = canRender ? <DocItemTOCMobile /> : undefined;
  const desktop =
    canRender && (windowSize === 'desktop' || windowSize === 'ssr') ? (
      <DocItemTOCDesktop />
    ) : undefined;
  return { hidden, mobile, desktop };
}

function RawMarkdownButton({ source }: { source: string }) {
  const [open, setOpen] = useState(false);
  const [copied, setCopied] = useState(false);
  const ref = useRef<HTMLDivElement>(null);

  const rawUrl = source.replace('@site/', '/').replace(/\.mdx$/, '.md');

  useEffect(() => {
    function onClickOutside(e: MouseEvent) {
      if (ref.current && !ref.current.contains(e.target as Node)) {
        setOpen(false);
      }
    }
    document.addEventListener('mousedown', onClickOutside);
    return () => document.removeEventListener('mousedown', onClickOutside);
  }, []);

  async function copyMarkdown() {
    try {
      const res = await fetch(rawUrl);
      if (!res.ok) return;
      const text = await res.text();
      await navigator.clipboard.writeText(text);
      setCopied(true);
      setOpen(false);
      setTimeout(() => setCopied(false), 2000);
    } catch {
      // raw files not available (e.g. dev without prestart)
    }
  }

  return (
    <div className={styles.rawBtn} ref={ref}>
      <button
        className={clsx(styles.rawBtnTrigger, copied && styles.rawBtnCopied, 'raw-markdown-btn')}
        onClick={() => setOpen((o) => !o)}
        aria-label="Markdown options"
      >
        {copied ? <Check size={13} weight="bold" /> : <FileText size={13} />}
        <span>{copied ? 'Copied!' : 'Markdown'}</span>
      </button>
      {open && (
        <div className={styles.rawBtnDropdown}>
          <button className={styles.rawBtnItem} onClick={copyMarkdown}>
            <Copy size={13} />
            Copy Markdown
          </button>
          <a
            className={styles.rawBtnItem}
            href={rawUrl}
            target="_blank"
            rel="noreferrer"
            onClick={() => setOpen(false)}
          >
            <ArrowSquareOut size={13} />
            View as Markdown
          </a>
        </div>
      )}
    </div>
  );
}

export default function DocItemLayout({ children }: Props): ReactNode {
  const docTOC = useDocTOC();
  const { metadata } = useDoc();
  return (
    <div className="row">
      <div className={clsx('col', !docTOC.hidden && styles.docItemCol)}>
        <ContentVisibility metadata={metadata} />
        <DocVersionBanner />
        <div className={styles.docItemContainer}>
          <article>
            <DocBreadcrumbs />
            <DocVersionBadge />
            <div className={styles.docToolbar}>
              <RawMarkdownButton source={metadata.source} />
            </div>
            {docTOC.mobile}
            <DocItemContent>{children}</DocItemContent>
            <DocItemFooter />
          </article>
          <DocItemPaginator />
        </div>
      </div>
      {docTOC.desktop && <div className="col col--3">{docTOC.desktop}</div>}
    </div>
  );
}
