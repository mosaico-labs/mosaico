import { Children, type ReactNode } from 'react';
import { CaretRightIcon } from '@phosphor-icons/react';
import styles from './styles.module.css';

/**
 * A section that folds under its heading.
 *
 * The first child (a Markdown heading) becomes the clickable title, the rest
 * is the folded content. It renders a plain native `<details>`, so it doesn't
 * get the admonition look of the theme's `<details>`, and the heading still
 * shows up in the table of contents.
 */
export default function CollapsibleSection({
  children,
  open = false,
}: {
  children: ReactNode;
  open?: boolean;
}): ReactNode {
  const [heading, ...content] = Children.toArray(children);
  return (
    <details className={styles.section} open={open}>
      <summary className={styles.summary}>
        <CaretRightIcon className={styles.caret} weight="bold" aria-hidden />
        {heading}
      </summary>
      {content}
    </details>
  );
}
