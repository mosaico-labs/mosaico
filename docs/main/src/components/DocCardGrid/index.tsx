import type {ReactNode} from 'react';
import Link from '@docusaurus/Link';
import {
  BookOpenIcon,
  RocketIcon,
  SquaresFourIcon,
  DownloadSimpleIcon,
  MagnifyingGlassIcon,
  CodeIcon,
  ListBulletsIcon,
  WaveformIcon,
  RobotIcon,
  BinocularsIcon,
  TreeStructureIcon,
  CaretRightIcon,
} from '@phosphor-icons/react';
import styles from './styles.module.css';

const ICON_PROPS = {size: 20, weight: 'fill'} as const;

const ICONS: Record<string, ReactNode> = {
  book:       <BookOpenIcon {...ICON_PROPS} />,
  rocket:     <RocketIcon {...ICON_PROPS} />,
  grid:       <SquaresFourIcon {...ICON_PROPS} />,
  download:   <DownloadSimpleIcon {...ICON_PROPS} />,
  search:     <MagnifyingGlassIcon {...ICON_PROPS} />,
  code:       <CodeIcon {...ICON_PROPS} />,
  list:       <ListBulletsIcon {...ICON_PROPS} />,
  waveform:   <WaveformIcon {...ICON_PROPS} />,
  robot:      <RobotIcon {...ICON_PROPS} />,
  binoculars: <BinocularsIcon {...ICON_PROPS} />,
  tree:       <TreeStructureIcon {...ICON_PROPS} />,
};

type CardProps = {
  title: string;
  href: string;
  icon?: string;
  full?: boolean;
  children?: ReactNode;
};

type SectionProps = {
  title: string;
  children: ReactNode;
};

export function Card({title, href, icon, full, children}: CardProps): ReactNode {
  return (
    <Link to={href} className={`${styles.card}${full ? ` ${styles.cardFull}` : ''}`}>
      {icon && ICONS[icon] && <span className={styles.icon}>{ICONS[icon]}</span>}
      <span className={styles.cardText}>
        <span className={styles.cardTitle}>{title}</span>
        {children && <span className={styles.cardDescription}>{children}</span>}
      </span>
      <span className={styles.chevron}><CaretRightIcon size={14} weight="bold" /></span>
    </Link>
  );
}

export function Section({title, children}: SectionProps): ReactNode {
  return (
    <div className={styles.section}>
      <div className={styles.sectionTitle}>{title}</div>
      <div className={styles.grid}>{children}</div>
    </div>
  );
}

export default function DocCardGrid({children}: {children: ReactNode}): ReactNode {
  return <div>{children}</div>;
}
