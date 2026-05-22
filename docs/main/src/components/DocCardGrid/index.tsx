import type { ReactNode } from 'react';
import Link from '@docusaurus/Link';
import {
  BookOpenTextIcon,
  RocketIcon,
  EngineIcon,
  TrayArrowUpIcon,
  StackPlusIcon,
  ShuffleIcon,
  BroadcastIcon,
  FunnelIcon,
  SlidersHorizontalIcon,
  IntersectThreeIcon,
  FlowArrowIcon,
  ChartLineIcon,
  PulseIcon,
  FilePyIcon,
  FileCppIcon,
  FileCIcon,
  FileCodeIcon,
  CaretRightIcon,
  RobotIcon,
} from '@phosphor-icons/react';
import { siPython, siRust, siCplusplus, siC } from 'simple-icons';
import styles from './styles.module.css';

const ICON_PROPS = { size: 20, weight: 'fill' } as const;

function SimpleIcon({ icon, size = 20 }: { icon: { path: string }; size?: number }): ReactNode {
  return (
    <svg role="img" viewBox="0 0 24 24" width={size} height={size} fill="currentColor">
      <path d={icon.path} />
    </svg>
  );
}

const ICONS: Record<string, ReactNode> = {
  book: <BookOpenTextIcon {...ICON_PROPS} />,
  rocket: <RocketIcon {...ICON_PROPS} />,
  cpu: <EngineIcon {...ICON_PROPS} />,
  tray_up: <TrayArrowUpIcon {...ICON_PROPS} />,
  stack_plus: <StackPlusIcon {...ICON_PROPS} />,
  shuffle: <ShuffleIcon {...ICON_PROPS} />,
  broadcast: <BroadcastIcon {...ICON_PROPS} />,
  funnel: <FunnelIcon {...ICON_PROPS} />,
  sliders: <SlidersHorizontalIcon {...ICON_PROPS} />,
  intersect: <IntersectThreeIcon {...ICON_PROPS} />,
  flow: <FlowArrowIcon {...ICON_PROPS} />,
  chart: <ChartLineIcon {...ICON_PROPS} />,
  pulse: <PulseIcon {...ICON_PROPS} />,
  robot: <RobotIcon {...ICON_PROPS} />,
  python: <SimpleIcon icon={siPython} />,
  rust: <SimpleIcon icon={siRust} />,
  cpp: <SimpleIcon icon={siCplusplus} />,
  c: <SimpleIcon icon={siC} />,
  file_py: <FilePyIcon {...ICON_PROPS} />,
  file_cpp: <FileCppIcon {...ICON_PROPS} />,
  file_c: <FileCIcon {...ICON_PROPS} />,
  file_rs: <FileCodeIcon {...ICON_PROPS} />,
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
  description?: string;
  children: ReactNode;
};

export function Card({ title, href, icon, full, children }: CardProps): ReactNode {
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

export function Section({ title, description, children }: SectionProps): ReactNode {
  return (
    <div className={styles.section}>
      <div className={styles.sectionTitle}>{title}</div>
      {description && <p className={styles.sectionDescription}>{description}</p>}
      <div className={styles.grid}>{children}</div>
    </div>
  );
}

export default function DocCardGrid({ children }: { children: ReactNode }): ReactNode {
  return <div>{children}</div>;
}
