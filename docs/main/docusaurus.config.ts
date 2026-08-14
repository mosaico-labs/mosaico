import { themes as prismThemes } from 'prism-react-renderer';
import type { Config } from '@docusaurus/types';
import type * as Preset from '@docusaurus/preset-classic';

// This runs in Node.js - Don't use client-side code here (browser APIs, JSX...)
const config: Config = {
  title: 'Mosaico Doc',
  tagline: 'The Data Platform for Robotics and Physical AI',
  favicon: 'img/favicon-48.svg',

  // Future flags, see https://docusaurus.io/docs/api/docusaurus-config#future
  future: {
    v4: true, // Improve compatibility with the upcoming Docusaurus v4
  },

  // Set the production url of your site here
  url: 'https://docs.mosaico.dev',
  // Set the /<baseUrl>/ pathname under which your site is served
  // For GitHub pages deployment, it is often '/<projectName>/'
  baseUrl: '/',

  // GitHub pages deployment config.
  // If you aren't using GitHub pages, you don't need these.
  organizationName: 'mosaico-labs', // Usually your GitHub org/user name.
  projectName: 'mosaico', // Usually your repo name.

  onBrokenLinks: 'throw',

  // Even if you don't use internationalization, you can use this field to set
  // useful metadata like html lang. For example, if your site is Chinese, you
  // may want to replace "en" with "zh-Hans".
  i18n: {
    defaultLocale: 'en',
    locales: ['en'],
  },

  stylesheets: [
    {
      href: 'https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;700&display=swap',
      type: 'text/css',
    },
    {
      href: 'https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@300;400;500;600;700&display=swap',
      type: 'text/css',
    },
    {
      href: 'https://fonts.googleapis.com/css2?family=Noto+Sans:wght@300;400;500;600;700&display=swap',
      type: 'text/css',
    },
  ],
  
  plugins: [[
    'docusaurus-plugin-llms',
    {
      generateLLMsTxt: true,
      generateLLMsFullTxt: true,
      generateMarkdownFiles: true,
      customSections: [
        {
          title: 'SDKs',
          content: '- [Python SDK](https://github.com/your-repo/python-sdk) - The official Python client for our API.',
        }
      ],
    }
  ]],

  presets: [
    [
      'classic',
      {
        docs: {
          sidebarPath: './sidebars.ts',
          routeBasePath: "/",
          // Please change this to your repo.
          // Remove this to remove the "edit this page" links.
          breadcrumbs: false, // This disables breadcrumbs globally
        },
        theme: {
          customCss: './src/css/custom.css',
        },
      } satisfies Preset.Options,
    ],
  ],

  themeConfig: {
    image: 'img/social-card.png',
    colorMode: {
      defaultMode: "dark",
      disableSwitch: true,
      respectPrefersColorScheme: false,
    },
    announcementBar: {
      id: 'support_us',
      content:
        'Ready to explore? Connect to <a target="_blank" rel="noopener noreferrer" href="https://demo.mosaico.dev">demo.mosaico.dev</a> to claim your API key.',
      textColor: '#FFFFFF',
      isCloseable: false,
    },
    navbar: {
      logo: {
        alt: 'Mosaico',
        src: 'img/logo.svg',
      },
      items: [
        {
          href: 'https://github.com/mosaico-labs/mosaico',
          label: 'GitHub',
          position: 'right',
        },
      ],
    },
    footer: {
      style: 'dark',
      links: [
        {
          // A single flat list of links
          items: [
            { label: 'Website', href: 'https://mosaico.dev' },
            { label: 'llm.txt', to: 'https://docs.mosaico.dev/llms.txt' },
            { label: 'llm-full.txt', to: 'https://docs.mosaico.dev/llms-full.txt' },
          ],
        },
      ],
      copyright: `© ${new Date().getFullYear()} Mosaico. All your base are belong to us.`,
    },
    prism: {
      additionalLanguages: ['bash'],
      theme: prismThemes.palenight,
      darkTheme: prismThemes.vsDark,
    },
    algolia: {
      // The application ID provided by Algolia
      appId: 'L83UJF4D2C',

      // Public API key: it is safe to commit it
      apiKey: 'f2da1153b86c45b9ab7f0418f8dafbd1',

      indexName: 'mosaico-doc-prod',

      // Optional: see doc section below
      contextualSearch: true,

      // Optional: Algolia search parameters
      searchParameters: {},

      // Optional: path for search page that enabled by default (`false` to disable it)
      searchPagePath: 'search',

      // Optional: whether the insights feature is enabled or not on Docsearch (`false` by default)
      insights: false,

    },
  } satisfies Preset.ThemeConfig,

};

export default config;
