This is the primary documentation site for Mosaico, built with [Docusaurus](https://docusaurus.io/).

## Running locally

```bash
npm start       # start dev server with live reload
npm run build   # build the static site
```

## Build output

After `npm run build`, the `build/` directory contains the complete static website along with:

- `build/llms.txt` — concise site index for LLMs
- `build/llms-full.txt` — full content dump for LLMs

## Search

The site uses [Algolia DocSearch](https://docsearch.algolia.com/) for full-text search.
