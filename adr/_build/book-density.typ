// adr/_build/book-density.typ
//
// Purely presentational page-density tuning for the ADR PDF builds, injected
// via `pandoc --include-in-header`. Changes NO content and NO decision — it
// only tightens whitespace so the bundled book renders at a sane page count
// instead of the very loose pandoc/typst defaults. Fully reversible: drop the
// `--include-in-header` / `--metadata-file` / `-V fontsize` / `-V linestretch`
// flags from the build command and the document is byte-identical in content.
//
// Pairs with `_build/book-margins.yaml` (page margin) and the build
// invocations in `adr/README.md` (`-V fontsize=10pt -V linestretch=1.0`).
// Measured effect on the reading-order book: ~429 -> ~258 pp without
// mermaid-filter (≈ ~230 pp mermaid-rendered), section-level TOC retained.

#set par(leading: 0.55em, spacing: 0.62em)
#show raw: set text(size: 0.86em)
#set table(inset: 0.4em)
#show heading: set block(above: 0.85em, below: 0.45em)
