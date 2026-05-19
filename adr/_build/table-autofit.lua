-- adr/_build/table-autofit.lua
--
-- Purely presentational table-width fix for the ADR PDF builds, injected via
-- `pandoc --lua-filter`. Changes NO content and NO decision.
--
-- GFM pipe tables derive column widths from the delimiter dashes. Our tables
-- all use uniform `| --- | --- | --- |`, so pandoc emits equal relative widths
-- (e.g. `columns: (20%, 20%, …)` in the typst output). For heterogeneous
-- tables — a one-word `ADR` column next to a paragraph-length
-- `OZ Base Contracts` column (see ADR 016 § Contract Inventory) — equal widths
-- squish the prose column into a tall ribbon while the short columns waste
-- their share.
--
-- Clearing each column's width hands sizing to the typst engine: pandoc emits
-- a bare `columns: N`, which typst lays out as content-sized `auto` columns —
-- narrow columns shrink, prose columns get the room, text still wraps within
-- the page. Fully reversible: drop the `--lua-filter=_build/table-autofit.lua`
-- flag from the build command and the document is byte-identical in content.
--
-- Pairs with the page-density config (`_build/book-density.typ` /
-- `_build/book-margins.yaml`); see the presentational-config note in
-- adr/README.md § Building a single PDF. Wired into both PDF builds.

function Table(t)
  for i, colspec in ipairs(t.colspecs) do
    -- colspec is { Alignment, ColWidth }; keep alignment, drop the width.
    t.colspecs[i] = { colspec[1], 'ColWidthDefault' }
  end
  return t
end
