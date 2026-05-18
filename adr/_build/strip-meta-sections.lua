-- adr/_build/strip-meta-sections.lua
--
-- Drops debate/deferral scaffolding from the rendered PDF while leaving the
-- source `.md` files untouched. Canonical ADR section taxonomy (see
-- adr/README.md § Canonical section taxonomy):
--
--   * "Deferred & Open"  — STRIPPED. The canonical home for deferred work
--     and open questions. Legacy names ("Open Questions", "Future Work")
--     are kept in the patterns below defensively so any un-migrated or
--     future drift is still caught. ("Forward references" is NOT here — it
--     is a Cross-ADR Impact alias, retained; see below.)
--   * "Alternatives Considered" / "Considered Alternatives" / "Why not …"
--     — STRIPPED. Rejected-alternative records (a distinct concept; most live
--     in _history/, these patterns catch the inline remainder).
--   * "Reading Order" — STRIPPED (book only). The architecture overview's
--     chapter-by-chapter reading guide; in the book the part dividers and
--     generated ToC *are* the reading order, so the prose list is
--     redundant. Kept verbatim in the source and the numeric build.
--   * "Cross-ADR Impact" — NOT stripped. It carries substantive cross-cutting
--     design content (amendments to other ADRs, coupling notes), not
--     scaffolding. Do not add it to strip_patterns.
--   * "Non-Goals" / "References" — NOT stripped. Substantive.
--
-- Also drops the per-ADR `**Date:** … / **Status:** …` preamble paragraphs
-- (book-formatting noise that ages poorly) and rewrites internal
-- cross-references pointing at the stripped sections (same-file and
-- cross-file) to plain text so typst doesn't choke on dangling labels.
--
-- Wired into the reading-order book build only (see adr/README.md
-- § Building a single PDF). The numeric build keeps everything so it stays
-- usable as the complete-spec reference.

local strip_patterns = {
  "^deferred & open",            -- canonical: deferred work / open questions
  "^alternatives considered$",
  "^considered alternatives$",
  "alternatives considered$",   -- "§6 — Alternatives Considered"
  "^open questions$",            -- legacy alias, retained defensively
  "^future work",                -- legacy: "Future Work" / "Future Work: …"
  "^why not ",                   -- "Why not Uniswap V3, …"
  "^reading order$",             -- architecture overview's chapter list;
                                 -- book ToC + part dividers supersede it
}

local function should_strip(text)
  for _, pat in ipairs(strip_patterns) do
    if text:match(pat) then return true end
  end
  return false
end

-- Collect every identifier from `node` and its full subtree into `into`.
-- Headers are the common case but Div, CodeBlock, Figure, Table, and
-- Inline elements (Span, Link) can also carry IDs that other parts of
-- the doc reference; missing them produces dangling-label errors in
-- typst when the host section gets stripped.
local function collect_ids(node, into)
  if node.identifier and node.identifier ~= "" then
    into[node.identifier] = true
  end
  if node.walk then
    node:walk({
      Block = function(b)
        if b.identifier and b.identifier ~= "" then into[b.identifier] = true end
      end,
      Inline = function(i)
        if i.identifier and i.identifier ~= "" then into[i.identifier] = true end
      end,
    })
  end
end

-- Run as a single document-level pass so the link-rewrite step can see the
-- complete set of stripped IDs. Defining only `Pandoc` (not `Block`/`Link`)
-- avoids the ordering hazard where a Block filter that drops a header runs
-- before a Link filter that needs to know that header's ID.
function Pandoc(doc)
  local stripped_ids = {}
  local out_blocks = {}
  local stripping_at_level = nil

  for _, el in ipairs(doc.blocks) do
    if el.t == "Header" then
      -- A header at the trigger level (or shallower) ends the active
      -- strip before we evaluate whether *this* header is itself a new
      -- trigger.
      if stripping_at_level and el.level <= stripping_at_level then
        stripping_at_level = nil
      end
      local text = pandoc.utils.stringify(el):lower()
      if (not stripping_at_level) and should_strip(text) then
        stripping_at_level = el.level
        collect_ids(el, stripped_ids)
      else
        if stripping_at_level then
          -- Header nested under a stripped section — collect its ID
          -- and any descendant IDs so inbound links to *sub*-elements
          -- also rewrite cleanly.
          collect_ids(el, stripped_ids)
        else
          table.insert(out_blocks, el)
        end
      end
    else
      if stripping_at_level then
        -- Non-header block inside a stripped section. Walk its full
        -- subtree for any identifier (Div, CodeBlock, Figure, Table,
        -- Span, Link, …) before discarding so links pointing at those
        -- IDs from elsewhere can be rewritten to plain text.
        collect_ids(el, stripped_ids)
      else
        table.insert(out_blocks, el)
      end
    end
  end

  doc.blocks = out_blocks

  -- Two transformations in the same `doc:walk` pass:
  --
  -- 1. Link rewriting — replace links whose anchor portion matches a
  --    stripped ID with the link's display content so the surrounding
  --    prose still reads as plain text. Handles both `#anchor` (same-
  --    file) and `file.md#anchor` (cross-file → internal after
  --    concatenation) targets.
  --
  -- 2. Per-ADR Date/Status preamble — each ADR opens with
  --    `**Date:** YYYY-MM-DD` followed by `**Status:** Draft` (or
  --    similar) between the H1 title and `## Context`. Without a
  --    blank line between them pandoc parses the pair as a single
  --    Para containing a SoftBreak; with a blank line (e.g. ADR 022)
  --    they parse as two separate Paras. The leading stringified text
  --    starts with `Date:` or `Status:` either way — drop those.
  doc = doc:walk({
    Link = function(link)
      local target = link.target
      if not target then return nil end
      local hash_pos = target:find("#")
      if not hash_pos then return nil end
      local anchor = target:sub(hash_pos + 1)
      if stripped_ids[anchor] then
        return link.content
      end
      return nil
    end,
    Para = function(p)
      local text = pandoc.utils.stringify(p)
      if text:match("^Date:%s") or text:match("^Status:%s") then
        return {}
      end
      return nil
    end,
  })

  return doc
end
