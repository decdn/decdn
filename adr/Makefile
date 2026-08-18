# adr/Makefile — build the ADR PDFs without copy-pasting pandoc from README.md
#
#   make              # build both PDFs (mermaid-rendered, the canonical build)
#   make book         # reading-order adrs-book.pdf only
#   make numeric      # numeric adrs.pdf only
#   make MERMAID=0    # build without mermaid-filter (content-soundness /
#                     # environments lacking mmdc/puppeteer); diagrams ship
#                     # as raw source text — see README "Building a single PDF"
#   make clean        # remove the generated PDFs
#   make check-book-list   # fail if a numbered/appendix .md is missing from
#                          # the reading-order list — or listed but not on disk
#   make check-bare-refs        # fail on any bare (un-linked) ADR reference
#   make check-md-anchors       # fail on ADR cross-links missing a #anchor
#   make check-anchor-targets   # fail on #anchors resolving to no heading
#   make check-no-section-numbers # fail on numeric §N refs or numbered headings
#   make check-refs             # all four reference-hygiene guards above
#
# This Makefile is the single source of truth for the build invocation;
# README.md documents *what* the builds are and points here for *how*.
# Recipes run from the adr/ directory, so every path below is adr/-relative.

DATE    := $(shell date +%F)
PANDOC  := pandoc --from=markdown+gfm_auto_identifiers --toc --toc-depth=2 --pdf-engine=typst

# Purely-presentational page-density tuning (see _build/book-density.typ).
# Changes no content/decision; drop these four flags for byte-identical
# content at the loose pandoc/typst default density.
DENSITY := --metadata-file=_build/book-margins.yaml -V fontsize=10pt \
           -V linestretch=1.0 --include-in-header=_build/book-density.typ

# Purely-presentational table-width fix (see _build/table-autofit.lua).
# Hands column sizing to typst (content-aware auto columns) instead of
# pandoc's uniform GFM-derived equal widths, which squish prose-heavy
# columns. Drop this flag for byte-identical content at pandoc's defaults.
TABLEFIT := --lua-filter=_build/table-autofit.lua

# Mermaid rendering is the canonical build (CI). MERMAID=0 omits it for
# environments without mmdc/puppeteer and for the no-mermaid soundness check.
MERMAID ?= 1
ifeq ($(MERMAID),1)
  MERMAID_FLAG := -F mermaid-filter
  # Make npm's global bin visible even if the shell profile hasn't.
  NPMPATH      := PATH="$$(npm config get prefix)/bin:$$PATH"
else
  MERMAID_FLAG :=
  NPMPATH      :=
endif

# Numeric build: architecture.md, then every numbered ADR, then every
# appendix, then glossary.md (back matter). Numbered/appendix files are
# picked up automatically; glossary.md is wired in explicitly below.
NUMERIC_SRCS  := $(sort $(wildcard [0-9]*.md))
APPENDIX_SRCS := $(sort $(wildcard appendix-*.md))

# Reading-order build: hand-maintained thematic order with part dividers.
# MUST stay in lockstep with architecture.md § Reading Order — there is no
# auto-generation (see README and `make check-book-list`).
BOOK_SRCS := \
  _build/preface.md \
  architecture.md \
  _build/part-1-foundations.md \
  000-language.md 001-network.md 002-content-addressing.md 005-protocol.md \
  038-bao-verified-range-streaming.md \
  _build/part-2-discovery.md \
  022-content-discovery.md 037-regional-proxy-warming.md \
  039-multi-source-parallel-fetch.md \
  _build/part-3-payments.md \
  003-payments.md 012-client.md 024-account-abstraction.md \
  _build/part-4-tokenomics.md \
  026-tokenomics.md 018-liquidity-strategy.md \
  _build/part-5-verification.md \
  014-on-chain-verification.md 008-reputation.md 011-content-takedown.md \
  028-slashing-appeals.md \
  030-node-region-self-attestation.md \
  _build/part-6-governance.md \
  009-governance.md 036-served-bytes-voting-weight.md 016-contract-interactions.md \
  _build/part-7-operations.md \
  019-node-onboarding.md \
  _build/part-8-supporting.md \
  013-schema-evolution.md 017-privacy.md \
  _build/part-9-appendices.md \
  appendix-bundles.md appendix-observability.md \
  appendix-blob-cache-eviction.md appendix-l2-deployment.md \
  appendix-poc-production-seams.md appendix-binaries.md \
  appendix-local-admin-http.md appendix-operator-key-rotation.md \
  appendix-operator-upgrade-path.md appendix-fraud-detection.md \
  glossary.md

DENSITY_CFG := _build/book-density.typ _build/book-margins.yaml _build/table-autofit.lua

.DEFAULT_GOAL := all
.PHONY: all numeric book clean help check-book-list check-bare-refs check-md-anchors check-anchor-targets check-no-section-numbers check-no-history-links check-refs

all: adrs.pdf adrs-book.pdf

numeric: adrs.pdf
book: adrs-book.pdf

adrs.pdf: architecture.md $(NUMERIC_SRCS) $(APPENDIX_SRCS) glossary.md $(DENSITY_CFG)
	$(NPMPATH) $(PANDOC) $(MERMAID_FLAG) $(DENSITY) $(TABLEFIT) \
	  -V title="deCDN — Architecture Decision Records" \
	  -V date="$(DATE)" \
	  architecture.md $(NUMERIC_SRCS) $(APPENDIX_SRCS) glossary.md \
	  -o $@

adrs-book.pdf: $(BOOK_SRCS) _build/strip-meta-sections.lua $(DENSITY_CFG)
	$(NPMPATH) $(PANDOC) $(MERMAID_FLAG) \
	  --lua-filter=_build/strip-meta-sections.lua $(DENSITY) $(TABLEFIT) \
	  -V title="deCDN — Architecture Decision Records (Reading Order)" \
	  -V date="$(DATE)" \
	  $(BOOK_SRCS) \
	  -o $@

clean:
	rm -f adrs.pdf adrs-book.pdf

# Drift guard (bidirectional): every numbered ADR and appendix on disk must
# appear in the hand-maintained BOOK_SRCS (else the book silently omits it),
# AND every BOOK_SRCS entry must still exist on disk (else `make book` dies
# with "No rule to make target" when an ADR is retired/renamed).
check-book-list:
	@miss=""; for f in $(NUMERIC_SRCS) $(APPENDIX_SRCS); do \
	  case " $(BOOK_SRCS) " in *" $$f "*) ;; *) miss="$$miss $$f";; esac; \
	done; \
	if [ -n "$$miss" ]; then \
	  echo "ERROR: missing from BOOK_SRCS / architecture.md reading order:$$miss"; \
	  exit 1; \
	fi; \
	gone=""; for f in $(BOOK_SRCS); do \
	  [ -f "$$f" ] || gone="$$gone $$f"; \
	done; \
	if [ -n "$$gone" ]; then \
	  echo "ERROR: BOOK_SRCS names file(s) not on disk (retired/renamed?):$$gone"; \
	  exit 1; \
	fi; \
	echo "book list in sync ($(words $(NUMERIC_SRCS)) ADRs + $(words $(APPENDIX_SRCS)) appendices)"

# Reference-hygiene guards. Basic tools only (awk/sed/grep), no perl/python.
# Fenced ```code```, the H1 title line, inline `code`, and existing markdown
# links are excluded — they can't be links or already are one. H2-H6 headings
# ARE checked: an ADR named in a section title must be linked too.
#   check-bare-refs      : every `ADR NNN` mention (with or without § Section)
#                          must be a markdown link, not bare prose/table text.
#   check-md-anchors     : every cross-ADR *.md link must carry a #heading
#                          anchor, so a retitled/renamed ADR breaks the link
#                          (drift caught) vs silently pointing at the wrong file.
#   check-anchor-targets : every #anchor carried above must name a heading that
#                          exists in the target file. Carrying an anchor only
#                          makes drift *catchable*; GitHub renders the top of
#                          the page for a fragment it cannot resolve, so
#                          without this the retitle the rule guards against
#                          still reads as a working link (#1455).
#   check-no-section-numbers: no numeric §N refs and no numbered headings
#                          (### N. Title) anywhere — sections are referenced
#                          by NAME only. Scans code too; RFC/EIP/BIP § exempt.
SRCS_REF := architecture.md $(NUMERIC_SRCS) $(APPENDIX_SRCS) glossary.md

# check-anchor-targets also reads the docs/ and root markdown that link into the
# ADRs with anchors that rot the same way: adr/README.md, docs/*.md (runbook),
# and the repo-root CONTRIBUTING.md / CLAUDE.md / CHANGELOG.md / README.md. These
# are deliberately NOT in SRCS_REF — the other guards police ADR *prose*
# conventions, which build/reading documentation does not follow. The awk
# resolves each link relative to its own file's directory, so the `../adr/…`
# paths these carry resolve correctly.
ANCHOR_SRCS := $(SRCS_REF) README.md $(wildcard ../docs/*.md) ../CONTRIBUTING.md ../CLAUDE.md ../CHANGELOG.md ../README.md

check-bare-refs:
	@bad=""; for f in $(SRCS_REF); do \
	  hits=$$(awk -v F="$$f" '/^```/{c=!c;next} c{next} /^# /{next} {print F":"FNR": "$$0}' "$$f" \
	    | sed -e 's/`[^`]*`//g' -e 's/\[[^]]*\]([^)]*)//g' \
	    | grep -E '(^|[^[*])ADR ?[0-9]{1,3}' || true); \
	  if [ -n "$$hits" ]; then printf '%s\n' "$$hits"; bad=1; fi; \
	done; \
	if [ -n "$$bad" ]; then \
	  echo "ERROR: bare ADR reference(s) above — every ADR mention must be a markdown link, e.g. [ADR 016 § Deployment](016-contract-interactions.md#smart-contract-interaction-model)"; \
	  exit 1; \
	else echo "no bare ADR references ($(words $(SRCS_REF)) files; code + H1 titles excluded, H2-H6 checked)"; fi

check-md-anchors:
	@bad=""; for f in $(SRCS_REF); do \
	  hits=$$(awk -v F="$$f" '/^```/{c=!c;next} !c{print F":"FNR": "$$0}' "$$f" \
	    | grep -E '\]\((\./)?([0-9]{3}-[a-z0-9-]+|appendix-[a-z0-9-]+|architecture|glossary)\.md\)' || true); \
	  if [ -n "$$hits" ]; then printf '%s\n' "$$hits"; bad=1; fi; \
	done; \
	if [ -n "$$bad" ]; then \
	  echo "ERROR: ADR cross-link(s) above point to a bare *.md with no #anchor — add a heading anchor, e.g. 016-contract-interactions.md#smart-contract-interaction-model"; \
	  exit 1; \
	else echo "all ADR cross-links carry a #anchor"; fi

# The resolver needs a heading index per target file, so it is one awk program
# over the whole set rather than a per-file grep like its siblings; it lives in
# _build/ for the same reason the lua filters do. LC_ALL=C keeps the slug
# character class byte-oriented across awk implementations.
check-anchor-targets: _build/check-anchor-targets.awk
	@LC_ALL=C awk -f _build/check-anchor-targets.awk $(ANCHOR_SRCS) || { \
	  echo "ERROR: ADR #anchor(s) above name no heading in the file they point at — GitHub silently renders the top of the page instead. Fix the anchor to the target's current heading slug (lowercase, punctuation dropped, spaces to hyphens)"; \
	  exit 1; \
	}

check-no-section-numbers:
	@bad=""; for f in $(SRCS_REF); do \
	  hits=$$( { grep -nE '§ ?[0-9]' "$$f" \
	      | grep -vE '(RFC|EIP|BIP)[ -]?[0-9]+,? *§' | sed "s|^|$$f:|"; \
	    awk -v F="$$f" '/^```/{c=!c;next} c{next} /^#+ [0-9]+\. |^#+ [0-9]+\.[0-9]/{print F":"FNR": "$$0}' "$$f"; \
	  } || true); \
	  if [ -n "$$hits" ]; then printf '%s\n' "$$hits"; bad=1; fi; \
	done; \
	if [ -n "$$bad" ]; then \
	  echo "ERROR: numeric §N reference(s) or numbered heading(s) above — reference sections by NAME ([... § Section Name](file.md#anchor)) and drop heading numbers (RFC/EIP/BIP § exempt)"; \
	  exit 1; \
	else echo "no numeric §N refs or numbered headings (RFC/EIP/BIP § exempt)"; fi

check-no-history-links:
	@bad=""; for f in $(SRCS_REF); do \
	  hits=$$(awk -v F="$$f" '/^```/{c=!c;next} !c{print F":"FNR": "$$0}' "$$f" \
	    | grep -E '\]\([^)]*_history' || true); \
	  if [ -n "$$hits" ]; then printf '%s\n' "$$hits"; bad=1; fi; \
	done; \
	if [ -n "$$bad" ]; then \
	  echo "ERROR: canonical ADR link(s) above point into adr/_history/ — retired ADRs are archived for audit but MUST NOT be linked from canonical ADRs; reference them as inline-code names, e.g. \`ADR 033\`"; \
	  exit 1; \
	else echo "no canonical ADR links into _history"; fi

check-refs: check-bare-refs check-md-anchors check-anchor-targets check-no-section-numbers check-no-history-links

help:
	@echo "make            build both PDFs (mermaid-rendered)"
	@echo "make book       reading-order adrs-book.pdf"
	@echo "make numeric    numeric adrs.pdf"
	@echo "make MERMAID=0  build without mermaid-filter (no mmdc needed)"
	@echo "make clean      remove generated PDFs"
	@echo "make check-book-list   verify the reading-order list is complete"
	@echo "make check-bare-refs        fail on any bare (un-linked) ADR reference"
	@echo "make check-md-anchors       fail on ADR links missing a #anchor"
	@echo "make check-anchor-targets   fail on #anchors that resolve to no heading"
	@echo "make check-no-section-numbers  fail on numeric §N refs / numbered headings"
	@echo "make check-refs             run all reference-hygiene guards"
