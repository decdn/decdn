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
#                          # the hand-maintained reading-order list below
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
  _build/part-2-discovery.md \
  022-content-discovery.md 015-zero-rtt.md \
  _build/part-3-payments.md \
  003-payments.md 012-client.md 024-account-abstraction.md \
  _build/part-4-tokenomics.md \
  026-gauge-boost-tokenomics.md 033-safety-insurance-reserve.md 018-liquidity-strategy.md \
  _build/part-5-verification.md \
  014-on-chain-verification.md 008-reputation.md 011-content-takedown.md \
  028-slashing-appeals.md 032-safety-reserve-appeals-contract.md \
  031-content-blacklist-appeals-contract.md 030-node-region-self-attestation.md \
  _build/part-6-governance.md \
  009-governance.md 016-contract-interactions.md \
  _build/part-7-operations.md \
  019-node-onboarding.md \
  _build/part-8-supporting.md \
  013-schema-evolution.md 017-privacy.md \
  _build/part-9-appendices.md \
  appendix-encrypted-content-publishing.md appendix-bundles.md \
  appendix-observability.md appendix-peer-table-eviction.md \
  appendix-blob-cache-eviction.md appendix-l2-deployment.md \
  appendix-poc-production-seams.md appendix-binaries.md \
  appendix-local-admin-http.md appendix-operator-key-rotation.md \
  appendix-operator-upgrade-path.md appendix-fraud-detection.md \
  glossary.md

DENSITY_CFG := _build/book-density.typ _build/book-margins.yaml _build/table-autofit.lua

.DEFAULT_GOAL := all
.PHONY: all numeric book clean help check-book-list

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

# Drift guard: every numbered ADR and appendix on disk must appear in the
# hand-maintained BOOK_SRCS, or the reading-order book silently omits it.
check-book-list:
	@miss=""; for f in $(NUMERIC_SRCS) $(APPENDIX_SRCS); do \
	  case " $(BOOK_SRCS) " in *" $$f "*) ;; *) miss="$$miss $$f";; esac; \
	done; \
	if [ -n "$$miss" ]; then \
	  echo "ERROR: missing from BOOK_SRCS / architecture.md reading order:$$miss"; \
	  exit 1; \
	else echo "book list in sync ($(words $(NUMERIC_SRCS)) ADRs + $(words $(APPENDIX_SRCS)) appendices)"; fi

help:
	@echo "make            build both PDFs (mermaid-rendered)"
	@echo "make book       reading-order adrs-book.pdf"
	@echo "make numeric    numeric adrs.pdf"
	@echo "make MERMAID=0  build without mermaid-filter (no mmdc needed)"
	@echo "make clean      remove generated PDFs"
	@echo "make check-book-list   verify the reading-order list is complete"
