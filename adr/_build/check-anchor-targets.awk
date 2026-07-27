# check-anchor-targets — resolve every `file.md#anchor` / `#anchor` link in the
# ADR set against the headings of the file it names. Driven by `make
# check-anchor-targets` (see ../Makefile); basic tools only, no perl/python.
#
# The sibling `check-md-anchors` guard asserts a cross-ADR link *carries* an
# anchor. This one asserts the anchor *resolves* — the drift the anchor rule
# exists to catch (a retitled heading) is otherwise silent, because GitHub
# renders the top of the page for a fragment it cannot find.
#
# Slugs follow GitHub's algorithm (github-slugger), which is also what
# `pandoc --from=markdown+gfm_auto_identifiers` reproduces for the PDF builds:
# take the heading's *rendered* text, lowercase it, drop every character that
# is not a letter, digit, underscore, space or hyphen, then turn spaces into
# hyphens. Two consequences the ADR tree already depends on:
#
#   * A stripped character leaves its surrounding spaces behind, so `## Tier 1
#     — Minor` slugs to `tier-1--minor` (double hyphen). Approximating the
#     algorithm would flag correct links.
#   * Punctuation vanishes rather than splitting a word: `` ### `cdn/probe/v1`
#     — latency probe `` slugs to `cdnprobev1--latency-probe`.
#
# Run under LC_ALL=C so the character class is byte-oriented and every awk
# (mawk in CI, gawk/BSD awk locally) strips multibyte punctuation identically.
#
# Skipped, matching the other guards: fenced ```code``` blocks, and link targets
# that are not local `.md` files (URLs, assets). Headings inside fenced blocks
# are not headings, so they are skipped on the load side too.

# Rendered heading text -> GitHub anchor slug.
function slugify(s,   r) {
  r = tolower(s);
  gsub(/[^a-z0-9 _-]/, "", r);
  gsub(/ /, "-", r);
  return r;
}

# Read FILE once and memoize its heading slugs in slugs[FILE "#" SLUG].
# Returns 0 if the file cannot be opened (dangling link target), else 1.
function load(f,   line, incode, ret, t, seg, key, base) {
  if (f in loaded) return loaded[f];
  ret = (getline line < f);
  if (ret < 0) { close(f); loaded[f] = 0; return 0; }
  loaded[f] = 1;
  incode = 0;
  while (ret > 0) {
    if (line ~ /^```/) incode = !incode;
    else if (!incode && line ~ /^#+[ \t]/) {
      t = line;
      sub(/^#+[ \t]+/, "", t);
      sub(/[ \t]+$/, "", t);
      # A heading may contain inline links; GitHub slugs the rendered text, so
      # [text](target) contributes `text` only.
      while (match(t, /\[[^]]*\]\([^)]*\)/)) {
        seg = substr(t, RSTART, RLENGTH);
        sub(/\]\([^)]*\)$/, "", seg);
        sub(/^\[/, "", seg);
        t = substr(t, 1, RSTART - 1) seg substr(t, RSTART + RLENGTH);
      }
      key = f "#" slugify(t);
      # Repeated heading text gets `-1`, `-2`, … appended, as GitHub does.
      if (key in slugs) { base = key; do { key = base "-" ++dup[base]; } while (key in slugs); }
      slugs[key] = 1;
      headings++;
    }
    ret = (getline line < f);
  }
  close(f);
  return 1;
}

FNR == 1 { incode = 0; files++ }
/^```/   { incode = !incode; next }
incode   { next }

{
  rest = $0;
  while (match(rest, /\]\([^)#]*#[^)]*\)/)) {
    # Strip the leading `](` and the trailing `)` from the matched link target.
    target = substr(rest, RSTART + 2, RLENGTH - 3);
    rest = substr(rest, RSTART + RLENGTH);
    hash = index(target, "#");
    file = substr(target, 1, hash - 1);
    frag = substr(target, hash + 1);
    sub(/^\.\//, "", file);
    if (file == "") file = FILENAME;            # same-file `#anchor` link
    else if (file ~ /:\/\// || file !~ /\.md$/) continue;
    links++;
    if (!load(file)) {
      print FILENAME ":" FNR ": " target " -> no such file";
      bad = 1;
    } else if (!((file "#" frag) in slugs)) {
      print FILENAME ":" FNR ": " target " -> no heading in " file " slugs to #" frag;
      bad = 1;
    }
  }
}

END {
  if (bad) exit 1;
  print links " ADR #anchor link(s) resolve (" files " files scanned, " headings " headings indexed)";
}
