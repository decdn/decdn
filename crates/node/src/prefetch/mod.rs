//! Speculative-prefetch operator policy (ADR 022 §Popularity Signals and
//! Market Dynamics). Off-by-default; the `FIND_VALUE` handler feeds the
//! [`popularity::PopularityTracker`] and, on a threshold-cross, consults the
//! decision engine. This crate slice decides and meters but does not yet fire
//! the real acquisition (see #650 follow-up).

pub mod popularity;
