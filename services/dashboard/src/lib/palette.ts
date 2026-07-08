// Chart color tokens for Recharts (which needs concrete color strings, not CSS
// utility classes). These mirror the CSS custom properties in globals.css and are
// the validated dark-mode categorical steps from the dataviz palette, checked
// against the #131519 card surface (contrast >= 3:1).

export const CHART = {
  // Primary data hue (blue) — the default single-series color.
  primary: "#3987e5",
  // Recessive chrome.
  grid: "#23262d",
  axis: "#33373f",
  ink: "#e6e8ec",
  inkDim: "#9aa0aa",
  inkFaint: "#6b7280",
  surface: "#131519",
  // Status semantics (reserved).
  good: "#2ee6a6",
  amber: "#fab219",
  danger: "#e5484d",
} as const;

// Fixed-order categorical series — assign by slot, never cycle past 8.
export const SERIES = [
  "#3987e5", // 1 blue
  "#199e70", // 2 aqua
  "#c98500", // 3 yellow
  "#9085e9", // 4 violet
  "#e66767", // 5 red
  "#d55181", // 6 magenta
  "#d95926", // 7 orange
  "#4f9d3a", // 8 green
] as const;

// Ordinal blue ramp for the funnel stages (dark, step 250 -> 550, kept >= 2:1 on
// the dark surface per the dataviz ordinal rule). Four ordered, progressively
// deeper steps read as "descending through the funnel".
export const FUNNEL_RAMP = ["#5598e7", "#3987e5", "#256abf", "#1c5cab"] as const;
