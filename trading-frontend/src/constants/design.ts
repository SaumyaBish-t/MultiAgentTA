/** FORGE Trading System — Design Token System */

export const colors = {
  page: '#F0F4FF',
  surface: '#FFFFFF',
  sidebar: '#1A1F3A',
  indigo: {
    50: '#EEF1FF',
    200: '#B8C3F8',
    400: '#6B7EE8',
    500: '#4F63D2',
    600: '#3B4DB0',
    700: '#2D3A8C',
    900: '#1A1F3A'
  },
  amber: {
    100: '#FEF3D7',
    300: '#F9C05A',
    400: '#F5A623',
    500: '#F08C00',
    700: '#B85C00'
  },
  cyan: {
    100: '#CFFAFE',
    400: '#22D3EE',
    500: '#06B6D4',
    600: '#0891B2'
  },
  positive: '#059669',
  'positive-bg': '#ECFDF5',
  negative: '#DC2626',
  'negative-bg': '#FEF2F2',
  warning: '#D97706',
  'warning-bg': '#FFFBEB',
  text: {
    primary: '#0F172A',
    secondary: '#475569',
    muted: '#94A3B8'
  },
  border: {
    DEFAULT: '#E2E8F0',
    strong: '#CBD5E1'
  }
} as const

export const fonts = {
  sans: "'Inter', sans-serif",
  mono: "'JetBrains Mono', 'IBM Plex Mono', monospace",
} as const

export const spacing = {
  unit: 4,
  gutter: 16,
  margin: 24,
  panelPadding: 20,
} as const
