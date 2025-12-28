/**
 * Long Island Grit - Glassjaw-inspired color palette
 *
 * Think: basement show flyer, xeroxed zine, VFW hall at 11pm.
 * Inspired by Glassjaw's album artwork - Worship and Tribute,
 * Everything You Ever Wanted to Know About Silence.
 * Raw, distorted, emotionally intense.
 */

export const colors = {
  // Primary - Dried blood, violence, intensity
  blood: '#8B0000', // deep arterial red
  maroon: '#4A0E0E', // dried, oxidized blood

  // Earth - Worn, weathered, gritty
  rust: '#8B4513', // burnt sienna, oxidized metal
  bone: '#D4C4A8', // aged paper, dried bone
  paper: '#E3D7C3', // flyer paper, warm neutral
  ash: '#696969', // cigarette ash, smoke

  // Background - Basement shows, dark rooms
  void: '#0A0A0A', // near-black, the void
  charcoal: '#1C1C1C', // worn black t-shirt

  // Accent - Moments of clarity, burning urgency
  ember: '#CC5500', // burning, urgent
  neonBlue: '#4CC9F0', // stage gel blue
  neonPink: '#FF4D9D', // stage gel magenta

  // Studio - Recording studio equipment aesthetic
  tungsten: '#FFD4A3', // warm incandescent light
  varnish: '#8B4513', // wood console finish
  brushedMetal: '#6E6E6E', // equipment casings
  chrome: '#C0C0C0', // knobs, connectors
  vinylBlack: '#0D0D0D', // speaker cones, panels

  // LEDs - Equipment status indicators
  ledGreen: '#00FF00', // healthy
  ledYellow: '#FFD700', // warning
  ledRed: '#FF0000', // error
  ledAmber: '#FF8C00', // VU meter peak

  // VU Meter - Analog meter colors
  vuNeedle: '#FF3333', // needle color
  vuBackground: '#1A1A0A', // meter face background
  vuScale: '#CCCCAA', // scale markings
} as const;

export type ColorName = keyof typeof colors;
