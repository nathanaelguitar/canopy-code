# Web Shell Oak Theme

## Motivation

The daemon web shell ships the Canopy name and forest/oak accent tokens, but its
surfaces are generic near-black (`#0d0d0d`). The CanopyChat iOS app has an
established visual identity — a warm oak-plank interior background, oak-brown
user bubbles, a 26px pill composer with a gradient send button. This change
ports that identity to the web shell so the daemon UI reads as the same
product.

## Approach

Token and focused visual reskin only. No JSX or layout structure changes; portal
and shadow-DOM plumbing (`data-web-shell-*`, `--web-shell-*`,
`useWebShellPortalRoot`) is untouched. All runtime theming flows through
`.themeDark` / `.themeLight` in `client/App.module.css`, and those variables
are copied onto the portal root at runtime (App.tsx `syncVariables`), so
popovers and pickers inherit the retune automatically.

Palette and treatments are taken from the iOS app (`iphone/AetherChat/Theme.swift`,
`OakBackground.swift`, `ChatView.swift`, `ConversationListView.swift`):

- Oak ramp: `#3D2914 / #6B4423 / #A0784A / #D4B896 / #F5EDE0`; forest
  `#4A7C4A`; copper `#B87333`; amber `#D4A017`; warm-gray ramp
  `#F0EBE3 … #1A1612`.
- Dark background: layered gradient `#201A14 → #2C2318 → #3A2E1F` with subtle
  warm dot texture and amber/forest atmospheric glows. Light uses
  `#F5EDE0 → #EADDC6 → #D8C5A4` with the same restrained treatment.
- User bubble: `#6B4423` (light: 92% opacity), white text, 20px radius with a
  4px tail corner. Assistant messages stay transparent over the grain.
- Composer: 26px radius, fill `#302820` (dark) / white (light), 1px gradient
  border stroke, oak-tinted shadows; send button gradient `#A0784A → #6B4423`.
- The welcome-screen dot field stays interactive; its palette moves to warm
  grain tones so it reads as oak texture.

The background treatment rides on a dedicated `--app-bg-layers` token so
`--background` keeps a sensible flat value for components that consume it as a
fill. The existing interactive dot canvas supplies the texture without adding
an image payload or changing the shell CSP. The composer’s existing WebGL
specular edge is retinted to oak as part of the visual pass.

## Scope

- `client/App.module.css` — theme token retune + background layers
- `client/components/messages/UserMessage.module.css` — bubble geometry/fill
- `client/components/ChatEditor.module.css` — composer radius/border/shadow/send
- `client/components/WelcomeHeader.module.css` — serif title treatment
- `client/components/sidebar/WebShellSidebar.module.css` — warm tints
- `client/components/NewSessionDotField.tsx` — dot palette
- `client/components/SpecularComposerEffect.tsx` — focused-edge color
- Stragglers: `client/styles/standalone.css`,
  `client/components/dialogs/DialogShell.module.css`, `client/index.html`
  theme-color meta
