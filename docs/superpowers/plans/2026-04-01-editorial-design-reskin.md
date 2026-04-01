# Editorial Design Reskin Plan

**Goal:** Restyle the recommender app frontend to match "The Culinary Editorial" design system from the provided mockup.

**Design reference:** `/Users/jonathan.whiteley/Downloads/stitch_checkout_design_noimgs/`

## Summary of Changes

### 1. Global Styles & Fonts
- Add Epilogue + Plus Jakarta Sans via Google Fonts to `index.html`
- Add Material Symbols Outlined for icons
- Update `globals.css` with the design system colors as CSS variables
- Update `tailwind.config` (in `package.json` or separate config) with custom colors, fonts, border-radius scale

### 2. Layout (index.tsx)
- Switch from max-width centered grid to full-width flex layout: catalog left, sidebar right (sticky)
- Sidebar: white background, border-left, fixed 460px width on desktop
- Catalog area: generous padding (p-12), editorial header with "Editor's Pick" label + large display heading + category pill filters
- Move Known/Guest toggle and user selector into the top nav bar

### 3. Navbar (new component or update __root.tsx)
- Sticky top bar with `bg-[#f6f3f2]`
- Left: brand name "The Culinary Editorial" in Epilogue, flame red
- Left nav: Menu, Discover, Checkout links
- Right: Known/Guest pill toggle, user selector with person icon, settings icon

### 4. Product Cards (product-grid.tsx)
- Rounded cards (`rounded-3xl`) with white background, no borders (hover: subtle primary border)
- Icon placeholder area: aspect-square, light grey bg, Material Symbol icon + category label
- Product name in Epilogue bold, price in primary color
- Description text in stone-500
- "Add to Cart" button: full-width, rounded-full, uppercase Epilogue text, hover transitions to primary bg

### 5. Category Filters (product-grid.tsx)
- Replace Badge-based filter with pill chips (`rounded-full`)
- Active: primary bg + white text
- Inactive: secondary-fixed-dim bg, hover effect

### 6. Cart Panel (cart-panel.tsx)
- White bg, generous padding (p-10)
- "Your Order" heading in 32px Epilogue black
- Cart items: circular icon thumbnails, name + price, quantity stepper (rounded pill with +/- buttons)
- Subtotal row with large price
- "Complete Order" button: full-width, primary bg, rounded-full, uppercase, shadow

### 7. Recommendations (recommendations.tsx)
- Background: `bg-[#f5f2f0]` section below cart
- Header: "Curated for [User]" with sparkle icon, "Personalized" badge
- Rec cards: white, `rounded-[2rem]`, circular icon thumbnail, product name, match % badge (green pill), price, "+ Add" button in soft primary tint
- For anonymous mode: "Based on your cart" instead of "Curated for [User]"

### 8. Mode Toggle (mode-toggle.tsx)
- Replace shadcn Tabs with custom pill toggle: rounded-full container, two buttons
- Active: primary bg + white text
- Inactive: transparent + muted text

### 9. User Search (user-search.tsx)
- Replace Select dropdown with a custom pill-style selector showing person icon + user name + chevron
- Dropdown content stays similar but styled to match

### 10. User Profile
- Integrate store info into the sidebar header area or the user selector display rather than a separate card

## Files to Modify
- `src/recommender_app/ui/index.html` (add fonts)
- `src/recommender_app/ui/styles/globals.css` (design system colors + custom styles)
- `src/recommender_app/ui/routes/index.tsx` (layout restructure)
- `src/recommender_app/ui/routes/__root.tsx` (add navbar)
- `src/recommender_app/ui/components/product-grid.tsx` (editorial card design)
- `src/recommender_app/ui/components/cart-panel.tsx` (editorial cart)
- `src/recommender_app/ui/components/recommendations.tsx` (curated section)
- `src/recommender_app/ui/components/mode-toggle.tsx` (pill toggle)
- `src/recommender_app/ui/components/user-search.tsx` (pill selector)
- `package.json` or tailwind config (custom theme)
