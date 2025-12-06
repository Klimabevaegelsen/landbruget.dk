# H3 PFAS Visualization Frontend

A modern, interactive web application for visualizing PFAS exposure data across Denmark using H3 hexagonal grids, built with Next.js 15, React 19, and cutting-edge web technologies.

## 🚀 Features

- **Interactive H3 Heatmaps**: High-resolution hexagonal grid visualization of PFAS exposure and pesticide load data
- **Multi-Layer Visualization**: BNBO protected areas and BBR building registry data overlays
- **Temporal Controls**: Year-by-year analysis with cumulative view options and animated playback
- **Advanced Interactions**: Detailed hover tooltips, layer controls, and data export functionality
- **Responsive Design**: Mobile-first design that works seamlessly across all devices
- **Performance Optimized**: Built with React 19 features, Turbopack, and modern optimization techniques

## 🛠 Technology Stack

### Core Framework
- **Next.js 15** with Turbopack for ultra-fast development
- **React 19** with new features like React Compiler and Partial Prerendering
- **TypeScript 5.6** for enhanced type safety and performance
- **Tailwind CSS v4** with CSS-in-JS support and container queries

### Visualization & Mapping
- **Kepler.gl v3.2** for advanced geospatial visualization
- **Deck.gl v9.1** for high-performance WebGL rendering
- **Protomaps v2.1** with PMTiles v3 for custom base map tiles

### State Management & Data
- **Zustand v5** for lightweight, performant state management
- **Supabase v2.45** for real-time database and API
- **React 19 Cache** for automatic request deduplication

### UI Components & Animation
- **Radix UI** primitives for accessible, unstyled components
- **Framer Motion v11** for fluid animations and micro-interactions
- **Lucide React** for consistent, beautiful icons

### Development & Quality
- **ESLint 9** with modern configuration
- **Prettier** with Tailwind plugin for consistent formatting
- **Performance monitoring** with Core Web Vitals tracking

## 📦 Installation

### Prerequisites
- Node.js 18+ and npm 8+
- Modern browser with WebGL support

### Setup

1. **Clone the repository**
   ```bash
   git clone https://github.com/your-org/landbruget.dk
   cd landbruget.dk/frontend-pesticide
   ```

2. **Install dependencies**
   ```bash
   npm install
   ```

3. **Environment Configuration**
   ```bash
   cp .env.example .env.local
   # Edit .env.local with your configuration
   ```

4. **Start development server**
   ```bash
   npm run dev
   ```

5. **Open in browser**
   ```
   http://localhost:3000
   ```

## 🏗 Project Structure

```
frontend-pesticide/
├── src/
│   ├── app/                     # Next.js 15 app router
│   │   ├── layout.tsx          # Root layout with React 19 features
│   │   ├── page.tsx            # Main application page
│   │   ├── globals.css         # Global styles with Tailwind v4
│   │   └── api/                # API routes (Developer 1)
│   ├── components/
│   │   ├── map/                # Map-related components
│   │   │   ├── TimeControls.tsx    # Year slider and playback controls
│   │   │   ├── HeatmapToggle.tsx   # Pesticide/PFAS data toggle
│   │   │   ├── LayerControls.tsx   # Layer visibility controls (Developer 2)
│   │   │   └── MapPlaceholder.tsx  # Development placeholder
│   │   ├── ui/                 # Reusable UI components
│   │   │   ├── button.tsx          # Button with variants
│   │   │   ├── card.tsx            # Card components
│   │   │   ├── badge.tsx           # Status badges
│   │   │   ├── tooltip.tsx         # Tooltip component
│   │   │   ├── loading-spinner.tsx # Loading states
│   │   │   └── responsive-layout.tsx # Responsive container
│   │   └── overlays/           # Overlay components
│   │       └── DataPanel.tsx       # Statistics and data export
│   ├── stores/                 # Zustand v5 stores
│   │   ├── map-store.ts            # Map state management
│   │   ├── ui-store.ts             # UI preferences
│   │   └── data-store.ts           # Data caching (interfaces with Developer 1)
│   ├── hooks/                  # Custom React hooks
│   │   ├── use-data-fetching.ts    # Data loading orchestration
│   │   ├── use-performance.ts      # Performance monitoring
│   │   └── use-simple-viewport.ts  # Viewport management
│   ├── lib/                    # Utility functions
│   │   └── utils.ts                # Common utilities and constants
│   └── types/                  # TypeScript type definitions
├── public/                     # Static assets
├── docs/                       # Documentation
├── tailwind.config.ts          # Tailwind v4 configuration
├── next.config.ts              # Next.js 15 configuration
├── tsconfig.json               # TypeScript configuration
└── package.json                # Dependencies and scripts
```

## 🎯 Developer Responsibilities

This project follows a **3-developer architecture** with clear separation of concerns:

### Developer 1: Backend Infrastructure & Data Management
- Database schema and API routes
- Data synchronization from GCS
- Performance optimization and caching
- Type definitions for data structures

### Developer 2: Map Visualization & Kepler.gl Integration  
- Kepler.gl map component implementation
- Protomaps base layer integration
- Advanced layer controls and settings
- Color schemes and visual design

### Developer 3: UI Components, Controls & Application Shell (This Implementation)
- Next.js application foundation
- Time controls with animations
- Heatmap toggle system
- State management with Zustand
- UI component library
- Responsive design system

## 🎮 Usage

### Basic Navigation
- **Pan**: Click and drag to move around the map
- **Zoom**: Use mouse wheel or zoom controls
- **Layers**: Toggle BNBO and BBR layers using the layer controls
- **Data**: Switch between Pesticide Load and PFAS Mass using the heatmap toggle

### Time Controls
- **Year Selection**: Use the slider to select specific years (2020-2025)
- **Cumulative Mode**: Toggle to view cumulative data from 2020 to selected year
- **Playback**: Use play/pause controls for animated year progression
- **Speed Control**: Adjust playback speed (slow/normal/fast)

### Data Analysis
- **Statistics Panel**: View aggregated statistics for current selection
- **Export Data**: Download current view data as JSON
- **Hover Details**: Hover over hexagons for detailed information
- **Layer Legend**: Reference color coding and data ranges

## 🔧 Configuration

### Environment Variables

```bash
# Required
NEXT_PUBLIC_SUPABASE_URL=your-supabase-url
NEXT_PUBLIC_SUPABASE_ANON_KEY=your-supabase-key
NEXT_PUBLIC_MAPBOX_TOKEN=your-mapbox-token

# Optional
NEXT_PUBLIC_ENABLE_PWA=true
NEXT_PUBLIC_ENABLE_PERFORMANCE_MONITORING=true
NEXT_PUBLIC_DEBUG_MODE=false
```

### Feature Flags
- `ENABLE_PWA`: Progressive Web App functionality
- `ENABLE_OFFLINE_MODE`: Offline data caching
- `ENABLE_STREAMING`: Server-side streaming responses
- `ENABLE_PERFORMANCE_MONITORING`: Analytics and performance tracking

## 🚀 Deployment

### Vercel (Recommended)
```bash
npm run build
vercel deploy
```

### Docker
```bash
docker build -t pfas-frontend .
docker run -p 3000:3000 pfas-frontend
```

### Static Export
```bash
npm run build
npm run export
```

## 📊 Performance

### Optimization Features
- **React 19 Compiler**: Automatic component optimization
- **Partial Prerendering**: Static + dynamic content combination
- **Dynamic IO**: Streaming for faster page loads
- **Code Splitting**: Automatic bundle optimization
- **Image Optimization**: Next.js 15 enhanced image handling

### Performance Targets
- **Initial Load**: <3 seconds
- **Layer Toggle**: <500ms
- **Year Transition**: <1 second
- **Hover Interaction**: <100ms

## 🧪 Testing

```bash
# Type checking
npm run type-check

# Linting
npm run lint

# Format code
npm run format

# Build verification
npm run build
```

## 🤝 Contributing

1. Follow the 3-developer architecture pattern
2. Respect component boundaries between developers
3. Use TypeScript for all new code
4. Follow the established naming conventions
5. Add performance monitoring for new features
6. Ensure mobile responsiveness
7. Test accessibility compliance

## 📄 License

MIT License - see [LICENSE](LICENSE) file for details.

## 🙋‍♂️ Support

For questions about this frontend implementation:
- Create an issue in the repository
- Check the [documentation](docs/)
- Review the component interfaces for integration

---

**Built with ❤️ for environmental data visualization in Denmark** 