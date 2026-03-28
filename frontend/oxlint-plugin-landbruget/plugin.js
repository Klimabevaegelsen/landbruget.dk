/**
 * oxlint custom plugin for landbruget.dk
 *
 * Architectural lint rules that drive the codebase toward production quality.
 * These rules encode "where we want to be" — existing violations are allowlisted
 * and remediated over time.
 *
 * API: https://oxc.rs/docs/guide/usage/linter/writing-js-plugins
 */

// ---------------------------------------------------------------------------
// Rule 1: max-component-size
// Vision: No component file over 150 lines. Forces decomposition.
// Migration allowlist: existing files grandfathered in (2026-03-21).
// This list should SHRINK over time — do NOT add new files.
// ---------------------------------------------------------------------------
const MAX_COMPONENT_SIZE_ALLOWLIST = new Set([
  'src/app/(main)/kilder/page.tsx',
  'src/app/(main)/kommuner/page.tsx',
  'src/app/(main)/om-os/page.tsx',
  'src/app/api/pmtiles-warmup/route.ts',
  'src/app/markanalyse/components/field-analysis-main.tsx',
  'src/app/markanalyse/components/sidebar/field-sidebar.tsx',
  'src/app/markanalyse/components/sidebar/mobile-menu.tsx',
  'src/app/test-components/page.tsx',
  'src/components/chart/chart-container.tsx',
  'src/components/chart/enhanced-tooltip.tsx',
  'src/components/environmental/EnvironmentalStatusIndicator.tsx',
  'src/components/field-analysis/ColorLegend.tsx',
  'src/components/field-analysis/FieldAnalysisVisualization.tsx',
  'src/components/field-analysis/FieldDetailsPanel.tsx',
  'src/components/field-analysis/LayerControlPanel.tsx',
  'src/components/field-analysis/SearchBar.tsx',
  'src/components/field-analysis/colorUtils.ts',
  'src/components/global-search.tsx',
  'src/components/homepage/AllRankings.tsx',
  'src/components/homepage/HomepageRankings.tsx',
  'src/components/homepage/IndividualRankingTable.tsx',
  'src/components/homepage/RankingTable.tsx',
  'src/components/kommuner/MunicipalityDetailsModal.tsx',
  'src/components/layout/sidenav.tsx',
  'src/components/layout/templates/navbar.tsx',
  'src/components/page-sections/hero.tsx',
  'src/components/pagebuilder/pageBlocks/block-bar-chart.tsx',
  'src/components/pagebuilder/pageBlocks/block-combo-chart.tsx',
  'src/components/pagebuilder/pageBlocks/block-iterated-section.tsx',
  'src/components/pagebuilder/pageBlocks/block-map-chart.tsx',
  'src/components/pagebuilder/pageBlocks/block-timeline.tsx',
  'src/components/pagebuilder/pageBlocks/chart-utils.ts',
  'src/components/pagebuilder/pagebuilder-progressive.tsx',
  'src/components/pagebuilder/pagebuilder.tsx',
  'src/components/pesticide-analysis/CompanyDetailsPanel.tsx',
  'src/components/pesticide-analysis/CompanyFieldsMap.tsx',
  'src/components/pesticide-analysis/CompanyFilterPanel.tsx',
  'src/components/pesticide-analysis/CompanyListView.tsx',
  'src/components/pesticide-analysis/PesticideAnalysisVisualization.tsx',
  'src/components/pipeline/h3-pfas-trigger.tsx',
  'src/components/table/dynamic-table.tsx',
  'src/components/ui/agricultural-breadcrumb.tsx',
  'src/components/ui/agricultural-dashboard.tsx',
  'src/components/ui/agricultural-date-picker.tsx',
  'src/components/ui/agricultural-notifications.tsx',
  'src/components/ui/alert-dialog.tsx',
  'src/components/ui/calendar.tsx',
  'src/components/ui/command.tsx',
  'src/components/ui/company-hover-card.tsx',
  'src/components/ui/dropdown-menu.tsx',
  'src/components/ui/form.tsx',
  'src/components/ui/global-search.tsx',
  'src/components/ui/menubar.tsx',
  'src/components/ui/select.tsx',
  'src/components/ui/sheet.tsx',
  'src/components/ui/sidebar.tsx',
  'src/hooks/use-touch-gestures.ts',
  'src/hooks/useCompanyCache.ts',
  'src/hooks/useMapTheme.ts',
  'src/hooks/useRankingsCache.ts',
  'src/lib/csv-download.ts',
  'src/lib/server-cache.ts',
  'src/lib/pmtiles-cache-service.ts',
  'src/services/supabase/types.ts',
  'src/utils/csv-export.ts',
]);

const maxComponentSize = {
  create(context) {
    return {
      Program(node) {
        const filename = context.filename || '';
        if (!filename.includes('/src/')) return;

        // Skip test files
        if (filename.includes('.test.') || filename.includes('__tests__'))
          return;

        // Skip allowlisted files (migration: shrink this list over time)
        const srcIndex = filename.indexOf('/src/');
        if (srcIndex !== -1) {
          const relativePath = filename.slice(srcIndex + 1);
          if (MAX_COMPONENT_SIZE_ALLOWLIST.has(relativePath)) return;
        }

        const source = context.sourceCode.text;
        const lineCount = source.split('\n').length;
        const MAX_LINES = 150;

        if (lineCount > MAX_LINES) {
          context.report({
            node,
            message: `File has ${lineCount} lines (max ${MAX_LINES}). Split into smaller, focused modules. See oxlint-plugin-landbruget/README.md.`,
          });
        }
      },
    };
  },
};

// ---------------------------------------------------------------------------
// Rule 2: no-service-import-in-components
// Vision: Components get data via props/hooks, not direct service imports.
// ---------------------------------------------------------------------------
const noServiceImportInComponents = {
  create(context) {
    return {
      ImportDeclaration(node) {
        const filename = context.filename || '';
        if (!filename.includes('/src/components/')) return;

        const source = node.source.value;
        if (!source.startsWith('@/services/')) return;

        // Allow type-only imports (importing interfaces/types is fine)
        if (node.importKind === 'type') return;

        // Allow importing from types files specifically
        if (source.endsWith('/types')) return;

        context.report({
          node,
          message: `Components must not import from services. Data should flow through props or hooks. Move data fetching to a server component (page.tsx) or custom hook.`,
        });
      },
    };
  },
};

// ---------------------------------------------------------------------------
// Rule 3: no-raw-process-env
// Vision: Centralized env access via @/lib/env.ts only.
// ---------------------------------------------------------------------------
const noRawProcessEnv = {
  create(context) {
    return {
      MemberExpression(node) {
        const filename = context.filename || '';

        // Allow in env.ts itself and API routes
        if (filename.includes('/lib/env.ts')) return;
        if (filename.includes('/app/api/')) return;
        if (!filename.includes('/src/')) return;

        // Match process.env.ANYTHING
        if (
          node.object.type === 'MemberExpression' &&
          node.object.object.type === 'Identifier' &&
          node.object.object.name === 'process' &&
          node.object.property.type === 'Identifier' &&
          node.object.property.name === 'env'
        ) {
          context.report({
            node,
            message: `Use env helpers from @/lib/env instead of process.env directly. Centralized access makes variables easier to audit, type, and document.`,
          });
        }
      },
    };
  },
};

// ---------------------------------------------------------------------------
// Rule 4: require-data-testid
// Vision: Every interactive element must have a data-testid attribute.
// ---------------------------------------------------------------------------
const requireDataTestid = {
  create(context) {
    const INTERACTIVE_ELEMENTS = new Set([
      'button',
      'input',
      'select',
      'textarea',
    ]);

    return {
      JSXOpeningElement(node) {
        const filename = context.filename || '';
        if (!filename.includes('/src/')) return;
        // Skip test files
        if (filename.includes('.test.') || filename.includes('__tests__'))
          return;

        // Only check HTML elements (lowercase), not React components (PascalCase)
        const name = node.name.name;
        if (!name || !INTERACTIVE_ELEMENTS.has(name)) return;

        // Check if data-testid attribute exists
        const hasTestId = node.attributes.some(
          (attr) =>
            attr.type === 'JSXAttribute' && attr.name.name === 'data-testid'
        );

        if (!hasTestId) {
          context.report({
            node,
            message: `<${name}> elements must have a data-testid attribute for Playwright test selectors. Example: <${name} data-testid="descriptive-name">`,
          });
        }
      },
    };
  },
};

// ---------------------------------------------------------------------------
// Rule 5: no-default-export
// Vision: Named exports are grep-able. Agents find `export function FarmDetails`
// but struggle with anonymous default exports.
// Next.js App Router files (page/layout/loading/error/not-found) are exempt.
// ---------------------------------------------------------------------------
const noDefaultExport = {
  create(context) {
    const NEXTJS_EXEMPT =
      /\/(page|layout|loading|error|not-found|template|default)\.(tsx?|jsx?)$/;

    return {
      ExportDefaultDeclaration(node) {
        const filename = context.filename || '';
        if (!filename.includes('/src/')) return;
        if (NEXTJS_EXEMPT.test(filename)) return;

        context.report({
          node,
          message: `Use named exports instead of default exports. Named exports are grep-able and help agents find code: \`export function MyComponent\` instead of \`export default function\`.`,
        });
      },
    };
  },
};

// ---------------------------------------------------------------------------
// Rule 6: enforce-absolute-imports
// Vision: Relative imports like ../../components/Button are hard for agents
// to grep. Enforce @/ alias for cross-directory imports.
// ---------------------------------------------------------------------------
const enforceAbsoluteImports = {
  create(context) {
    return {
      ImportDeclaration(node) {
        const filename = context.filename || '';
        if (!filename.includes('/src/')) return;

        const source = node.source.value;
        // Only flag imports that go up directories (not ./local-file)
        if (!source.startsWith('../')) return;

        context.report({
          node,
          message: `Use absolute imports with @/ alias instead of relative paths. Example: \`import { X } from '@/components/X'\` instead of \`import { X } from '${source}'\`.`,
        });
      },
    };
  },
};

// ---------------------------------------------------------------------------
// Rule 7: no-inline-styles
// Vision: Enforce Tailwind classes over inline style objects.
// Allows style={variable} for truly dynamic/computed values.
// ---------------------------------------------------------------------------
const noInlineStyles = {
  create(context) {
    return {
      JSXAttribute(node) {
        const filename = context.filename || '';
        if (!filename.includes('/src/')) return;

        if (node.name.name !== 'style') return;

        // Allow style={variable} — only flag style={{ ... }} object literals
        if (
          node.value &&
          node.value.type === 'JSXExpressionContainer' &&
          node.value.expression.type === 'ObjectExpression'
        ) {
          context.report({
            node,
            message: `Use Tailwind CSS classes instead of inline styles. Inline style objects bypass the design system and are harder to maintain.`,
          });
        }
      },
    };
  },
};

// ---------------------------------------------------------------------------
// Rule 8: no-hardcoded-api-urls
// Vision: API URLs must come from env variables, not hardcoded strings.
// ---------------------------------------------------------------------------
const noHardcodedApiUrls = {
  create(context) {
    const URL_PATTERNS = [
      /supabase\.co/,
      /localhost:\d+/,
      /127\.0\.0\.1/,
      /0\.0\.0\.0/,
    ];

    return {
      Literal(node) {
        const filename = context.filename || '';
        if (!filename.includes('/src/')) return;
        // Allow in env.ts, config files, and test files
        if (filename.includes('/lib/env.ts')) return;
        if (filename.includes('.test.')) return;
        if (filename.includes('config.ts')) return;

        if (typeof node.value !== 'string') return;

        for (const pattern of URL_PATTERNS) {
          if (pattern.test(node.value)) {
            context.report({
              node,
              message: `Hardcoded API URL detected. Use environment variables via @/lib/env.ts instead: \`import { env } from '@/lib/env'\`.`,
            });
            break;
          }
        }
      },
    };
  },
};

// ---------------------------------------------------------------------------
// Rule 9: require-test-coverage
// Vision: Every page, hook, lib, and service file must have test coverage.
// Migration allowlist: existing files grandfathered in (2026-03-22).
// This list should SHRINK over time — do NOT add new files.
// ---------------------------------------------------------------------------
const TEST_COVERAGE_ALLOWLIST = new Set([
  // Pages without E2E tests
  'src/app/(main)/pesticidanalyse/page.tsx',
  'src/app/(main)/pesticidanalyse/metode/page.tsx',
  'src/app/(main)/kommuner/page.tsx',
  'src/app/(main)/kilder/page.tsx',
  'src/app/(main)/brugsvilkaar/page.tsx',
  'src/app/(main)/om-os/page.tsx',
  'src/app/(main)/privatlivspolitik/page.tsx',
  'src/app/test-components/page.tsx',
  'src/app/admin/pipeline/page.tsx',
  // Pages with E2E tests (remove once coverage is verified)
  'src/app/(main)/page.tsx',
  'src/app/(main)/virksomhed/[id]/page.tsx',
  'src/app/(main)/markanalyse/page.tsx',
  // Hooks without unit tests
  'src/hooks/use-mobile-detection.ts',
  'src/hooks/use-touch-gestures.ts',
  'src/hooks/useCompanyData.ts',
  'src/hooks/useCompanyNavigation.ts',
  'src/hooks/useHomepageStatsCache.ts',
  'src/hooks/useLoadingToast.ts',
  'src/hooks/useSearch.ts',
  // Hooks with unit tests (remove once coverage is verified)
  'src/hooks/useCompanyCache.ts',
  'src/hooks/useMapTheme.ts',
  'src/hooks/useRankingsCache.ts',
  // Lib without unit tests
  'src/lib/cache-utils.ts',
  'src/lib/category-utils.ts',
  'src/lib/chart-colors.ts',
  'src/lib/climate-data.ts',
  'src/lib/csv-download.ts',
  'src/lib/env.ts',
  'src/lib/formatting.ts',
  'src/lib/pmtiles-cache-service.ts',
  'src/lib/rankings.ts',
  'src/lib/server-cache.ts',
  'src/lib/utils.ts',
  // Services without unit tests
  'src/services/supabase/climate.ts',
  'src/services/supabase/company-basic.ts',
  'src/services/supabase/company.ts',
  'src/services/supabase/config.ts',
  // Services with unit tests (remove once coverage is verified)
  'src/services/pmtiles-cache-service.ts',
]);

const requireTestCoverage = {
  create(context) {
    return {
      Program(node) {
        const filename = context.filename || '';
        if (!filename.includes('/src/')) return;

        // Skip test files themselves
        if (filename.includes('.test.') || filename.includes('__tests__'))
          return;

        // Skip type-only files
        if (filename.endsWith('.d.ts') || filename.endsWith('/types.ts'))
          return;

        // Skip config files
        if (filename.endsWith('/config.ts')) return;

        // Skip UI primitives (Radix)
        if (filename.includes('/components/ui/')) return;

        const srcIndex = filename.indexOf('/src/');
        if (srcIndex === -1) return;
        const relativePath = filename.slice(srcIndex + 1);

        // Determine if this file requires test coverage
        const needsTests =
          /^src\/app\/.*\/page\.tsx$/.test(relativePath) ||
          (/^src\/hooks\/[^/]+\.ts$/.test(relativePath) &&
            !relativePath.includes('__tests__')) ||
          /^src\/lib\/[^/]+\.ts$/.test(relativePath) ||
          /^src\/services\/.*\.ts$/.test(relativePath);

        if (!needsTests) return;

        // Skip allowlisted files (migration: shrink this list over time)
        if (TEST_COVERAGE_ALLOWLIST.has(relativePath)) return;

        context.report({
          node,
          message: `This file requires test coverage. Pages need E2E tests in tests/*.spec.ts. Hooks/lib/services need unit tests in __tests__/. See .claude/rules/testing.md.`,
        });
      },
    };
  },
};

// ---------------------------------------------------------------------------
// Rule 10: require-semantic-colors
// Vision: No hardcoded Tailwind color classes (zinc-500, red-600, bg-white,
// text-black, etc.). Use semantic theme tokens instead (foreground, muted,
// card, border, primary, destructive, etc.) so dark/light mode works.
// Migration allowlist: existing files grandfathered in (2026-03-28).
// This list should SHRINK over time — do NOT add new files.
// ---------------------------------------------------------------------------
const SEMANTIC_COLORS_ALLOWLIST = new Set([
  // Migration complete (2026-03-28) — all files converted to semantic tokens.
  // Add files here ONLY as temporary exceptions during migration.
]);

// All Tailwind color scales (gray-scale + chromatic)
const TW_COLOR_SCALES = [
  // Gray-scale
  'zinc',
  'slate',
  'gray',
  'neutral',
  'stone',
  // Chromatic
  'red',
  'orange',
  'amber',
  'yellow',
  'lime',
  'green',
  'emerald',
  'teal',
  'cyan',
  'sky',
  'blue',
  'indigo',
  'violet',
  'purple',
  'fuchsia',
  'pink',
  'rose',
].join('|');

// Matches: text-zinc-800, bg-red-500, border-emerald-200, from-amber-100, etc.
const HARDCODED_SCALE_PATTERN = new RegExp(
  `(?:^|\\s)(?:text|bg|border|ring|shadow|outline|divide|from|to|via|placeholder|decoration|accent)-(?:${TW_COLOR_SCALES})-\\d+`
);

// Matches: text-white, bg-white, text-black, bg-black, border-white, border-black
const HARDCODED_BW_PATTERN =
  /(?:^|\s)(?:text|bg|border|ring|shadow|outline|divide|from|to|via)-(?:white|black)(?:\s|\/|$)/;

function hasHardcodedColor(str) {
  return HARDCODED_SCALE_PATTERN.test(str) || HARDCODED_BW_PATTERN.test(str);
}

const SEMANTIC_MSG =
  'Hardcoded Tailwind color detected. Use semantic theme tokens instead (e.g., text-foreground, text-muted-foreground, bg-card, bg-muted, border-border, text-primary, text-destructive) so dark/light mode works automatically.';

const requireSemanticColors = {
  create(context) {
    return {
      JSXAttribute(node) {
        const filename = context.filename || '';
        if (!filename.includes('/src/')) return;

        // Skip test files
        if (filename.includes('.test.') || filename.includes('__tests__'))
          return;

        // Skip allowlisted files
        const srcIndex = filename.indexOf('/src/');
        if (srcIndex !== -1) {
          const relativePath = filename.slice(srcIndex + 1);
          if (SEMANTIC_COLORS_ALLOWLIST.has(relativePath)) return;
        }

        // Only check className attributes
        if (node.name.name !== 'className') return;

        const value = node.value;
        if (!value) return;

        // Handle string literals: className="text-zinc-800 ..."
        if (value.type === 'Literal' && typeof value.value === 'string') {
          if (hasHardcodedColor(value.value)) {
            context.report({ node, message: SEMANTIC_MSG });
          }
        }

        // Handle template literals: className={`text-zinc-800 ...`}
        if (
          value.type === 'JSXExpressionContainer' &&
          value.expression.type === 'TemplateLiteral'
        ) {
          for (const quasi of value.expression.quasis) {
            if (hasHardcodedColor(quasi.value.raw)) {
              context.report({ node, message: SEMANTIC_MSG });
              break;
            }
          }
        }

        // Handle cn() and string args in call expressions
        if (
          value.type === 'JSXExpressionContainer' &&
          value.expression.type === 'CallExpression'
        ) {
          for (const arg of value.expression.arguments) {
            if (arg.type === 'Literal' && typeof arg.value === 'string') {
              if (hasHardcodedColor(arg.value)) {
                context.report({ node, message: SEMANTIC_MSG });
                break;
              }
            }
            if (arg.type === 'TemplateLiteral') {
              for (const quasi of arg.quasis) {
                if (hasHardcodedColor(quasi.value.raw)) {
                  context.report({ node, message: SEMANTIC_MSG });
                  break;
                }
              }
            }
          }
        }
      },
    };
  },
};

// ---------------------------------------------------------------------------
// Plugin export
// ---------------------------------------------------------------------------
const plugin = {
  meta: { name: 'landbruget' },
  rules: {
    'max-component-size': maxComponentSize,
    'no-service-import-in-components': noServiceImportInComponents,
    'no-raw-process-env': noRawProcessEnv,
    'require-data-testid': requireDataTestid,
    'no-default-export': noDefaultExport,
    'enforce-absolute-imports': enforceAbsoluteImports,
    'no-inline-styles': noInlineStyles,
    'no-hardcoded-api-urls': noHardcodedApiUrls,
    'require-test-coverage': requireTestCoverage,
    'require-semantic-colors': requireSemanticColors,
  },
};

export default plugin;
