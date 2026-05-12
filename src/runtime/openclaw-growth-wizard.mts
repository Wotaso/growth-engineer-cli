#!/usr/bin/env node

import { promises as fs } from 'node:fs';
import path from 'node:path';
import process from 'node:process';
import { spawn } from 'node:child_process';
import { createInterface } from 'node:readline/promises';
import { emitKeypressEvents } from 'node:readline';
import { createPrivateKey } from 'node:crypto';
import {
  buildExtraSourceConfig,
  getDefaultSourceCommand,
} from './openclaw-growth-shared.mjs';
import { loadOpenClawGrowthSecrets } from './openclaw-growth-env.mjs';

const DEFAULT_CONFIG_PATH = 'data/openclaw-growth-engineer/config.json';
const SELF_UPDATE_INTERVAL_MS = 24 * 60 * 60 * 1000;
const ENABLE_ISOLATED_SECRET_RUNNER_WIZARD = false;
const DEFAULT_GROWTH_INTERVAL_MINUTES = 1440;
const DEFAULT_CONNECTOR_HEALTH_INTERVAL_MINUTES = 360;
const CONNECTOR_KEYS = ['analytics', 'github', 'revenuecat', 'sentry', 'asc'] as const;
type ConnectorKey = (typeof CONNECTOR_KEYS)[number];
type ConnectorDefinition = {
  key: ConnectorKey;
  label: string;
  summary: string;
  needs: string;
};
type ConnectorPickerCopy = {
  introTitle?: string;
  introDetail?: string | null;
  actionTitle?: string;
  helpText?: string;
  mode?: 'setup' | 'input';
};

class WizardAbortError extends Error {
  exitCode: number;

  constructor(message: string, exitCode = 130) {
    super(message);
    this.name = 'WizardAbortError';
    this.exitCode = exitCode;
  }
}

const CONNECTOR_DEFINITIONS: ConnectorDefinition[] = [
  {
    key: 'analytics',
    label: 'AnalyticsCLI product analytics',
    summary: 'Read product events, funnels, retention, users, and feedback.',
    needs: 'An AnalyticsCLI readonly token from dash.analyticscli.com.',
  },
  {
    key: 'github',
    label: 'GitHub code access',
    summary: 'Read repo context and optionally create issues or draft PRs.',
    needs: 'Create a GitHub token with the scopes you want; you can change it later by rerunning the wizard.',
  },
  {
    key: 'revenuecat',
    label: 'RevenueCat monetization data',
    summary: 'Read subscription, product, entitlement, and revenue context.',
    needs: 'A RevenueCat v2 secret API key with read-only project permissions.',
  },
  {
    key: 'sentry',
    label: 'Sentry-compatible crash monitoring',
    summary: 'Read unresolved crashes, regressions, affected users, releases, and production stability signals.',
    needs: 'A Sentry or GlitchTip-compatible auth token plus the org slug. Project scope is inferred later from app context or config.',
  },
  {
    key: 'asc',
    label: 'ASC / App Store Connect CLI',
    summary: 'Read App Store analytics, reviews/ratings, builds/TestFlight/release context, subscriptions, purchases, and crash totals.',
    needs: 'ASC_KEY_ID, ASC_ISSUER_ID, and the AuthKey_XXXX.p8 content or path.',
  },
];

const DEFAULT_CADENCE_PLAN = [
  {
    key: 'daily',
    title: 'Daily Sentry and production guardrail',
    intervalDays: 1,
    criticalOnly: true,
    focusAreas: ['sentry_errors', 'crash', 'onboarding', 'conversion', 'paywall', 'purchase'],
    sourcePriorities: ['sentry', 'glitchtip', 'analytics', 'revenuecat', 'asc_cli', 'feedback', 'github'],
    objective:
      'Analyze every configured project for critical production blockers: Sentry/GlitchTip errors, crashes, onboarding or purchase drop-offs, zero-conversion days, missing buyers, very low users, and other silent business anomalies.',
    instructions:
      'Compare against recent baselines across connected sources and code changes. If the finding is critical, produce the exact fix or next debugging step and prefer a GitHub issue or draft PR when GitHub write access is configured; otherwise hand off via OpenClaw chat. Avoid generic growth ideas.',
  },
  {
    key: 'weekly',
    title: 'Weekly executive product and growth summary',
    intervalDays: 7,
    criticalOnly: false,
    focusAreas: ['conversion', 'paywall', 'onboarding', 'marketing', 'retention', 'stability'],
    sourcePriorities: ['analytics', 'revenuecat', 'asc_cli', 'feedback', 'sentry', 'github'],
    objective:
      'Create an executive summary across all configured projects, connectors, recent releases, code changes, revenue, activation, retention, reviews, and production stability.',
    instructions:
      'Pick one to three high-confidence improvements with evidence, expected KPI movement, likely code/store surfaces, owner-ready next steps, and a verification plan. Create GitHub issues or draft PR proposals only when the evidence is specific enough.',
  },
  {
    key: 'monthly',
    title: 'Monthly deep product, business, and code review',
    intervalDays: 30,
    criticalOnly: false,
    focusAreas: ['conversion', 'paywall', 'retention', 'marketing', 'onboarding', 'codebase'],
    sourcePriorities: ['analytics', 'revenuecat', 'asc_cli', 'feedback', 'sentry', 'github'],
    objective:
      'Compare all configured projects month-over-month: MRR, trial conversion, churn, acquisition quality, store conversion, retention, review themes, feature usage, crash totals, and codebase changes.',
    instructions:
      'Decide what should be built, changed, deleted, or instrumented next. Tie conclusions to connector data plus codebase evidence and explain why each recommendation should move revenue, activation, retention, stability, or acquisition quality.',
  },
  {
    key: 'quarterly',
    title: 'Quarterly positioning, pricing, and roadmap review',
    intervalDays: 91,
    criticalOnly: false,
    focusAreas: ['marketing', 'paywall', 'retention', 'conversion', 'onboarding'],
    sourcePriorities: ['analytics', 'revenuecat', 'asc_cli', 'feedback', 'github', 'sentry'],
    objective:
      'Revisit positioning, pricing/packaging, onboarding architecture, roadmap assumptions, tracking quality, codebase constraints, and major funnel bets across every configured project.',
    instructions:
      'Find structural constraints and durable opportunities. Tie recommendations to cohort behavior, monetization, reviews, channel quality, and shipped changes.',
  },
  {
    key: 'six_months',
    title: 'Six-month instrumentation and growth-system audit',
    intervalDays: 182,
    criticalOnly: false,
    focusAreas: ['retention', 'conversion', 'paywall', 'marketing', 'general'],
    sourcePriorities: ['analytics', 'revenuecat', 'asc_cli', 'feedback', 'sentry'],
    objective:
      'Audit connector coverage, SDK instrumentation, event taxonomy, data reliability, memory, growth loops, and whether product/code strategy still matches the best users across configured projects.',
    instructions:
      'Prioritize measurement fixes and system changes that make future analysis more trustworthy. Identify stale events, missing attribution, weak identity, and misleading dashboards.',
  },
  {
    key: 'yearly',
    title: 'Yearly evidence reset',
    intervalDays: 365,
    criticalOnly: false,
    focusAreas: ['marketing', 'retention', 'paywall', 'conversion', 'general'],
    sourcePriorities: ['analytics', 'revenuecat', 'asc_cli', 'feedback', 'sentry'],
    objective:
      'Reset strategy from evidence across every configured project: market/channel fit, monetization model, retention ceiling, product scope, and whether to double down, reposition, rebuild, or sunset major surfaces/features.',
    instructions:
      'Use the full year of memory, releases, revenue, acquisition, reviews, code changes, and cohort behavior. Produce strategic experiments and stop-doing decisions.',
  },
];

const ANSI = {
  bold: '\x1b[1m',
  cyan: '\x1b[36m',
  dim: '\x1b[2m',
  green: '\x1b[32m',
  hideCursor: '\x1b[?25l',
  reset: '\x1b[0m',
  showCursor: '\x1b[?25h',
};

async function ensureDirForFile(filePath) {
  await fs.mkdir(path.dirname(filePath), { recursive: true });
}

async function fileExists(filePath) {
  try {
    await fs.access(filePath);
    return true;
  } catch {
    return false;
  }
}

async function readJsonFile(filePath) {
  return JSON.parse(await fs.readFile(filePath, 'utf8'));
}

async function readJsonIfPresent(filePath) {
  if (!(await fileExists(filePath))) return null;
  return readJsonFile(filePath);
}

async function writeJsonFile(filePath, value) {
  await ensureDirForFile(filePath);
  await fs.writeFile(filePath, `${JSON.stringify(value, null, 2)}\n`, 'utf8');
}

function isTruthyEnv(value) {
  return ['1', 'true', 'yes', 'y', 'on'].includes(String(value || '').trim().toLowerCase());
}

function isFalseyEnv(value) {
  return ['0', 'false', 'no', 'n', 'off'].includes(String(value || '').trim().toLowerCase());
}

function parseArgs(argv) {
  const args = {
    config: DEFAULT_CONFIG_PATH,
    connectorWizard: false,
    connectors: '',
    noSelfUpdate: false,
    out: DEFAULT_CONFIG_PATH,
  };

  for (let i = 0; i < argv.length; i += 1) {
    const token = argv[i];
    const next = argv[i + 1];
    if (token === '--') {
      continue;
    } else if (token === '--config') {
      args.config = next || args.config;
      args.out = next || args.out;
      i += 1;
    } else if (token === '--connectors' || token === '--connector-setup') {
      args.connectorWizard = true;
      if (next && !next.startsWith('-')) {
        args.connectors = next;
        i += 1;
      }
    } else if (token === '--out') {
      args.out = next;
      args.config = next;
      i += 1;
    } else if (token === '--no-self-update') {
      args.noSelfUpdate = true;
    } else if (token === '--help' || token === '-h') {
      printHelpAndExit(0);
    } else {
      printHelpAndExit(1, `Unknown argument: ${token}`);
    }
  }
  return args;
}

function printHelpAndExit(exitCode, reason = null) {
  if (reason) {
    process.stderr.write(`${reason}\n\n`);
  }
  process.stdout.write(`
OpenClaw Growth Setup Wizard

Usage:
  node scripts/openclaw-growth-wizard.mjs [--out <config-path>]
  node scripts/openclaw-growth-wizard.mjs --connectors [analytics,github,revenuecat,sentry,asc] [--config <config-path>]

Options:
  --no-self-update   Skip the ClawHub skill update check for this run
`);
  process.exit(exitCode);
}

function quote(value) {
  if (/^[a-zA-Z0-9_./:-]+$/.test(String(value))) {
    return String(value);
  }
  return `'${String(value).replace(/'/g, `'\\''`)}'`;
}

function normalizeConnectorKey(value): ConnectorKey | 'all' | null {
  const normalized = String(value || '').trim().toLowerCase().replace(/[_\s]+/g, '-');
  if (!normalized) return null;
  if (normalized === 'all') return 'all';
  if (['analytics', 'analyticscli', 'product-analytics', 'events'].includes(normalized)) return 'analytics';
  if (['github', 'gh', 'github-code', 'codebase', 'code-access'].includes(normalized)) return 'github';
  if (['revenuecat', 'revenue-cat', 'rc', 'revenuecat-mcp'].includes(normalized)) return 'revenuecat';
  if (['sentry', 'sentry-api', 'sentry-mcp', 'crashes', 'errors', 'crash-reporting'].includes(normalized)) return 'sentry';
  if (['asc', 'asc-cli', 'app-store-connect', 'appstoreconnect', 'app-store'].includes(normalized)) return 'asc';
  return null;
}

function parseConnectorList(value): ConnectorKey[] {
  const selected = new Set<ConnectorKey>();
  for (const entry of String(value || '').split(',')) {
    const connector = normalizeConnectorKey(entry);
    if (!connector) continue;
    if (connector === 'all') {
      CONNECTOR_KEYS.forEach((key) => selected.add(key));
    } else {
      selected.add(connector);
    }
  }
  return [...selected];
}

function isConnectorLocallyConfigured(key: ConnectorKey) {
  if (key === 'analytics') {
    return Boolean(process.env.ANALYTICSCLI_ACCESS_TOKEN?.trim() || process.env.ANALYTICSCLI_READONLY_TOKEN?.trim());
  }
  if (key === 'github') return Boolean(process.env.GITHUB_TOKEN?.trim());
  if (key === 'revenuecat') return Boolean(process.env.REVENUECAT_API_KEY?.trim());
  if (key === 'sentry') return Boolean(process.env.SENTRY_AUTH_TOKEN?.trim());
  if (key === 'asc') {
    return Boolean(
      process.env.ASC_KEY_ID?.trim() &&
      process.env.ASC_ISSUER_ID?.trim() &&
      (process.env.ASC_PRIVATE_KEY_PATH?.trim() || process.env.ASC_PRIVATE_KEY?.trim()),
    );
  }
  return false;
}

function getRequiredConnectorKeys() {
  return new Set<ConnectorKey>(isConnectorLocallyConfigured('analytics') ? [] : ['analytics']);
}

function withMissingRequiredAnalyticsConnector(selected: ConnectorKey[]): ConnectorKey[] {
  if (isConnectorLocallyConfigured('analytics') || selected.includes('analytics')) return orderConnectors(selected);
  return orderConnectors(['analytics', ...selected]);
}

async function askConnectorSelectionWithHealth(
  rl,
  healthByConnector: Record<string, any> = {},
  initialSelected: ConnectorKey[] = [],
  copy: ConnectorPickerCopy = {},
): Promise<ConnectorKey[]> {
  if (!process.stdin.isTTY || !process.stdout.isTTY || !process.stdin.setRawMode) {
    return await askConnectorSelectionByText(rl, healthByConnector, copy);
  }

  rl.pause();
  let completed = false;
  try {
    const selected = await askConnectorSelectionByKeys(healthByConnector, initialSelected, copy);
    completed = true;
    return selected;
  } finally {
    if (completed) {
      rl.resume();
    } else {
      process.stdin.pause();
    }
  }
}

async function askConnectorSelectionByText(
  rl,
  healthByConnector: Record<string, any> = {},
  copy: ConnectorPickerCopy = {},
): Promise<ConnectorKey[]> {
  printConnectorIntro(copy);
  for (const group of connectorPickerGroups(healthByConnector)) {
    process.stdout.write(`${ANSI.bold}${group.title}${ANSI.reset}\n`);
    for (const connector of group.connectors) {
      const number = CONNECTOR_DEFINITIONS.findIndex((entry) => entry.key === connector.key) + 1;
      process.stdout.write(`  ${number}) ${connector.label}\n`);
      writeWrapped(formatConnectorHealthText(connector.key, healthByConnector), '     ', ANSI.dim);
      writeWrapped(connector.summary, '     ');
    }
    process.stdout.write('\n');
  }
  while (true) {
    const answer = await ask(rl, 'Select connectors (comma-separated numbers/names, or all)', 'all');
    const selected = parseConnectorAnswer(answer);
    if (selected.length > 0) return selected;
    process.stdout.write('\nChoose at least one connector.\n\n');
  }
}

function parseConnectorAnswer(answer): ConnectorKey[] {
  const selected = new Set<ConnectorKey>();
  for (const rawEntry of String(answer || '').split(',')) {
    const entry = rawEntry.trim().toLowerCase();
    const numericConnector = CONNECTOR_DEFINITIONS[Number(entry) - 1]?.key;
    if (numericConnector) selected.add(numericConnector);
    const key = normalizeConnectorKey(entry);
    if (key === 'all') CONNECTOR_KEYS.forEach((connector) => selected.add(connector));
    if (key && key !== 'all') selected.add(key);
  }
  return orderConnectors([...selected]);
}

function orderConnectors(keys: ConnectorKey[]): ConnectorKey[] {
  const selected = new Set(keys);
  return CONNECTOR_KEYS.filter((key) => selected.has(key));
}

function printConnectorIntro(copy: ConnectorPickerCopy = {}) {
  process.stdout.write(`\n${ANSI.bold}${copy.introTitle || 'OpenClaw connector setup'}${ANSI.reset}\n`);
  const detail = copy.introDetail === undefined
    ? 'You can configure connector secrets here. API keys stay in this host\'s local secrets file, not in chat or config JSON.'
    : copy.introDetail;
  if (detail) {
    process.stdout.write(`${ANSI.dim}${detail}${ANSI.reset}\n`);
  }
  process.stdout.write('\n');
}

async function askMenuChoice<T extends string>(
  rl,
  {
    title,
    subtitle = 'Use Up/Down to move, Enter to continue.',
    options,
    defaultValue,
    renderHeader,
  }: {
    title: string;
    subtitle?: string;
    options: Array<{ value: T; label: string; detail: string }>;
    defaultValue: T;
    renderHeader?: () => void;
  },
): Promise<T> {
  if (!process.stdin.isTTY || !process.stdout.isTTY || !process.stdin.setRawMode) {
    process.stdout.write(`\n${title}\n`);
    options.forEach((option, index) => {
      process.stdout.write(`  ${index + 1}) ${option.label}: ${option.detail}\n`);
    });
    const defaultIndex = Math.max(0, options.findIndex((option) => option.value === defaultValue));
    const answer = await ask(rl, `Setup area (1-${options.length})`, String(defaultIndex + 1));
    const selected = options[Number(answer.trim()) - 1] || options[defaultIndex];
    return selected.value;
  }

  rl.pause();
  let completed = false;
  try {
    const selected = await askMenuChoiceByKeys({ title, subtitle, options, defaultValue, renderHeader });
    completed = true;
    return selected;
  } finally {
    if (completed) {
      rl.resume();
    } else {
      process.stdin.pause();
    }
  }
}

async function askMenuChoiceByKeys<T extends string>({
  title,
  subtitle,
  options,
  defaultValue,
  renderHeader,
}: {
  title: string;
  subtitle: string;
  options: Array<{ value: T; label: string; detail: string }>;
  defaultValue: T;
  renderHeader?: () => void;
}): Promise<T> {
  emitKeypressEvents(process.stdin);
  const wasRaw = process.stdin.isRaw;
  const wasPaused = process.stdin.isPaused();
  process.stdin.setRawMode(true);
  process.stdin.resume();

  let cursorIndex = Math.max(0, options.findIndex((option) => option.value === defaultValue));

  return await new Promise<T>((resolve, reject) => {
    const cleanup = () => {
      process.stdin.off('keypress', onKeypress);
      process.stdin.setRawMode(Boolean(wasRaw));
      if (wasPaused) {
        process.stdin.pause();
      }
      process.stdout.write(ANSI.showCursor);
    };

    const render = () => {
      process.stdout.write('\x1b[2J\x1b[H');
      renderHeader?.();
      process.stdout.write(`\n${ANSI.bold}${title}${ANSI.reset}\n`);
      process.stdout.write(`${ANSI.dim}${subtitle}${ANSI.reset}\n\n`);
      for (let index = 0; index < options.length; index += 1) {
        const option = options[index];
        const pointer = index === cursorIndex ? `${ANSI.cyan}>${ANSI.reset}` : ' ';
        const number = `${index + 1})`;
        process.stdout.write(`${pointer} ${number} ${ANSI.bold}${option.label}${ANSI.reset}\n`);
        writeWrapped(option.detail, '     ', ANSI.dim);
      }
      process.stdout.write(`\n${ANSI.dim}Esc/Q cancels. Number keys 1-${options.length} select directly.${ANSI.reset}\n`);
    };

    const cancel = () => {
      cleanup();
      process.stdout.write('\n');
      reject(new WizardAbortError('Setup cancelled.'));
    };

    const finish = () => {
      cleanup();
      process.stdout.write('\x1b[2J\x1b[H');
      resolve(options[cursorIndex]?.value || defaultValue);
    };

    const onKeypress = (_text, key) => {
      if (key?.ctrl && key?.name === 'c') {
        cancel();
        return;
      }
      if (key?.name === 'escape' || key?.name === 'q') {
        cancel();
        return;
      }
      if (key?.name === 'up' || key?.name === 'k') {
        cursorIndex = (cursorIndex - 1 + options.length) % options.length;
      } else if (key?.name === 'down' || key?.name === 'j') {
        cursorIndex = (cursorIndex + 1) % options.length;
      } else if (key?.name === 'return' || key?.name === 'enter') {
        finish();
        return;
      } else if (/^[1-9]$/.test(String(_text || ''))) {
        const selectedIndex = Number(_text) - 1;
        if (options[selectedIndex]) {
          cursorIndex = selectedIndex;
          finish();
          return;
        }
      }
      render();
    };

    process.stdin.on('keypress', onKeypress);
    process.stdout.write(ANSI.hideCursor);
    render();
  });
}

function normalizeConnectorProgressKey(key): ConnectorKey | null {
  const normalized = String(key || '').trim().toLowerCase();
  if (normalized === 'analytics' || normalized === 'analyticscli') return 'analytics';
  if (normalized === 'github') return 'github';
  if (normalized === 'revenuecat') return 'revenuecat';
  if (normalized === 'sentry') return 'sentry';
  if (normalized === 'asc' || normalized === 'appstoreconnect' || normalized === 'app-store-connect') return 'asc';
  return null;
}

async function withConnectorHealthLoading<T>(
  taskFactory: (onProgress: (event: any) => void) => Promise<T>,
): Promise<T> {
  const frames = ['-', '\\', '|', '/'];
  const completed = new Set<ConnectorKey>();
  let index = 0;
  let current = 'starting';
  const render = () => {
    const count = Math.min(completed.size, CONNECTOR_KEYS.length);
    process.stdout.write(`\rChecking connector health ${count}/${CONNECTOR_KEYS.length} (${current}) ${frames[index]}`);
  };
  const timer = setInterval(() => {
    index = (index + 1) % frames.length;
    render();
  }, 120);
  render();
  try {
    const result = await taskFactory((event) => {
      const key = normalizeConnectorProgressKey(event?.key);
      if (!key) return;
      current = connectorLabel(key);
      if (event?.phase === 'finish') completed.add(key);
      render();
    });
    CONNECTOR_KEYS.forEach((key) => completed.add(key));
    current = 'done';
    render();
    process.stdout.write('\n');
    return result;
  } finally {
    clearInterval(timer);
  }
}

function connectorLabel(key: ConnectorKey) {
  return CONNECTOR_DEFINITIONS.find((connector) => connector.key === key)?.label ?? key;
}

function toConfigId(value, fallback) {
  return String(value || fallback)
    .trim()
    .toLowerCase()
    .replace(/https?:\/\//g, '')
    .replace(/[^a-zA-Z0-9._-]+/g, '_')
    .replace(/^_+|_+$/g, '') || fallback;
}

function toEnvName(value, fallback) {
  return String(value || fallback)
    .trim()
    .toUpperCase()
    .replace(/[^A-Z0-9]+/g, '_')
    .replace(/^_+|_+$/g, '') || fallback;
}

function connectorHealthLabel(status) {
  if (status === 'connected') return 'healthy';
  if (status === 'partial') return 'partial';
  if (status === 'blocked') return 'blocked';
  if (status === 'not_enabled') return 'not enabled';
  if (status === 'not_connected') return 'not connected';
  if (status === 'unknown') return 'unknown';
  return status || 'not checked';
}

function getConnectorHealth(key: ConnectorKey, healthByConnector: Record<string, any> = {}) {
  const fallbackStatus = isConnectorLocallyConfigured(key) ? 'unknown' : 'not_connected';
  const fallbackDetail = isConnectorLocallyConfigured(key)
    ? 'credentials exist, but live health was not verified'
    : '';
  return healthByConnector[key] || { status: fallbackStatus, detail: fallbackDetail };
}

function connectorStatusLabel(key: ConnectorKey, healthByConnector: Record<string, any> = {}) {
  const health = getConnectorHealth(key, healthByConnector);
  const configured = isConnectorLocallyConfigured(key);
  if (health.status === 'connected') return configured ? 'configured, healthy' : 'healthy via local tool auth';
  if (!configured) return 'not configured';
  return `configured, ${connectorHealthLabel(health.status)}`;
}

function formatConnectorHealthText(key: ConnectorKey, healthByConnector: Record<string, any> = {}) {
  const health = getConnectorHealth(key, healthByConnector);
  const label = connectorStatusLabel(key, healthByConnector);
  const detail = health.detail ? ` - ${health.detail}` : '';
  return `Status: ${label}${detail}`;
}

function wrapText(text, indent = '', width = process.stdout.columns || 100) {
  const available = Math.max(32, width - indent.length);
  const words = String(text || '').split(/\s+/).filter(Boolean);
  const lines: string[] = [];
  let current = '';
  for (const word of words) {
    if (!current) {
      current = word;
    } else if (`${current} ${word}`.length <= available) {
      current = `${current} ${word}`;
    } else {
      lines.push(current);
      current = word;
    }
    while (current.length > available) {
      lines.push(current.slice(0, available));
      current = current.slice(available);
    }
  }
  if (current) lines.push(current);
  return lines.length > 0 ? lines.map((line) => `${indent}${line}`) : [indent.trimEnd()];
}

function writeWrapped(text, indent = '', style = '') {
  for (const line of wrapText(text, indent)) {
    process.stdout.write(style ? `${style}${line}${ANSI.reset}\n` : `${line}\n`);
  }
}

function connectorPickerGroups(healthByConnector: Record<string, any> = {}) {
  const groups = [
    { title: 'Configured - needs attention', connectors: [] as typeof CONNECTOR_DEFINITIONS },
    { title: 'Configured - healthy', connectors: [] as typeof CONNECTOR_DEFINITIONS },
    { title: 'Not configured', connectors: [] as typeof CONNECTOR_DEFINITIONS },
  ];
  for (const connector of CONNECTOR_DEFINITIONS) {
    const configured = isConnectorLocallyConfigured(connector.key);
    const health = getConnectorHealth(connector.key, healthByConnector);
    if (!configured && health.status !== 'connected') {
      groups[2].connectors.push(connector);
    } else if (health.status === 'connected') {
      groups[1].connectors.push(connector);
    } else {
      groups[0].connectors.push(connector);
    }
  }
  return groups.filter((group) => group.connectors.length > 0);
}

function connectorPickerDisplayItems(healthByConnector: Record<string, any> = {}) {
  return connectorPickerGroups(healthByConnector).flatMap((group) => group.connectors);
}

function connectorKeysNeedingAttention(healthByConnector: Record<string, any> = {}): ConnectorKey[] {
  return CONNECTOR_KEYS.filter((key) =>
    ['blocked', 'partial', 'unknown', 'not_connected'].includes(String(getConnectorHealth(key, healthByConnector).status || '')),
  );
}

async function getConnectorPickerHealth(configPath, onProgress: (event: any) => void = () => {}) {
  if (!(await fileExists(configPath))) {
    return Object.fromEntries(
      CONNECTOR_KEYS.map((key) => [
        key,
        {
          status: isConnectorLocallyConfigured(key) ? 'unknown' : 'not_connected',
          detail: isConnectorLocallyConfigured(key)
            ? `config file not found at ${configPath}; live check could not run`
            : '',
        },
      ]),
    );
  }
  const result = await runCommandCaptureWithProgress(
    `node scripts/openclaw-growth-status.mjs --config ${quote(configPath)} --json --progress-json`,
    onProgress,
  );
  const payload = parseJsonFromStdout(result.stdout);
  const connectors = payload?.connectors && typeof payload.connectors === 'object' ? payload.connectors : {};
  const healthByConnector = {
    analytics: connectors.analyticscli,
    github: connectors.github,
    revenuecat: connectors.revenuecat,
    sentry: connectors.sentry,
    asc: connectors.appStoreConnect,
  };
  return Object.fromEntries(
    CONNECTOR_KEYS.map((key) => [key, getConnectorHealth(key, healthByConnector)]),
  );
}

function renderConnectorPicker(
  cursorIndex: number,
  selected: Set<ConnectorKey>,
  required: Set<ConnectorKey>,
  healthByConnector: Record<string, any> = {},
  warning = '',
  copy: ConnectorPickerCopy = {},
) {
  process.stdout.write('\x1b[2J\x1b[H');
  printConnectorIntro(copy);
  process.stdout.write(`${ANSI.bold}${copy.actionTitle || 'Select connectors to set up or overwrite now'}${ANSI.reset}\n`);
  writeWrapped(copy.helpText || 'Use Up/Down to move, Space to toggle optional connectors, A to toggle all optional connectors, Enter to continue.', '', ANSI.dim);
  process.stdout.write('\n');

  let index = 0;
  for (const group of connectorPickerGroups(healthByConnector)) {
    process.stdout.write(`${ANSI.bold}${group.title}${ANSI.reset}\n`);
    for (const connector of group.connectors) {
      const active = index === cursorIndex;
      const isRequired = required.has(connector.key);
      const checked = isRequired || selected.has(connector.key);
      const pointer = active ? `${ANSI.cyan}>${ANSI.reset}` : ' ';
      const box = checked ? `${ANSI.green}[x]${ANSI.reset}` : '[ ]';
      const suffix = isRequired ? ' (required baseline)' : '';
      const label = `${connector.label}${suffix}`;
      const title = active ? `${ANSI.bold}${label}${ANSI.reset}` : label;
      process.stdout.write(`${pointer} ${box} ${title}\n`);
      writeWrapped(connector.summary, '    ');
      writeWrapped(formatConnectorHealthText(connector.key, healthByConnector), '    ', ANSI.dim);
      process.stdout.write('\n');
      index += 1;
    }
  }

  if (warning) {
    process.stdout.write(`${ANSI.bold}${warning}${ANSI.reset}\n\n`);
  }
  process.stdout.write(`${ANSI.dim}Esc/Q cancels. Number keys 1-${CONNECTOR_DEFINITIONS.length} also toggle connectors.${ANSI.reset}\n`);
}

async function askConnectorSelectionByKeys(
  healthByConnector: Record<string, any> = {},
  initialSelected: ConnectorKey[] = [],
  copy: ConnectorPickerCopy = {},
): Promise<ConnectorKey[]> {
  emitKeypressEvents(process.stdin);
  const wasRaw = process.stdin.isRaw;
  const wasPaused = process.stdin.isPaused();
  process.stdin.setRawMode(true);
  process.stdin.resume();

  let cursorIndex = 0;
  const required = copy.mode === 'input' ? new Set<ConnectorKey>() : getRequiredConnectorKeys();
  const initial = new Set(initialSelected);
  const selected = new Set<ConnectorKey>(
    CONNECTOR_KEYS.filter((key) =>
      required.has(key) ||
      initial.has(key) ||
      (copy.mode !== 'input' && !isConnectorLocallyConfigured(key)),
    ),
  );
  let warning = '';

  return await new Promise<ConnectorKey[]>((resolve, reject) => {
    const displayItems = () => connectorPickerDisplayItems(healthByConnector);
    const selectedDisplayConnector = () => displayItems()[cursorIndex] || displayItems()[0];
    const displayIndexForConnector = (key: ConnectorKey) =>
      Math.max(0, displayItems().findIndex((connector) => connector.key === key));

    const cleanup = () => {
      process.stdin.off('keypress', onKeypress);
      process.stdin.setRawMode(Boolean(wasRaw));
      if (wasPaused) {
        process.stdin.pause();
      }
      process.stdout.write(ANSI.showCursor);
    };

    const finish = () => {
      required.forEach((key) => selected.add(key));
      if (selected.size === 0) {
        warning = 'No connectors selected. Select a connector to update or press Esc to cancel.';
        renderConnectorPicker(cursorIndex, selected, required, healthByConnector, warning, copy);
        return;
      }
      cleanup();
      process.stdout.write('\x1b[2J\x1b[H');
      resolve(orderConnectors([...selected]));
    };

    const cancel = () => {
      cleanup();
      process.stdout.write('\n');
      reject(new WizardAbortError('Connector setup cancelled.'));
    };

    const toggleCurrent = () => {
      const connector = selectedDisplayConnector();
      if (!connector) return;
      const key = connector.key;
      if (required.has(key)) {
        selected.add(key);
        warning = 'AnalyticsCLI is missing and required for the Growth Engineer baseline.';
        return;
      }
      if (selected.has(key)) selected.delete(key);
      else selected.add(key);
      warning = '';
    };

    const toggleAll = () => {
      const optionalKeys = CONNECTOR_KEYS.filter((key) => !required.has(key));
      const allOptionalSelected = optionalKeys.every((key) => selected.has(key));
      if (allOptionalSelected) optionalKeys.forEach((key) => selected.delete(key));
      else optionalKeys.forEach((key) => selected.add(key));
      required.forEach((key) => selected.add(key));
      warning = '';
    };

    const onKeypress = (_text, key) => {
      if (key?.ctrl && key?.name === 'c') {
        cancel();
        return;
      }
      if (key?.name === 'escape' || key?.name === 'q') {
        cancel();
        return;
      }
      if (key?.name === 'up' || key?.name === 'k') {
        const itemCount = displayItems().length || CONNECTOR_DEFINITIONS.length;
        cursorIndex = (cursorIndex - 1 + itemCount) % itemCount;
        warning = '';
      } else if (key?.name === 'down' || key?.name === 'j') {
        const itemCount = displayItems().length || CONNECTOR_DEFINITIONS.length;
        cursorIndex = (cursorIndex + 1) % itemCount;
        warning = '';
      } else if (key?.name === 'space') {
        toggleCurrent();
      } else if (key?.name === 'a') {
        toggleAll();
      } else if (key?.name === 'return' || key?.name === 'enter') {
        finish();
        return;
      } else if (/^[1-9]$/.test(String(_text || ''))) {
        const index = Number(_text) - 1;
        const connector = CONNECTOR_DEFINITIONS[index];
        if (connector) {
          cursorIndex = displayIndexForConnector(connector.key);
          if (required.has(connector.key)) {
            selected.add(connector.key);
            warning = 'AnalyticsCLI is missing and required for the Growth Engineer baseline.';
          } else {
            if (selected.has(connector.key)) selected.delete(connector.key);
            else selected.add(connector.key);
            warning = '';
          }
        }
      }
      renderConnectorPicker(cursorIndex, selected, required, healthByConnector, warning, copy);
    };

    process.stdin.on('keypress', onKeypress);
    process.stdout.write(ANSI.hideCursor);
    renderConnectorPicker(cursorIndex, selected, required, healthByConnector, warning, copy);
  });
}

async function commandExists(commandName) {
  const result = await runInteractiveCommand(`command -v ${quote(commandName)} >/dev/null 2>&1`, {
    silent: true,
  });
  return result === 0;
}

async function runInteractiveCommand(command, options: { env?: NodeJS.ProcessEnv; silent?: boolean } = {}) {
  return await new Promise<number | null>((resolve) => {
    const child = spawn('/bin/sh', ['-lc', command], {
      env: options.env ?? process.env,
      stdio: options.silent ? 'ignore' : 'inherit',
    });
    child.on('close', (code) => resolve(code));
  });
}

async function runInteractiveProcess(
  command,
  args: string[],
  options: { env?: NodeJS.ProcessEnv; silent?: boolean; rl?: any } = {},
) {
  return await new Promise<number | null>((resolve) => {
    options.rl?.pause?.();
    const child = spawn(command, args, {
      env: options.env ?? process.env,
      stdio: options.silent ? 'ignore' : 'inherit',
    });
    child.on('error', () => {
      options.rl?.resume?.();
      resolve(127);
    });
    child.on('close', (code) => {
      options.rl?.resume?.();
      resolve(code);
    });
  });
}

async function runCommandCapture(command, options: { env?: NodeJS.ProcessEnv } = {}) {
  return await new Promise<{ ok: boolean; stdout: string; stderr: string; code: number | null }>((resolve) => {
    const child = spawn('/bin/sh', ['-lc', command], {
      env: options.env ?? process.env,
      stdio: ['ignore', 'pipe', 'pipe'],
    });
    let stdout = '';
    let stderr = '';
    child.stdout.on('data', (chunk) => {
      stdout += String(chunk);
    });
    child.stderr.on('data', (chunk) => {
      stderr += String(chunk);
    });
    child.on('error', (error) => {
      resolve({ ok: false, stdout, stderr: error.message, code: null });
    });
    child.on('close', (code) => {
      resolve({ ok: code === 0, stdout, stderr, code });
    });
  });
}

async function runCommandCaptureWithTimeout(
  command,
  options: { env?: NodeJS.ProcessEnv; timeoutMs?: number } = {},
) {
  return await new Promise<{ ok: boolean; stdout: string; stderr: string; code: number | null }>((resolve) => {
    const child = spawn('/bin/sh', ['-lc', command], {
      env: options.env ?? process.env,
      stdio: ['ignore', 'pipe', 'pipe'],
    });
    let stdout = '';
    let stderr = '';
    let settled = false;
    const timer = setTimeout(() => {
      if (settled) return;
      settled = true;
      child.kill('SIGTERM');
      resolve({ ok: false, stdout, stderr: `${stderr}\nTimed out after ${options.timeoutMs}ms`, code: null });
    }, options.timeoutMs ?? 60_000);
    child.stdout.on('data', (chunk) => {
      stdout += String(chunk);
    });
    child.stderr.on('data', (chunk) => {
      stderr += String(chunk);
    });
    child.on('error', (error) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      resolve({ ok: false, stdout, stderr: error.message, code: null });
    });
    child.on('close', (code) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      resolve({ ok: code === 0, stdout, stderr, code });
    });
  });
}

async function runCommandCaptureWithProgress(
  command,
  onProgress,
  options: { env?: NodeJS.ProcessEnv; timeoutMs?: number } = {},
) {
  return await new Promise<{ ok: boolean; stdout: string; stderr: string; code: number | null }>((resolve) => {
    const child = spawn('/bin/sh', ['-lc', command], {
      env: options.env ?? process.env,
      stdio: ['ignore', 'pipe', 'pipe'],
    });
    let stdout = '';
    let stderr = '';
    let stderrBuffer = '';
    let settled = false;
    const timeoutMs = options.timeoutMs ?? 180_000;
    const timer = setTimeout(() => {
      if (settled) return;
      settled = true;
      child.kill('SIGTERM');
      resolve({ ok: false, stdout, stderr: `${stderr}\nTimed out after ${timeoutMs}ms`, code: null });
    }, timeoutMs);
    child.stdout.on('data', (chunk) => {
      stdout += String(chunk);
    });
    child.stderr.on('data', (chunk) => {
      const text = String(chunk);
      stderr += text;
      stderrBuffer += text;
      const lines = stderrBuffer.split(/\r?\n/);
      stderrBuffer = lines.pop() || '';
      for (const line of lines) {
        const match = line.match(/^OPENCLAW_PROGRESS\s+(.+)$/);
        if (!match) continue;
        try {
          onProgress(JSON.parse(match[1]));
        } catch {
          // Ignore malformed progress events; the final JSON result is authoritative.
        }
      }
    });
    child.on('error', (error) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      resolve({ ok: false, stdout, stderr: error.message, code: null });
    });
    child.on('close', (code) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      const match = stderrBuffer.match(/^OPENCLAW_PROGRESS\s+(.+)$/);
      if (match) {
        try {
          onProgress(JSON.parse(match[1]));
        } catch {
          // Ignore malformed progress events; the final JSON result is authoritative.
        }
      }
      resolve({ ok: code === 0, stdout, stderr, code });
    });
  });
}

function truncate(value, maxLength = 900) {
  const text = String(value || '').trim();
  if (text.length <= maxLength) return text;
  return `${text.slice(0, maxLength - 1)}...`;
}

function parseJsonFromStdout(stdout) {
  const raw = String(stdout || '').trim();
  if (!raw) return null;
  const firstBrace = raw.indexOf('{');
  const firstBracket = raw.indexOf('[');
  const starts = [firstBrace, firstBracket].filter((index) => index >= 0);
  if (starts.length === 0) return null;
  try {
    return JSON.parse(raw.slice(Math.min(...starts)));
  } catch {
    return null;
  }
}

function clearTerminal() {
  if (process.stdout.isTTY) {
    process.stdout.write('\x1b[2J\x1b[H');
  }
}

function printConnectorSetupProgress(payload) {
  const connectorSetup = Array.isArray(payload?.connectorSetup) ? payload.connectorSetup : [];
  const okConnectors = connectorSetup.filter((entry) => entry?.ok).map((entry) => entry.connector).filter(Boolean);
  if (okConnectors.length > 0) {
    process.stdout.write(`Configured locally: ${okConnectors.map(connectorTitle).join(', ')}.\n`);
  }
}

function checkConnectorKey(check) {
  return connectorFromCheckName(`${check?.name || ''} ${check?.detail || ''}`);
}

function getConfiguredConnectorKeys(payload) {
  const connectorSetup = Array.isArray(payload?.connectorSetup) ? payload.connectorSetup : [];
  return new Set(
    connectorSetup
      .filter((entry) => entry?.ok)
      .map((entry) => entry.connector)
      .filter(Boolean),
  );
}

function getPassingConnectorKeys(payload, failedConnectors = new Set()) {
  const checks = Array.isArray(payload?.checks) ? payload.checks : [];
  const configuredConnectors = getConfiguredConnectorKeys(payload);
  const passing = new Set<ConnectorKey>();
  for (const check of checks) {
    if (check?.status !== 'pass') continue;
    const connector = checkConnectorKey(check);
    if (!connector || failedConnectors.has(connector)) continue;
    if (configuredConnectors.size > 0 && !configuredConnectors.has(connector)) continue;
    passing.add(connector);
  }
  return orderConnectors([...passing]);
}

function summarizeFailureReason(detail) {
  const text = String(detail || '').replace(/\s+/g, ' ').trim();
  if (/token has been revoked/i.test(text)) return 'token has been revoked';
  if (/unauthorized|UNAUTHORIZED/i.test(text)) return 'token is unauthorized';
  if (/Sentry API 404|Not Found/i.test(text)) return 'API returned 404 Not Found';
  if (/project\.githubRepo is missing/i.test(text)) return 'GitHub repo is not configured';
  if (/missing/i.test(text)) return text;
  return cleanHealthDetail(text);
}

function summarizeFailureFix(connector, blockers) {
  const combined = blockers.map((blocker) => `${blocker.check || ''} ${blocker.detail || ''}`).join('\n');
  if (connector === 'analytics') {
    if (/revoked|unauthorized|UNAUTHORIZED/i.test(combined)) {
      return 'Paste a fresh AnalyticsCLI readonly CLI token in the wizard, then let setup retest.';
    }
    return 'Verify the AnalyticsCLI token can list accessible projects. Per-project query failures are reported as warnings and should not block connector setup.';
  }
  if (connector === 'sentry') {
    if (/404|Not Found/i.test(combined)) {
      return 'Rerun Sentry/GlitchTip setup and use the correct base URL + visible org. Project scope stays unpinned and is resolved from app context later.';
    }
    return 'Verify the Sentry/GlitchTip token, base URL, and org, then rerun setup.';
  }
  if (connector === 'github') {
    return 'Verify the GitHub token. Repo scope is inferred from OPENCLAW_GITHUB_REPO, the local git remote, or runtime context.';
  }
  if (connector === 'revenuecat') {
    return 'Paste a RevenueCat v2 secret API key with read-only project permissions, then rerun setup.';
  }
  if (connector === 'asc') {
    return 'Rerun ASC setup and verify ASC credentials, key role access, and `asc apps list --output json`.';
  }
  return blockers.find((blocker) => blocker.remediation)?.remediation || 'Fix the failing configuration and rerun setup.';
}

function connectorForBlocker(blocker) {
  return connectorFromCheckName(`${blocker?.check || ''} ${blocker?.detail || ''}`) || 'setup';
}

function groupBlockersByConnector(blockers, focusConnectors = null) {
  const groups = new Map<ConnectorKey | 'setup', any[]>();
  const focus = focusConnectors ? new Set(focusConnectors) : null;
  for (const blocker of blockers) {
    if (isDeferredGitHubFailure(blocker)) continue;
    const connector = connectorForBlocker(blocker);
    if (focus && !focus.has(connector)) continue;
    const entries = groups.get(connector) || [];
    entries.push(blocker);
    groups.set(connector, entries);
  }
  return groups;
}

function printDeferredSetupNotes(blockers, focusConnectors = null) {
  const focus = focusConnectors ? new Set(focusConnectors) : null;
  const deferredGitHub = blockers.some((blocker) => isDeferredGitHubFailure(blocker));
  if (!deferredGitHub || (focus && !focus.has('github'))) return;
  process.stdout.write('\nDeferred / optional:\n');
  process.stdout.write('- GitHub: repo is not configured. This is only needed for GitHub issue/PR delivery.\n');
}

function printConciseSetupBlockers(payload, command, options: Record<string, any> = {}) {
  const blockers = Array.isArray(payload?.blockers) ? payload.blockers : [];
  const focusConnectors = Array.isArray(options.focusConnectors) ? options.focusConnectors : null;
  const groups = groupBlockersByConnector(blockers, focusConnectors);
  const failedConnectors = new Set([...groups.keys()].filter((key) => key !== 'setup'));
  let passingConnectors = getPassingConnectorKeys(payload, failedConnectors);
  if (focusConnectors) {
    const focus = new Set(focusConnectors);
    passingConnectors = passingConnectors.filter((connector) => focus.has(connector));
  }

  if (passingConnectors.length > 0) {
    process.stdout.write(`Live checks passed: ${passingConnectors.map(connectorTitle).join(', ')}.\n`);
  }

  if (groups.size > 0) {
    process.stdout.write('\nNeeds fix:\n');
    for (const [connector, connectorBlockers] of groups.entries()) {
      const primary = connectorBlockers[0] || {};
      const reasons = [
        ...new Set(
          connectorBlockers
            .map((blocker) => summarizeFailureReason(blocker.detail || blocker.check))
            .filter(Boolean),
        ),
      ];
      process.stdout.write(`- ${connectorTitle(connector)}: ${summarizeFailureReason(primary.detail || primary.check)}\n`);
      process.stdout.write(`  Why: ${reasons.join('; ')}.\n`);
      process.stdout.write(`  Fix: ${summarizeFailureFix(connector, connectorBlockers)}\n`);
    }
  }

  printDeferredSetupNotes(blockers, focusConnectors);
  if (groups.size > 0 || !options.hideRerunWhenClean) {
    process.stdout.write(`\nRerun: ${command}\n`);
  }
}

function payloadHasConnectorFailures(payload, connector) {
  const blockers = Array.isArray(payload?.blockers) ? payload.blockers : [];
  return blockers.some((blocker) => !isDeferredGitHubFailure(blocker) && connectorForBlocker(blocker) === connector);
}

async function askListSelection(rl, label, entries, options: Record<string, any> = {}) {
  const includeManual = Boolean(options.includeManual);
  const includeDefer = Boolean(options.includeDefer);
  entries.forEach((entry, index) => {
    const description = entry.description ? ` - ${entry.description}` : '';
    process.stdout.write(`  ${index + 1}) ${entry.label}${description}\n`);
  });
  const manualIndex = includeManual ? entries.length + 1 : null;
  const deferIndex = includeDefer ? entries.length + (includeManual ? 2 : 1) : null;
  if (manualIndex) process.stdout.write(`  ${manualIndex}) Enter manually\n`);
  if (deferIndex) process.stdout.write(`  ${deferIndex}) Defer\n`);

  while (true) {
    const answer = (await ask(rl, label, entries.length === 1 ? '1' : '')).trim();
    const numericIndex = Number.parseInt(answer, 10);
    if (Number.isInteger(numericIndex)) {
      if (numericIndex >= 1 && numericIndex <= entries.length) return entries[numericIndex - 1].value;
      if (manualIndex && numericIndex === manualIndex) return '__manual__';
      if (deferIndex && numericIndex === deferIndex) return '';
    }
    const matchingEntry = entries.find((entry) =>
      [entry.value, entry.label].some((value) => String(value || '').toLowerCase() === answer.toLowerCase()),
    );
    if (matchingEntry) return matchingEntry.value;
    process.stdout.write('Choose one of the listed numbers.\n');
  }
}

function printSetupFailure({ result, payload, command }) {
  process.stdout.write('\nFAILED: Connector setup needs attention.\n');
  printConnectorSetupProgress(payload);

  const blockers = Array.isArray(payload?.blockers) ? payload.blockers : [];
  if (blockers.length > 0) {
    printConciseSetupBlockers(payload, command);
    return;
  }

  const reason = result.code === null ? 'setup command did not report an exit code' : `setup command exited with code ${result.code}`;
  process.stdout.write(`Reason: ${reason}.\n`);
  const output = truncate(result.stderr || result.stdout);
  if (output) {
    process.stdout.write(`Details: ${output}\n`);
  }
  process.stdout.write(`Run manually for full output: ${command}\n`);
}

function printSetupSuccess(payload) {
  process.stdout.write('\nSUCCESS: Connector setup finished.\n');
  printConnectorSetupProgress(payload);
  if (payload?.message) {
    process.stdout.write(`${payload.message}\n`);
  }
}

function connectorFromCheckName(name) {
  const value = String(name || '');
  if (value.includes('analytics') || value.includes('ANALYTICSCLI')) return 'analytics';
  if (value.includes('github') || value.includes('GITHUB')) return 'github';
  if (value.includes('revenuecat') || value.includes('REVENUECAT')) return 'revenuecat';
  if (value.includes('sentry') || value.includes('SENTRY') || value.includes('GLITCHTIP')) return 'sentry';
  if (value.includes('asc') || value.includes('ASC_')) return 'asc';
  return null;
}

function connectorTitle(key) {
  return CONNECTOR_DEFINITIONS.find((connector) => connector.key === key)?.label || key || 'General setup';
}

function compactJsonError(value) {
  const text = String(value || '');
  const jsonStart = text.indexOf('{"error"');
  if (jsonStart < 0) return '';
  try {
    const payload = JSON.parse(text.slice(jsonStart).replace(/\)+\s*$/g, '').trim());
    const error = payload?.error || payload;
    const parts = [
      error.code ? `code=${error.code}` : '',
      error.message ? `message=${error.message}` : '',
      error.details?.reason ? `reason=${error.details.reason}` : '',
      error.details?.upgradeUrl ? `upgradeUrl=${error.details.upgradeUrl}` : '',
    ].filter(Boolean);
    return parts.join(', ');
  } catch {
    return '';
  }
}

function cleanHealthDetail(detail) {
  const raw = String(detail || '').replace(/\s+/g, ' ').trim();
  const compactError = compactJsonError(raw);

  if (/project\.githubRepo is required/i.test(raw)) {
    return 'No GitHub repo is configured yet. This is optional unless you want GitHub issue/PR delivery now.';
  }
  if (/project\.githubRepo is missing/i.test(raw)) {
    return 'GitHub repo access test is deferred until a repo is known.';
  }
  if (/invalid token|unauthorized|token has been revoked/i.test(raw)) {
    return `AnalyticsCLI token is invalid${compactError ? ` (${compactError})` : ''}.`;
  }
  if (/No Sentry projects configured/i.test(raw)) {
    return 'Sentry project scope is deferred; the AI can discover visible projects from org + token.';
  }
  if (/smoke test failed/i.test(raw)) {
    const withoutWrappedJson = raw.replace(/\{"error".*$/, '').replace(/\s*\(+\s*$/, '').trim();
    return withoutWrappedJson || raw;
  }
  return truncate(raw, 180);
}

function isDeferredGitHubFailure(failure) {
  const name = String(failure?.name || '');
  const detail = String(failure?.detail || '');
  return (
    name === 'project:github-repo' ||
    (name === 'connection:github' && /project\.githubRepo|repo is missing|repo is not configured/i.test(detail))
  );
}

function healthStatusLabel(status) {
  if (status === 'running') return 'running';
  if (status === 'pass') return 'done';
  if (status === 'warn') return 'needs attention';
  if (status === 'fail') return 'needs attention';
  if (status === 'deferred') return 'deferred';
  return 'pending';
}

function renderHealthProgress(items, message = 'Live checks running...', title = 'Health check') {
  if (process.stdout.isTTY) clearTerminal();
  const finished = items.filter((item) => !['pending', 'running'].includes(String(item.status || ''))).length;
  process.stdout.write(`${title}\n`);
  process.stdout.write('------------\n');
  process.stdout.write(`${message}\n\n`);
  process.stdout.write(`${finished}/${items.length} checks finished.\n\n`);
  for (const item of items) {
    process.stdout.write(`[${healthStatusLabel(item.status)}] ${item.label}: ${item.detail}\n`);
  }
}

function updateHealthProgress(items, event) {
  const key = String(event?.key || '');
  const item = items.find((entry) => entry.key === key);
  if (!item) return false;
  if (event.phase === 'start') {
    item.status = 'running';
    if (event.detail) item.detail = String(event.detail);
    if (event.label) item.label = String(event.label);
    return true;
  }
  if (event.phase === 'finish') {
    item.status = event.status || 'pass';
    if (event.detail) item.detail = String(event.detail);
    if (event.label) item.label = String(event.label);
    return true;
  }
  return false;
}

function buildSetupTestProgressPlan(selected: ConnectorKey[]) {
  const selectedSet = new Set(selected);
  const items = [
    {
      key: 'connectorSetup',
      label: 'Connector helpers',
      detail: 'waiting to install and enable selected helpers',
      status: 'pending',
    },
    {
      key: 'analyticsProject',
      label: 'AnalyticsCLI scope',
      detail: 'waiting to check accessible analytics projects',
      status: 'pending',
    },
  ];
  if (selectedSet.has('asc')) {
    items.push({
      key: 'ascApp',
      label: 'ASC app scope',
      detail: 'waiting to resolve App Store Connect app scope',
      status: 'pending',
    });
  }
  items.push({
    key: 'preflight',
    label: 'Local preflight',
    detail: 'waiting to validate config, dependencies, and source wiring',
    status: 'pending',
  });
  if (selectedSet.has('analytics')) {
    items.push({ key: 'analytics', label: 'AnalyticsCLI', detail: 'waiting for token auth + readonly query', status: 'pending' });
  }
  if (selectedSet.has('sentry')) {
    items.push({ key: 'sentry', label: 'Sentry / GlitchTip', detail: 'waiting for token/org API + project discovery', status: 'pending' });
  }
  if (selectedSet.has('revenuecat')) {
    items.push({ key: 'revenuecat', label: 'RevenueCat', detail: 'waiting for API key auth + project read', status: 'pending' });
  }
  if (selectedSet.has('github')) {
    items.push({ key: 'github', label: 'GitHub', detail: 'waiting for repo/token access check', status: 'pending' });
  }
  items.push({
    key: 'finalize',
    label: 'Finalizing result',
    detail: 'waiting for command output, parsing, and follow-up checks',
    status: 'pending',
  });
  return items;
}

function primaryProgressItemsFinished(items) {
  return items
    .filter((item) => item.key !== 'finalize')
    .every((item) => !['pending', 'running'].includes(String(item.status || '')));
}

function updateProgressItem(items, key, status, detail) {
  const item = items.find((entry) => entry.key === key);
  if (!item) return;
  item.status = status;
  if (detail) item.detail = detail;
}

async function runSetupCommandWithProgress(command, env, selected: ConnectorKey[], message) {
  const plan = buildSetupTestProgressPlan(selected);
  renderHealthProgress(plan, `${message}\nDo not close this terminal yet.`, 'Connector setup test');
  const progressCommand = command.includes('--progress-json') ? command : `${command} --progress-json`;
  const result = await runCommandCaptureWithProgress(progressCommand, (event) => {
    if (updateHealthProgress(plan, event)) {
      const primaryFinished = primaryProgressItemsFinished(plan);
      if (primaryFinished) {
        updateProgressItem(plan, 'finalize', 'running', 'command still running; parsing final output and follow-up work');
      }
      const message = primaryFinished
        ? 'Checks finished. Finalizing result; do not close this terminal yet.'
        : 'Connector setup test is still running. Do not close this terminal yet.';
      renderHealthProgress(plan, message, 'Connector setup test');
    }
  }, { env, timeoutMs: 180_000 });
  updateProgressItem(plan, 'finalize', 'pass', 'result received');
  renderHealthProgress(plan, 'Connector setup test finished.', 'Connector setup test');
  return result;
}

async function saveSecretsImmediately(secrets: Record<string, string>) {
  if (Object.keys(secrets).length === 0) return false;
  const secretsFile = resolveSecretsFile();
  await writeSecretsFile(secretsFile, secrets);
  Object.assign(process.env, secrets);
  process.stdout.write(`Saved local secrets to ${secretsFile} with chmod 600.\n`);
  return true;
}

async function runImmediateConnectorHealthCheck({
  rl,
  configPath,
  connector,
  secrets,
  sentryAccounts = [],
}) {
  if (connector === 'sentry' && sentryAccounts.length > 0) {
    await upsertSentryAccountsConfig(configPath, sentryAccounts);
  }
  await saveSecretsImmediately(secrets);

  const env = {
    ...process.env,
    ...secrets,
  };
  const command = `node scripts/openclaw-growth-start.mjs --config ${quote(configPath)} --setup-only --connectors ${quote(connector)} --only-connectors ${quote(connector)}`;
  let result = await runSetupCommandWithProgress(
    command,
    env,
    [connector],
    `Checking ${connectorLabel(connector)} immediately after setup...`,
  );
  let payload = parseJsonFromStdout(result.stdout);

  if (connector === 'asc') {
    try {
      const ascWebAuthChanged = await ensureAscWebAnalyticsAuth(rl, secrets);
      if (ascWebAuthChanged) {
        result = await runSetupCommandWithProgress(
          command,
          env,
          [connector],
          'Retesting ASC after web analytics login...',
        );
        payload = parseJsonFromStdout(result.stdout);
      }
    } catch (error) {
      process.stdout.write(
        `ASC web analytics still needs attention: ${error instanceof Error ? error.message : String(error)}\n`,
      );
    }
  }

  if (payloadHasConnectorFailures(payload, connector)) {
    process.stdout.write(`\n${connectorLabel(connector)} needs attention before continuing.\n`);
    printConciseSetupBlockers(payload, command, {
      focusConnectors: [connector],
      hideRerunWhenClean: true,
    });
    const retry = await askYesNo(rl, `Re-enter ${connectorLabel(connector)} configuration now?`, true);
    return { ok: false, retry, result, payload };
  }

  process.stdout.write(`\n${connectorLabel(connector)} immediate health check passed or is only waiting on optional/deferred context.\n`);
  return { ok: true, retry: false, result, payload };
}

function getUserLocalBinDir() {
  return process.env.HOME ? path.join(process.env.HOME, '.local', 'bin') : null;
}

function prependPath(dir: string) {
  const current = process.env.PATH || '';
  if (!current.split(':').includes(dir)) {
    process.env.PATH = `${dir}:${current}`;
  }
}

function getGitHubCliReleaseAssetName(version: string) {
  const arch = process.arch === 'x64' ? 'amd64' : process.arch === 'arm64' ? 'arm64' : '';
  if (process.platform === 'linux' && arch) {
    return `gh_${version}_linux_${arch}.tar.gz`;
  }
  return null;
}

async function resolveGitHubCliReleaseAssetUrl() {
  const response = await fetch('https://api.github.com/repos/cli/cli/releases/latest', {
    headers: {
      Accept: 'application/vnd.github+json',
      'User-Agent': 'openclaw-growth-wizard',
    },
  });
  if (!response.ok) {
    throw new Error(`GitHub CLI release lookup failed (${response.status})`);
  }
  const release = await response.json() as {
    tag_name?: string;
    assets?: Array<{ name?: string; browser_download_url?: string }>;
  };
  const version = String(release.tag_name || '').replace(/^v/, '');
  const assetName = getGitHubCliReleaseAssetName(version);
  if (!assetName) {
    throw new Error(`No user-local gh installer is defined for ${process.platform}/${process.arch}`);
  }
  const asset = release.assets?.find((entry) => entry.name === assetName);
  if (!asset?.browser_download_url) {
    throw new Error(`GitHub CLI release asset not found: ${assetName}`);
  }
  return asset.browser_download_url;
}

async function installGitHubCliUserLocal() {
  const binDir = getUserLocalBinDir();
  if (!binDir) {
    process.stdout.write('Cannot install gh automatically because HOME is not set.\n');
    return false;
  }
  if (!(await commandExists('curl'))) {
    process.stdout.write('Cannot install gh automatically because curl is not available.\n');
    return false;
  }
  if (!(await commandExists('tar'))) {
    process.stdout.write('Cannot install gh automatically because tar is not available.\n');
    return false;
  }

  try {
    const url = await resolveGitHubCliReleaseAssetUrl();
    const cacheDir = process.env.HOME
      ? path.join(process.env.HOME, '.cache', 'openclaw-gh')
      : path.join(process.cwd(), '.openclaw-gh-cache');
    const command = [
      'set -eu',
      `mkdir -p ${quote(binDir)} ${quote(cacheDir)}`,
      `tmp="$(mktemp -d ${quote(path.join(cacheDir, 'gh.XXXXXX'))})"`,
      'trap \'rm -rf "$tmp"\' EXIT',
      `curl -fsSL ${quote(url)} -o "$tmp/gh.tar.gz"`,
      'tar -xzf "$tmp/gh.tar.gz" -C "$tmp"',
      'gh_bin="$(find "$tmp" -path "*/bin/gh" -type f | head -n 1)"',
      'test -n "$gh_bin"',
      `cp "$gh_bin" ${quote(path.join(binDir, 'gh'))}`,
      `chmod 755 ${quote(path.join(binDir, 'gh'))}`,
      'for profile in "$HOME/.profile" "$HOME/.bashrc" "$HOME/.bash_profile" "$HOME/.zshrc" "$HOME/.zprofile"; do touch "$profile"; grep -Fq \'export PATH="$HOME/.local/bin:$PATH"\' "$profile" || printf \'\\n# OpenClaw user-local bin\\nexport PATH="$HOME/.local/bin:$PATH"\\n\' >> "$profile"; done',
    ].join(' && ');
    process.stdout.write(`Installing GitHub CLI locally into ${binDir}/gh...\n`);
    const code = await runInteractiveCommand(command);
    prependPath(binDir);
    return code === 0 && await commandExists('gh');
  } catch (error) {
    process.stdout.write(`Automatic gh install failed: ${error instanceof Error ? error.message : String(error)}\n`);
    return false;
  }
}

function parseGitHubRepoFromRemote(remoteUrl) {
  const value = String(remoteUrl || '').trim();
  if (!value) return null;

  const sshMatch = value.match(/^git@github\.com:([^/]+\/[^/]+?)(?:\.git)?$/i);
  if (sshMatch) return sshMatch[1];

  const httpsMatch = value.match(/^https?:\/\/github\.com\/([^/]+\/[^/]+?)(?:\.git)?$/i);
  if (httpsMatch) return httpsMatch[1];

  return null;
}

function isConfiguredGitHubRepo(value) {
  const repo = String(value || '').trim();
  return Boolean(repo && repo !== 'owner/repo' && /^[^/\s]+\/[^/\s]+$/.test(repo));
}

async function detectGitHubRepo() {
  const explicit = String(process.env.OPENCLAW_GITHUB_REPO || '').trim();
  if (isConfiguredGitHubRepo(explicit)) return explicit;

  const remoteResult = await runCommandCapture('git config --get remote.origin.url');
  if (!remoteResult.ok) return null;
  return parseGitHubRepoFromRemote(remoteResult.stdout);
}

function resolveSecretsFile() {
  const explicit = process.env.OPENCLAW_GROWTH_SECRETS_FILE?.trim();
  if (explicit) return path.resolve(explicit);
  if (process.env.HOME) return path.join(process.env.HOME, '.config', 'openclaw-growth', 'secrets.env');
  return path.resolve('.openclaw-growth-secrets.env');
}

function resolveAscPrivateKeyPath(keyId: string) {
  const safeKeyId = (keyId || 'OPENCLAW').trim().replace(/[^a-zA-Z0-9_-]/g, '_') || 'OPENCLAW';
  const baseDir = process.env.HOME
    ? path.join(process.env.HOME, '.config', 'openclaw-growth')
    : path.resolve('.openclaw-growth');
  return path.join(baseDir, `AuthKey_${safeKeyId}.p8`);
}

function renderEnvValue(value) {
  return `"${String(value).replace(/\\/g, '\\\\').replace(/"/g, '\\"').replace(/\$/g, '\\$')}"`;
}

async function readSecretsFile(filePath) {
  const values = new Map<string, string>();
  let raw = '';
  try {
    raw = await fs.readFile(filePath, 'utf8');
  } catch {
    return values;
  }
  for (const line of raw.split(/\r?\n/)) {
    const match = line.match(/^\s*(?:export\s+)?([A-Z0-9_]+)=(.*)\s*$/);
    if (!match) continue;
    values.set(match[1], match[2].replace(/^"|"$/g, ''));
  }
  return values;
}

async function writeSecretsFile(filePath, nextValues: Record<string, string>) {
  const current = await readSecretsFile(filePath);
  for (const [key, value] of Object.entries(nextValues)) {
    if (value.trim()) current.set(key, value.trim());
  }
  await fs.mkdir(path.dirname(filePath), { recursive: true, mode: 0o700 });
  const lines = [
    '# OpenClaw Growth local secrets.',
    '# This file is generated by openclaw-growth-wizard.mjs and should not be committed.',
    ...[...current.entries()].sort(([a], [b]) => a.localeCompare(b)).map(([key, value]) => `export ${key}=${renderEnvValue(value)}`),
    '',
  ];
  await fs.writeFile(filePath, lines.join('\n'), { encoding: 'utf8', mode: 0o600 });
  await fs.chmod(filePath, 0o600);
}

function renderBashSingleQuoted(value) {
  return `'${String(value).replace(/'/g, `'\\''`)}'`;
}

function renderIsolatedSecretRunnerInstallScript({
  workspaceRoot,
  configPath,
  serviceUser,
  agentUser,
}) {
  const workspaceLiteral = renderBashSingleQuoted(workspaceRoot);
  const configLiteral = renderBashSingleQuoted(path.relative(workspaceRoot, configPath) || configPath);
  const serviceUserLiteral = renderBashSingleQuoted(serviceUser);
  const agentUserLiteral = renderBashSingleQuoted(agentUser);
  return `#!/usr/bin/env bash
set -euo pipefail

SERVICE_USER=\${OPENCLAW_GROWTH_SERVICE_USER:-${serviceUserLiteral}}
AGENT_USER=\${OPENCLAW_GROWTH_AGENT_USER:-${agentUserLiteral}}
WORKSPACE=${workspaceLiteral}
CONFIG_PATH=\${OPENCLAW_GROWTH_CONFIG_PATH:-${configLiteral}}
STATE_PATH=\${OPENCLAW_GROWTH_STATE_PATH:-data/openclaw-growth-engineer/state.json}
RUNTIME_DIR=/var/lib/openclaw-growth
SECRETS_FILE="\${RUNTIME_DIR}/secrets.env"
LOCAL_SECRETS_FILE="\${OPENCLAW_GROWTH_LOCAL_SECRETS_FILE:-\${HOME}/.config/openclaw-growth/secrets.env}"
SUDOERS_FILE=/etc/sudoers.d/openclaw-growth

if [ "$(id -u)" -ne 0 ]; then
  echo "Run with sudo: sudo bash .openclaw/secret-runner/install.sh" >&2
  exit 1
fi

if ! id "$SERVICE_USER" >/dev/null 2>&1; then
  if command -v useradd >/dev/null 2>&1; then
    useradd --system --create-home --home-dir "$RUNTIME_DIR" --shell /usr/sbin/nologin "$SERVICE_USER"
  elif command -v dscl >/dev/null 2>&1; then
    echo "macOS service-user creation is not automated by this script. Create $SERVICE_USER manually or use launchd/keychain." >&2
    exit 1
  else
    echo "No supported user creation tool found." >&2
    exit 1
  fi
fi

install -d -m 0750 -o "$SERVICE_USER" -g "$SERVICE_USER" "$RUNTIME_DIR"
install -d -m 0750 -o "$SERVICE_USER" -g "$SERVICE_USER" "$RUNTIME_DIR/keys"
install -d -m 0775 -o "$AGENT_USER" -g "$SERVICE_USER" "$WORKSPACE/data/openclaw-growth-engineer" "$WORKSPACE/.openclaw"
chmod g+rwX "$WORKSPACE/data/openclaw-growth-engineer" "$WORKSPACE/.openclaw"

if [ ! -f "$SECRETS_FILE" ]; then
  install -m 0600 -o "$SERVICE_USER" -g "$SERVICE_USER" /dev/null "$SECRETS_FILE"
fi

if [ -s "$LOCAL_SECRETS_FILE" ] && [ ! -s "$SECRETS_FILE" ]; then
  cp "$LOCAL_SECRETS_FILE" "$SECRETS_FILE"
  chown "$SERVICE_USER:$SERVICE_USER" "$SECRETS_FILE"
  chmod 0600 "$SECRETS_FILE"
  echo "Migrated existing local secrets into $SECRETS_FILE."
  echo "After verifying the isolated runner, delete the old local file if OpenClaw runs as that same user:"
  echo "  rm -f $LOCAL_SECRETS_FILE"
fi

cat >/usr/local/bin/openclaw-growth-health <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
WORKSPACE="\${OPENCLAW_GROWTH_WORKSPACE:-__WORKSPACE__}"
CONFIG_PATH="\${OPENCLAW_GROWTH_CONFIG_PATH:-__CONFIG_PATH__}"
cd "$WORKSPACE"
export OPENCLAW_GROWTH_SECRETS_FILE="\${OPENCLAW_GROWTH_SECRETS_FILE:-/var/lib/openclaw-growth/secrets.env}"
exec node scripts/openclaw-growth-status.mjs --config "$CONFIG_PATH" --timeout-ms "\${OPENCLAW_GROWTH_STATUS_TIMEOUT_MS:-15000}" --json "$@"
EOF
sed -i.bak "s#__WORKSPACE__#$WORKSPACE#g; s#__CONFIG_PATH__#$CONFIG_PATH#g" /usr/local/bin/openclaw-growth-health
rm -f /usr/local/bin/openclaw-growth-health.bak

cat >/usr/local/bin/openclaw-growth-run <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
WORKSPACE="\${OPENCLAW_GROWTH_WORKSPACE:-__WORKSPACE__}"
CONFIG_PATH="\${OPENCLAW_GROWTH_CONFIG_PATH:-__CONFIG_PATH__}"
STATE_PATH="\${OPENCLAW_GROWTH_STATE_PATH:-data/openclaw-growth-engineer/state.json}"
cd "$WORKSPACE"
export OPENCLAW_GROWTH_SECRETS_FILE="\${OPENCLAW_GROWTH_SECRETS_FILE:-/var/lib/openclaw-growth/secrets.env}"
exec node scripts/openclaw-growth-runner.mjs --config "$CONFIG_PATH" --state "$STATE_PATH" "$@"
EOF
sed -i.bak "s#__WORKSPACE__#$WORKSPACE#g; s#__CONFIG_PATH__#$CONFIG_PATH#g" /usr/local/bin/openclaw-growth-run
rm -f /usr/local/bin/openclaw-growth-run.bak

chown root:root /usr/local/bin/openclaw-growth-health /usr/local/bin/openclaw-growth-run
chmod 0755 /usr/local/bin/openclaw-growth-health /usr/local/bin/openclaw-growth-run

cat >"$SUDOERS_FILE" <<EOF
# OpenClaw Growth isolated secret runner.
# Allows the agent user to run only the sanitized Growth Engineer wrappers as the secret-owning service user.
$AGENT_USER ALL=($SERVICE_USER) NOPASSWD: /usr/local/bin/openclaw-growth-health
$AGENT_USER ALL=($SERVICE_USER) NOPASSWD: /usr/local/bin/openclaw-growth-run
EOF
chmod 0440 "$SUDOERS_FILE"
if command -v visudo >/dev/null 2>&1; then
  visudo -cf "$SUDOERS_FILE"
fi

echo "Installed isolated OpenClaw Growth secret runner."
echo "Persisted secret file: $SECRETS_FILE"
echo "Edit secrets as root/service operator only:"
echo "  sudoedit $SECRETS_FILE"
echo "OpenClaw may run:"
echo "  sudo -n -u $SERVICE_USER /usr/local/bin/openclaw-growth-health"
echo "  sudo -n -u $SERVICE_USER /usr/local/bin/openclaw-growth-run"
`;
}

async function writeIsolatedSecretRunnerKit(configPath, config, options: Record<string, any> = {}) {
  const serviceUser = String(options.serviceUser || config?.security?.connectorSecrets?.serviceUser || 'openclaw-growth');
  const agentUser = String(
    options.agentUser ||
      config?.security?.connectorSecrets?.agentUser ||
      process.env.SUDO_USER ||
      process.env.USER ||
      'openclaw',
  );
  const kitDir = path.resolve('.openclaw/secret-runner');
  const installScriptPath = path.join(kitDir, 'install.sh');
  const readmePath = path.join(kitDir, 'README.md');
  await fs.mkdir(kitDir, { recursive: true });
  await fs.writeFile(
    installScriptPath,
    renderIsolatedSecretRunnerInstallScript({
      workspaceRoot: process.cwd(),
      configPath,
      serviceUser,
      agentUser,
    }),
    { encoding: 'utf8', mode: 0o700 },
  );
  await fs.chmod(installScriptPath, 0o700);
  await fs.writeFile(
    readmePath,
    [
      '# OpenClaw Growth Isolated Secret Runner',
      '',
      'This kit keeps connector API keys out of the OpenClaw-readable workspace.',
      '',
      '1. Run `sudo bash .openclaw/secret-runner/install.sh` from this workspace.',
      '2. Put connector secrets in `/var/lib/openclaw-growth/secrets.env` with `sudoedit`.',
      '3. Configure OpenClaw/heartbeat jobs to use the generated sudo commands.',
      '',
      'OpenClaw can read and modify non-secret connector config, but must not read or write API keys.',
      '',
    ].join('\n'),
    'utf8',
  );

  config.security = {
    ...(config.security || {}),
    connectorSecrets: {
      mode: 'isolated-runner',
      persisted: true,
      agentReadable: false,
      serviceUser,
      agentUser,
      secretsFile: '/var/lib/openclaw-growth/secrets.env',
      installScript: path.relative(process.cwd(), installScriptPath),
      healthCommand: `sudo -n -u ${serviceUser} /usr/local/bin/openclaw-growth-health`,
      runCommand: `sudo -n -u ${serviceUser} /usr/local/bin/openclaw-growth-run`,
    },
  };
  return { installScriptPath, readmePath, serviceUser };
}

async function askSecretAccessModel(rl, configPath, config) {
  if (!ENABLE_ISOLATED_SECRET_RUNNER_WIZARD) {
    config.security = {
      ...(config.security || {}),
      connectorSecrets: {
        ...(config.security?.connectorSecrets || {}),
        mode: 'openclaw-secret-refs',
        persisted: true,
        agentReadable: 'runtime_resolves_secret_refs',
        secretsFile: resolveSecretsFile(),
      },
    };
    return { config, kit: null };
  }

  process.stdout.write('\nSecret access model\n');
  process.stdout.write('  1) Local user secrets file: simplest, same OS user can read it\n');
  process.stdout.write('  2) Isolated secret runner: separate service user owns persisted secrets; OpenClaw only gets allowlisted run/health commands\n');
  const currentMode = config?.security?.connectorSecrets?.mode === 'isolated-runner' ? '2' : '1';
  const answer = await ask(rl, 'Secret access model (1/2)', currentMode);
  if (answer.trim() !== '2') {
    config.security = {
      ...(config.security || {}),
      connectorSecrets: {
        ...(config.security?.connectorSecrets || {}),
        mode: 'local-user-file',
        persisted: true,
        agentReadable: 'same-os-user-can-read',
        secretsFile: resolveSecretsFile(),
      },
    };
    return { config, kit: null };
  }

  const serviceUser = await ask(
    rl,
    'Service user that owns connector secrets',
    config?.security?.connectorSecrets?.serviceUser || 'openclaw-growth',
  );
  const agentUser = await ask(
    rl,
    'Agent OS user allowed to run health/growth commands',
    config?.security?.connectorSecrets?.agentUser || process.env.SUDO_USER || process.env.USER || 'openclaw',
  );
  const kit = await writeIsolatedSecretRunnerKit(configPath, config, { serviceUser, agentUser });
  return { config, kit };
}

function printSecretRunnerKitInstructions(kit) {
  if (!kit) return;
  process.stdout.write(`Saved isolated secret runner setup: ${kit.installScriptPath}\n`);
  process.stdout.write('Run once from this workspace after the wizard finishes:\n');
  process.stdout.write(`  sudo bash ${path.relative(process.cwd(), kit.installScriptPath)}\n`);
  process.stdout.write('Then move/persist connector secrets under /var/lib/openclaw-growth/secrets.env with sudoedit.\n');
}

function getGrowthRunCommand(config, displayConfigPath) {
  if (config?.security?.connectorSecrets?.mode === 'isolated-runner' && config.security.connectorSecrets.runCommand) {
    return config.security.connectorSecrets.runCommand;
  }
  return `node scripts/openclaw-growth-runner.mjs --config ${displayConfigPath}`;
}

function getConnectorHealthCommand(config, displayConfigPath) {
  if (config?.security?.connectorSecrets?.mode === 'isolated-runner' && config.security.connectorSecrets.healthCommand) {
    return config.security.connectorSecrets.healthCommand;
  }
  return `node scripts/openclaw-growth-runner.mjs --config ${displayConfigPath}`;
}

async function maybePromptSecret(rl, label, envName) {
  const existing = process.env[envName]?.trim();
  const suffix = existing ? 'already set in current environment; press Enter to keep' : 'leave empty to skip';
  const value = await ask(rl, `${label} (${suffix})`, '');
  const trimmed = value.trim();
  if (trimmed) return trimmed;
  if (existing) {
    process.stdout.write(`Keeping existing ${envName} from the local environment.\n`);
    return existing;
  }
  return '';
}

function defaultSentryTokenEnv({ index, label, baseUrl }) {
  const value = `${label || ''} ${baseUrl || ''}`.toLowerCase();
  if (index === 0 && !value.includes('glitchtip')) return 'SENTRY_AUTH_TOKEN';
  if (value.includes('glitchtip')) return 'GLITCHTIP_AUTH_TOKEN';
  return `${toEnvName(label || `SENTRY_${index + 1}`, `SENTRY_${index + 1}`)}_AUTH_TOKEN`;
}

function defaultSentryAccountLabel({ index, baseUrl }) {
  const value = String(baseUrl || '').toLowerCase();
  if (value.includes('glitchtip')) return 'GlitchTip';
  if (index === 0) return 'Sentry Cloud';
  return `Sentry Account ${index + 1}`;
}

function isSentryCloudBaseUrl(baseUrl) {
  const normalized = String(baseUrl || '').trim().replace(/\/$/, '').toLowerCase();
  return normalized === 'https://sentry.io' || normalized === 'https://www.sentry.io';
}

function printSentryTokenGuidance({ baseUrl, tokenEnv }) {
  if (isSentryCloudBaseUrl(baseUrl)) {
    process.stdout.write('\nToken type: use a Sentry personal user/auth token, not an organization integration token.\n');
    process.stdout.write('Sentry token page: https://sentry.io/settings/account/api/auth-tokens/\n');
  } else {
    process.stdout.write('\nToken type: use a GlitchTip/Sentry-compatible user auth token for this host.\n');
    process.stdout.write('GlitchTip token page: Profile -> Auth Tokens on your GlitchTip instance.\n');
  }
  printBullets([
    `Paste it as ${tokenEnv}.`,
    'Required scopes: `org:read`, `team:read`, `project:read`, and `event:read`.',
    'Optional for richer release context: `project:releases`.',
  ]);
}

function buildUrl(baseUrl, pathname, params: Record<string, string | number | boolean | null | undefined> = {}) {
  const url = new URL(pathname, `${String(baseUrl || 'https://sentry.io').replace(/\/$/, '')}/`);
  for (const [key, value] of Object.entries(params)) {
    if (value === undefined || value === null || value === '') continue;
    url.searchParams.set(key, String(value));
  }
  return url;
}

function apiListItems(payload) {
  if (Array.isArray(payload)) return payload;
  if (!payload || typeof payload !== 'object') return [];
  if (Array.isArray(payload.results)) return payload.results;
  if (Array.isArray(payload.data)) return payload.data;
  if (Array.isArray(payload.projects)) return payload.projects;
  if (Array.isArray(payload.teams)) return payload.teams;
  return [];
}

async function fetchSentryJsonPage({ token, url }) {
  const normalizedToken = String(token || '').trim();
  const response = await fetch(url, {
    method: 'GET',
    headers: {
      Accept: 'application/json',
      Authorization: `Bearer ${normalizedToken}`,
      'User-Agent': 'openclaw-growth-wizard',
    },
  });
  const body = await response.text();
  if (!response.ok) {
    return {
      ok: false,
      payload: null,
      detail: `${url.pathname}: HTTP ${response.status}: ${truncate(body, 220)}`,
    };
  }
  try {
    return { ok: true, payload: body ? JSON.parse(body) : null, detail: url.pathname };
  } catch (error) {
    return {
      ok: false,
      payload: null,
      detail: `${url.pathname}: invalid JSON (${error instanceof Error ? error.message : String(error)})`,
    };
  }
}

async function fetchSentryJsonList({ baseUrl, token, url }) {
  const items = [];
  const pages = [];
  let nextUrl: URL | null = url;
  for (let page = 0; nextUrl && page < 10; page += 1) {
    const result = await fetchSentryJsonPage({ token, url: nextUrl });
    pages.push(result.detail);
    if (!result.ok) return { ...result, payload: items, detail: pages.join('; ') };
    items.push(...apiListItems(result.payload));
    const next = result.payload && typeof result.payload === 'object' ? result.payload.next : null;
    nextUrl = typeof next === 'string' && next.trim() ? new URL(next, `${String(baseUrl || 'https://sentry.io').replace(/\/$/, '')}/`) : null;
  }
  return { ok: true, payload: items, detail: pages.join('; ') };
}

async function discoverSentryOrganizations({ baseUrl, token }) {
  const normalizedToken = String(token || '').trim();
  if (!normalizedToken) return { ok: false, organizations: [], detail: 'missing token' };
  const url = buildUrl(baseUrl, '/api/0/organizations/', { per_page: 100 });
  const result = await fetchSentryJsonList({ baseUrl, token: normalizedToken, url });
  if (!result.ok) return { ok: false, organizations: [], detail: result.detail };
  const organizations: Array<{ slug: string; name: string }> = apiListItems(result.payload)
    .map((organization) => ({
      slug: String(organization?.slug || organization?.name || '').trim(),
      name: String(organization?.name || organization?.slug || '').trim(),
    }))
    .filter((organization) => organization.slug);
  return {
    ok: true,
    organizations: Array.from(new Map(organizations.map((organization) => [organization.slug, organization])).values()),
    detail: `found ${organizations.length} org(s)`,
  };
}

async function discoverSentryProjects({ baseUrl, token, org }) {
  const normalizedOrg = String(org || '').trim();
  const normalizedToken = String(token || '').trim();
  if (!normalizedOrg || !normalizedToken) {
    return { ok: false, projects: [], detail: 'missing org or token' };
  }

  const projectSlugs = (payload) =>
    apiListItems(payload)
      .map((project) => String(project?.slug || project?.name || '').trim())
      .filter(Boolean);

  const attempted = [];
  try {
    const orgProjectsUrl = buildUrl(baseUrl, `/api/0/organizations/${encodeURIComponent(normalizedOrg)}/projects/`, {
      per_page: 100,
    });
    const orgProjects = await fetchSentryJsonList({ baseUrl, token: normalizedToken, url: orgProjectsUrl });
    attempted.push(orgProjects.detail);
    if (orgProjects.ok) {
      const projects = projectSlugs(orgProjects.payload);
      if (projects.length > 0) {
        return { ok: true, projects: [...new Set(projects)], detail: `found ${projects.length} project(s)` };
      }
    }

    const teamsUrl = buildUrl(baseUrl, `/api/0/organizations/${encodeURIComponent(normalizedOrg)}/teams/`, {
      per_page: 100,
    });
    const teams = await fetchSentryJsonList({ baseUrl, token: normalizedToken, url: teamsUrl });
    attempted.push(teams.detail);
    if (teams.ok) {
      const teamSlugs = apiListItems(teams.payload)
        .map((team) => String(team?.slug || team?.name || '').trim())
        .filter(Boolean);
      const allTeamProjects = [];
      for (const teamSlug of teamSlugs) {
        const teamProjectsUrl = buildUrl(
          baseUrl,
          `/api/0/teams/${encodeURIComponent(normalizedOrg)}/${encodeURIComponent(teamSlug)}/projects/`,
          { per_page: 100 },
        );
        const teamProjects = await fetchSentryJsonList({ baseUrl, token: normalizedToken, url: teamProjectsUrl });
        attempted.push(teamProjects.detail);
        if (teamProjects.ok) allTeamProjects.push(...projectSlugs(teamProjects.payload));
      }
      if (allTeamProjects.length > 0) {
        return {
          ok: true,
          projects: [...new Set(allTeamProjects)],
          detail: `found ${allTeamProjects.length} project(s) via teams`,
        };
      }
    }

    const allProjectsUrl = buildUrl(baseUrl, '/api/0/projects/', { per_page: 100 });
    const allProjects = await fetchSentryJsonList({ baseUrl, token: normalizedToken, url: allProjectsUrl });
    attempted.push(allProjects.detail);
    if (allProjects.ok) {
      const projects = apiListItems(allProjects.payload)
        .filter((project) => {
          const projectOrg = String(project?.organization?.slug || project?.organization?.name || '').trim();
          return !projectOrg || projectOrg === normalizedOrg;
        })
        .map((project) => String(project?.slug || project?.name || '').trim())
        .filter(Boolean);
      if (projects.length > 0) {
        return { ok: true, projects: [...new Set(projects)], detail: `found ${projects.length} project(s)` };
      }
    }

    return {
      ok: false,
      projects: [],
      detail: `found 0 project(s); tried ${attempted.filter(Boolean).join('; ')}`,
    };
  } catch (error) {
    return {
      ok: false,
      projects: [],
      detail: `${error instanceof Error ? error.message : String(error)}; tried ${attempted.filter(Boolean).join('; ')}`,
    };
  }
}

async function upsertSentryAccountsConfig(configPath, accounts) {
  if (!accounts.length || !(await fileExists(configPath))) return false;
  const config = await readJsonFile(configPath);
  const existingAccounts = Array.isArray(config?.sources?.sentry?.accounts)
    ? config.sources.sentry.accounts
    : [];
  const merged = new Map();
  for (const account of existingAccounts) {
    const id = String(account?.id || account?.key || account?.label || '').trim();
    if (id) merged.set(id, account);
  }
  for (const account of accounts) {
    merged.set(account.id, {
      ...(merged.get(account.id) || {}),
      ...account,
    });
  }

  config.sources = {
    ...(config.sources || {}),
    sentry: {
      ...(config.sources?.sentry || {}),
      enabled: true,
      mode: 'command',
      command: getDefaultSourceCommand('sentry'),
      accounts: [...merged.values()],
    },
  };

  await writeJsonFile(configPath, config);
  return true;
}

const ASC_PRIVATE_KEY_BEGIN = '-----BEGIN PRIVATE KEY-----';
const ASC_PRIVATE_KEY_END = '-----END PRIVATE KEY-----';
const BRACKETED_PASTE_START = new RegExp(`${String.fromCharCode(27)}\\[200~`, 'g');
const BRACKETED_PASTE_END = new RegExp(`${String.fromCharCode(27)}\\[201~`, 'g');

function formatPemBase64(value) {
  return String(value || '').match(/.{1,64}/g)?.join('\n') || '';
}

function normalizeAscPrivateKeyContent(value) {
  const raw = String(value || '')
    .replace(BRACKETED_PASTE_START, '')
    .replace(BRACKETED_PASTE_END, '')
    .replace(/\r\n/g, '\n')
    .trim();
  if (!raw) {
    return { ok: false, value: '', error: 'No private key content pasted.' };
  }

  const beginIndex = raw.indexOf(ASC_PRIVATE_KEY_BEGIN);
  const endIndex = raw.indexOf(ASC_PRIVATE_KEY_END);
  if (beginIndex < 0 || endIndex < 0 || endIndex <= beginIndex) {
    if (raw.includes('-----BEGIN PRIVATE KEY') && beginIndex < 0) {
      return {
        ok: false,
        value: '',
        error: `Malformed .p8 header. The first line must be exactly ${ASC_PRIVATE_KEY_BEGIN}`,
      };
    }
    if (raw.includes('-----END PRIVATE KEY') && endIndex < 0) {
      return {
        ok: false,
        value: '',
        error: `Malformed .p8 footer. The last line must be exactly ${ASC_PRIVATE_KEY_END}`,
      };
    }
    return {
      ok: false,
      value: '',
      error: `Missing exact .p8 markers. Paste from ${ASC_PRIVATE_KEY_BEGIN} through ${ASC_PRIVATE_KEY_END}.`,
    };
  }

  const body = raw
    .slice(beginIndex + ASC_PRIVATE_KEY_BEGIN.length, endIndex)
    .replace(/\s+/g, '');
  if (!body) {
    return { ok: false, value: '', error: 'The .p8 key body is empty.' };
  }
  if (!/^[A-Za-z0-9+/=]+$/.test(body)) {
    return {
      ok: false,
      value: '',
      error: 'The .p8 key body contains non-base64 characters. Copy the downloaded AuthKey file content without redactions or extra text.',
    };
  }

  return {
    ok: true,
    value: `${ASC_PRIVATE_KEY_BEGIN}\n${formatPemBase64(body)}\n${ASC_PRIVATE_KEY_END}\n`,
    error: null,
  };
}

function validateAscPrivateKeyContent(value) {
  const normalized = normalizeAscPrivateKeyContent(value);
  if (!normalized.ok) return normalized;
  try {
    createPrivateKey(normalized.value);
    return normalized;
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    return {
      ok: false,
      value: '',
      error: `Invalid .p8 private key content: ${message}. Make sure you copied the downloaded AuthKey_<KEY_ID>.p8 file, including both marker lines, with no truncation.`,
    };
  }
}

async function askAscPrivateKeyContent(rl) {
  process.stdout.write(
    '\nPaste the full .p8 file content here. Leave the first line empty if you already saved the .p8 file on this host.\n',
  );
  process.stdout.write('The wizard validates the pasted key, stores it locally with chmod 600, and only saves ASC_PRIVATE_KEY_PATH.\n');

  while (true) {
    const value = await readAscPrivateKeyPaste(rl);
    if (!value.trim()) return '';
    const validation = validateAscPrivateKeyContent(value);
    if (validation.ok) return validation.value;

    process.stdout.write(`${validation.error}\n`);
    process.stdout.write('The .p8 was not saved. Paste the full file again from BEGIN to END, or leave empty to use a path.\n');
  }
}

async function readAscPrivateKeyPaste(rl) {
  return await new Promise<string>((resolve, reject) => {
    let buffer = '';
    let settled = false;
    let finishing = false;
    let lineCount = 0;
    const previousEncoding = process.stdin.readableEncoding;

    const cleanup = () => {
      process.stdin.off('data', onData);
      process.stdin.off('error', onError);
      if (previousEncoding) process.stdin.setEncoding(previousEncoding);
      rl.resume();
    };

    const complete = (value) => {
      settled = true;
      cleanup();
      resolve(value ? `${String(value).trim()}\n` : '');
    };

    const finish = (value, options: { drainMs?: number } = {}) => {
      if (settled || finishing) return;
      finishing = true;
      const drainMs = options.drainMs ?? 0;
      if (drainMs > 0) {
        setTimeout(() => complete(value), drainMs);
      } else {
        complete(value);
      }
    };

    const onError = (error) => {
      if (settled || finishing) return;
      settled = true;
      cleanup();
      reject(error);
    };

    const onData = (chunk) => {
      if (finishing) return;
      buffer += String(chunk);
      lineCount = buffer.split(/\r?\n/).length;

      if (/^\s*(?:\r?\n)/.test(buffer)) {
        finish('');
        return;
      }

      const endMatch = buffer.match(/-----END PRIVATE KEY-+[^\r\n]*(?:\r?\n|$)/);
      if (endMatch?.index !== undefined) {
        finish(buffer.slice(0, endMatch.index + endMatch[0].length), { drainMs: 750 });
        return;
      }

      if (lineCount > 80) {
        process.stdout.write('Paste looks incomplete: no -----END PRIVATE KEY----- line found within 80 lines.\n');
        finish('');
      }
    };

    rl.pause();
    process.stdin.setEncoding('utf8');
    process.stdin.on('data', onData);
    process.stdin.on('error', onError);
    process.stdout.write('ASC_PRIVATE_KEY content: ');
    process.stdin.resume();
  });
}

async function validateAscPrivateKeyPath(filePath) {
  const raw = await fs.readFile(filePath, 'utf8');
  return validateAscPrivateKeyContent(raw);
}

async function askAscPrivateKeyPath(rl) {
  while (true) {
    const privateKeyPath = await ask(
      rl,
      'ASC_PRIVATE_KEY_PATH (path to AuthKey_XXXX.p8, leave empty to skip)',
      process.env.ASC_PRIVATE_KEY_PATH || '',
    );
    const trimmedPath = privateKeyPath.trim();
    if (!trimmedPath) return '';

    try {
      const validation = await validateAscPrivateKeyPath(trimmedPath);
      if (validation.ok) return trimmedPath;
      process.stdout.write(`${validation.error}\n`);
    } catch (error) {
      process.stdout.write(`Could not read .p8 file: ${error instanceof Error ? error.message : String(error)}\n`);
    }
    process.stdout.write('The ASC private key path was not saved. Paste a valid path, or leave empty to skip.\n');
  }
}

function isAscWebAuthAuthenticated(stdout) {
  try {
    const payload = JSON.parse(String(stdout || '{}'));
    return payload?.authenticated === true;
  } catch {
    return false;
  }
}

function resolveAscWebAppleId() {
  return (
    process.env.ASC_WEB_APPLE_ID?.trim() ||
    process.env.ASC_APPLE_ID?.trim() ||
    process.env.APPLE_ID?.trim() ||
    ''
  );
}

function ascWebAuthEnv() {
  return {
    ...process.env,
    ASC_TIMEOUT: process.env.ASC_TIMEOUT || '90s',
    ASC_TIMEOUT_SECONDS: process.env.ASC_TIMEOUT_SECONDS || '90',
  };
}

async function ensureAscWebAnalyticsAuth(rl = null, secrets: Record<string, string> = {}) {
  process.stdout.write('\nChecking ASC web analytics authentication...\n');
  process.stdout.write('Still working: verifying whether the ASC web session is active.\n');
  if (!(await commandExists('asc'))) {
    throw new Error(
      'The asc CLI is not installed yet. Install it with `openclaw start --connectors asc`, then rerun the connector wizard so it can run `asc web auth login`.',
    );
  }

  const ascEnv = ascWebAuthEnv();
  if (!process.env.ASC_TIMEOUT && !process.env.ASC_TIMEOUT_SECONDS) {
    process.stdout.write('Using ASC_TIMEOUT=90s for ASC web auth because Apple web endpoints can be slow.\n');
  }

  const status = await runCommandCapture('asc web auth status --output json', { env: ascEnv });
  if (status.ok && isAscWebAuthAuthenticated(status.stdout)) {
    process.stdout.write('ASC web analytics authentication is active.\n');
    return false;
  }

  let appleId = resolveAscWebAppleId();
  if (!appleId && rl) {
    appleId = (await ask(rl, 'Apple Account email for ASC web analytics login (ASC_WEB_APPLE_ID)', '')).trim();
    if (appleId) {
      secrets.ASC_WEB_APPLE_ID = appleId;
      await saveSecretsImmediately({ ASC_WEB_APPLE_ID: appleId });
    }
  }
  if (!appleId) {
    throw new Error('ASC web analytics login needs an Apple Account email. Rerun the connector wizard and enter ASC_WEB_APPLE_ID.');
  }

  let attempts = 0;
  while (true) {
    attempts += 1;
    process.stdout.write(`\nASC web login: ${appleId}\n`);
    process.stdout.write('The next prompts are from asc. Enter the Apple Account password/2FA there.\n\n');
    const loginCode = await runInteractiveProcess('asc', ['web', 'auth', 'login', '--apple-id', appleId], {
      env: ascEnv,
      rl,
    });
    if (loginCode === 0) {
      break;
    }

    process.stdout.write('\nASC web login failed.\n');
    process.stdout.write('Reason: asc/Apple rejected the Apple Account login. The .p8 API key is not used here.\n\n');
    if (!rl || attempts >= 3) {
      throw new Error(
        'ASC web analytics login failed. Check the Apple Account email/password/2FA, then rerun the connector wizard.',
      );
    }
    const retry = await askYesNo(rl, 'Retry ASC web analytics login now?', true);
    if (!retry) {
      throw new Error(
        'ASC web analytics login was not completed. Rerun the connector wizard when the Apple Account login is ready.',
      );
    }
    const nextAppleId = (
      await ask(rl, 'Apple Account email for ASC web analytics login (press Enter to keep)', appleId)
    ).trim();
    if (nextAppleId && nextAppleId !== appleId) {
      appleId = nextAppleId;
      secrets.ASC_WEB_APPLE_ID = appleId;
      await saveSecretsImmediately({ ASC_WEB_APPLE_ID: appleId });
    }
  }

  process.stdout.write('\nStill working: verifying the ASC web analytics session after login...\n');
  const verify = await runCommandCapture('asc web auth status --output json', { env: ascEnv });
  if (!verify.ok || !isAscWebAuthAuthenticated(verify.stdout)) {
    throw new Error(
      'ASC web analytics login did not verify. Run `asc web auth status --output json --pretty` to inspect the session, then rerun the connector wizard.',
    );
  }

  process.stdout.write('ASC web analytics authentication verified.\n');
  return true;
}

function printSection(title: string, lines: string[] = []) {
  process.stdout.write(`\n${ANSI.bold}${title}${ANSI.reset}\n`);
  process.stdout.write(`${'-'.repeat(title.length)}\n`);
  for (const line of lines) {
    process.stdout.write(`${line}\n`);
  }
  if (lines.length > 0) process.stdout.write('\n');
}

function printBullets(lines: string[]) {
  for (const line of lines) {
    process.stdout.write(`  - ${line}\n`);
  }
  process.stdout.write('\n');
}

async function guideGitHubConnector(rl, secrets: Record<string, string>) {
  printSection('GitHub code access', [
    'Use this when OpenClaw should read repo context or create GitHub delivery artifacts.',
  ]);
  printBullets([
    'Open the token page, select the scopes you want, then paste the token here.',
    'You can rerun this wizard later to change GitHub permissions.',
  ]);

  let hasGh = await commandExists('gh');
  if (!hasGh) {
    hasGh = await installGitHubCliUserLocal();
  }
  if (hasGh) {
    process.stdout.write('GitHub CLI is available for helper commands.\n\n');
  }

  process.stdout.write('Token URL: https://github.com/settings/tokens/new\n\n');
  process.stdout.write(`${ANSI.bold}Suggested scopes${ANSI.reset}\n`);
  printBullets([
    'Public repo only: select `public_repo`.',
    'Private repo access: select `repo` (classic GitHub tokens make private repo access broad).',
    'Create issues / draft PRs in private repos: `repo` is the relevant classic-token scope.',
    'Edit GitHub Actions workflow files: add `workflow` only if you explicitly want this.',
    'Usually do not select: packages, admin:org, hooks, gist, user, delete_repo, enterprise, codespace, copilot.',
  ]);

  const token = await maybePromptSecret(rl, 'Paste GITHUB_TOKEN into this local terminal', 'GITHUB_TOKEN');
  if (token) secrets.GITHUB_TOKEN = token;
  else process.stdout.write('No GitHub token saved. GitHub setup remains pending; rerun this wizard when ready.\n\n');

  const detectedRepo = await detectGitHubRepo();
  if (detectedRepo) {
    secrets.OPENCLAW_GITHUB_REPO = detectedRepo;
    process.stdout.write(`Detected GitHub repo for this workspace: ${detectedRepo}\n\n`);
  } else if (token || process.env.GITHUB_TOKEN) {
    process.stdout.write('GitHub auth is saved. Repo selection is deferred per app/task; no global repo is required.\n\n');
  }
}

function shouldForceFreshAnalyticsToken(healthByConnector: Record<string, any> = {}) {
  const health = getConnectorHealth('analytics', healthByConnector);
  const detail = String(health?.detail || '');
  return ['blocked', 'partial'].includes(String(health?.status || '')) || /revoked|unauthorized|invalid token/i.test(detail);
}

async function guideAnalyticsConnector(rl, secrets: Record<string, string>, options: Record<string, any> = {}) {
  printSection('AnalyticsCLI');
  process.stdout.write('Create a readonly CLI token:\n');
  process.stdout.write('1. Open https://dash.analyticscli.com/\n');
  process.stdout.write('2. Account -> API Keys\n');
  process.stdout.write('3. Create Access Token\n');
  process.stdout.write('4. Copy the Readonly CLI Token and paste it below\n\n');
  const forceFresh = Boolean(options.forceFresh);
  if (forceFresh && process.env.ANALYTICSCLI_ACCESS_TOKEN) {
    process.stdout.write('Stored token failed. Paste a new token.\n\n');
  }
  const token = forceFresh
    ? await ask(rl, 'Paste the new AnalyticsCLI readonly CLI token into this local terminal', '')
    : await maybePromptSecret(
        rl,
        'Paste AnalyticsCLI readonly CLI token into this local terminal',
        'ANALYTICSCLI_ACCESS_TOKEN',
      );
  if (token) {
    secrets.ANALYTICSCLI_ACCESS_TOKEN = token;
    secrets.ANALYTICSCLI_READONLY_TOKEN = token;
  }
  else process.stdout.write('No AnalyticsCLI token saved. Product analytics setup remains pending; rerun this wizard when ready.\n\n');
}

async function guideRevenueCatConnector(rl, secrets: Record<string, string>) {
  printSection('RevenueCat monetization data', [
    'Use this when OpenClaw should read subscription, product, entitlement, and revenue context.',
  ]);
  process.stdout.write('\nCreate a RevenueCat secret API key here:\n  https://app.revenuecat.com/\n\n');
  printBullets([
    'Select your app.',
    'In the sidebar, choose "Apps & providers".',
    'Click "API keys" and generate a new secret API key.',
    'Name it "analyticscli" and choose API version 2.',
    'Set Charts metrics permissions to read.',
    'Set Customer information permissions to read.',
    'Set Project configuration permissions to read.',
  ]);
  const apiKey = await maybePromptSecret(rl, 'Paste REVENUECAT_API_KEY into this local terminal', 'REVENUECAT_API_KEY');
  if (apiKey) secrets.REVENUECAT_API_KEY = apiKey;
}

async function guideSentryConnector(rl, secrets: Record<string, string>) {
  printSection('Sentry / GlitchTip', [
    'Paste token, org, and base URL. Projects are discovered automatically.',
    'Use `https://sentry.io` for Sentry Cloud or your GlitchTip/self-hosted base URL.',
  ]);

  const accounts = [];
  let index = 0;
  while (true) {
    const baseUrl = await ask(
      rl,
      `Sentry account ${index + 1} base URL`,
      index === 0 ? process.env.SENTRY_BASE_URL || 'https://sentry.io' : 'https://sentry.io',
    );
    const defaultLabel = defaultSentryAccountLabel({ index, baseUrl });
    const label = await ask(rl, `Sentry account ${index + 1} label`, defaultLabel);
    const id = toConfigId(label || baseUrl, `sentry_${index + 1}`);
    const tokenEnv = defaultSentryTokenEnv({ index, label, baseUrl });
    printSentryTokenGuidance({ baseUrl, tokenEnv });
    const token = await maybePromptSecret(rl, `Paste ${tokenEnv} into this local terminal`, tokenEnv);
    if (token) secrets[tokenEnv] = token;

    let discoveredOrganizations: Array<{ slug: string; name: string }> = [];
    if (token) {
      process.stdout.write(`Discovering Sentry / GlitchTip organizations for ${label}...\n`);
      const organizationDiscovery = await discoverSentryOrganizations({ baseUrl, token });
      if (organizationDiscovery.ok && organizationDiscovery.organizations.length > 0) {
        discoveredOrganizations = organizationDiscovery.organizations;
        process.stdout.write(
          `Found org(s): ${discoveredOrganizations.map((organization) => organization.slug).join(', ')}\n`,
        );
      } else if (!organizationDiscovery.ok) {
        process.stdout.write(`${ANSI.dim}Could not list organizations automatically (${organizationDiscovery.detail}).${ANSI.reset}\n`);
      }
    }

    let org = '';
    if (discoveredOrganizations.length === 1) {
      org = discoveredOrganizations[0].slug;
      process.stdout.write(`Using organization: ${org}\n`);
    } else if (discoveredOrganizations.length > 1) {
      process.stdout.write('Select organization:\n');
      const orgChoice = await askListSelection(
        rl,
        `Organization for ${label}`,
        discoveredOrganizations.map((organization) => ({
          value: organization.slug,
          label: organization.slug,
          description: organization.name && organization.name !== organization.slug ? organization.name : '',
        })),
        { includeManual: true, includeDefer: true },
      );
      org = orgChoice === '__manual__'
        ? await ask(rl, `Sentry org slug for ${label}`, index === 0 ? process.env.SENTRY_ORG || '' : '')
        : orgChoice;
    } else {
      org = await ask(
        rl,
        `Sentry org slug for ${label} (leave empty to defer)`,
        index === 0 ? process.env.SENTRY_ORG || '' : '',
      );
    }
    const environment = await ask(
      rl,
      `Sentry environment for ${label}`,
      index === 0 ? process.env.SENTRY_ENVIRONMENT || 'production' : 'production',
    );

    if (org.trim() && token) {
      process.stdout.write(`Checking visible Sentry projects for ${label} without pinning project scope...\n`);
      const discovery = await discoverSentryProjects({ baseUrl, token, org });
      let verifiedVisibleProjects = false;
      if (discovery.ok && discovery.projects.length > 0) {
        verifiedVisibleProjects = true;
        process.stdout.write(
          `Found ${discovery.projects.length} visible project(s). Project scope remains unpinned so OpenClaw/Hermes can decide per run.\n`,
        );
      } else {
        const fallbackOrgs = discoveredOrganizations
          .map((organization) => organization.slug)
          .filter((slug) => slug && slug !== org.trim());
        for (const fallbackOrg of fallbackOrgs) {
          process.stdout.write(`Trying visible org ${fallbackOrg}...\n`);
          const fallbackDiscovery = await discoverSentryProjects({ baseUrl, token, org: fallbackOrg });
          if (fallbackDiscovery.ok && fallbackDiscovery.projects.length > 0) {
            org = fallbackOrg;
            verifiedVisibleProjects = true;
            process.stdout.write(
              `Using org ${fallbackOrg}; found ${fallbackDiscovery.projects.length} visible project(s). Project scope remains unpinned.\n`,
            );
            break;
          }
        }
        if (!verifiedVisibleProjects && !discovery.ok) {
          process.stdout.write(`Could not verify visible projects automatically (${discovery.detail}). Project scope will be resolved from app context later.\n`);
        }
      }
    } else {
      process.stdout.write('Project discovery needs both a token and org slug. Project scope will be resolved from app context later.\n');
    }

    accounts.push({
      id,
      label,
      baseUrl,
      tokenEnv,
      ...(org.trim() ? { org: org.trim() } : {}),
      ...(environment.trim() ? { environment: environment.trim() } : {}),
    });

    if (index === 0) {
      if (tokenEnv === 'SENTRY_AUTH_TOKEN' && token) secrets.SENTRY_AUTH_TOKEN = token;
      if (org.trim()) secrets.SENTRY_ORG = org.trim();
      if (environment.trim()) secrets.SENTRY_ENVIRONMENT = environment.trim();
      if (baseUrl.trim() && baseUrl.trim() !== 'https://sentry.io') secrets.SENTRY_BASE_URL = baseUrl.trim();
    }

    const addAnother = await askYesNo(
      rl,
      'Configure another Sentry / GlitchTip account now, for example on another base URL?',
      false,
    );
    if (!addAnother) break;
    index += 1;
  }

  return accounts;
}

async function guideAscConnector(rl, secrets: Record<string, string>) {
  printSection('App Store Connect CLI', [
    'Use this mainly for App Store analytics, plus builds, TestFlight, reviews, ratings, and store context.',
    'ASC web analytics also needs a website login; this wizard verifies it after helper setup.',
  ]);
  process.stdout.write('Create an App Store Connect API key here:\n  https://appstoreconnect.apple.com/access/integrations/api\n\n');
  process.stdout.write('Roles to choose for this key:\n');
  printBullets([
    'Required: Sales, for App Analytics, Sales and Trends, downloads, revenue, and conversion context.',
    'Recommended: Customer Support, for App Store ratings and review text.',
    'Recommended: Developer, for builds, TestFlight, and delivery status.',
    'Optional: App Manager, only if OpenClaw should also read or manage app metadata, pricing, or release settings.',
    'Avoid: Admin unless a one-off App Store Connect permission requires it.',
  ]);
  process.stdout.write('\nAfter creating the key, copy these values into this wizard:\n');
  printBullets([
    'Issuer ID from the API keys page.',
    'Key ID from the API key row or from the downloaded file name: AuthKey_<KEY_ID>.p8.',
    'Download the .p8 file, open it, then paste the full file content into this terminal.',
    'If the .p8 is already on this host, leave the content prompt empty and paste the file path instead.',
  ]);

  const keyId = await ask(rl, 'ASC_KEY_ID (leave empty to skip)', process.env.ASC_KEY_ID || '');
  const issuerId = await ask(rl, 'ASC_ISSUER_ID (leave empty to skip)', process.env.ASC_ISSUER_ID || '');
  if (keyId.trim()) secrets.ASC_KEY_ID = keyId.trim();
  if (issuerId.trim()) secrets.ASC_ISSUER_ID = issuerId.trim();

  const appleId = await ask(
    rl,
    'Apple Account email for ASC web analytics login (ASC_WEB_APPLE_ID, leave empty to skip)',
    resolveAscWebAppleId(),
  );
  if (appleId.trim()) secrets.ASC_WEB_APPLE_ID = appleId.trim();
  process.stdout.write('ASC web password and 2FA are not stored by this wizard; asc asks for them interactively during web login.\n');

  const privateKeyContent = await askAscPrivateKeyContent(rl);
  if (privateKeyContent) {
    const privateKeyPath = resolveAscPrivateKeyPath(keyId);
    await fs.mkdir(path.dirname(privateKeyPath), { recursive: true, mode: 0o700 });
    await fs.writeFile(privateKeyPath, privateKeyContent, { encoding: 'utf8', mode: 0o600 });
    await fs.chmod(privateKeyPath, 0o600);
    secrets.ASC_PRIVATE_KEY_PATH = privateKeyPath;
    process.stdout.write(`Saved ASC private key to ${privateKeyPath} with chmod 600.\n`);
  } else {
    const privateKeyPath = await askAscPrivateKeyPath(rl);
    if (privateKeyPath.trim()) secrets.ASC_PRIVATE_KEY_PATH = privateKeyPath.trim();
  }
}

async function shouldRunSelfUpdate(workspaceRoot, force) {
  if (force) return true;
  const statePath = path.join(workspaceRoot, 'data/openclaw-growth-engineer/self-update.json');
  const state = await readJsonIfPresent(statePath).catch(() => null);
  const lastCheckedAt = Date.parse(String(state?.lastCheckedAt || ''));
  return !Number.isFinite(lastCheckedAt) || Date.now() - lastCheckedAt > SELF_UPDATE_INTERVAL_MS;
}

async function writeSelfUpdateState(workspaceRoot, value) {
  const statePath = path.join(workspaceRoot, 'data/openclaw-growth-engineer/self-update.json');
  await writeJsonFile(statePath, {
    version: 1,
    checkedAt: new Date().toISOString(),
    ...value,
  });
}

async function rerunCurrentWizardWithoutSelfUpdate() {
  return await new Promise<number | null>((resolve) => {
    const child = spawn(process.execPath, process.argv.slice(1), {
      env: {
        ...process.env,
        OPENCLAW_GROWTH_SKIP_SELF_UPDATE: '1',
      },
      stdio: 'inherit',
    });
    child.on('error', () => resolve(1));
    child.on('close', (code) => resolve(code));
  });
}

async function filesHaveSameContent(leftPath, rightPath) {
  try {
    const [left, right] = await Promise.all([fs.readFile(leftPath), fs.readFile(rightPath)]);
    return left.equals(right);
  } catch {
    return false;
  }
}

async function maybeSelfUpdateFromClawHub(args) {
  if (args.noSelfUpdate) return false;
  if (isTruthyEnv(process.env.OPENCLAW_GROWTH_SKIP_SELF_UPDATE)) return false;
  if (isTruthyEnv(process.env.OPENCLAW_GROWTH_DISABLE_SELF_UPDATE)) return false;
  if (isFalseyEnv(process.env.OPENCLAW_GROWTH_SELF_UPDATE)) return false;

  const workspaceRoot = process.cwd();
  const skillOriginPath = path.join(workspaceRoot, 'skills/openclaw-growth-engineer/.clawhub/origin.json');
  if (!(await fileExists(skillOriginPath))) return false;
  if (!(await commandExists('npx'))) return false;

  const force = String(process.env.OPENCLAW_GROWTH_SELF_UPDATE || '').trim().toLowerCase() === 'always';
  if (!(await shouldRunSelfUpdate(workspaceRoot, force))) return false;

  const beforeOrigin = await readJsonIfPresent(skillOriginPath).catch(() => null);
  const beforeVersion = String(beforeOrigin?.installedVersion || '');
  process.stdout.write('Checking for OpenClaw Growth Engineer skill updates...\n');

  const updateResult = await runCommandCaptureWithTimeout(
    'npx -y clawhub --no-input --dir skills update openclaw-growth-engineer --force',
    { timeoutMs: 120_000 },
  );
  const afterOrigin = await readJsonIfPresent(skillOriginPath).catch(() => null);
  const afterVersion = String(afterOrigin?.installedVersion || beforeVersion || '');
  const workspaceWizardPath = path.resolve(process.argv[1] || 'scripts/openclaw-growth-wizard.mjs');
  const skillWizardPath = path.join(workspaceRoot, 'skills/openclaw-growth-engineer/scripts/openclaw-growth-wizard.mjs');
  const runtimeOutdated = !(await filesHaveSameContent(workspaceWizardPath, skillWizardPath));

  await writeSelfUpdateState(workspaceRoot, {
    lastCheckedAt: new Date().toISOString(),
    ok: updateResult.ok,
    previousVersion: beforeVersion || null,
    installedVersion: afterVersion || null,
  }).catch(() => {});

  if (!updateResult.ok) {
    const detail = String(updateResult.stderr || updateResult.stdout || 'update failed').trim().split(/\r?\n/).pop();
    process.stdout.write(`${ANSI.dim}Skill update check skipped: ${detail}${ANSI.reset}\n`);
    return false;
  }
  if ((!afterVersion || afterVersion === beforeVersion) && !runtimeOutdated) {
    return false;
  }

  if (afterVersion && afterVersion !== beforeVersion) {
    process.stdout.write(`Updated OpenClaw Growth Engineer skill ${beforeVersion || 'unknown'} -> ${afterVersion}. Refreshing workspace runtime...\n`);
  } else {
    process.stdout.write('Refreshing workspace runtime from the installed OpenClaw Growth Engineer skill...\n');
  }
  const bootstrapResult = await runCommandCaptureWithTimeout(
    'bash skills/openclaw-growth-engineer/scripts/bootstrap-openclaw-workspace.sh',
    { timeoutMs: 60_000 },
  );
  if (!bootstrapResult.ok) {
    process.stdout.write(`${ANSI.dim}Workspace runtime refresh failed; continuing with current process.${ANSI.reset}\n`);
    return false;
  }

  process.stdout.write('Restarting wizard with refreshed runtime...\n');
  const code = await rerunCurrentWizardWithoutSelfUpdate();
  process.exit(code ?? 0);
}

async function runConnectorSetupWizard(args) {
  if (!process.stdin.isTTY || !process.stdout.isTTY) {
    throw new Error('Connector wizard requires an interactive terminal.');
  }

  const rl = createInterface({ input: process.stdin, output: process.stdout });
  try {
    clearTerminal();
    printConnectorIntro();
    const healthByConnector = await withConnectorHealthLoading((onProgress) =>
      getConnectorPickerHealth(args.config, onProgress),
    );
    const existingFixes = connectorKeysNeedingAttention(healthByConnector);
    const requestedConnectors = args.connectors ? parseConnectorList(args.connectors) : [];
    const chosenConnectors =
      requestedConnectors.length > 0
        ? orderConnectors([...new Set([...requestedConnectors, ...existingFixes])])
        : await askConnectorSelectionWithHealth(rl, healthByConnector, existingFixes);
    const selected = withMissingRequiredAnalyticsConnector(
      chosenConnectors,
    );
    if (selected.length === 0) {
      throw new Error('No supported connectors selected. Use analytics, github, revenuecat, sentry, asc, or all.');
    }

    clearTerminal();
    printConnectorIntro();
    process.stdout.write(`${ANSI.bold}Selected connectors${ANSI.reset}\n`);
    for (const key of selected) {
      process.stdout.write(`  - ${connectorLabel(key)}\n`);
    }
    process.stdout.write('\n');

    const secrets: Record<string, string> = {};
    let sentryAccounts: any[] = [];
    if (selected.includes('analytics')) {
      let forceFreshAnalyticsToken = shouldForceFreshAnalyticsToken(healthByConnector);
      while (true) {
        clearTerminal();
        await guideAnalyticsConnector(rl, secrets, { forceFresh: forceFreshAnalyticsToken });
        const check = await runImmediateConnectorHealthCheck({
          rl,
          configPath: args.config,
          connector: 'analytics',
          secrets,
        });
        if (!check.retry) break;
        forceFreshAnalyticsToken = true;
      }
    }
    if (selected.includes('github')) {
      while (true) {
        clearTerminal();
        await guideGitHubConnector(rl, secrets);
        const check = await runImmediateConnectorHealthCheck({
          rl,
          configPath: args.config,
          connector: 'github',
          secrets,
        });
        if (!check.retry) break;
      }
    }
    if (selected.includes('revenuecat')) {
      while (true) {
        clearTerminal();
        await guideRevenueCatConnector(rl, secrets);
        const check = await runImmediateConnectorHealthCheck({
          rl,
          configPath: args.config,
          connector: 'revenuecat',
          secrets,
        });
        if (!check.retry) break;
      }
    }
    if (selected.includes('sentry')) {
      while (true) {
        clearTerminal();
        sentryAccounts = await guideSentryConnector(rl, secrets);
        const check = await runImmediateConnectorHealthCheck({
          rl,
          configPath: args.config,
          connector: 'sentry',
          secrets,
          sentryAccounts,
        });
        if (!check.retry) break;
      }
    }
    if (selected.includes('asc')) {
      while (true) {
        clearTerminal();
        await guideAscConnector(rl, secrets);
        const check = await runImmediateConnectorHealthCheck({
          rl,
          configPath: args.config,
          connector: 'asc',
          secrets,
        });
        if (!check.retry) break;
      }
    }

    const secretsFile = resolveSecretsFile();
    const wroteSecrets = Object.keys(secrets).length > 0;
    clearTerminal();
    if (wroteSecrets) {
      await writeSecretsFile(secretsFile, secrets);
      process.stdout.write(`\nSaved local secrets to ${secretsFile} with chmod 600.\n`);
    } else {
      process.stdout.write('\nNo new secrets were written.\n');
    }

    if (sentryAccounts.length > 0 && await upsertSentryAccountsConfig(args.config, sentryAccounts)) {
      process.stdout.write(`Configured ${sentryAccounts.length} Sentry-compatible account(s) in ${args.config}.\n`);
    }

    const env = {
      ...process.env,
      ...secrets,
    };
    const command = `node scripts/openclaw-growth-start.mjs --config ${quote(args.config)} --setup-only --connectors ${quote(selected.join(','))}`;
    let setupResult = await runSetupCommandWithProgress(command, env, selected, 'Testing connector setup...');
    let setupPayload = parseJsonFromStdout(setupResult.stdout);

    if (sentryAccounts.length > 0 && await upsertSentryAccountsConfig(args.config, sentryAccounts)) {
      process.stdout.write(`Sentry-compatible account config is up to date in ${args.config}.\n`);
    }

    if (selected.includes('asc')) {
      try {
        const ascWebAuthChanged = await ensureAscWebAnalyticsAuth(rl, secrets);
        if (ascWebAuthChanged) {
          setupResult = await runSetupCommandWithProgress(
            command,
            env,
            selected,
            'Retesting connector setup after ASC web analytics login...',
          );
          setupPayload = parseJsonFromStdout(setupResult.stdout);
        }
      } catch (error) {
        process.stdout.write(
          `ASC web analytics still needs attention: ${error instanceof Error ? error.message : String(error)}\n`,
        );
      }
    }

    if (setupResult.ok && setupPayload?.ok !== false) {
      printSetupSuccess(setupPayload);
      if (wroteSecrets) {
        process.stdout.write('Future OpenClaw Growth commands load this secrets file automatically.\n');
      }
      const configureIsolation = ENABLE_ISOLATED_SECRET_RUNNER_WIZARD && await askYesNo(
        rl,
        'Generate an isolated secret runner so OpenClaw can run health checks without reading API keys?',
        true,
      );
      if (configureIsolation) {
        const config = await loadEditableConfig(args.config);
        const secretAccess = await askSecretAccessModel(rl, path.resolve(args.config), config);
        await writeJsonFile(path.resolve(args.config), config);
        const manifestPath = await writeOpenClawJobManifest(path.resolve(args.config), config);
        process.stdout.write(`Saved OpenClaw job manifest: ${manifestPath}\n`);
        printSecretRunnerKitInstructions(secretAccess.kit);
      }
      return;
    }

    printSetupFailure({ result: setupResult, payload: setupPayload, command });
    process.exitCode = 1;
  } finally {
    rl.close();
  }
}

function clearPromptInput(rl) {
  try {
    rl.write?.(null, { ctrl: true, name: 'u' });
  } catch {
    // Best-effort cleanup for stale pasted terminal input before showing a prompt.
  }
}

async function ask(rl, label, defaultValue = '') {
  const suffix = defaultValue ? ` (${defaultValue})` : '';
  clearPromptInput(rl);
  const answer = (await rl.question(`${label}${suffix}: `)).trim();
  return answer || defaultValue;
}

async function askYesNo(rl, label, defaultYes = true) {
  const suffix = defaultYes ? '[Y/n]' : '[y/N]';
  while (true) {
    clearPromptInput(rl);
    const answer = (await rl.question(`${label} ${suffix} `)).trim().toLowerCase();
    if (!answer) return defaultYes;
    if (answer === 'y' || answer === 'yes') return true;
    if (answer === 'n' || answer === 'no') return false;
    if (answer.includes('private key')) {
      process.stdout.write('That looks like leftover .p8 key text, not a yes/no answer. Ignoring it.\n');
    } else {
      process.stdout.write('Please answer y or n.\n');
    }
  }
}

function printCadencePlan(cadences) {
  process.stdout.write('\nDefault growth cadence:\n');
  for (const cadence of cadences) {
    const critical = cadence.criticalOnly ? 'critical only' : 'full review';
    process.stdout.write(`- ${cadence.title} (${critical}): ${cadence.objective}\n`);
  }
  process.stdout.write('\n');
}

async function askToolUsage(rl) {
  process.stdout.write('\nHow should OpenClaw Growth Engineer use this tool?\n');
  process.stdout.write('  1) Production autopilot: notify, draft issues/PR handoffs, and analyze on schedule\n');
  process.stdout.write('  2) Advisory only: analyze and write OpenClaw chat summaries, no GitHub artifacts by default\n');
  process.stdout.write('  3) Manual reports: mostly one-off runs; keep scheduling conservative\n');
  const answer = await ask(rl, 'Usage mode (1/2/3)', '1');
  if (answer.trim() === '2') return 'advisory';
  if (answer.trim() === '3') return 'manual_reports';
  return 'production_autopilot';
}

async function askCadencePlan(rl) {
  const cadences: any[] = DEFAULT_CADENCE_PLAN.map((cadence) => ({ ...cadence }));
  printCadencePlan(cadences);
  const customize = await askYesNo(
    rl,
    'Use this default cadence plan? Answer no to edit daily/weekly/monthly/3-month/6-month/1-year instructions.',
    true,
  );
  if (customize) return cadences;

  for (const cadence of cadences) {
    process.stdout.write(`\n${cadence.title}\n`);
    const enabled = await askYesNo(rl, `Enable ${cadence.key}?`, true);
    cadence.enabled = enabled;
    if (!enabled) continue;
    cadence.objective = await ask(rl, `${cadence.key} objective`, cadence.objective);
    cadence.instructions = await ask(rl, `${cadence.key} instructions`, cadence.instructions);
    const focusAreas = await ask(rl, `${cadence.key} focus areas (comma-separated)`, cadence.focusAreas.join(','));
    cadence.focusAreas = focusAreas.split(',').map((value) => value.trim()).filter(Boolean);
    const sourcePriorities = await ask(
      rl,
      `${cadence.key} source priorities (comma-separated)`,
      cadence.sourcePriorities.join(','),
    );
    cadence.sourcePriorities = sourcePriorities.split(',').map((value) => value.trim()).filter(Boolean);
    cadence.criticalOnly = await askYesNo(rl, `${cadence.key} should only alert on critical findings?`, cadence.criticalOnly);
  }

  return cadences;
}

async function askWizardGoal(rl) {
  return await askMenuChoice(rl, {
    title: 'What do you want to configure?',
    subtitle: 'Use Up/Down to move, Enter to continue, or press 1-4.',
    defaultValue: 'full',
    renderHeader: printWizardHeader,
    options: [
      {
        value: 'connectors',
        label: 'Connectors',
        detail: 'Credentials, provider setup, and health checks.',
      },
      {
        value: 'outputs_intervals',
        label: 'Outputs and intervals',
        detail: 'Daily/weekly/monthly jobs, GitHub issue/PR delivery, and OpenClaw chat notifications.',
      },
      {
        value: 'full',
        label: 'Full setup',
        detail: 'Project, connectors, outputs, intervals, and sources.',
      },
      {
        value: 'intervals',
        label: 'Advanced intervals only',
        detail: 'Runner wake-up interval and connector health check cadence.',
      },
    ],
  });
}

function printWizardHeader() {
  process.stdout.write('OpenClaw Growth Engineer - Setup Wizard\n');
  process.stdout.write('This wizard can configure connector secrets. Normal config is written to config JSON; API keys stay in the local chmod 600 secrets file.\n\n');
}

async function buildDefaultWizardConfig() {
  return {
    version: 7,
    generatedAt: new Date().toISOString(),
    project: {
      githubRepo: '',
      repoRoot: '.',
      outFile: 'data/openclaw-growth-engineer/issues.generated.json',
      maxIssues: 4,
      titlePrefix: '[Growth]',
      labels: ['ai-growth', 'autogenerated', 'product'],
    },
    sources: {
      analytics: {
        enabled: true,
        mode: 'command',
        command: getDefaultSourceCommand('analytics'),
      },
      revenuecat: {
        enabled: false,
        mode: 'command',
        command: getDefaultSourceCommand('revenuecat'),
      },
      sentry: {
        enabled: true,
        mode: 'command',
        command: getDefaultSourceCommand('sentry'),
      },
      feedback: {
        enabled: true,
        mode: 'command',
        command: getDefaultSourceCommand('feedback'),
        cursorMode: 'auto_since_last_fetch',
        initialLookback: '30d',
      },
      extra: [
        buildExtraSourceConfig('asc-cli', { enabled: false, mode: 'command', command: getDefaultSourceCommand('asc') }),
      ],
    },
    schedule: {
      intervalMinutes: DEFAULT_GROWTH_INTERVAL_MINUTES,
      connectorHealthCheckIntervalMinutes: DEFAULT_CONNECTOR_HEALTH_INTERVAL_MINUTES,
      skipIfNoDataChange: true,
      skipIfIssueSetUnchanged: true,
      cadences: DEFAULT_CADENCE_PLAN.map((cadence) => ({ ...cadence })),
    },
    actions: {
      autoCreateIssues: false,
      autoCreatePullRequests: false,
      autoCreateWhenGitHubWriteAccess: true,
      disableAutoCreateGitHubArtifacts: false,
      mode: 'issue',
      usageMode: 'production_autopilot',
      draftPullRequests: true,
      proposalBranchPrefix: 'openclaw/proposals',
    },
    deliveries: {
      openclawChat: {
        enabled: true,
        markdownPath: '.openclaw/chat/latest.md',
        jsonPath: '.openclaw/chat/latest.json',
      },
      github: {
        enabled: false,
        mode: 'issue',
        autoCreate: false,
        draftPullRequests: true,
        proposalBranchPrefix: 'openclaw/proposals',
      },
      slack: {
        enabled: false,
        webhookEnv: 'SLACK_WEBHOOK_URL',
      },
      webhook: {
        enabled: false,
        urlEnv: 'OPENCLAW_WEBHOOK_URL',
        method: 'POST',
        headers: {},
      },
      discord: {
        enabled: false,
        command: 'node scripts/discord-openclaw-bridge.mjs send --stdin',
      },
    },
    charting: {
      enabled: false,
      command: null,
    },
    notifications: {
      connectorHealth: {
        enabled: true,
        channels: [
          {
            type: 'openclaw-chat',
            enabled: true,
            markdownPath: '.openclaw/chat/connector-health.md',
            jsonPath: '.openclaw/chat/connector-health.json',
          },
        ],
      },
      growthRun: {
        enabled: true,
        channels: [
          {
            type: 'openclaw-chat',
            enabled: true,
            markdownPath: '.openclaw/chat/growth-summary.md',
            jsonPath: '.openclaw/chat/growth-summary.json',
          },
        ],
      },
    },
    secrets: {
      githubTokenEnv: 'GITHUB_TOKEN',
      githubTokenRef: { source: 'env', provider: 'default', id: 'GITHUB_TOKEN' },
      analyticsTokenEnv: 'ANALYTICSCLI_ACCESS_TOKEN',
      analyticsTokenRef: { source: 'env', provider: 'default', id: 'ANALYTICSCLI_ACCESS_TOKEN' },
      revenuecatTokenEnv: 'REVENUECAT_API_KEY',
      revenuecatTokenRef: { source: 'env', provider: 'default', id: 'REVENUECAT_API_KEY' },
      sentryTokenEnv: 'SENTRY_AUTH_TOKEN',
      sentryTokenRef: { source: 'env', provider: 'default', id: 'SENTRY_AUTH_TOKEN' },
    },
  };
}

function buildRecommendedSourceConfig() {
  return {
    analytics: {
      enabled: true,
      mode: 'command',
      command: getDefaultSourceCommand('analytics'),
    },
    revenuecat: {
      enabled: false,
      mode: 'command',
      command: getDefaultSourceCommand('revenuecat'),
    },
    sentry: {
      enabled: true,
      mode: 'command',
      command: getDefaultSourceCommand('sentry'),
    },
    feedback: {
      enabled: true,
      mode: 'command',
      command: getDefaultSourceCommand('feedback'),
      cursorMode: 'auto_since_last_fetch',
      initialLookback: '30d',
    },
    extra: [
      buildExtraSourceConfig('asc-cli', { enabled: false, mode: 'command', command: getDefaultSourceCommand('asc') }),
    ],
  };
}

function getInputChannelInitialSelection(config): ConnectorKey[] {
  const sources = config?.sources || {};
  const extraSources = Array.isArray(sources.extra) ? sources.extra : [];
  const selected = new Set<ConnectorKey>();
  const hasExplicitSources = Boolean(config?.sources);

  if (!hasExplicitSources || sources.analytics?.enabled !== false) selected.add('analytics');
  if (sources.revenuecat?.enabled === true || isConnectorLocallyConfigured('revenuecat')) selected.add('revenuecat');
  if (!hasExplicitSources || sources.sentry?.enabled !== false) selected.add('sentry');
  if (
    extraSources.some((source) =>
      ['asc', 'asc-cli', 'app-store-connect', 'app_store_connect'].includes(String(source?.service || source?.key || '').toLowerCase()) &&
      source?.enabled !== false,
    ) ||
    isConnectorLocallyConfigured('asc')
  ) {
    selected.add('asc');
  }
  if (
    config?.deliveries?.github?.enabled ||
    config?.actions?.autoCreateIssues ||
    config?.actions?.autoCreatePullRequests ||
    isConnectorLocallyConfigured('github')
  ) {
    selected.add('github');
  }

  return orderConnectors([...selected]);
}

function buildSourceConfigFromInputChannels(selectedConnectors: ConnectorKey[], existingSources: Record<string, any> = {}) {
  const selected = new Set(selectedConnectors);
  const recommended = buildRecommendedSourceConfig();
  const existingExtra = Array.isArray(existingSources.extra) ? existingSources.extra : [];
  const ascSource = existingExtra.find((source) =>
    ['asc', 'asc-cli', 'app-store-connect', 'app_store_connect'].includes(String(source?.service || source?.key || '').toLowerCase()),
  );
  const nonAscExtra = existingExtra.filter((source) => source !== ascSource);

  return {
    ...recommended,
    ...existingSources,
    analytics: {
      ...recommended.analytics,
      ...(existingSources.analytics || {}),
      enabled: selected.has('analytics'),
    },
    revenuecat: {
      ...recommended.revenuecat,
      ...(existingSources.revenuecat || {}),
      enabled: selected.has('revenuecat'),
    },
    sentry: {
      ...recommended.sentry,
      ...(existingSources.sentry || {}),
      enabled: selected.has('sentry'),
    },
    feedback: {
      ...recommended.feedback,
      ...(existingSources.feedback || {}),
      enabled: selected.has('analytics'),
    },
    extra: [
      ...nonAscExtra,
      {
        ...buildExtraSourceConfig('asc-cli', {
          enabled: selected.has('asc'),
          mode: 'command',
          command: getDefaultSourceCommand('asc'),
        }),
        ...(ascSource || {}),
        enabled: selected.has('asc'),
      },
    ],
  };
}

async function loadEditableConfig(configPath) {
  const existing = await readJsonIfPresent(configPath).catch(() => null);
  if (existing && typeof existing === 'object') return existing;
  return await buildDefaultWizardConfig();
}

function mergeNotificationChannels(baseChannels, extraChannels) {
  const channels = [];
  const seen = new Set();
  for (const channel of [...baseChannels, ...extraChannels]) {
    if (!channel || channel.enabled === false) continue;
    const key = `${channel.type}:${channel.markdownPath || channel.jsonPath || channel.webhookEnv || channel.urlEnv || channel.command || channel.label || ''}`;
    if (seen.has(key)) continue;
    seen.add(key);
    channels.push(channel);
  }
  return channels;
}

async function askNotificationChannels(rl, config) {
  const channels: any[] = [
    {
      type: 'openclaw-chat',
      enabled: true,
      markdownPath: '.openclaw/chat/growth-summary.md',
      jsonPath: '.openclaw/chat/growth-summary.json',
    },
  ];

  const slackDefault = Boolean(config?.deliveries?.slack?.enabled);
  if (await askYesNo(rl, 'Send summaries and connector-health alerts to Slack?', slackDefault)) {
    const webhookEnv = await ask(rl, 'Slack webhook env var', config?.deliveries?.slack?.webhookEnv || 'SLACK_WEBHOOK_URL');
    channels.push({ type: 'slack', enabled: true, webhookEnv });
  }

  const webhookDefault = Boolean(config?.deliveries?.webhook?.enabled);
  if (await askYesNo(rl, 'Send summaries and connector-health alerts to a generic webhook/social bridge?', webhookDefault)) {
    const urlEnv = await ask(rl, 'Webhook URL env var', config?.deliveries?.webhook?.urlEnv || 'OPENCLAW_WEBHOOK_URL');
    channels.push({ type: 'webhook', enabled: true, urlEnv, method: 'POST', headers: {} });
  }

  const commandDefault = Boolean(config?.deliveries?.discord?.enabled);
  if (await askYesNo(rl, 'Send summaries and connector-health alerts through a local command channel?', commandDefault)) {
    const command = await ask(
      rl,
      'Command that receives the message on stdin',
      config?.deliveries?.discord?.command || 'node scripts/discord-openclaw-bridge.mjs send --stdin',
    );
    channels.push({ type: 'command', enabled: true, label: 'command', command });
  }

  return channels;
}

async function askOutputConfig(rl, config) {
  printSection('Outputs and notifications', [
    'OpenClaw chat is always enabled so the agent has a readable handoff.',
    'GitHub issues or draft PRs are optional and only run when a token plus an inferred repo are available.',
  ]);
  process.stdout.write('  1) OpenClaw chat only, with GitHub left as runtime fallback\n');
  process.stdout.write('  2) Auto-create GitHub issues for concrete findings\n');
  process.stdout.write('  3) Auto-create draft PR proposals for implementation-ready fixes\n');
  const currentMode = config?.actions?.mode || config?.deliveries?.github?.mode || 'issue';
  const currentAutoCreate = Boolean(config?.actions?.autoCreateIssues || config?.actions?.autoCreatePullRequests || config?.deliveries?.github?.autoCreate);
  const defaultChoice = currentAutoCreate ? (currentMode === 'pull_request' ? '3' : '2') : '1';
  const outputChoice = await ask(rl, 'Output type (1/2/3)', defaultChoice);
  const summaryOnly = outputChoice.trim() === '1';
  const mode = outputChoice.trim() === '3' ? 'pull_request' : 'issue';
  const autoCreate = summaryOnly
    ? false
    : await askYesNo(
        rl,
        mode === 'pull_request'
          ? 'Automatically create draft pull requests when new findings are found?'
          : 'Automatically create GitHub issues when new findings are found?',
        currentAutoCreate,
      );

  if (!summaryOnly) {
    process.stdout.write('GitHub repo scope is not pinned by the wizard; OpenClaw/Hermes will infer it from OPENCLAW_GITHUB_REPO, the local git remote, or runtime context when creating issues/PRs.\n');
  }

  const channels = await askNotificationChannels(rl, config);
  const connectorHealthChannels = channels.map((channel) => {
    if (channel.type !== 'openclaw-chat') return channel;
    return {
      ...channel,
      markdownPath: '.openclaw/chat/connector-health.md',
      jsonPath: '.openclaw/chat/connector-health.json',
    };
  });

  config.actions = {
    ...(config.actions || {}),
    mode,
    autoCreateIssues: mode === 'issue' && autoCreate,
    autoCreatePullRequests: mode === 'pull_request' && autoCreate,
    autoCreateWhenGitHubWriteAccess: config.actions?.autoCreateWhenGitHubWriteAccess !== false,
    disableAutoCreateGitHubArtifacts: config.actions?.disableAutoCreateGitHubArtifacts === true,
    draftPullRequests: true,
    proposalBranchPrefix: config?.actions?.proposalBranchPrefix || 'openclaw/proposals',
  };
  config.deliveries = {
    ...(config.deliveries || {}),
    openclawChat: {
      ...(config.deliveries?.openclawChat || {}),
      enabled: true,
      markdownPath: config.deliveries?.openclawChat?.markdownPath || '.openclaw/chat/latest.md',
      jsonPath: config.deliveries?.openclawChat?.jsonPath || '.openclaw/chat/latest.json',
    },
    github: {
      ...(config.deliveries?.github || {}),
      enabled: !summaryOnly,
      mode,
      autoCreate,
      draftPullRequests: true,
      proposalBranchPrefix: config?.actions?.proposalBranchPrefix || 'openclaw/proposals',
    },
    slack: {
      ...(config.deliveries?.slack || {}),
      enabled: channels.some((channel) => channel.type === 'slack'),
      webhookEnv: channels.find((channel) => channel.type === 'slack')?.webhookEnv || config.deliveries?.slack?.webhookEnv || 'SLACK_WEBHOOK_URL',
    },
    webhook: {
      ...(config.deliveries?.webhook || {}),
      enabled: channels.some((channel) => channel.type === 'webhook'),
      urlEnv: channels.find((channel) => channel.type === 'webhook')?.urlEnv || config.deliveries?.webhook?.urlEnv || 'OPENCLAW_WEBHOOK_URL',
      method: 'POST',
      headers: config.deliveries?.webhook?.headers || {},
    },
    discord: {
      ...(config.deliveries?.discord || {}),
      enabled: channels.some((channel) => channel.type === 'command'),
      command: channels.find((channel) => channel.type === 'command')?.command || config.deliveries?.discord?.command || 'node scripts/discord-openclaw-bridge.mjs send --stdin',
    },
  };
  config.notifications = {
    ...(config.notifications || {}),
    connectorHealth: {
      ...(config.notifications?.connectorHealth || {}),
      enabled: true,
      channels: mergeNotificationChannels([], connectorHealthChannels),
    },
    growthRun: {
      ...(config.notifications?.growthRun || {}),
      enabled: true,
      channels: mergeNotificationChannels([], channels),
    },
  };

  return config;
}

async function askGitHubArtifactDetails(rl, config) {
  const githubEnabled = Boolean(
    config?.actions?.autoCreateIssues ||
      config?.actions?.autoCreatePullRequests ||
      config?.deliveries?.github?.enabled ||
      config?.deliveries?.github?.autoCreate,
  );

  config.project = {
    ...(config.project || {}),
    githubRepo: '',
    repoRoot: config.project?.repoRoot || '.',
    outFile: config.project?.outFile || 'data/openclaw-growth-engineer/issues.generated.json',
    maxIssues: Number(config.project?.maxIssues || 4),
    titlePrefix: config.project?.titlePrefix || '[Growth]',
    labels: Array.isArray(config.project?.labels) && config.project.labels.length > 0
      ? config.project.labels
      : ['ai-growth', 'autogenerated', 'product'],
  };

  if (!githubEnabled) {
    return config;
  }

  process.stdout.write('\nGitHub repo scope is not pinned by the wizard. OpenClaw/Hermes infers it from OPENCLAW_GITHUB_REPO, the local git remote, or runtime context.\n');
  const customize = await askYesNo(
    rl,
    'Customize GitHub issue/PR limits, labels, or chart attachment settings?',
    false,
  );
  if (!customize) {
    config.charting = {
      ...(config.charting || {}),
      enabled: config.charting?.enabled === true,
      command: config.charting?.command || null,
    };
    return config;
  }

  const labelsRaw = await ask(
    rl,
    'GitHub labels for created issues/PRs',
    config.project.labels.join(','),
  );
  config.project.labels = labelsRaw
    .split(',')
    .map((value) => value.trim())
    .filter(Boolean);
  config.project.maxIssues = Number.parseInt(
    await ask(rl, 'Maximum GitHub artifacts per run', String(config.project.maxIssues || 4)),
    10,
  ) || 4;
  config.project.titlePrefix = await ask(rl, 'GitHub artifact title prefix', config.project.titlePrefix || '[Growth]');

  const enableCharting = await askYesNo(
    rl,
    'Attach generated charts to GitHub artifacts when useful?',
    config.charting?.enabled === true,
  );
  config.charting = {
    ...(config.charting || {}),
    enabled: enableCharting,
    command: enableCharting
      ? await ask(rl, 'Optional chart command override', config.charting?.command || '')
      : null,
  };
  return config;
}

async function askIntervalConfig(rl, config) {
  printSection('Schedule and analysis depth', [
    'The runner wakes up often, but larger reviews only run on their daily/weekly/monthly cadence.',
    'Connector health checks are separate and default to every 6 hours.',
  ]);
  const currentSchedule = config?.schedule || {};
  const usageMode = await askToolUsage(rl);
  const intervalMinutes = Number.parseInt(
    await ask(rl, 'Growth runner wake-up interval in minutes', String(currentSchedule.intervalMinutes || DEFAULT_GROWTH_INTERVAL_MINUTES)),
    10,
  ) || DEFAULT_GROWTH_INTERVAL_MINUTES;
  const connectorHealthCheckIntervalMinutes = Number.parseInt(
    await ask(
      rl,
      'Connector health check interval in minutes',
      String(currentSchedule.connectorHealthCheckIntervalMinutes || DEFAULT_CONNECTOR_HEALTH_INTERVAL_MINUTES),
    ),
    10,
  ) || DEFAULT_CONNECTOR_HEALTH_INTERVAL_MINUTES;
  const cadences = await askCadencePlan(rl);

  config.schedule = {
    ...currentSchedule,
    intervalMinutes,
    connectorHealthCheckIntervalMinutes,
    skipIfNoDataChange: currentSchedule.skipIfNoDataChange !== false,
    skipIfIssueSetUnchanged: currentSchedule.skipIfIssueSetUnchanged !== false,
    cadences,
  };
  config.actions = {
    ...(config.actions || {}),
    usageMode,
  };
  return config;
}

async function askOutputsAndIntervalsConfig(rl, config) {
  const withIntervals = await askIntervalConfig(rl, config);
  const withOutput = await askOutputConfig(rl, withIntervals);
  return await askGitHubArtifactDetails(rl, withOutput);
}

async function askInputSourceConfig(rl, config, configPath) {
  const healthByConnector = await withConnectorHealthLoading((onProgress) =>
    getConnectorPickerHealth(configPath, onProgress),
  );
  const selected = await askConnectorSelectionWithHealth(
    rl,
    healthByConnector,
    getInputChannelInitialSelection(config),
    {
      introTitle: 'Input channels',
      introDetail: null,
      actionTitle: 'Select input channels',
      helpText: 'Use Up/Down to move, Space to toggle channels, A to toggle all channels, Enter to continue.',
      mode: 'input',
    },
  );
  config.sources = buildSourceConfigFromInputChannels(selected, config.sources || {});
  return config;
}

async function writeOpenClawJobManifest(configPath, config) {
  const manifestPath = path.resolve('.openclaw/jobs/openclaw-growth-engineer.json');
  const displayConfigPath = path.relative(process.cwd(), configPath) || configPath;
  const intervalMinutes = Math.max(1, Number(config?.schedule?.intervalMinutes || DEFAULT_GROWTH_INTERVAL_MINUTES));
  const connectorHealthCheckIntervalMinutes = Math.max(
    1,
    Number(config?.schedule?.connectorHealthCheckIntervalMinutes || DEFAULT_CONNECTOR_HEALTH_INTERVAL_MINUTES),
  );
  const actionMode = config?.actions?.mode || config?.deliveries?.github?.mode || 'issue';
  const growthRunCommand = getGrowthRunCommand(config, displayConfigPath);
  const connectorHealthCommand = getConnectorHealthCommand(config, displayConfigPath);
  const manifest = {
    version: 1,
    generatedAt: new Date().toISOString(),
    managedBy: 'openclaw-growth-wizard',
    agentPolicy: {
      openClawCanRunGrowthJobs: true,
      openClawCanEditGrowthCadences: true,
      openClawCanEditOutputDelivery: true,
      openClawCanEditConnectors: true,
      openClawCanEditConnectorSecrets: false,
      connectorChanges: 'OpenClaw may read and modify non-secret connector config such as enabled flags, source commands, project/app mappings, and source priorities. Use `node scripts/openclaw-growth-wizard.mjs --connectors` for API keys or other connector secrets; never write secret values into config files, manifests, issues, PRs, or chat output.',
      secretAccessMode: config?.security?.connectorSecrets?.mode || 'local-user-file',
      secretPolicy: config?.security?.connectorSecrets?.mode === 'isolated-runner'
        ? 'OpenClaw must use the allowlisted sudo wrapper commands and must not read the persisted secret file.'
        : 'Secrets are persisted in a local chmod 600 file. This protects against other OS users, not against the same OS user.',
    },
    jobs: [
      {
        key: 'connector-health',
        kind: 'health-check',
        intervalMinutes: connectorHealthCheckIntervalMinutes,
        command: connectorHealthCommand,
        notificationPolicy: 'once_per_unhealthy_incident_until_recovery_or_changed_fingerprint',
      },
      {
        key: 'growth-runner',
        kind: 'growth-analysis',
        intervalMinutes,
        command: growthRunCommand,
        outputMode: actionMode,
        cadences: Array.isArray(config?.schedule?.cadences) ? config.schedule.cadences : [],
      },
    ],
  };
  await writeJsonFile(manifestPath, manifest);
  return manifestPath;
}

async function main() {
  await loadOpenClawGrowthSecrets();
  const args = parseArgs(process.argv.slice(2));
  await maybeSelfUpdateFromClawHub(args);
  if (args.connectorWizard) {
    await runConnectorSetupWizard(args);
    return;
  }

  const configPath = path.resolve(args.out);

  if (!process.stdin.isTTY || !process.stdout.isTTY) {
    throw new Error('Wizard requires an interactive terminal.');
  }

  const rl = createInterface({ input: process.stdin, output: process.stdout });
  try {
    printWizardHeader();

    const goal = await askWizardGoal(rl);
    if (goal === 'connectors') {
      rl.close();
      await runConnectorSetupWizard({ ...args, connectorWizard: true });
      return;
    }
    if (goal === 'intervals') {
      const config = await askIntervalConfig(rl, await loadEditableConfig(configPath));
      const secretAccess = await askSecretAccessModel(rl, configPath, config);
      await writeJsonFile(configPath, config);
      const manifestPath = await writeOpenClawJobManifest(configPath, config);
      process.stdout.write(`\nSaved schedule config: ${configPath}\n`);
      process.stdout.write(`Saved OpenClaw job manifest: ${manifestPath}\n`);
      printSecretRunnerKitInstructions(secretAccess.kit);
      process.stdout.write('OpenClaw can run and update growth jobs plus non-secret connector config from the manifest; connector API keys stay behind the connector wizard.\n');
      return;
    }
    if (goal === 'outputs_intervals') {
      const config = await askOutputsAndIntervalsConfig(rl, await loadEditableConfig(configPath));
      const secretAccess = await askSecretAccessModel(rl, configPath, config);
      await writeJsonFile(configPath, config);
      const manifestPath = await writeOpenClawJobManifest(configPath, config);
      process.stdout.write(`\nSaved output and interval config: ${configPath}\n`);
      process.stdout.write(`Saved OpenClaw job manifest: ${manifestPath}\n`);
      printSecretRunnerKitInstructions(secretAccess.kit);
      process.stdout.write('Daily checks prioritize Sentry and production anomalies; larger cadences analyze all configured projects and connectors.\n');
      return;
    }
    let config = await loadEditableConfig(configPath);
    config.version = Number(config.version || 7);
    config.generatedAt = new Date().toISOString();

    config = await askInputSourceConfig(rl, config, configPath);
    config = await askIntervalConfig(rl, config);
    config = await askOutputConfig(rl, config);
    config = await askGitHubArtifactDetails(rl, config);

    const secretAccess = await askSecretAccessModel(rl, configPath, config);

    await ensureDirForFile(configPath);
    await fs.writeFile(configPath, JSON.stringify(config, null, 2), 'utf8');
    const manifestPath = await writeOpenClawJobManifest(configPath, config);

    process.stdout.write(`\nSaved config: ${configPath}\n`);
    process.stdout.write(`Saved OpenClaw job manifest: ${manifestPath}\n`);
    printSecretRunnerKitInstructions(secretAccess.kit);
    process.stdout.write('\nNext steps:\n');
    process.stdout.write(`1) Set secrets in OpenClaw secret store (env var names in config.secrets)\n`);
    process.stdout.write(`2) Run once: node scripts/openclaw-growth-runner.mjs --config ${configPath}\n`);
    process.stdout.write(
      `3) Run interval loop: node scripts/openclaw-growth-runner.mjs --config ${configPath} --loop\n`,
    );
  } finally {
    rl.close();
  }
}

main().catch((error) => {
  process.stderr.write(`${error instanceof Error ? error.message : String(error)}\n`);
  process.exitCode = error instanceof WizardAbortError ? error.exitCode : 1;
});
