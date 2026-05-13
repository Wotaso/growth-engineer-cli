#!/usr/bin/env node

import { existsSync, promises as fs } from 'node:fs';
import path from 'node:path';
import process from 'node:process';
import { createHash } from 'node:crypto';
import { spawn } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import {
  getActionMode,
  getAllSourceEntries,
  getGitHubArtifactModes,
  getGitHubRequirementText,
  shouldAutoCreateGitHubArtifact,
} from './openclaw-growth-shared.mjs';
import { applyOpenClawSecretRefs, loadOpenClawGrowthSecrets } from './openclaw-growth-env.mjs';

const DEFAULT_CONFIG_PATH = 'data/openclaw-growth-engineer/config.json';
const DEFAULT_STATE_PATH = 'data/openclaw-growth-engineer/state.json';
const DEFAULT_RUNTIME_DIR = 'data/openclaw-growth-engineer/runtime';
const DEFAULT_CONNECTOR_HEALTH_INTERVAL_MINUTES = 360;
const SELF_UPDATE_INTERVAL_MS = 24 * 60 * 60 * 1000;
const RUNTIME_DIR = path.dirname(fileURLToPath(import.meta.url));
const DEFAULT_CADENCES = [
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
      'Compare against recent baselines across Sentry/GlitchTip, AnalyticsCLI, RevenueCat, ASC, feedback, release metadata, memory/state, and recent code changes. If the finding is critical, produce the exact fix or next debugging step and prefer a GitHub issue or draft PR when GitHub write access is configured; otherwise hand off via OpenClaw chat. Do not invent generic growth ideas.',
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
      'Choose one to three high-confidence improvements with evidence, expected KPI movement, likely code/store surfaces, owner-ready next steps, and verification plan. Create GitHub issues or draft PR proposals only when the evidence is specific enough. Kill or adjust experiments without signal.',
  },
  {
    key: 'monthly',
    title: 'Monthly deep product, business, and code review',
    intervalDays: 30,
    criticalOnly: false,
    focusAreas: ['conversion', 'paywall', 'retention', 'marketing', 'onboarding', 'codebase'],
    sourcePriorities: ['analytics', 'revenuecat', 'asc_cli', 'feedback', 'sentry', 'github'],
    objective:
      'Compare all configured projects month-over-month: MRR, trial conversion, churn, acquisition channel quality, store/listing conversion, retention, review themes, feature usage, crash totals, and codebase changes.',
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
      'Find structural constraints and durable opportunities, not small UI tweaks. Tie recommendations to cohort behavior, monetization, reviews, channel quality, and shipped changes.',
  },
  {
    key: 'six_months',
    title: 'Six-month instrumentation and growth-system audit',
    intervalDays: 182,
    criticalOnly: false,
    focusAreas: ['retention', 'conversion', 'paywall', 'marketing', 'general'],
    sourcePriorities: ['analytics', 'revenuecat', 'asc_cli', 'feedback', 'sentry'],
    objective:
      'Audit connector coverage, SDK instrumentation, event taxonomy, data reliability, data memory, growth loops, and whether product/code strategy still matches the best users across configured projects.',
    instructions:
      'Prioritize measurement fixes and system changes that make future analysis more trustworthy. Identify stale events, missing attribution, weak identity, broken feedback loops, and misleading dashboards.',
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
      'Use the full year of memory, releases, revenue, acquisition, reviews, code changes, and cohort behavior. Produce a strategic operating plan with specific experiments and stop-doing decisions.',
  },
];

type ShellResult = {
  ok: boolean;
  code: number | null;
  stdout: string;
  stderr: string;
};

function parseArgs(argv) {
  const args = {
    config: DEFAULT_CONFIG_PATH,
    state: DEFAULT_STATE_PATH,
    loop: false,
    noSelfUpdate: false,
  };
  for (let i = 0; i < argv.length; i += 1) {
    const token = argv[i];
    const next = argv[i + 1];
    if (token === '--') {
      continue;
    } else if (token === '--config') {
      args.config = next;
      i += 1;
    } else if (token === '--state') {
      args.state = next;
      i += 1;
    } else if (token === '--loop') {
      args.loop = true;
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
OpenClaw Growth Runner

Usage:
  node scripts/openclaw-growth-runner.mjs [--config <file>] [--state <file>] [--loop]

Options:
  --no-self-update   Skip the ClawHub skill update check for this run

Default config: ${DEFAULT_CONFIG_PATH}
Default state:  ${DEFAULT_STATE_PATH}
`);
  process.exit(exitCode);
}

function resolveRuntimeScriptPath(scriptName) {
  const candidates = [
    path.join(RUNTIME_DIR, scriptName),
    path.resolve('scripts', scriptName),
    path.resolve('skills/openclaw-growth-engineer/scripts', scriptName),
  ];
  return candidates.find((candidate) => existsSync(candidate)) || path.join(RUNTIME_DIR, scriptName);
}

function nodeRuntimeScriptCommand(scriptName) {
  return `node ${quote(resolveRuntimeScriptPath(scriptName))}`;
}

function replaceLegacyRuntimeScriptCommand(command) {
  const trimmed = String(command || '').trim();
  if (!trimmed) return trimmed;
  return trimmed.replace(
    /^node\s+scripts\/(export-analytics-summary\.mjs|export-revenuecat-summary\.mjs|export-sentry-summary\.mjs|export-asc-summary\.mjs|openclaw-growth-engineer\.mjs|openclaw-growth-status\.mjs|openclaw-growth-preflight\.mjs|openclaw-growth-runner\.mjs)(?=\s|$)/,
    (_match, scriptName) => nodeRuntimeScriptCommand(scriptName),
  );
}

function commandHasConfigArg(command) {
  return /(?:^|\s)--config(?:=|\s|$)/.test(String(command || ''));
}

function commandShouldReceiveActiveConfig(command) {
  return /(?:^|\s)(?:node\s+)?(?:\S*\/)?(?:export-analytics-summary|export-revenuecat-summary|export-sentry-summary|export-asc-summary)\.mjs(?:\s|$)/.test(
    String(command || ''),
  );
}

function withActiveConfigArg(command, configPath) {
  const trimmed = String(command || '').trim();
  if (!trimmed || !configPath || commandHasConfigArg(trimmed) || !commandShouldReceiveActiveConfig(trimmed)) {
    return trimmed;
  }
  return `${trimmed} --config ${quote(configPath)}`;
}

async function readJson(filePath): Promise<any> {
  const raw = await fs.readFile(filePath, 'utf8');
  return JSON.parse(raw);
}

async function readJsonOptional(filePath, fallback) {
  try {
    return await readJson(filePath);
  } catch {
    return fallback;
  }
}

async function ensureDir(dirPath) {
  await fs.mkdir(dirPath, { recursive: true });
}

function sha256(input) {
  return createHash('sha256').update(input).digest('hex');
}

function stableStringify(value) {
  return JSON.stringify(value, Object.keys(value).sort(), 2);
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function isTruthyEnv(value) {
  return ['1', 'true', 'yes', 'y', 'on'].includes(String(value || '').trim().toLowerCase());
}

function isFalseyEnv(value) {
  return ['0', 'false', 'no', 'n', 'off'].includes(String(value || '').trim().toLowerCase());
}

async function commandExists(commandName) {
  const result = await runShellCommand(`command -v ${quote(commandName)} >/dev/null 2>&1`, 10_000);
  return result.ok;
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

async function inferGitHubRepo(config) {
  const configured = String(config?.project?.githubRepo || '').trim();
  if (isConfiguredGitHubRepo(configured)) return configured;

  const explicit = String(process.env.OPENCLAW_GITHUB_REPO || '').trim();
  if (isConfiguredGitHubRepo(explicit)) return explicit;

  const repoRoot = path.resolve(config?.project?.repoRoot || '.');
  const remoteResult = await runShellCommand('git config --get remote.origin.url', 10_000, {
    cwd: repoRoot,
  });
  if (!remoteResult.ok) return '';
  return parseGitHubRepoFromRemote(remoteResult.stdout.trim()) || '';
}

async function filesHaveSameContent(leftPath, rightPath) {
  try {
    const [left, right] = await Promise.all([fs.readFile(leftPath), fs.readFile(rightPath)]);
    return left.equals(right);
  } catch {
    return false;
  }
}

async function shouldRunSelfUpdate(workspaceRoot, force) {
  if (force) return true;
  const statePath = path.join(workspaceRoot, 'data/openclaw-growth-engineer/self-update.json');
  const state = await readJsonOptional(statePath, null);
  const lastCheckedAt = Date.parse(String(state?.lastCheckedAt || ''));
  return !Number.isFinite(lastCheckedAt) || Date.now() - lastCheckedAt > SELF_UPDATE_INTERVAL_MS;
}

async function writeSelfUpdateState(workspaceRoot, value) {
  const statePath = path.join(workspaceRoot, 'data/openclaw-growth-engineer/self-update.json');
  await fs.mkdir(path.dirname(statePath), { recursive: true });
  await fs.writeFile(
    statePath,
    `${JSON.stringify({ version: 1, checkedAt: new Date().toISOString(), ...value }, null, 2)}\n`,
    'utf8',
  );
}

async function rerunCurrentProcessWithoutSelfUpdate() {
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

async function maybeSelfUpdateFromClawHub(args) {
  if (args.noSelfUpdate) return false;
  if (isTruthyEnv(process.env.OPENCLAW_GROWTH_SKIP_SELF_UPDATE)) return false;
  if (isTruthyEnv(process.env.OPENCLAW_GROWTH_DISABLE_SELF_UPDATE)) return false;
  if (isFalseyEnv(process.env.OPENCLAW_GROWTH_SELF_UPDATE)) return false;

  const workspaceRoot = process.cwd();
  const skillOriginPath = path.join(workspaceRoot, 'skills/openclaw-growth-engineer/.clawhub/origin.json');
  if (!existsSync(skillOriginPath)) return false;
  if (!(await commandExists('npx'))) return false;

  const force = String(process.env.OPENCLAW_GROWTH_SELF_UPDATE || '').trim().toLowerCase() === 'always';
  if (!(await shouldRunSelfUpdate(workspaceRoot, force))) return false;

  const beforeOrigin = await readJsonOptional(skillOriginPath, null);
  const beforeVersion = String(beforeOrigin?.installedVersion || '');
  process.stdout.write('Checking for OpenClaw Growth Engineer skill updates...\n');
  const updateResult = await runShellCommand(
    'npx -y clawhub --no-input --dir skills update openclaw-growth-engineer --force',
    120_000,
  );
  const afterOrigin = await readJsonOptional(skillOriginPath, null);
  const afterVersion = String(afterOrigin?.installedVersion || beforeVersion || '');
  const workspaceRunnerPath = path.resolve(process.argv[1] || 'scripts/openclaw-growth-runner.mjs');
  const skillRunnerPath = path.join(workspaceRoot, 'skills/openclaw-growth-engineer/scripts/openclaw-growth-runner.mjs');
  const runtimeOutdated = !(await filesHaveSameContent(workspaceRunnerPath, skillRunnerPath));

  await writeSelfUpdateState(workspaceRoot, {
    lastCheckedAt: new Date().toISOString(),
    ok: updateResult.ok,
    previousVersion: beforeVersion || null,
    installedVersion: afterVersion || null,
  }).catch(() => {});

  if (!updateResult.ok) {
    const detail = String(updateResult.stderr || updateResult.stdout || 'update failed').trim().split(/\r?\n/).pop();
    process.stdout.write(`Skill update check skipped: ${detail}\n`);
    return false;
  }
  if ((!afterVersion || afterVersion === beforeVersion) && !runtimeOutdated) return false;

  process.stdout.write(
    afterVersion && afterVersion !== beforeVersion
      ? `Updated OpenClaw Growth Engineer skill ${beforeVersion || 'unknown'} -> ${afterVersion}. Refreshing workspace runtime...\n`
      : 'Refreshing workspace runtime from the installed OpenClaw Growth Engineer skill...\n',
  );
  const bootstrapResult = await runShellCommand(
    'bash skills/openclaw-growth-engineer/scripts/bootstrap-openclaw-workspace.sh',
    60_000,
  );
  if (!bootstrapResult.ok) {
    process.stdout.write('Workspace runtime refresh failed; continuing with current process.\n');
    return false;
  }
  process.stdout.write('Restarting runner with refreshed runtime...\n');
  const code = await rerunCurrentProcessWithoutSelfUpdate();
  process.exit(code ?? 0);
}

function resolveShellCommand(): string {
  const candidates = [
    process.env.OPENCLAW_SHELL,
    process.env.SHELL,
    '/bin/zsh',
    '/bin/bash',
    '/usr/bin/bash',
    '/bin/sh',
    '/usr/bin/sh',
  ].filter((value): value is string => typeof value === 'string' && value.trim().length > 0);

  for (const candidate of candidates) {
    if (existsSync(candidate)) {
      return candidate;
    }
  }

  return 'sh';
}

function runShellCommand(command, timeoutMs = 120_000, options: { cwd?: string; input?: string } = {}): Promise<ShellResult> {
  return new Promise((resolve) => {
    const child = spawn(resolveShellCommand(), ['-c', command], {
      stdio: options.input === undefined ? ['ignore', 'pipe', 'pipe'] : ['pipe', 'pipe', 'pipe'],
      cwd: options.cwd,
    });
    let stdout = '';
    let stderr = '';
    let settled = false;
    const timer = setTimeout(() => {
      if (settled) return;
      settled = true;
      child.kill('SIGTERM');
      resolve({ ok: false, code: null, stdout, stderr: `${stderr}\nTimed out after ${timeoutMs}ms` });
    }, timeoutMs);

    child.stdout.on('data', (chunk) => {
      stdout += String(chunk);
    });
    child.stderr.on('data', (chunk) => {
      stderr += String(chunk);
    });
    if (options.input !== undefined) {
      child.stdin.end(options.input);
    }
    child.on('close', (code) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      resolve({
        ok: code === 0,
        code,
        stdout,
        stderr,
      });
    });
  });
}

function getSecretName(config, key, fallback) {
  const value = config?.secrets?.[key];
  return typeof value === 'string' && value.trim() ? value.trim() : fallback;
}

async function assertHardRequirements(config) {
  const missing = [];
  const analyticsSource = config?.sources?.analytics;
  const actionMode = getActionMode(config);
  const requiresGitHubDelivery = shouldAutoCreateGitHubArtifact(config);
  if (!analyticsSource || analyticsSource.enabled === false) {
    missing.push('sources.analytics must be enabled');
  }

  const analyticscliExists = await commandExists('analyticscli');
  if (!analyticscliExists) {
    missing.push('analyticscli binary is required');
  }

  if (requiresGitHubDelivery) {
    const githubRepo = String(config?.project?.githubRepo || '').trim();
    const githubTokenEnv = getSecretName(config, 'githubTokenEnv', 'GITHUB_TOKEN');
    if (githubRepo && !process.env[githubTokenEnv]) {
      missing.push(`${githubTokenEnv} env var is required (${getGitHubRequirementText(actionMode)})`);
    }
  }

  if (missing.length > 0) {
    const message = `Hard requirements missing:\n- ${missing.join('\n- ')}`;
    throw new Error(message);
  }
}

function getProjectCommandCwd(config) {
  const repoRoot = String(config?.project?.repoRoot || '').trim();
  return repoRoot ? path.resolve(repoRoot) : process.cwd();
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

function getConnectorHealthIntervalMinutes(config) {
  const configured = Number(config?.schedule?.connectorHealthCheckIntervalMinutes);
  return Number.isFinite(configured) && configured > 0
    ? configured
    : DEFAULT_CONNECTOR_HEALTH_INTERVAL_MINUTES;
}

function isDue(lastCheckedAt, intervalMinutes) {
  if (!lastCheckedAt) return true;
  const last = Date.parse(String(lastCheckedAt));
  if (!Number.isFinite(last)) return true;
  return Date.now() - last >= intervalMinutes * 60_000;
}

function normalizeCadenceKey(value) {
  const normalized = String(value || '')
    .trim()
    .toLowerCase()
    .replace(/[\s-]+/g, '_');
  if (['3_months', 'three_months', 'quarter', 'quarterly'].includes(normalized)) return 'quarterly';
  if (['6_months', 'six_months', 'half_year', 'half_yearly'].includes(normalized)) return 'six_months';
  if (['1y', '1_year', 'one_year', 'annual', 'annually'].includes(normalized)) return 'yearly';
  return normalized;
}

function getCadenceDefinitions(config) {
  const configured = Array.isArray(config?.schedule?.cadences) ? config.schedule.cadences : [];
  const byKey = new Map(DEFAULT_CADENCES.map((cadence) => [cadence.key, { ...cadence }]));
  for (const cadence of configured) {
    if (!cadence || typeof cadence !== 'object') continue;
    const key = normalizeCadenceKey(cadence.key || cadence.id || cadence.label);
    if (!key) continue;
    const base: any = byKey.get(key) || { key };
    byKey.set(key, {
      ...base,
      ...cadence,
      key,
      enabled: cadence.enabled !== false,
      focusAreas: Array.isArray(cadence.focusAreas) ? cadence.focusAreas : base.focusAreas || [],
      sourcePriorities: Array.isArray(cadence.sourcePriorities)
        ? cadence.sourcePriorities
        : base.sourcePriorities || [],
    });
  }
  return ([...byKey.values()] as any[]).filter((cadence) => cadence.enabled !== false);
}

function cadenceIsDue(cadence, state) {
  const lastRanAt = state?.cadences?.[cadence.key]?.lastRanAt;
  const intervalDays = Number(cadence.intervalDays || 1);
  if (!lastRanAt) return true;
  const last = Date.parse(String(lastRanAt));
  if (!Number.isFinite(last)) return true;
  return Date.now() - last >= Math.max(1, intervalDays) * 24 * 60 * 60 * 1000;
}

function getDueCadences(config, state) {
  const due = getCadenceDefinitions(config).filter((cadence) => cadenceIsDue(cadence, state));
  if (due.length > 0) return due;
  const daily = getCadenceDefinitions(config).find((cadence) => cadence.key === 'daily');
  return daily ? [daily] : [];
}

function markCadencesRan(state, cadences, ranAt) {
  const nextCadences = { ...(state?.cadences || {}) };
  for (const cadence of cadences) {
    nextCadences[cadence.key] = {
      ...(nextCadences[cadence.key] || {}),
      lastRanAt: ranAt,
      title: cadence.title,
    };
  }
  return nextCadences;
}

function getConnectorEntries(statusPayload) {
  return Object.entries(statusPayload?.connectors || {}).map(([key, value]: [string, any]) => ({
    key,
    status: String(value?.status || 'unknown'),
    detail: String(value?.detail || ''),
    nextAction: typeof value?.nextAction === 'string' ? value.nextAction : null,
  }));
}

function getUnhealthyConfiguredConnectors(statusPayload) {
  return getConnectorEntries(statusPayload).filter((entry) =>
    ['blocked', 'partial', 'unknown'].includes(entry.status),
  );
}

function getConnectedConnectorKeys(statusPayload) {
  return getConnectorEntries(statusPayload)
    .filter((entry) => entry.status === 'connected')
    .map((entry) => entry.key)
    .sort();
}

function buildConnectorHealthFingerprint(unhealthyConnectors) {
  return sha256(
    unhealthyConnectors
      .map((entry) => `${entry.key}|${entry.status}|${entry.detail}|${entry.nextAction || ''}`)
      .sort()
      .join('\n'),
  );
}

function humanConnectorName(key) {
  if (key === 'analyticscli') return 'AnalyticsCLI';
  if (key === 'appStoreConnect') return 'App Store Connect';
  if (key === 'revenuecat') return 'RevenueCat';
  if (key === 'sentry') return 'Sentry';
  if (key === 'github') return 'GitHub';
  return key;
}

function buildConnectorHealthAlert(statusPayload, unhealthyConnectors) {
  const lines = [
    `OpenClaw Growth connector health needs attention (${new Date().toISOString()}).`,
    `Config: ${statusPayload?.configPath || DEFAULT_CONFIG_PATH}`,
    '',
    'Unhealthy connector(s):',
  ];

  for (const entry of unhealthyConnectors) {
    lines.push(`- ${humanConnectorName(entry.key)}: ${entry.status} - ${entry.detail}`);
    if (entry.nextAction) {
      lines.push(`  Next: ${entry.nextAction}`);
    }
    if (entry.key === 'appStoreConnect' && entry.status === 'partial') {
      lines.push(
        '  Note: ASC web analytics uses a user-owned web session. If Apple expires it after a few hours, refresh it with `asc web auth login`; API-key ASC auth cannot replace this web session.',
      );
    }
  }

  lines.push('');
  lines.push('Do not send secrets through chat or social channels. Refresh credentials only in the host terminal or secret store.');
  return `${lines.join('\n')}\n`;
}

async function writeConnectorHealthAlert(runtimeDir, message, statusPayload, unhealthyConnectors, fingerprint) {
  const alertDir = path.join(runtimeDir, 'connector-health');
  await ensureDir(alertDir);
  const markdownPath = path.join(alertDir, 'latest.md');
  const jsonPath = path.join(alertDir, 'latest.json');
  await fs.writeFile(markdownPath, message, 'utf8');
  await fs.writeFile(
    jsonPath,
    JSON.stringify(
      {
        generatedAt: new Date().toISOString(),
        fingerprint,
        unhealthyConnectors,
        status: statusPayload,
      },
      null,
      2,
    ),
    'utf8',
  );
  return { markdownPath, jsonPath };
}

function getConnectorHealthChannels(config) {
  const configuredChannels = Array.isArray(config?.notifications?.connectorHealth?.channels)
    ? config.notifications.connectorHealth.channels.filter((channel) => channel?.enabled !== false)
    : [];
  if (configuredChannels.length > 0) return configuredChannels;

  const channels = [];
  const deliveries = config?.deliveries || {};
  if (deliveries.openclawChat?.enabled) {
    channels.push({
      type: 'openclaw-chat',
      label: 'openclaw_chat',
      markdownPath: deliveries.openclawChat.connectorHealthMarkdownPath || deliveries.openclawChat.markdownPath,
      jsonPath: deliveries.openclawChat.connectorHealthJsonPath || deliveries.openclawChat.jsonPath,
    });
  }
  if (deliveries.slack?.enabled) {
    channels.push({
      type: 'slack',
      label: 'slack',
      webhookEnv: deliveries.slack.webhookEnv || 'SLACK_WEBHOOK_URL',
    });
  }
  if (deliveries.webhook?.enabled) {
    channels.push({
      type: 'webhook',
      label: 'webhook',
      urlEnv: deliveries.webhook.urlEnv || 'OPENCLAW_WEBHOOK_URL',
      method: deliveries.webhook.method || 'POST',
      headers: deliveries.webhook.headers || {},
    });
  }
  if (deliveries.discord?.enabled) {
    channels.push({
      type: 'command',
      label: 'discord',
      command: deliveries.discord.command || 'node scripts/discord-openclaw-bridge.mjs send --stdin',
    });
  }
  return channels;
}

async function writeConfiguredOpenClawChatAlert(configPath, channel, message, statusPayload, unhealthyConnectors, fingerprint) {
  const baseDir = path.dirname(path.resolve(configPath));
  const markdownPath = path.resolve(baseDir, channel.markdownPath || '.openclaw/chat/connector-health.md');
  const jsonPath = path.resolve(baseDir, channel.jsonPath || '.openclaw/chat/connector-health.json');
  await fs.mkdir(path.dirname(markdownPath), { recursive: true });
  await fs.mkdir(path.dirname(jsonPath), { recursive: true });
  await fs.writeFile(markdownPath, message, 'utf8');
  await fs.writeFile(
    jsonPath,
    JSON.stringify(
      {
        channel: channel.label || 'openclaw_chat',
        generatedAt: new Date().toISOString(),
        fingerprint,
        unhealthyConnectors,
        status: statusPayload,
      },
      null,
      2,
    ),
    'utf8',
  );
  return {
    sent: true,
    target: channel.label || 'openclaw_chat',
    detail: `wrote ${markdownPath} and ${jsonPath}`,
  };
}

async function sendSlackConnectorHealthAlert(channel, message) {
  const webhookEnv = channel.webhookEnv || 'SLACK_WEBHOOK_URL';
  const webhookUrl = process.env[webhookEnv];
  if (!webhookUrl) {
    return { sent: false, target: channel.label || 'slack', detail: `${webhookEnv} not set` };
  }
  const response = await fetch(webhookUrl, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ text: message }),
  });
  return {
    sent: response.ok,
    target: channel.label || 'slack',
    detail: response.ok ? `HTTP ${response.status}` : `HTTP ${response.status}: ${await response.text()}`,
  };
}

async function sendWebhookConnectorHealthAlert(channel, message, statusPayload, unhealthyConnectors, fingerprint) {
  const urlEnv = channel.urlEnv || channel.webhookEnv || 'OPENCLAW_WEBHOOK_URL';
  const webhookUrl = process.env[urlEnv];
  if (!webhookUrl) {
    return { sent: false, target: channel.label || 'webhook', detail: `${urlEnv} not set` };
  }
  const response = await fetch(webhookUrl, {
    method: channel.method || 'POST',
    headers: {
      'content-type': 'application/json',
      ...(channel.headers || {}),
    },
    body: JSON.stringify({
      type: 'openclaw.connector_health',
      generatedAt: new Date().toISOString(),
      text: message,
      fingerprint,
      unhealthyConnectors,
      status: statusPayload,
    }),
  });
  return {
    sent: response.ok,
    target: channel.label || 'webhook',
    detail: response.ok ? `HTTP ${response.status}` : `HTTP ${response.status}: ${await response.text()}`,
  };
}

async function sendCommandConnectorHealthAlert(channel, message) {
  if (!channel.command) {
    return { sent: false, target: channel.label || 'command', detail: 'command not configured' };
  }
  const result = await runShellCommand(String(channel.command), 60_000, { input: message });
  return {
    sent: result.ok,
    target: channel.label || 'command',
    detail: result.ok ? result.stdout.trim() : result.stderr.trim() || result.stdout.trim() || `exit ${result.code}`,
  };
}

async function deliverConnectorHealthAlert({ config, configPath, message, statusPayload, unhealthyConnectors, fingerprint }) {
  const channels = getConnectorHealthChannels(config);
  if (config?.notifications?.connectorHealth?.enabled === false) {
    return [{ sent: false, target: 'notifications', detail: 'connector health notifications disabled' }];
  }
  if (channels.length === 0) {
    return [{ sent: false, target: 'none', detail: 'no connector health notification channels configured' }];
  }

  const results = [];
  for (const channel of channels) {
    try {
      if (channel.type === 'openclaw-chat') {
        results.push(await writeConfiguredOpenClawChatAlert(configPath, channel, message, statusPayload, unhealthyConnectors, fingerprint));
      } else if (channel.type === 'slack') {
        results.push(await sendSlackConnectorHealthAlert(channel, message));
      } else if (channel.type === 'webhook') {
        results.push(await sendWebhookConnectorHealthAlert(channel, message, statusPayload, unhealthyConnectors, fingerprint));
      } else if (channel.type === 'command') {
        results.push(await sendCommandConnectorHealthAlert(channel, message));
      } else {
        results.push({ sent: false, target: channel.label || String(channel.type || 'unknown'), detail: 'unsupported channel type' });
      }
    } catch (error) {
      results.push({
        sent: false,
        target: channel.label || String(channel.type || 'unknown'),
        detail: error instanceof Error ? error.message : String(error),
      });
    }
  }
  return results;
}

function getGrowthRunChannels(config) {
  const configuredChannels = Array.isArray(config?.notifications?.growthRun?.channels)
    ? config.notifications.growthRun.channels.filter((channel) => channel?.enabled !== false)
    : [];
  if (configuredChannels.length > 0) return configuredChannels;

  const channels = [];
  const deliveries = config?.deliveries || {};
  if (deliveries.openclawChat?.enabled) {
    channels.push({
      type: 'openclaw-chat',
      label: 'openclaw_chat',
      markdownPath: deliveries.openclawChat.growthRunMarkdownPath || '.openclaw/chat/growth-summary.md',
      jsonPath: deliveries.openclawChat.growthRunJsonPath || '.openclaw/chat/growth-summary.json',
    });
  }
  if (deliveries.slack?.enabled) {
    channels.push({
      type: 'slack',
      label: 'slack',
      webhookEnv: deliveries.slack.webhookEnv || 'SLACK_WEBHOOK_URL',
    });
  }
  if (deliveries.webhook?.enabled) {
    channels.push({
      type: 'webhook',
      label: 'webhook',
      urlEnv: deliveries.webhook.urlEnv || 'OPENCLAW_WEBHOOK_URL',
      method: deliveries.webhook.method || 'POST',
      headers: deliveries.webhook.headers || {},
    });
  }
  if (deliveries.discord?.enabled) {
    channels.push({
      type: 'command',
      label: 'discord',
      command: deliveries.discord.command || 'node scripts/discord-openclaw-bridge.mjs send --stdin',
    });
  }
  return channels;
}

async function readChartAttachments(chartManifestPath) {
  if (!chartManifestPath) return [];
  try {
    const manifest = await readJson(chartManifestPath);
    return Array.isArray(manifest?.charts)
      ? manifest.charts
          .map((chart) => ({
            signalId: String(chart.signal_id || chart.signalId || '').trim(),
            filePath: String(chart.file_path || chart.filePath || '').trim(),
            caption: String(chart.caption || chart.title || 'Data chart').trim(),
          }))
          .filter((chart) => chart.filePath)
      : [];
  } catch {
    return [];
  }
}

function buildGrowthRunSummaryMessage({ issuesPayload, activeCadences, sourceFiles, createdGitHubArtifact, charts = [] }) {
  const issueCount = Number(issuesPayload?.issue_count || 0);
  const cadenceNames = activeCadences.length > 0
    ? activeCadences.map((cadence) => cadence.title || cadence.key).join(', ')
    : 'ad-hoc growth pass';
  const sourceNames = Object.keys(sourceFiles || {}).sort().join(', ') || 'none';
  const lines = [
    `OpenClaw Growth run finished (${new Date().toISOString()}).`,
    `Cadence: ${cadenceNames}`,
    `Sources inspected: ${sourceNames}`,
    `Generated proposals: ${issueCount}`,
  ];
  if (issuesPayload?.summary) {
    lines.push(`Summary: ${issuesPayload.summary}`);
  }
  if (createdGitHubArtifact) {
    lines.push('GitHub artifact creation was attempted for the generated proposals.');
  }
  if (charts.length > 0) {
    lines.push(`Charts generated: ${charts.length}`);
    for (const chart of charts.slice(0, 5)) {
      lines.push(`- ${chart.caption}: ${chart.filePath}`);
    }
  }
  const issues = Array.isArray(issuesPayload?.issues) ? issuesPayload.issues.slice(0, 3) : [];
  if (issues.length > 0) {
    lines.push('');
    lines.push('Top findings:');
    for (const issue of issues) {
      lines.push(`- ${issue.title} (${issue.priority || 'medium'}, ${issue.area || 'general'})`);
    }
  }
  lines.push('');
  lines.push('No secrets were included. Use the generated issue drafts or OpenClaw chat handoff for details.');
  return `${lines.join('\n')}\n`;
}

async function writeConfiguredOpenClawChatGrowthSummary(configPath, channel, message, issuesPayload, activeCadences, fingerprint, charts) {
  const baseDir = path.dirname(path.resolve(configPath));
  const markdownPath = path.resolve(baseDir, channel.markdownPath || '.openclaw/chat/growth-summary.md');
  const jsonPath = path.resolve(baseDir, channel.jsonPath || '.openclaw/chat/growth-summary.json');
  await fs.mkdir(path.dirname(markdownPath), { recursive: true });
  await fs.mkdir(path.dirname(jsonPath), { recursive: true });
  await fs.writeFile(markdownPath, message, 'utf8');
  await fs.writeFile(
    jsonPath,
    JSON.stringify(
      {
        channel: channel.label || 'openclaw_chat',
        generatedAt: new Date().toISOString(),
        fingerprint,
        activeCadences,
        issueCount: Number(issuesPayload?.issue_count || 0),
        issues: Array.isArray(issuesPayload?.issues) ? issuesPayload.issues : [],
        charts,
        attachments: charts.map((chart) => ({
          type: 'image/png',
          path: chart.filePath,
          caption: chart.caption,
        })),
      },
      null,
      2,
    ),
    'utf8',
  );
  return {
    sent: true,
    target: channel.label || 'openclaw_chat',
    detail: `wrote ${markdownPath} and ${jsonPath}`,
  };
}

async function sendSlackGrowthSummary(channel, message) {
  const webhookEnv = channel.webhookEnv || 'SLACK_WEBHOOK_URL';
  const webhookUrl = process.env[webhookEnv];
  if (!webhookUrl) {
    return { sent: false, target: channel.label || 'slack', detail: `${webhookEnv} not set` };
  }
  const response = await fetch(webhookUrl, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ text: message }),
  });
  return {
    sent: response.ok,
    target: channel.label || 'slack',
    detail: response.ok ? `HTTP ${response.status}` : `HTTP ${response.status}: ${await response.text()}`,
  };
}

async function sendWebhookGrowthSummary(channel, message, issuesPayload, activeCadences, fingerprint, charts) {
  const urlEnv = channel.urlEnv || channel.webhookEnv || 'OPENCLAW_WEBHOOK_URL';
  const webhookUrl = process.env[urlEnv];
  if (!webhookUrl) {
    return { sent: false, target: channel.label || 'webhook', detail: `${urlEnv} not set` };
  }
  const response = await fetch(webhookUrl, {
    method: channel.method || 'POST',
    headers: {
      'content-type': 'application/json',
      ...(channel.headers || {}),
    },
    body: JSON.stringify({
      type: 'openclaw.growth_run',
      generatedAt: new Date().toISOString(),
      text: message,
      fingerprint,
      activeCadences,
      issueCount: Number(issuesPayload?.issue_count || 0),
      issues: Array.isArray(issuesPayload?.issues) ? issuesPayload.issues : [],
      charts,
      attachments: charts.map((chart) => ({
        type: 'image/png',
        path: chart.filePath,
        caption: chart.caption,
      })),
    }),
  });
  return {
    sent: response.ok,
    target: channel.label || 'webhook',
    detail: response.ok ? `HTTP ${response.status}` : `HTTP ${response.status}: ${await response.text()}`,
  };
}

async function sendCommandGrowthSummary(channel, message) {
  if (!channel.command) {
    return { sent: false, target: channel.label || 'command', detail: 'command not configured' };
  }
  const result = await runShellCommand(String(channel.command), 60_000, { input: message });
  return {
    sent: result.ok,
    target: channel.label || 'command',
    detail: result.ok ? result.stdout.trim() : result.stderr.trim() || result.stdout.trim() || `exit ${result.code}`,
  };
}

async function deliverGrowthRunSummary({
  config,
  configPath,
  issuesPayload,
  activeCadences,
  sourceFiles,
  fingerprint,
  createdGitHubArtifact,
  chartManifestPath,
}) {
  if (config?.notifications?.growthRun?.enabled === false) {
    return [{ sent: false, target: 'notifications', detail: 'growth run notifications disabled' }];
  }
  const channels = getGrowthRunChannels(config);
  if (channels.length === 0) {
    return [{ sent: false, target: 'none', detail: 'no growth run notification channels configured' }];
  }
  const charts = await readChartAttachments(chartManifestPath);
  const message = buildGrowthRunSummaryMessage({
    issuesPayload,
    activeCadences,
    sourceFiles,
    createdGitHubArtifact,
    charts,
  });
  const results = [];
  for (const channel of channels) {
    try {
      if (channel.type === 'openclaw-chat') {
        results.push(
          await writeConfiguredOpenClawChatGrowthSummary(
            configPath,
            channel,
            message,
            issuesPayload,
            activeCadences,
            fingerprint,
            charts,
          ),
        );
      } else if (channel.type === 'slack') {
        results.push(await sendSlackGrowthSummary(channel, message));
      } else if (channel.type === 'webhook') {
        results.push(await sendWebhookGrowthSummary(channel, message, issuesPayload, activeCadences, fingerprint, charts));
      } else if (channel.type === 'command') {
        results.push(await sendCommandGrowthSummary(channel, message));
      } else {
        results.push({ sent: false, target: channel.label || String(channel.type || 'unknown'), detail: 'unsupported channel type' });
      }
    } catch (error) {
      results.push({
        sent: false,
        target: channel.label || String(channel.type || 'unknown'),
        detail: error instanceof Error ? error.message : String(error),
      });
    }
  }
  return results;
}

async function maybeRunConnectorHealthCheck({ config, configPath, state, statePath, runtimeDir }) {
  const healthState = state?.connectorHealth || {};
  const intervalMinutes = getConnectorHealthIntervalMinutes(config);
  if (!isDue(healthState.lastCheckedAt, intervalMinutes)) {
    return state;
  }

  await ensureDir(runtimeDir);
  const statusCommand = [
    nodeRuntimeScriptCommand('openclaw-growth-status.mjs'),
    '--config',
    quote(configPath),
    '--timeout-ms',
    '15000',
    '--json',
  ].join(' ');
  const checkedAt = new Date().toISOString();
  const statusResult = await runShellCommand(statusCommand, 90_000);
  const statusPayload = parseJsonFromStdout(statusResult.stdout);
  if (!statusPayload) {
    const nextState = {
      ...state,
      connectorHealth: {
        ...healthState,
        lastCheckedAt: checkedAt,
        lastError: statusResult.stderr.trim() || statusResult.stdout.trim() || 'connector status returned no JSON',
      },
    };
    await fs.mkdir(path.dirname(statePath), { recursive: true });
    await fs.writeFile(statePath, JSON.stringify(nextState, null, 2), 'utf8');
    return nextState;
  }

  const unhealthyConnectors = getUnhealthyConfiguredConnectors(statusPayload);
  const connectedConnectors = getConnectedConnectorKeys(statusPayload);
  const fingerprint = buildConnectorHealthFingerprint(unhealthyConnectors);
  const nextHealthState: Record<string, any> = {
    ...healthState,
    lastCheckedAt: checkedAt,
    lastStatusOk: unhealthyConnectors.length === 0,
    lastFingerprint: fingerprint,
    connectedConnectors,
    lastError: null,
  };
  const previousIncidentFingerprint = healthState.lastStatusOk === false
    ? healthState.activeIncidentFingerprint || healthState.lastAlertedFingerprint || null
    : null;
  if (unhealthyConnectors.length === 0) {
    nextHealthState.activeIncidentFingerprint = null;
    if (healthState.lastStatusOk === false) {
      nextHealthState.lastRecoveredAt = checkedAt;
    }
  } else {
    nextHealthState.activeIncidentFingerprint = fingerprint;
  }

  if (
    unhealthyConnectors.length > 0 &&
    previousIncidentFingerprint !== fingerprint
  ) {
    const message = buildConnectorHealthAlert(statusPayload, unhealthyConnectors);
    const paths = await writeConnectorHealthAlert(runtimeDir, message, statusPayload, unhealthyConnectors, fingerprint);
    const deliveries = await deliverConnectorHealthAlert({
      config,
      configPath,
      message,
      statusPayload,
      unhealthyConnectors,
      fingerprint,
    });
    nextHealthState.lastAlertedAt = checkedAt;
    nextHealthState.lastAlertedFingerprint = fingerprint;
    nextHealthState.lastAlertMarkdownPath = paths.markdownPath;
    nextHealthState.lastAlertJsonPath = paths.jsonPath;
    nextHealthState.lastAlertDeliveries = deliveries;
  }

  const nextState = {
    ...state,
    connectorHealth: nextHealthState,
  };
  await fs.mkdir(path.dirname(statePath), { recursive: true });
  await fs.writeFile(statePath, JSON.stringify(nextState, null, 2), 'utf8');
  return nextState;
}

function buildIssueFingerprint(issuesPayload) {
  const titles = Array.isArray(issuesPayload?.issues)
    ? issuesPayload.issues.map((issue) => `${issue.title}|${issue.priority}|${issue.area}`).sort()
    : [];
  return sha256(titles.join('\n'));
}

async function runAnalyzer({
  config,
  runtimeDir,
  sourceFiles,
  createGitHubArtifact,
  githubArtifactMode = getActionMode(config),
  chartManifestPath,
  cadencePlanPath,
}) {
  await ensureDir(runtimeDir);

  if (!sourceFiles.analytics) {
    throw new Error('Analytics source is required (enable and configure `sources.analytics`).');
  }

  const outFile = path.resolve(config.project?.outFile || 'data/openclaw-growth-engineer/issues.generated.json');
  const args = [
    resolveRuntimeScriptPath('openclaw-growth-engineer.mjs'),
    '--analytics',
    sourceFiles.analytics,
    '--repo-root',
    path.resolve(config.project?.repoRoot || '.'),
    '--out',
    outFile,
    '--max-issues',
    String(config.project?.maxIssues || 4),
    '--title-prefix',
    String(config.project?.titlePrefix || '[Growth]'),
  ];

  if (sourceFiles.revenuecat) {
    args.push('--revenuecat', sourceFiles.revenuecat);
  }
  if (sourceFiles.sentry) {
    args.push('--sentry', sourceFiles.sentry);
  }
  if (sourceFiles.feedback) {
    args.push('--feedback', sourceFiles.feedback);
  }
  for (const source of getAllSourceEntries(config).filter((entry) => !entry.builtIn)) {
    if (sourceFiles[source.key]) {
      args.push('--source', `${source.key}=${sourceFiles[source.key]}`);
    }
  }
  if (createGitHubArtifact) {
    const repo = String(config.project?.githubRepo || '').trim();
    args.push(
      githubArtifactMode === 'pull_request' ? '--create-pull-requests' : '--create-issues',
      '--repo',
      repo,
    );
    if (githubArtifactMode === 'pull_request') {
      args.push('--allow-proposal-pull-requests');
    }
    const labels = Array.isArray(config.project?.labels) ? config.project.labels : [];
    if (labels.length > 0) {
      args.push('--labels', labels.join(','));
    }
    if (config.actions?.proposalBranchPrefix) {
      args.push('--branch-prefix', String(config.actions.proposalBranchPrefix));
    }
    if (config.actions?.draftPullRequests === false) {
      args.push('--no-draft-pull-requests');
    }
  }
  if (chartManifestPath) {
    args.push('--chart-manifest', chartManifestPath);
  }
  if (cadencePlanPath) {
    args.push('--cadence-plan', cadencePlanPath);
  }

  const analyzer = await runShellCommand(`node ${args.map(quote).join(' ')}`);
  if (!analyzer.ok) {
    throw new Error(`Analyzer failed: ${analyzer.stderr || `exit ${analyzer.code}`}`);
  }

  const issuesPayload = await readJson(outFile);
  return {
    outFile,
    sourceFiles,
    issuesPayload,
    analyzerStdout: analyzer.stdout.trim(),
  };
}

async function maybeGenerateCharts({ config, payloads, runtimeDir }) {
  if (!config.charting?.enabled) {
    return null;
  }
  const analyticsPayload = payloads.analytics;
  if (!analyticsPayload) {
    return null;
  }

  await ensureDir(runtimeDir);
  const chartsDir = path.join(runtimeDir, 'charts');
  await ensureDir(chartsDir);
  const analyticsForChartsPath = path.join(runtimeDir, 'analytics_for_charts.json');
  const manifestPath = path.join(chartsDir, 'manifest.json');
  await fs.writeFile(analyticsForChartsPath, JSON.stringify(analyticsPayload, null, 2), 'utf8');

  const defaultCommand = [
    'python3',
    resolveRuntimeScriptPath('openclaw-growth-charts.py'),
    '--analytics',
    analyticsForChartsPath,
    '--out-dir',
    chartsDir,
    '--manifest',
    manifestPath,
  ]
    .map(quote)
    .join(' ');

  const command = String(config.charting?.command || defaultCommand);
  const result = await runShellCommand(command);
  if (!result.ok) {
    process.stderr.write(
      `[${new Date().toISOString()}] Chart generation failed: ${result.stderr || `exit ${result.code}`}\n`,
    );
    return null;
  }
  return manifestPath;
}

function quote(value) {
  if (/^[a-zA-Z0-9_./:-]+$/.test(value)) {
    return value;
  }
  return `'${String(value).replace(/'/g, `'\\''`)}'`;
}

function computeSourceHashes(sourcePayloadMap) {
  const hashes = {};
  for (const [key, value] of Object.entries(sourcePayloadMap)) {
    hashes[key] = sha256(stableStringify(value));
  }
  return hashes;
}

function normalizeLookback(value, fallback = '30d') {
  const normalized = String(value || fallback).trim();
  return /^[0-9]+[dhm]$/.test(normalized) ? normalized : fallback;
}

function commandHasExplicitTimeBounds(command) {
  return /(^|\s)--(?:since|until|last)\b/.test(String(command));
}

function resolveCursorAwareCommand(command, sourceConfig, cursorState) {
  const rawCommand = String(command || '').trim();
  if (!rawCommand) {
    return rawCommand;
  }

  if (sourceConfig?.cursorMode !== 'auto_since_last_fetch') {
    return rawCommand;
  }

  if (commandHasExplicitTimeBounds(rawCommand)) {
    return rawCommand;
  }

  const lastCollectedAt = String(cursorState?.lastCollectedAt || '').trim();
  if (lastCollectedAt) {
    return `${rawCommand} --since ${quote(lastCollectedAt)}`;
  }

  const lookback = normalizeLookback(sourceConfig?.initialLookback, '30d');
  return `${rawCommand} --last ${quote(lookback)}`;
}

async function resolveSourcePayloadWithCursor(sourceConfig, sourceName, cursorState, commandCwd = process.cwd(), configPath = null) {
  if (!sourceConfig || sourceConfig.enabled === false) {
    return {
      payload: null,
      nextCursor: cursorState || null,
      resolvedCommand: null,
    };
  }

  if (sourceConfig.mode === 'command') {
    if (!sourceConfig.command) {
      throw new Error(`Source "${sourceName}" has mode=command but no command configured.`);
    }
    const resolvedCommand = resolveCursorAwareCommand(
      withActiveConfigArg(replaceLegacyRuntimeScriptCommand(sourceConfig.command), configPath),
      sourceConfig,
      cursorState,
    );
    const result = await runShellCommand(String(resolvedCommand), 120_000, { cwd: commandCwd });
    if (!result.ok) {
      throw new Error(`Source "${sourceName}" command failed: ${result.stderr || `exit ${result.code}`}`);
    }
    const fetchedAt = new Date().toISOString();
    try {
      return {
        payload: JSON.parse(result.stdout),
        nextCursor:
          sourceConfig.cursorMode === 'auto_since_last_fetch'
            ? {
                lastCollectedAt: fetchedAt,
                updatedAt: fetchedAt,
                lastCommand: resolvedCommand,
              }
            : cursorState || null,
        resolvedCommand,
      };
    } catch {
      throw new Error(`Source "${sourceName}" returned non-JSON output.`);
    }
  }

  if (!sourceConfig.path) {
    throw new Error(`Source "${sourceName}" has mode=file but no path configured.`);
  }

  return {
    payload: await readJson(path.resolve(String(sourceConfig.path))),
    nextCursor: cursorState || null,
    resolvedCommand: null,
  };
}

async function loadSourcePayloads(config, state, configPath) {
  const payloads = {};
  const sourceCursors = { ...(state?.sourceCursors || {}) };
  const commandCwd = getProjectCommandCwd(config);
  for (const source of getAllSourceEntries(config)) {
    const currentCursor = sourceCursors[source.key] || null;
    const result = await resolveSourcePayloadWithCursor(source, source.key, currentCursor, commandCwd, configPath);
    const payload = result.payload;
    if (payload) {
      payloads[source.key] = payload;
    }
    if (result.nextCursor) {
      sourceCursors[source.key] = result.nextCursor;
    }
  }
  return {
    payloads,
    sourceCursors,
  };
}

async function materializeSourceFiles(config, payloads, runtimeDir) {
  await ensureDir(runtimeDir);
  const sourceFiles: Record<string, string> = {};
  for (const source of getAllSourceEntries(config)) {
    const payload = payloads[source.key];
    if (!payload) {
      continue;
    }
    const filePath = path.join(runtimeDir, `${source.key}.json`);
    await fs.writeFile(filePath, JSON.stringify(payload, null, 2), 'utf8');
    sourceFiles[source.key] = filePath;
  }
  return sourceFiles;
}

function hasSourceChanges(previousHashes, currentHashes) {
  const allKeys = new Set([...Object.keys(previousHashes || {}), ...Object.keys(currentHashes || {})]);
  for (const key of allKeys) {
    if ((previousHashes || {})[key] !== (currentHashes || {})[key]) {
      return true;
    }
  }
  return false;
}

async function runOnce(configPath, statePath) {
  const config = await readJson(configPath);
  await applyOpenClawSecretRefs(config);
  const inferredGitHubRepo = await inferGitHubRepo(config);
  if (inferredGitHubRepo) {
    config.project = {
      ...(config.project || {}),
      githubRepo: inferredGitHubRepo,
    };
  }
  await assertHardRequirements(config);
  const state = await readJsonOptional(statePath, {
    sourceHashes: {},
    lastIssueFingerprint: null,
    lastRunAt: null,
    sourceCursors: {},
  });
  const runtimeDir = path.resolve(DEFAULT_RUNTIME_DIR);
  const stateAfterHealthCheck = await maybeRunConnectorHealthCheck({
    config,
    configPath,
    state,
    statePath,
    runtimeDir,
  });
  const activeCadences = getDueCadences(config, stateAfterHealthCheck);

  const { payloads, sourceCursors } = await loadSourcePayloads(config, stateAfterHealthCheck, configPath);
  const currentHashes = computeSourceHashes(payloads);
  const changed = hasSourceChanges(stateAfterHealthCheck.sourceHashes, currentHashes);

  if (!changed && config.schedule?.skipIfNoDataChange !== false) {
    process.stdout.write(`[${new Date().toISOString()}] No data changes. Skip run.\n`);
    await fs.mkdir(path.dirname(statePath), { recursive: true });
    await fs.writeFile(
      statePath,
      JSON.stringify(
        {
          ...stateAfterHealthCheck,
          sourceHashes: currentHashes,
          sourceCursors,
          lastRunAt: new Date().toISOString(),
          skippedReason: 'no_data_change',
        },
        null,
        2,
      ),
      'utf8',
    );
    return;
  }

  const githubArtifactModes = getGitHubArtifactModes(config).filter((mode) =>
    shouldAutoCreateGitHubArtifact(config, mode),
  );
  const createGitHubArtifact =
    githubArtifactModes.length > 0 && Boolean(String(config.project?.githubRepo || '').trim());
  const sourceFiles = await materializeSourceFiles(config, payloads, runtimeDir);
  const cadencePlanPath = path.join(runtimeDir, 'cadence-plan.json');
  await fs.writeFile(
    cadencePlanPath,
    JSON.stringify(
      {
        generatedAt: new Date().toISOString(),
        cadences: activeCadences,
      },
      null,
      2,
    ),
    'utf8',
  );
  const chartManifestPath = await maybeGenerateCharts({
    config,
    payloads,
    runtimeDir,
  });
  const dryRun = await runAnalyzer({
    config,
    runtimeDir,
    sourceFiles,
    createGitHubArtifact: false,
    chartManifestPath,
    cadencePlanPath,
  });

  const issueFingerprint = buildIssueFingerprint(dryRun.issuesPayload);
  const unchangedIssueSet = issueFingerprint === stateAfterHealthCheck.lastIssueFingerprint;

  if (unchangedIssueSet && config.schedule?.skipIfIssueSetUnchanged !== false) {
    process.stdout.write(`[${new Date().toISOString()}] Issue set unchanged. Skip GitHub creation.\n`);
    await fs.mkdir(path.dirname(statePath), { recursive: true });
    await fs.writeFile(
      statePath,
      JSON.stringify(
        {
          ...stateAfterHealthCheck,
          sourceHashes: currentHashes,
          sourceCursors,
          lastIssueFingerprint: issueFingerprint,
          lastRunAt: new Date().toISOString(),
          lastOutFile: dryRun.outFile,
          cadences: markCadencesRan(stateAfterHealthCheck, activeCadences, new Date().toISOString()),
          skippedReason: 'issue_set_unchanged',
        },
        null,
        2,
      ),
      'utf8',
    );
    return;
  }

  const shouldCreateGitHubArtifact = createGitHubArtifact && Number(dryRun.issuesPayload?.issue_count || 0) > 0;
  if (shouldCreateGitHubArtifact) {
    for (const githubArtifactMode of githubArtifactModes) {
      await runAnalyzer({
        config,
        runtimeDir,
        sourceFiles,
        createGitHubArtifact: true,
        githubArtifactMode,
        chartManifestPath,
        cadencePlanPath,
      });
    }
    process.stdout.write(
      `[${new Date().toISOString()}] Created GitHub ${githubArtifactModes.map((mode) => (mode === 'pull_request' ? 'pull requests' : 'issues')).join(' and ')}.\n`,
    );
  } else {
    process.stdout.write(
      `[${new Date().toISOString()}] Drafts generated only (${getActionMode(config)} auto-create disabled).\n`,
    );
  }

  await fs.mkdir(path.dirname(statePath), { recursive: true });
  await fs.writeFile(
    statePath,
    JSON.stringify(
      {
        ...stateAfterHealthCheck,
        sourceHashes: currentHashes,
        sourceCursors,
        lastIssueFingerprint: issueFingerprint,
        lastRunAt: new Date().toISOString(),
        lastOutFile: dryRun.outFile,
        cadences: markCadencesRan(stateAfterHealthCheck, activeCadences, new Date().toISOString()),
        lastGrowthRunNotifications: await deliverGrowthRunSummary({
          config,
          configPath,
          issuesPayload: dryRun.issuesPayload,
          activeCadences,
          sourceFiles,
          fingerprint: issueFingerprint,
          createdGitHubArtifact: shouldCreateGitHubArtifact,
          chartManifestPath,
        }),
        skippedReason: null,
      },
      null,
      2,
    ),
    'utf8',
  );
}

async function main() {
  await loadOpenClawGrowthSecrets();
  const args = parseArgs(process.argv.slice(2));
  await maybeSelfUpdateFromClawHub(args);
  const configPath = path.resolve(args.config);
  const statePath = path.resolve(args.state);

  if (!args.loop) {
    await runOnce(configPath, statePath);
    return;
  }

  const config = await readJson(configPath);
  const intervalMinutes = Math.max(1, Number(config.schedule?.intervalMinutes || 1440));
  process.stdout.write(`Starting loop. Interval: ${intervalMinutes} minute(s)\n`);
  while (true) {
    try {
      await maybeSelfUpdateFromClawHub(args);
      await runOnce(configPath, statePath);
    } catch (error) {
      process.stderr.write(
        `[${new Date().toISOString()}] Run failed: ${error instanceof Error ? error.message : String(error)}\n`,
      );
    }
    await sleep(intervalMinutes * 60_000);
  }
}

main().catch((error) => {
  process.stderr.write(`${error instanceof Error ? error.message : String(error)}\n`);
  process.exitCode = 1;
});
