#!/usr/bin/env node

import { existsSync, promises as fs } from 'node:fs';
import path from 'node:path';
import process from 'node:process';
import { spawn } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { applyOpenClawSecretRefs, loadOpenClawGrowthSecrets } from './openclaw-growth-env.mjs';

const DEFAULT_CONFIG_PATH = 'data/openclaw-growth-engineer/config.json';
const DEFAULT_TIMEOUT_MS = 15_000;
const ASC_WEB_AUTH_REFRESH_COMMAND =
  'Set ASC_WEB_APPLE_ID to the Apple Account email, then run: asc web auth login --apple-id "$ASC_WEB_APPLE_ID" && asc web auth status --output json --pretty';
const RUNTIME_DIR = path.dirname(fileURLToPath(import.meta.url));

type ShellResult = {
  ok: boolean;
  code: number | null;
  stdout: string;
  stderr: string;
};

function printHelpAndExit(exitCode, reason = null) {
  if (reason) {
    process.stderr.write(`${reason}\n\n`);
  }
  process.stdout.write(`
OpenClaw Growth Connector Status

Usage:
  node scripts/openclaw-growth-status.mjs [options]

Options:
  --config <file>      Config path (default: ${DEFAULT_CONFIG_PATH})
  --timeout-ms <ms>    Live check timeout in milliseconds (default: ${DEFAULT_TIMEOUT_MS})
  --json               Print JSON (default)
  --progress-json      Emit machine-readable connector progress events on stderr
  --help, -h           Show help
`);
  process.exit(exitCode);
}

function parseArgs(argv) {
  const args = {
    config: DEFAULT_CONFIG_PATH,
    timeoutMs: DEFAULT_TIMEOUT_MS,
    json: true,
    progressJson: false,
  };

  for (let index = 0; index < argv.length; index += 1) {
    const token = argv[index];
    const next = argv[index + 1];
    if (token === '--') {
      continue;
    } else if (token === '--config') {
      args.config = next || args.config;
      index += 1;
    } else if (token === '--timeout-ms') {
      const parsed = Number.parseInt(String(next || ''), 10);
      if (!Number.isFinite(parsed) || parsed <= 0) {
        printHelpAndExit(1, `Invalid value for --timeout-ms: ${String(next || '')}`);
      }
      args.timeoutMs = parsed;
      index += 1;
    } else if (token === '--json') {
      args.json = true;
    } else if (token === '--progress-json') {
      args.progressJson = true;
    } else if (token === '--help' || token === '-h') {
      printHelpAndExit(0);
    } else {
      printHelpAndExit(1, `Unknown argument: ${token}`);
    }
  }

  return args;
}

function quote(value) {
  if (/^[a-zA-Z0-9_./:=@-]+$/.test(String(value))) {
    return String(value);
  }
  return `'${String(value).replace(/'/g, `'\\''`)}'`;
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

function emitProgress(enabled, event) {
  if (!enabled) return;
  process.stderr.write(`OPENCLAW_PROGRESS ${JSON.stringify(event)}\n`);
}

function runShell(
  command,
  options: { cwd?: string; timeoutMs?: number; onStderrLine?: (line: string) => void } = {},
): Promise<ShellResult> {
  return new Promise((resolve) => {
    const child = spawn('/bin/sh', ['-lc', command], {
      cwd: options.cwd || process.cwd(),
      env: process.env,
      stdio: ['ignore', 'pipe', 'pipe'],
    });
    let stdout = '';
    let stderr = '';
    let stderrBuffer = '';
    const timer = setTimeout(() => {
      child.kill('SIGTERM');
    }, options.timeoutMs || DEFAULT_TIMEOUT_MS);
    child.stdout.on('data', (chunk) => {
      stdout += String(chunk);
    });
    child.stderr.on('data', (chunk) => {
      const text = String(chunk);
      stderr += text;
      if (options.onStderrLine) {
        stderrBuffer += text;
        const lines = stderrBuffer.split(/\r?\n/);
        stderrBuffer = lines.pop() || '';
        for (const line of lines) options.onStderrLine(line);
      }
    });
    child.on('error', (error) => {
      clearTimeout(timer);
      resolve({ ok: false, code: null, stdout, stderr: error.message });
    });
    child.on('close', (code) => {
      clearTimeout(timer);
      if (options.onStderrLine && stderrBuffer.trim()) options.onStderrLine(stderrBuffer.trim());
      resolve({ ok: code === 0, code, stdout, stderr });
    });
  });
}

async function readJson(filePath): Promise<any> {
  const raw = await fs.readFile(filePath, 'utf8');
  return JSON.parse(raw);
}

function checkByName(preflight, name) {
  return (Array.isArray(preflight?.checks) ? preflight.checks : []).find((check) => check?.name === name) || null;
}

function checksByPrefix(preflight, prefix) {
  return (Array.isArray(preflight?.checks) ? preflight.checks : []).filter((check) =>
    String(check?.name || '').startsWith(prefix),
  );
}

function connector(status, detail, extra: Record<string, unknown> = {}) {
  return {
    status,
    detail,
    ...extra,
  };
}

function isEnabled(source) {
  return Boolean(source && source.enabled !== false);
}

function isConfiguredRepo(value) {
  const repo = String(value || '').trim();
  return Boolean(repo && repo !== 'owner/repo' && /^[^/\s]+\/[^/\s]+$/.test(repo));
}

async function runPreflight(configPath, timeoutMs, progressJson = false) {
  const command = [
    nodeRuntimeScriptCommand('openclaw-growth-preflight.mjs'),
    '--config',
    quote(configPath),
    '--test-connections',
    '--timeout-ms',
    String(timeoutMs),
    '--json',
    ...(progressJson ? ['--progress-json'] : []),
  ].join(' ');
  const result = await runShell(command, {
    timeoutMs: Math.max(timeoutMs + 5_000, 20_000),
    onStderrLine: progressJson
      ? (line) => {
          if (line.startsWith('OPENCLAW_PROGRESS ')) process.stderr.write(`${line}\n`);
        }
      : undefined,
  });
  const output = result.stdout.trim();
  if (!output) {
    return {
      ok: false,
      error: result.stderr.trim() || 'preflight returned no JSON output',
      payload: null,
    };
  }
  try {
    return {
      ok: true,
      error: null,
      payload: JSON.parse(output),
    };
  } catch {
    return {
      ok: false,
      error: result.stderr.trim() || 'preflight returned invalid JSON',
      payload: null,
    };
  }
}

async function checkGitHub(config, timeoutMs) {
  const repo = String(config?.project?.githubRepo || '').trim();
  const hasRepo = isConfiguredRepo(repo);
  const token = String(process.env.GITHUB_TOKEN || '').trim();

  if (token && hasRepo) {
    const result = await runShell(`gh api ${quote(`repos/${repo}`)} >/dev/null`, { timeoutMs });
    if (result.ok) {
      return connector('connected', `GitHub token can read ${repo}`, { repo });
    }
    const fallback = await runShell(
      `curl -fsS -H ${quote('Accept: application/vnd.github+json')} -H ${quote(`Authorization: Bearer ${token}`)} ${quote(`https://api.github.com/repos/${repo}`)} >/dev/null`,
      { timeoutMs },
    );
    return fallback.ok
      ? connector('connected', `GitHub token can read ${repo}`, { repo })
      : connector('blocked', `GITHUB_TOKEN is set, but repo access check failed for ${repo}`, { repo });
  }

  if (hasRepo) {
    const ghStatus = await runShell('gh auth status >/dev/null 2>&1', { timeoutMs });
    if (ghStatus.ok) {
      const repoCheck = await runShell(`gh api ${quote(`repos/${repo}`)} >/dev/null`, { timeoutMs });
      return repoCheck.ok
        ? connector('connected', `gh auth can read ${repo}`, { repo })
        : connector('blocked', `gh is logged in, but cannot read ${repo}`, { repo });
    }
  }

  if (token && !hasRepo) {
    const authCheck = await runShell(
      `curl -fsS -H ${quote('Accept: application/vnd.github+json')} -H ${quote(`Authorization: Bearer ${token}`)} https://api.github.com/user >/dev/null`,
      { timeoutMs },
    );
    return authCheck.ok ? connector('connected', 'GITHUB_TOKEN is valid; repo selection is deferred per app/task', {
      repoScope: 'per_app_or_task',
    }) : connector('blocked', 'GITHUB_TOKEN is set, but GitHub auth check failed', {
      nextAction: 'Run: node scripts/openclaw-growth-wizard.mjs --connectors github.',
    });
  }
  return connector('not_connected', hasRepo ? 'No GITHUB_TOKEN or gh auth found' : 'project.githubRepo is not configured', {
    nextAction: 'Run: node scripts/openclaw-growth-wizard.mjs --connectors github.',
  });
}

function summarizeAnalytics(preflight, config) {
  if (!isEnabled(config?.sources?.analytics)) {
    return connector('blocked', 'AnalyticsCLI source is disabled');
  }
  const connection = checkByName(preflight, 'connection:analytics');
  const command = checkByName(preflight, 'connection:analytics-command');
  if (connection?.status === 'pass' && (!command || command.status === 'pass')) {
    return connector('connected', 'AnalyticsCLI auth and exporter smoke test passed');
  }
  if (connection?.status === 'pass' && command?.status !== 'pass') {
    return connector('partial', command?.detail || 'AnalyticsCLI auth passed, exporter smoke test did not pass');
  }
  return connector('blocked', connection?.detail || 'AnalyticsCLI connection was not verified');
}

function summarizeRevenueCat(preflight, config) {
  if (!isEnabled(config?.sources?.revenuecat)) {
    return connector('not_enabled', 'RevenueCat source is disabled');
  }
  const connection = checkByName(preflight, 'connection:revenuecat');
  if (connection?.status === 'pass') {
    return connector('connected', 'RevenueCat API key live check passed');
  }
  return connector('blocked', connection?.detail || 'RevenueCat connection was not verified');
}

function summarizeSentry(preflight, config) {
  if (!isEnabled(config?.sources?.sentry)) {
    return connector('not_enabled', 'Sentry source is disabled');
  }
  const accountConnections = checksByPrefix(preflight, 'connection:sentry:');
  const connection = checkByName(preflight, 'connection:sentry');
  const command = checkByName(preflight, 'connection:sentry-command');
  if (accountConnections.length > 0) {
    const failed = accountConnections.filter((entry) => entry.status !== 'pass');
    const accounts = accountConnections.map((entry) => ({
      id: String(entry.name || '').replace(/^connection:sentry:/, ''),
      status: entry.status === 'pass' ? 'connected' : 'blocked',
      detail: entry.detail,
    }));
    if (failed.length === 0 && (!command || command.status === 'pass')) {
      return connector('connected', `Sentry API checks passed for ${accountConnections.length} account(s)`, { accounts });
    }
    if (failed.length === 0 && command?.status !== 'pass') {
      return connector('partial', command?.detail || 'Sentry API checks passed, exporter smoke test did not pass', {
        accounts,
      });
    }
    return connector('blocked', failed[0]?.detail || 'One or more Sentry accounts failed connection checks', {
      accounts,
      nextAction: 'Verify each sources.sentry.accounts[] tokenEnv/baseUrl/org/projects entry, then rerun status.',
    });
  }
  if (connection?.status === 'pass' && (!command || command.status === 'pass')) {
    return connector('connected', command ? 'Sentry API and exporter smoke test passed' : 'Sentry API auth check passed');
  }
  if (connection?.status === 'pass' && command?.status !== 'pass') {
    return connector('partial', command?.detail || 'Sentry API auth passed, exporter smoke test did not pass');
  }
  return connector('blocked', connection?.detail || 'Sentry connection was not verified', {
    nextAction: 'Run: node scripts/openclaw-growth-wizard.mjs --connectors sentry.',
  });
}

async function summarizeAsc(preflight, config, timeoutMs) {
  const ascSources = checksByPrefix(preflight, 'connection:asc_cli');
  const ascConnection = ascSources[0] || null;
  const ascConfigured = (Array.isArray(config?.sources?.extra) ? config.sources.extra : []).some(
    (source) => source?.service === 'asc-cli' && source.enabled !== false,
  );
  if (!ascConfigured) {
    return connector('not_enabled', 'App Store Connect CLI source is disabled');
  }
  if (ascConnection?.status === 'pass') {
    const webAuth = await runShell('asc web auth status --output json', { timeoutMs });
    if (!webAuth.ok) {
      return connector('partial', 'ASC API exporter works, but ASC web analytics login is not verified', {
        appScope: 'all_accessible_apps',
        nextAction: ASC_WEB_AUTH_REFRESH_COMMAND,
      });
    }
    try {
      const payload = JSON.parse(webAuth.stdout || '{}');
      if (payload?.authenticated !== true) {
        return connector('partial', 'ASC API exporter works, but ASC web analytics is not logged in', {
          appScope: 'all_accessible_apps',
          nextAction: ASC_WEB_AUTH_REFRESH_COMMAND,
        });
      }
    } catch {
      return connector('partial', 'ASC API exporter works, but ASC web analytics status returned invalid JSON', {
        appScope: 'all_accessible_apps',
        nextAction: ASC_WEB_AUTH_REFRESH_COMMAND,
      });
    }
    return connector('connected', 'ASC exporter smoke test passed for accessible apps', {
      appScope: 'all_accessible_apps',
      webAnalytics: 'authenticated',
    });
  }
  return connector('blocked', ascConnection?.detail || 'ASC connection was not verified', {
    appScope: 'all_accessible_apps',
  });
}

async function main() {
  const secretsInfo = await loadOpenClawGrowthSecrets();
  const args = parseArgs(process.argv.slice(2));
  const configPath = path.resolve(args.config);
  const config = await readJson(configPath);
  await applyOpenClawSecretRefs(config);
  const preflight = await runPreflight(configPath, args.timeoutMs, args.progressJson);
  const preflightPayload = preflight.payload;
  emitProgress(args.progressJson, {
    phase: 'start',
    key: 'appStoreConnect',
    label: 'App Store Connect',
    detail: 'ASC API + web analytics auth',
  });

  const [githubStatus, ascStatus] = await Promise.all([
    checkGitHub(config, args.timeoutMs),
    preflightPayload
      ? summarizeAsc(preflightPayload, config, args.timeoutMs)
      : Promise.resolve(connector('unknown', preflight.error || 'Preflight did not run')),
  ]);

  const connectors = preflightPayload
    ? {
        analyticscli: summarizeAnalytics(preflightPayload, config),
        github: githubStatus,
        revenuecat: summarizeRevenueCat(preflightPayload, config),
        sentry: summarizeSentry(preflightPayload, config),
        appStoreConnect: ascStatus,
      }
    : {
        analyticscli: connector('unknown', preflight.error || 'Preflight did not run'),
        github: githubStatus,
        revenuecat: connector('unknown', preflight.error || 'Preflight did not run'),
        sentry: connector('unknown', preflight.error || 'Preflight did not run'),
        appStoreConnect: ascStatus,
      };
  emitProgress(args.progressJson, {
    phase: 'finish',
    key: 'appStoreConnect',
    label: 'App Store Connect',
    detail: connectors.appStoreConnect?.detail || 'ASC check complete',
    status: connectors.appStoreConnect?.status === 'connected' ? 'pass' : connectors.appStoreConnect?.status === 'partial' ? 'warn' : 'fail',
  });

  const values = Object.values(connectors);
  const allConnected = values.every((entry: any) => entry.status === 'connected');
  const connectorNextActions = values
    .map((entry: any) => entry.nextAction)
    .filter((value): value is string => typeof value === 'string' && value.trim().length > 0);
  const result = {
    ok: allConnected,
    configPath,
    secretsFileLoaded: secretsInfo.loaded,
    secretsFilePath: secretsInfo.filePath,
    connectors,
    nextAction: allConnected
      ? null
      : connectorNextActions[0] || 'Run the connector wizard for any connector whose status is blocked, partial, not_enabled, or not_connected.',
  };

  process.stdout.write(`${JSON.stringify(result, null, 2)}\n`);
  if (!result.ok) {
    process.exitCode = 1;
  }
}

main().catch((error) => {
  process.stderr.write(`${error instanceof Error ? error.message : String(error)}\n`);
  process.exitCode = 1;
});
