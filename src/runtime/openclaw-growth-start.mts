#!/usr/bin/env node

import { existsSync, promises as fs } from 'node:fs';
import path from 'node:path';
import process from 'node:process';
import { spawn } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { getActionMode, getDefaultSourceCommand } from './openclaw-growth-shared.mjs';
import { applyOpenClawSecretRefs, loadOpenClawGrowthSecrets } from './openclaw-growth-env.mjs';

const DEFAULT_CONFIG_PATH = 'data/openclaw-growth-engineer/config.json';
const DEFAULT_TEMPLATE_PATH = 'data/openclaw-growth-engineer/config.example.json';
const DEFAULT_HEARTBEAT_PATH = 'HEARTBEAT.md';
const HEARTBEAT_MARKER_START = '<!-- openclaw-growth-engineer:start -->';
const HEARTBEAT_MARKER_END = '<!-- openclaw-growth-engineer:end -->';
const ANALYTICSCLI_PACKAGE_SPEC = process.env.ANALYTICSCLI_CLI_PACKAGE || '@analyticscli/cli@preview';
const ANALYTICSCLI_NPM_PREFIX =
  process.env.ANALYTICSCLI_NPM_PREFIX ||
  (process.env.HOME ? path.join(process.env.HOME, '.local') : path.join(process.cwd(), '.analyticscli-npm'));
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
OpenClaw Growth Start

Bootstraps setup and first run in one deterministic flow:
1) Ensure config exists (auto-bootstrap from template when missing)
2) Run preflight
3) If preflight passes, run first pass

Usage:
  node scripts/openclaw-growth-start.mjs [options]

Options:
  --config <file>        Config path (default: ${DEFAULT_CONFIG_PATH})
  --project <id>         Optional AnalyticsCLI project ID pin for generated source commands
  --asc-app <id>         Optional ASC app ID filter (defaults to all accessible apps)
  --connectors <list>    Install/enable connector helpers (analytics,github,asc,revenuecat,sentry,all)
  --only-connectors <list>
                         Limit live preflight checks to analytics,github,asc,revenuecat,sentry
  --setup-only           Run bootstrap + preflight only (skip first run)
  --no-test-connections  Skip live API smoke checks in preflight
  --progress-json        Emit machine-readable setup progress to stderr
  --help, -h             Show help
`);
  process.exit(exitCode);
}

function parseArgs(argv) {
  const args = {
    config: DEFAULT_CONFIG_PATH,
    project: '',
    ascApp: '',
    run: true,
    testConnections: true,
    connectors: [],
    onlyConnectors: [],
    progressJson: false,
  };

  for (let i = 0; i < argv.length; i += 1) {
    const token = argv[i];
    const next = argv[i + 1];
    if (token === '--') {
      continue;
    } else if (token === '--config') {
      args.config = next || args.config;
      i += 1;
    } else if (token === '--project') {
      args.project = String(next || '').trim();
      i += 1;
    } else if (token === '--asc-app') {
      args.ascApp = String(next || '').trim();
      i += 1;
    } else if (token === '--connectors') {
      args.connectors = parseConnectorList(next || '');
      i += 1;
    } else if (token === '--only-connectors') {
      args.onlyConnectors = parseConnectorList(next || '');
      i += 1;
    } else if (token === '--setup-only') {
      args.run = false;
    } else if (token === '--no-test-connections') {
      args.testConnections = false;
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

function normalizeConnectorKey(value) {
  const normalized = String(value || '').trim().toLowerCase().replace(/[_\s]+/g, '-');
  if (!normalized) return null;
  if (normalized === 'all') return 'all';
  if (['analytics', 'analyticscli', 'product-analytics', 'events'].includes(normalized)) return 'analytics';
  if (['github', 'gh', 'github-code', 'codebase', 'code-access'].includes(normalized)) return 'github';
  if (['asc', 'asc-cli', 'app-store-connect', 'appstoreconnect', 'app-store'].includes(normalized)) return 'asc';
  if (['revenuecat', 'revenue-cat', 'rc', 'revenuecat-mcp'].includes(normalized)) return 'revenuecat';
  if (['sentry', 'sentry-api', 'sentry-mcp', 'crashes', 'errors', 'crash-reporting'].includes(normalized)) return 'sentry';
  return null;
}

function parseConnectorList(value) {
  if (!String(value || '').trim()) return [];

  const connectors = new Set();
  for (const entry of String(value).split(',')) {
    const connector = normalizeConnectorKey(entry);
    if (!connector) {
      printHelpAndExit(1, `Unknown connector: ${entry.trim()}. Use analytics, github, asc, revenuecat, sentry, or all.`);
    }
    if (connector === 'all') {
      connectors.add('analytics');
      connectors.add('github');
      connectors.add('asc');
      connectors.add('revenuecat');
      connectors.add('sentry');
    } else {
      connectors.add(connector);
    }
  }
  return [...connectors];
}

function quote(value) {
  if (/^[a-zA-Z0-9_./:-]+$/.test(String(value))) {
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

function getRuntimeSourceCommand(sourceName) {
  const normalized = String(sourceName || '').trim().toLowerCase();
  if (normalized === 'analytics' || normalized === 'analyticscli') {
    return nodeRuntimeScriptCommand('export-analytics-summary.mjs');
  }
  if (normalized === 'revenuecat' || normalized === 'revenue-cat') {
    return nodeRuntimeScriptCommand('export-revenuecat-summary.mjs');
  }
  if (normalized === 'sentry' || normalized === 'glitchtip') {
    return nodeRuntimeScriptCommand('export-sentry-summary.mjs');
  }
  if (['asc', 'asc-cli', 'app-store-connect', 'app_store_connect'].includes(normalized)) {
    return nodeRuntimeScriptCommand('export-asc-summary.mjs');
  }
  return getDefaultSourceCommand(sourceName);
}

function replaceLegacyRuntimeScriptCommand(command) {
  const trimmed = String(command || '').trim();
  if (!trimmed) return trimmed;
  return trimmed.replace(
    /^node\s+scripts\/(export-analytics-summary\.mjs|export-revenuecat-summary\.mjs|export-sentry-summary\.mjs|export-asc-summary\.mjs|openclaw-growth-start\.mjs|openclaw-growth-status\.mjs|openclaw-growth-preflight\.mjs|openclaw-growth-runner\.mjs|openclaw-growth-engineer\.mjs)(?=\s|$)/,
    (_match, scriptName) => nodeRuntimeScriptCommand(scriptName),
  );
}

function normalizeSourceCommand(sourceName, source) {
  return replaceLegacyRuntimeScriptCommand(source?.command || '') || getRuntimeSourceCommand(sourceName);
}

function migrateRuntimeSourceCommands(config) {
  if (!config || typeof config !== 'object') return config;
  const sources = config.sources && typeof config.sources === 'object' ? config.sources : {};
  const nextSources = { ...sources };
  for (const sourceName of ['analytics', 'revenuecat', 'sentry']) {
    if (nextSources[sourceName]?.mode === 'command') {
      nextSources[sourceName] = {
        ...nextSources[sourceName],
        command: normalizeSourceCommand(sourceName, nextSources[sourceName]),
      };
    }
  }
  if (Array.isArray(nextSources.extra)) {
    nextSources.extra = nextSources.extra.map((source) => {
      if (!source || source.mode !== 'command') return source;
      const service = String(source.service || source.key || '').toLowerCase();
      const sourceName = ['asc', 'asc-cli', 'app-store-connect', 'app_store_connect'].includes(service)
        ? 'asc'
        : service;
      return {
        ...source,
        command: normalizeSourceCommand(sourceName, source),
      };
    });
  }
  return {
    ...config,
    sources: nextSources,
  };
}

function truncate(value, max = 240) {
  const text = String(value || '');
  if (text.length <= max) return text;
  return `${text.slice(0, max)}...`;
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

function emitProgress(enabled, event) {
  if (!enabled) return;
  process.stderr.write(`OPENCLAW_PROGRESS ${JSON.stringify(event)}\n`);
}

function runShellCommand(command, timeoutMs = 120_000, options: { onStderrLine?: (line: string) => void } = {}): Promise<ShellResult> {
  return new Promise((resolve) => {
    const child = spawn(resolveShellCommand(), ['-c', command], {
      stdio: ['ignore', 'pipe', 'pipe'],
    });

    let stdout = '';
    let stderr = '';
    let stderrBuffer = '';
    let settled = false;

    const timer = setTimeout(() => {
      if (settled) return;
      settled = true;
      child.kill('SIGTERM');
      resolve({
        ok: false,
        code: null,
        stdout,
        stderr: `${stderr}\nTimed out after ${timeoutMs}ms`,
      });
    }, timeoutMs);

    child.stdout.on('data', (chunk) => {
      stdout += String(chunk);
    });
    child.stderr.on('data', (chunk) => {
      const text = String(chunk);
      stderr += text;
      if (!options.onStderrLine) return;
      stderrBuffer += text;
      const lines = stderrBuffer.split(/\r?\n/);
      stderrBuffer = lines.pop() || '';
      for (const line of lines) {
        options.onStderrLine(line);
      }
    });

    child.on('close', (code) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      if (options.onStderrLine && stderrBuffer.trim()) {
        options.onStderrLine(stderrBuffer);
      }
      resolve({
        ok: code === 0,
        code,
        stdout,
        stderr,
      });
    });
  });
}

async function fileExists(filePath) {
  try {
    await fs.access(filePath);
    return true;
  } catch {
    return false;
  }
}

async function commandExists(commandName) {
  const result = await runShellCommand(`command -v ${quote(commandName)} >/dev/null 2>&1`, 30_000);
  return result.ok;
}

async function resolveCommandPath(commandName) {
  const result = await runShellCommand(`command -v ${quote(commandName)}`, 30_000);
  return result.ok ? result.stdout.trim() : null;
}

function prependToPath(binDir) {
  process.env.PATH = `${binDir}${path.delimiter}${process.env.PATH || ''}`;
}

function getPathProfileEntries(binDir) {
  const entries = [binDir];
  if (process.env.HOME && path.resolve(binDir) === path.resolve(process.env.HOME, '.local', 'bin')) {
    entries.push(path.join(process.env.HOME, '.local', 'analyticscli-npm', 'bin'));
  }
  return entries;
}

function renderProfilePathEntries(binDir) {
  const home = process.env.HOME ? path.resolve(process.env.HOME) : null;
  return getPathProfileEntries(binDir)
    .map((entry) => {
      const resolved = path.resolve(entry);
      if (home && (resolved === home || resolved.startsWith(`${home}${path.sep}`))) {
        return `$HOME/${path.relative(home, resolved)}`;
      }
      return entry;
    })
    .join(':');
}

async function ensureProfilePath(binDir) {
  if (process.env.ANALYTICSCLI_SKIP_PROFILE_UPDATE === 'true' || !process.env.HOME) {
    return false;
  }

  const line = `export PATH="${renderProfilePathEntries(binDir)}:$PATH"`;
  const profiles = ['.profile', '.bashrc', '.bash_profile', '.zshrc', '.zprofile'].map((name) =>
    path.join(process.env.HOME!, name),
  );
  let wrote = false;

  for (const profile of profiles) {
    let current = '';
    try {
      current = await fs.readFile(profile, 'utf8');
    } catch {
      await fs.mkdir(path.dirname(profile), { recursive: true });
    }

    if (!current.includes(line)) {
      await fs.appendFile(profile, `\n# AnalyticsCLI CLI user-local npm bin\n${line}\n`, 'utf8');
      wrote = true;
    }
  }

  return wrote;
}

async function verifyFreshShellProfile() {
  if (!process.env.HOME) {
    return false;
  }

  const cleanPath = '/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin';
  const probes = [
    {
      shell: '/bin/bash',
      command:
        'for f in "$HOME/.bash_profile" "$HOME/.bashrc" "$HOME/.profile"; do [[ -f "$f" ]] && source "$f" >/dev/null 2>&1 || true; done; command -v analyticscli >/dev/null 2>&1 && analyticscli --help >/dev/null 2>&1',
    },
    {
      shell: '/usr/bin/bash',
      command:
        'for f in "$HOME/.bash_profile" "$HOME/.bashrc" "$HOME/.profile"; do [[ -f "$f" ]] && source "$f" >/dev/null 2>&1 || true; done; command -v analyticscli >/dev/null 2>&1 && analyticscli --help >/dev/null 2>&1',
    },
    {
      shell: '/bin/zsh',
      command:
        'for f in "$HOME/.zprofile" "$HOME/.zshrc" "$HOME/.profile"; do [[ -f "$f" ]] && source "$f" >/dev/null 2>&1 || true; done; command -v analyticscli >/dev/null 2>&1 && analyticscli --help >/dev/null 2>&1',
    },
    {
      shell: '/usr/bin/zsh',
      command:
        'for f in "$HOME/.zprofile" "$HOME/.zshrc" "$HOME/.profile"; do [[ -f "$f" ]] && source "$f" >/dev/null 2>&1 || true; done; command -v analyticscli >/dev/null 2>&1 && analyticscli --help >/dev/null 2>&1',
    },
    {
      shell: '/bin/sh',
      command:
        '[ -f "$HOME/.profile" ] && . "$HOME/.profile" >/dev/null 2>&1 || true; command -v analyticscli >/dev/null 2>&1 && analyticscli --help >/dev/null 2>&1',
    },
    {
      shell: '/usr/bin/sh',
      command:
        '[ -f "$HOME/.profile" ] && . "$HOME/.profile" >/dev/null 2>&1 || true; command -v analyticscli >/dev/null 2>&1 && analyticscli --help >/dev/null 2>&1',
    },
  ];

  for (const probe of probes) {
    if (!(await fileExists(probe.shell))) {
      continue;
    }
    const result = await runShellCommand(
      `env HOME=${quote(process.env.HOME)} PATH=${quote(cleanPath)} ${quote(probe.shell)} -lc ${quote(probe.command)}`,
      30_000,
    );
    if (result.ok) {
      return true;
    }
  }

  return false;
}

function isUserLocalBin(binDir) {
  if (!process.env.HOME) {
    return false;
  }
  const home = path.resolve(process.env.HOME);
  const resolved = path.resolve(binDir);
  return resolved === home || resolved.startsWith(`${home}${path.sep}`);
}

function isPermissionFailure(output) {
  return /EACCES|permission denied|access denied|operation not permitted/i.test(String(output || ''));
}

async function ensureAnalyticsCliInstalled() {
  const beforePath = await resolveCommandPath('analyticscli');
  const npmExists = await commandExists('npm');
  if (!npmExists) {
    if (beforePath) {
      return {
        ok: true,
        detail: `analyticscli binary found at ${beforePath}; npm unavailable, so package update was skipped`,
      };
    }
    return {
      ok: false,
      detail: `analyticscli binary missing and npm is unavailable; install ${ANALYTICSCLI_PACKAGE_SPEC}`,
    };
  }

  const globalInstall = await runShellCommand(`npm install -g ${quote(ANALYTICSCLI_PACKAGE_SPEC)}`, 180_000);
  if (!globalInstall.ok) {
    const installOutput = `${globalInstall.stderr}\n${globalInstall.stdout}`;
    if (isPermissionFailure(installOutput)) {
      await fs.mkdir(ANALYTICSCLI_NPM_PREFIX, { recursive: true });
      const localInstall = await runShellCommand(
        `npm install -g --prefix ${quote(ANALYTICSCLI_NPM_PREFIX)} ${quote(ANALYTICSCLI_PACKAGE_SPEC)}`,
        180_000,
      );
      if (!localInstall.ok) {
        return beforePath
          ? {
              ok: true,
              detail: `analyticscli binary found at ${beforePath}; update failed globally and in user-local prefix (${truncate(localInstall.stderr || localInstall.stdout)})`,
            }
          : {
              ok: false,
              detail: `npm install failed globally and in user-local prefix ${ANALYTICSCLI_NPM_PREFIX}: ${truncate(localInstall.stderr || localInstall.stdout)}`,
            };
      }
      const localBinDir = path.join(ANALYTICSCLI_NPM_PREFIX, 'bin');
      prependToPath(localBinDir);
      await ensureProfilePath(localBinDir);
    } else {
      return beforePath
        ? {
            ok: true,
            detail: `analyticscli binary found at ${beforePath}; package update failed (${truncate(installOutput)})`,
          }
        : {
            ok: false,
            detail: `npm install -g ${ANALYTICSCLI_PACKAGE_SPEC} failed: ${truncate(installOutput)}`,
          };
    }
  }

  const afterPath = await resolveCommandPath('analyticscli');
  if (afterPath) {
    const helpCheck = await runShellCommand('analyticscli --help >/dev/null 2>&1', 30_000);
    if (!helpCheck.ok) {
      return {
        ok: false,
        detail: `analyticscli binary found at ${afterPath}, but --help failed: ${truncate(helpCheck.stderr || helpCheck.stdout)}`,
      };
    }

    const binDir = path.dirname(afterPath);
    if (isUserLocalBin(binDir)) {
      await ensureProfilePath(binDir);
      if (!(await verifyFreshShellProfile())) {
        return {
          ok: false,
          detail: `analyticscli works at ${afterPath}, but a fresh shell still cannot resolve it after profile update; add ${renderProfilePathEntries(binDir)} to PATH`,
        };
      }
      return {
        ok: true,
        detail: `analyticscli package ensured via ${ANALYTICSCLI_PACKAGE_SPEC}; binary found at ${afterPath}; shell profiles updated and fresh shell verification passed`,
      };
    }
  }

  return afterPath
    ? {
        ok: true,
        detail: `analyticscli package ensured via ${ANALYTICSCLI_PACKAGE_SPEC}; binary found at ${afterPath}`,
      }
    : {
        ok: false,
        detail: `Installed ${ANALYTICSCLI_PACKAGE_SPEC}, but analyticscli is still not on PATH`,
      };
}

async function readJson(filePath): Promise<any> {
  const raw = await fs.readFile(filePath, 'utf8');
  return JSON.parse(raw);
}

async function writeJson(filePath, value) {
  await fs.mkdir(path.dirname(filePath), { recursive: true });
  await fs.writeFile(filePath, `${JSON.stringify(value, null, 2)}\n`, 'utf8');
}

function formatHeartbeatInterval(minutes) {
  const intervalMinutes = Math.max(1, Math.floor(Number(minutes) || 1440));
  if (intervalMinutes % 1440 === 0) return `${intervalMinutes / 1440}d`;
  if (intervalMinutes % 60 === 0) return `${intervalMinutes / 60}h`;
  return `${intervalMinutes}m`;
}

function getHeartbeatInterval(config) {
  const scheduleInterval = Number(config?.schedule?.intervalMinutes);
  const healthInterval = Number(config?.schedule?.connectorHealthCheckIntervalMinutes);
  return Math.min(
    Number.isFinite(scheduleInterval) && scheduleInterval > 0 ? scheduleInterval : 1440,
    Number.isFinite(healthInterval) && healthInterval > 0 ? healthInterval : 360,
  );
}

function relativeWorkspacePath(filePath) {
  const relative = path.relative(process.cwd(), filePath);
  return relative && !relative.startsWith('..') && !path.isAbsolute(relative) ? relative : filePath;
}

function isEffectivelyEmptyHeartbeat(value) {
  return String(value || '')
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter((line) => line && !line.startsWith('#') && !line.startsWith('<!--') && !line.startsWith('-->'))
    .length === 0;
}

function renderHeartbeatBlock(configPath, config) {
  const interval = formatHeartbeatInterval(getHeartbeatInterval(config));
  const displayConfigPath = relativeWorkspacePath(configPath);
  return `${HEARTBEAT_MARKER_START}
tasks:

- name: openclaw-growth-engineer-run
  interval: ${interval}
  prompt: "Run \`node scripts/openclaw-growth-runner.mjs --config ${displayConfigPath}\` from the workspace if the config and runtime files exist. The runner owns schedule.cadences, connectorHealthCheckIntervalMinutes, skipIfNoDataChange, and skipIfIssueSetUnchanged. If it reports connector-health alerts, production crashes, generated issues, or actionable growth findings, summarize only the action and evidence. If setup files are missing, tell the user to run \`node scripts/openclaw-growth-wizard.mjs --connectors\`. If there is no actionable output, reply HEARTBEAT_OK."

# Keep this section small. Do not put secrets in HEARTBEAT.md.
${HEARTBEAT_MARKER_END}`;
}

async function ensureGrowthHeartbeat(configPath, config) {
  const heartbeatPath = path.resolve(DEFAULT_HEARTBEAT_PATH);
  const block = renderHeartbeatBlock(configPath, config);
  let existing = '';
  let existed = true;
  try {
    existing = await fs.readFile(heartbeatPath, 'utf8');
  } catch {
    existed = false;
  }

  const markerPattern = new RegExp(`${HEARTBEAT_MARKER_START}[\\s\\S]*?${HEARTBEAT_MARKER_END}`);
  const next = markerPattern.test(existing)
    ? existing.replace(markerPattern, block)
    : isEffectivelyEmptyHeartbeat(existing)
      ? `# OpenClaw heartbeat checklist\n\n${block}\n`
      : `${existing.trimEnd()}\n\n${block}\n`;

  if (next !== existing) {
    await fs.writeFile(heartbeatPath, next, 'utf8');
    return {
      path: heartbeatPath,
      interval: formatHeartbeatInterval(getHeartbeatInterval(config)),
      created: !existed || isEffectivelyEmptyHeartbeat(existing),
      updated: existed && !isEffectivelyEmptyHeartbeat(existing),
    };
  }

  return {
    path: heartbeatPath,
    interval: formatHeartbeatInterval(getHeartbeatInterval(config)),
    created: false,
    updated: false,
  };
}

async function appendHelperDetail(details, label, result) {
  if (result.ok) {
    details.push(`${label}: ok`);
    return;
  }
  details.push(`${label}: ${truncate(result.stderr || result.stdout || `exit ${result.code ?? 'unknown'}`)}`);
}

async function installClawHubSkill(skillName, details) {
  if (await commandExists('clawhub')) {
    const result = await runShellCommand(`clawhub install ${quote(skillName)} || clawhub install ${quote(skillName)} --force`, 180_000);
    await appendHelperDetail(details, `ClawHub skill ${skillName}`, result);
    return result.ok;
  }
  if (await commandExists('npx')) {
    const result = await runShellCommand(
      `npx -y clawhub install ${quote(skillName)} || npx -y clawhub install ${quote(skillName)} --force`,
      180_000,
    );
    await appendHelperDetail(details, `ClawHub skill ${skillName}`, result);
    return result.ok;
  }
  details.push(`ClawHub skill ${skillName}: skipped because neither clawhub nor npx is available`);
  return false;
}

async function installAgentSkill(repo, details) {
  if (!(await commandExists('npx'))) {
    details.push(`Agent skill ${repo}: skipped because npx is unavailable`);
    return false;
  }
  const result = await runShellCommand(`npx -y skills add ${quote(repo)}`, 180_000);
  await appendHelperDetail(details, `Agent skill ${repo}`, result);
  return result.ok;
}

async function installSystemBinary(commandName, details) {
  if (await commandExists(commandName)) {
    details.push(`${commandName} binary found at ${await resolveCommandPath(commandName)}`);
    return true;
  }

  if (await commandExists('brew')) {
    const result = await runShellCommand(`brew install ${quote(commandName)}`, 600_000);
    await appendHelperDetail(details, `brew install ${commandName}`, result);
  } else if (await commandExists('apt-get')) {
    const prefix = process.getuid?.() === 0 ? '' : 'sudo -n ';
    const result = await runShellCommand(`${prefix}apt-get update && ${prefix}apt-get install -y ${quote(commandName)}`, 600_000);
    await appendHelperDetail(details, `apt-get install ${commandName}`, result);
  } else if (await commandExists('winget')) {
    const packageId = commandName === 'gh' ? 'GitHub.cli' : commandName;
    const result = await runShellCommand(`winget install --id ${quote(packageId)} -e --silent`, 600_000);
    await appendHelperDetail(details, `winget install ${packageId}`, result);
  } else {
    details.push(`No supported non-interactive installer found for ${commandName}`);
  }

  const installedPath = await resolveCommandPath(commandName);
  if (installedPath) {
    details.push(`${commandName} binary found at ${installedPath}`);
    return true;
  }
  return false;
}

function getUserLocalBinDir() {
  return process.env.HOME ? path.join(process.env.HOME, '.local', 'bin') : null;
}

function prependPath(dir) {
  const current = process.env.PATH || '';
  if (!current.split(':').includes(dir)) {
    process.env.PATH = `${dir}:${current}`;
  }
}

function getGitHubCliReleaseAssetName(version) {
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
      'User-Agent': 'openclaw-growth-start',
    },
  });
  if (!response.ok) {
    throw new Error(`GitHub CLI release lookup failed (${response.status})`);
  }
  const release = await response.json();
  const version = String(release?.tag_name || '').replace(/^v/, '');
  const assetName = getGitHubCliReleaseAssetName(version);
  if (!assetName) {
    throw new Error(`No user-local gh installer is defined for ${process.platform}/${process.arch}`);
  }
  const asset = Array.isArray(release?.assets) ? release.assets.find((entry) => entry?.name === assetName) : null;
  if (!asset?.browser_download_url) {
    throw new Error(`GitHub CLI release asset not found: ${assetName}`);
  }
  return asset.browser_download_url;
}

async function installGitHubCliUserLocal(details) {
  const binDir = getUserLocalBinDir();
  if (!binDir) {
    details.push('gh user-local install skipped because HOME is not set');
    return false;
  }
  if (!(await commandExists('curl'))) {
    details.push('gh user-local install skipped because curl is unavailable');
    return false;
  }
  if (!(await commandExists('tar'))) {
    details.push('gh user-local install skipped because tar is unavailable');
    return false;
  }

  try {
    const url = await resolveGitHubCliReleaseAssetUrl();
    const cacheDir = path.join(process.env.HOME, '.cache', 'openclaw-gh');
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
    const result = await runShellCommand(command, 600_000);
    prependPath(binDir);
    await appendHelperDetail(details, `user-local gh install to ${path.join(binDir, 'gh')}`, result);
    const installedPath = await resolveCommandPath('gh');
    if (installedPath) {
      details.push(`gh binary found at ${installedPath}`);
      return true;
    }
  } catch (error) {
    details.push(`user-local gh install failed: ${error instanceof Error ? error.message : String(error)}`);
  }
  return false;
}

function resolveMcpNpmCacheDir() {
  return process.env.OPENCLAW_MCP_NPM_CACHE ||
    (process.env.HOME ? path.join(process.env.HOME, '.cache', 'openclaw-mcp-npm') : path.join(process.cwd(), '.openclaw-mcp-npm-cache'));
}

function escapeTomlString(value) {
  return String(value).replace(/\\/g, '\\\\').replace(/"/g, '\\"');
}

async function upsertRevenueCatCodexMcpConfig(apiKey) {
  if (!process.env.HOME) return null;

  const configDir = path.join(process.env.HOME, '.codex');
  const configFile = path.join(configDir, 'config.toml');
  await fs.mkdir(configDir, { recursive: true });
  let existing = '';
  try {
    existing = await fs.readFile(configFile, 'utf8');
  } catch {
    existing = '';
  }
  const block = `[mcp_servers.revenuecat]
command = "npx"
args = ["--yes", "--cache", "${escapeTomlString(resolveMcpNpmCacheDir())}", "mcp-remote", "https://mcp.revenuecat.ai/mcp", "--header", "Authorization: Bearer \${AUTH_TOKEN}"]
env = { AUTH_TOKEN = "${escapeTomlString(apiKey)}" }
type = "stdio"
startup_timeout_ms = 20000
`;
  const pattern = /(?:^|\n)\[mcp_servers\.revenuecat\]\n(?:.*\n)*?(?=\n\[|\s*$)/m;
  const next = pattern.test(existing)
    ? existing.replace(pattern, `${existing.startsWith('[mcp_servers.revenuecat]') ? '' : '\n'}${block}`)
    : `${existing.trimEnd()}${existing.trim() ? '\n\n' : ''}${block}`;
  await fs.writeFile(configFile, `${next.trimEnd()}\n`, 'utf8');
  return configFile;
}

async function upsertSentryCodexMcpConfig(token) {
  if (!process.env.HOME) return null;

  const configDir = path.join(process.env.HOME, '.codex');
  const configFile = path.join(configDir, 'config.toml');
  await fs.mkdir(configDir, { recursive: true });
  let existing = '';
  try {
    existing = await fs.readFile(configFile, 'utf8');
  } catch {
    existing = '';
  }
  const envEntries = [
    `SENTRY_ACCESS_TOKEN = "${escapeTomlString(token)}"`,
    process.env.SENTRY_BASE_URL && process.env.SENTRY_BASE_URL !== 'https://sentry.io'
      ? `SENTRY_HOST = "${escapeTomlString(String(process.env.SENTRY_BASE_URL).replace(/^https?:\/\//, '').replace(/\/$/, ''))}"`
      : null,
  ].filter(Boolean);
  const block = `[mcp_servers.sentry]
command = "npx"
args = ["--yes", "--cache", "${escapeTomlString(resolveMcpNpmCacheDir())}", "@sentry/mcp-server@latest"]
env = { ${envEntries.join(', ')} }
type = "stdio"
startup_timeout_ms = 30000
`;
  const pattern = /(?:^|\n)\[mcp_servers\.sentry\]\n(?:.*\n)*?(?=\n\[|\s*$)/m;
  const next = pattern.test(existing)
    ? existing.replace(pattern, `${existing.startsWith('[mcp_servers.sentry]') ? '' : '\n'}${block}`)
    : `${existing.trimEnd()}${existing.trim() ? '\n\n' : ''}${block}`;
  await fs.writeFile(configFile, `${next.trimEnd()}\n`, 'utf8');
  return configFile;
}

async function installRevenueCatConnector() {
  const details = [];
  if (!(await commandExists('npx'))) {
    return { connector: 'revenuecat', ok: false, detail: 'npx is required for RevenueCat MCP transport but is unavailable' };
  }
  const check = await runShellCommand(`npx --yes --cache ${quote(resolveMcpNpmCacheDir())} mcp-remote`, 120_000);
  const output = `${check.stderr}\n${check.stdout}`;
  const available = check.ok || /Usage: .*mcp-remote|Usage: .*proxy\.ts/i.test(output);
  if (!available) {
    await appendHelperDetail(details, 'npx mcp-remote availability check', check);
    return { connector: 'revenuecat', ok: false, detail: details.join('; ') };
  }
  details.push(`RevenueCat MCP transport mcp-remote is available via npx cache ${resolveMcpNpmCacheDir()}`);
  const apiKey = String(process.env.REVENUECAT_API_KEY || '').trim();
  if (apiKey) {
    const configFile = await upsertRevenueCatCodexMcpConfig(apiKey);
    details.push(configFile ? `RevenueCat MCP configured in ${configFile}` : 'RevenueCat MCP transport available; HOME missing so MCP config was not written');
  } else {
    details.push('Set REVENUECAT_API_KEY, then rerun this command to write the RevenueCat MCP client config');
  }
  return { connector: 'revenuecat', ok: true, detail: details.join('; ') };
}

async function installSentryConnector() {
  const details = [];
  if (await commandExists('npx')) {
    const check = await runShellCommand(`npx --yes --cache ${quote(resolveMcpNpmCacheDir())} @sentry/mcp-server@latest --help`, 120_000);
    const output = `${check.stderr}\n${check.stdout}`;
    const available = check.ok || /sentry|mcp-server|access-token/i.test(output);
    if (available) {
      details.push(`Sentry MCP server is available via npx cache ${resolveMcpNpmCacheDir()}`);
    } else {
      await appendHelperDetail(details, 'Sentry MCP availability check', check);
    }
  } else {
    details.push('npx unavailable; Sentry MCP config was skipped, direct API exporter remains available');
  }

  const token = String(process.env.SENTRY_AUTH_TOKEN || '').trim();
  if (token && (await commandExists('npx'))) {
    const configFile = await upsertSentryCodexMcpConfig(token);
    details.push(configFile ? `Sentry MCP configured in ${configFile}` : 'Sentry MCP available; HOME missing so MCP config was not written');
  } else if (!token) {
    details.push('Set SENTRY_AUTH_TOKEN, then rerun this command to write Sentry MCP client config');
  }

  details.push('Sentry direct API exporter enabled via node scripts/export-sentry-summary.mjs');
  return { connector: 'sentry', ok: true, detail: details.join('; ') };
}

async function installGitHubConnector() {
  const details = [];
  await installClawHubSkill('github', details);
  let ok = await installSystemBinary('gh', details);
  if (!ok) {
    ok = await installGitHubCliUserLocal(details);
  }
  const repo = await detectGitHubRepo();
  if (repo) {
    details.push(`GitHub repo configured for code access: ${repo}`);
  } else if (process.env.GITHUB_TOKEN) {
    details.push('GITHUB_TOKEN is set; repo selection is deferred per app/task');
  } else {
    details.push('GitHub token not configured yet; rerun connector wizard for github when ready');
  }
  return { connector: 'github', ok, detail: details.join('; ') };
}

async function installAscConnector() {
  const details = [];
  await installAgentSkill('rorkai/app-store-connect-cli-skills', details);
  let ok = await installSystemBinary('asc', details);
  if (!ok && (await commandExists('curl'))) {
    const result = await runShellCommand('curl -fsSL https://asccli.sh/install | bash', 600_000);
    await appendHelperDetail(details, 'asc install script', result);
    ok = Boolean(await resolveCommandPath('asc'));
  }
  return { connector: 'asc', ok, detail: `${details.join('; ')}${ok ? '; next run asc auth status --validate or asc auth login' : ''}` };
}

async function installAnalyticsConnector() {
  const analyticsCliPath = await resolveCommandPath('analyticscli');
  return {
    connector: 'analytics',
    ok: Boolean(analyticsCliPath),
    detail: analyticsCliPath
      ? `analyticscli binary found at ${analyticsCliPath}; token is read from the wizard-managed AnalyticsCLI environment`
      : 'analyticscli binary missing after dependency setup',
  };
}

async function enableConnectorConfig(configPath, connectors) {
  if (connectors.length === 0 || !(await fileExists(configPath))) return;
  const config = await readJson(configPath);
  const extra = Array.isArray(config.sources?.extra) ? config.sources.extra : [];
  const next = {
    ...config,
    sources: {
      ...(config.sources || {}),
      analytics: connectors.includes('analytics')
        ? { ...(config.sources?.analytics || {}), enabled: true, mode: 'command', command: normalizeSourceCommand('analytics', config.sources?.analytics) }
        : config.sources?.analytics,
      revenuecat: connectors.includes('revenuecat')
        ? { ...(config.sources?.revenuecat || {}), enabled: true, mode: 'command', command: normalizeSourceCommand('revenuecat', config.sources?.revenuecat) }
        : config.sources?.revenuecat,
      sentry: connectors.includes('sentry')
        ? { ...(config.sources?.sentry || {}), enabled: true, mode: 'command', command: normalizeSourceCommand('sentry', config.sources?.sentry) }
        : config.sources?.sentry,
      extra: extra.map((source) =>
        connectors.includes('asc') && source?.service === 'asc-cli'
          ? { ...source, enabled: true, mode: 'command', command: normalizeSourceCommand('asc', source) }
          : source,
      ),
    },
  };
  await writeJson(configPath, next);
}

async function installConnectorHelpers(configPath, connectors) {
  await enableConnectorConfig(configPath, connectors);
  const results = [];
  for (const connector of connectors) {
    if (connector === 'analytics') results.push(await installAnalyticsConnector());
    if (connector === 'github') results.push(await installGitHubConnector());
    if (connector === 'asc') results.push(await installAscConnector());
    if (connector === 'revenuecat') results.push(await installRevenueCatConnector());
    if (connector === 'sentry') results.push(await installSentryConnector());
  }
  return results;
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

  const remoteResult = await runShellCommand('git config --get remote.origin.url', 10_000);
  if (!remoteResult.ok) return null;
  return parseGitHubRepoFromRemote(remoteResult.stdout.trim());
}

async function ensureConfig(configPath) {
  if (await fileExists(configPath)) {
    const originalConfig = await readJson(configPath);
    const config = migrateRuntimeSourceCommands(originalConfig);
    let changed = JSON.stringify(originalConfig.sources || {}) !== JSON.stringify(config.sources || {});
    if (!isConfiguredGitHubRepo(config?.project?.githubRepo)) {
      const detectedRepo = await detectGitHubRepo();
      if (detectedRepo) {
        config.project = {
          ...(config.project || {}),
          githubRepo: detectedRepo,
        };
        changed = true;
      }
    }
    if (changed) {
      await writeJson(configPath, config);
      return {
        created: false,
        configPath,
        githubRepo: config.project?.githubRepo || null,
      };
    }
    return {
      created: false,
      configPath,
      githubRepo: null,
    };
  }

  const templatePath = path.resolve(DEFAULT_TEMPLATE_PATH);
  const template = await readJson(templatePath);
  const detectedRepo = await detectGitHubRepo();
  const githubRepo = detectedRepo || '';

  const config = {
    ...template,
    generatedAt: new Date().toISOString(),
    project: {
      ...template.project,
      githubRepo,
      repoRoot: '.',
    },
    sources: {
      ...template.sources,
      analytics: {
        enabled: true,
        mode: 'command',
        command: getRuntimeSourceCommand('analytics'),
      },
      revenuecat: {
        ...(template.sources?.revenuecat || {}),
        enabled: false,
        mode: 'command',
        command: getRuntimeSourceCommand('revenuecat'),
      },
      sentry: {
        ...(template.sources?.sentry || {}),
        enabled: false,
        mode: 'command',
        command: getRuntimeSourceCommand('sentry'),
      },
      feedback: {
        ...(template.sources?.feedback || {}),
        enabled: false,
      },
      extra: Array.isArray(template.sources?.extra) ? template.sources.extra : [],
    },
    actions: {
      ...template.actions,
      mode: 'issue',
      autoCreateIssues: true,
      autoCreatePullRequests: false,
      draftPullRequests: true,
      proposalBranchPrefix: 'openclaw/proposals',
    },
  };

  await writeJson(configPath, config);
  return {
    created: true,
    configPath,
    githubRepo,
  };
}

function parseJsonFromStdout(stdout) {
  const raw = String(stdout || '').trim();
  if (!raw) return null;
  const firstBrace = raw.indexOf('{');
  const firstBracket = raw.indexOf('[');
  const starts = [firstBrace, firstBracket].filter((index) => index >= 0);
  if (starts.length === 0) return null;
  const jsonStart = Math.min(...starts);
  try {
    return JSON.parse(raw.slice(jsonStart));
  } catch {
    return null;
  }
}

function normalizeString(value) {
  return typeof value === 'string' && value.trim() ? value.trim() : null;
}

function extractProjectChoices(payload) {
  const candidates = (() => {
    if (Array.isArray(payload)) return payload;
    if (payload && typeof payload === 'object') {
      if (Array.isArray(payload.projects)) return payload.projects;
      if (Array.isArray(payload.items)) return payload.items;
      if (Array.isArray(payload.data)) return payload.data;
    }
    return [];
  })();

  const byId = new Map();
  for (const candidate of candidates) {
    if (!candidate || typeof candidate !== 'object') continue;
    const id =
      normalizeString(candidate.id) ||
      normalizeString(candidate.projectId) ||
      normalizeString(candidate.project_id);
    if (!id) continue;
    const name = normalizeString(candidate.name) || normalizeString(candidate.displayName);
    const slug = normalizeString(candidate.slug);
    byId.set(id, {
      id,
      name,
      slug,
      label: name || slug || id,
    });
  }

  return [...byId.values()].sort((a, b) => String(a.label).localeCompare(String(b.label)));
}

function isMissingProjectSelection(text) {
  return /Project ID is missing|Pass --project <id>|analyticscli projects select/i.test(String(text || ''));
}

function commandHasProjectFlag(command) {
  return /(^|\s)--project(\s|=|$)/.test(String(command || ''));
}

function appendProjectFlag(command, projectId) {
  const raw = String(command || '').trim();
  if (!raw || commandHasProjectFlag(raw)) return raw;
  return `${raw} --project ${quote(projectId)}`;
}

function commandHasAscAppFlag(command) {
  return /(^|\s)--app(\s|=|$)/.test(String(command || ''));
}

function appendAscAppFlag(command, appId) {
  const raw = String(command || '').trim();
  if (!raw || commandHasAscAppFlag(raw)) return raw;
  return `${raw} --app ${quote(appId)}`;
}

async function configureAnalyticsProject(configPath, projectId) {
  const normalizedProjectId = normalizeString(projectId);
  if (!normalizedProjectId) return false;

  const config = await readJson(configPath);
  let changed = false;
  for (const sourceName of ['analytics', 'feedback']) {
    const source = config?.sources?.[sourceName];
    if (!source || source.enabled === false || source.mode !== 'command' || !source.command) {
      continue;
    }
    const nextCommand = appendProjectFlag(source.command, normalizedProjectId);
    if (nextCommand !== source.command) {
      source.command = nextCommand;
      changed = true;
    }
  }

  if (!config.project || typeof config.project !== 'object') {
    config.project = {};
  }
  if (config.project.analyticsProjectId !== normalizedProjectId) {
    config.project.analyticsProjectId = normalizedProjectId;
    changed = true;
  }

  if (changed) {
    await writeJson(configPath, config);
  }
  return changed;
}

async function configureAscApp(configPath, appId) {
  const normalizedAppId = normalizeString(appId);
  if (!normalizedAppId) return false;

  const config = await readJson(configPath);
  let changed = false;
  const extraSources = Array.isArray(config?.sources?.extra) ? config.sources.extra : [];

  for (const source of extraSources) {
    if (!source || typeof source !== 'object') continue;
    const service = String(source.service || source.key || '').trim().toLowerCase();
    if (!['asc', 'asc-cli', 'app-store-connect', 'app_store_connect'].includes(service)) continue;
    if (source.mode === 'command' && source.command) {
      const nextCommand = appendAscAppFlag(source.command, normalizedAppId);
      if (nextCommand !== source.command) {
        source.command = nextCommand;
        changed = true;
      }
    }
  }

  if (!config.project || typeof config.project !== 'object') {
    config.project = {};
  }
  if (config.project.ascAppId !== normalizedAppId) {
    config.project.ascAppId = normalizedAppId;
    changed = true;
  }

  if (changed) {
    await writeJson(configPath, config);
  }
  process.env.ASC_APP_ID = normalizedAppId;
  return changed;
}

function configHasEnabledAscSource(config) {
  const extraSources = Array.isArray(config?.sources?.extra) ? config.sources.extra : [];
  return extraSources.some((source) => {
    if (!source || typeof source !== 'object' || source.enabled === false) return false;
    const service = String(source.service || source.key || '').trim().toLowerCase();
    return ['asc', 'asc-cli', 'app-store-connect', 'app_store_connect'].includes(service);
  });
}

function extractAscAppChoices(payload) {
  const candidates = (() => {
    if (Array.isArray(payload)) return payload;
    if (payload && typeof payload === 'object') {
      if (Array.isArray(payload.apps)) return payload.apps;
      if (Array.isArray(payload.items)) return payload.items;
      if (Array.isArray(payload.data)) return payload.data;
    }
    return [];
  })();

  const byId = new Map();
  for (const candidate of candidates) {
    if (!candidate || typeof candidate !== 'object') continue;
    const attrs = candidate.attributes && typeof candidate.attributes === 'object' ? candidate.attributes : {};
    const id =
      normalizeString(candidate.id) ||
      normalizeString(candidate.appId) ||
      normalizeString(candidate.app_id);
    if (!id) continue;
    const name =
      normalizeString(candidate.name) ||
      normalizeString(candidate.appName) ||
      normalizeString(candidate.displayName) ||
      normalizeString(attrs.name) ||
      normalizeString(attrs.bundleId);
    const bundleId =
      normalizeString(candidate.bundleId) ||
      normalizeString(candidate.bundle_id) ||
      normalizeString(attrs.bundleId);
    byId.set(id, {
      id,
      name,
      bundleId,
      label: [name || id, bundleId ? `(${bundleId})` : null, id !== name ? id : null].filter(Boolean).join(' '),
    });
  }

  return [...byId.values()].sort((a, b) => String(a.label).localeCompare(String(b.label)));
}

async function listAscApps() {
  const result = await runShellCommand('asc apps list --output json', 60_000);
  if (!result.ok) {
    return {
      ok: false,
      error: result.stderr || `exit ${result.code}`,
      apps: [],
    };
  }
  const payload = parseJsonFromStdout(result.stdout);
  return {
    ok: true,
    error: null,
    apps: extractAscAppChoices(payload),
  };
}

async function ensureAscAppConfigured(configPath, explicitAppId) {
  if (normalizeString(explicitAppId)) {
    const changed = await configureAscApp(configPath, explicitAppId);
    return { ok: true, configured: true, changed, appId: explicitAppId, appScope: 'single_app', needsUserInput: false };
  }

  const config = await readJson(configPath);
  if (!configHasEnabledAscSource(config)) {
    return { ok: true, configured: false, changed: false, appId: null, appScope: 'disabled', needsUserInput: false };
  }

  const configuredAppId = normalizeString(config.project?.ascAppId) || normalizeString(process.env.ASC_APP_ID);
  if (configuredAppId) {
    const changed = await configureAscApp(configPath, configuredAppId);
    return { ok: true, configured: true, changed, appId: configuredAppId, appScope: 'single_app', needsUserInput: false };
  }

  const appList = await listAscApps();
  if (!appList.ok) {
    return {
      ok: false,
      configured: false,
      changed: false,
      appId: null,
      needsUserInput: false,
      error: truncate(appList.error, 800),
    };
  }

  return {
    ok: true,
    configured: true,
    changed: false,
    appId: null,
    appScope: 'all_accessible_apps',
    apps: appList.apps,
    appCount: appList.apps.length,
    needsUserInput: false,
  };
}

async function listAnalyticsProjects() {
  const result = await runShellCommand('analyticscli projects list --format json', 60_000);
  if (!result.ok) {
    return {
      ok: false,
      error: result.stderr || `exit ${result.code}`,
      projects: [],
    };
  }
  const payload = parseJsonFromStdout(result.stdout);
  return {
    ok: true,
    error: null,
    projects: extractProjectChoices(payload),
  };
}

function configHasEnabledAnalyticsSource(config) {
  return Boolean(config?.sources?.analytics && config.sources.analytics.enabled !== false);
}

async function ensureAnalyticsProjectConfigured(configPath, explicitProjectId) {
  if (normalizeString(explicitProjectId)) {
    const changed = await configureAnalyticsProject(configPath, explicitProjectId);
    return { ok: true, configured: true, changed, projectId: explicitProjectId, projectScope: 'single_project', needsUserInput: false };
  }

  const config = await readJson(configPath);
  if (!configHasEnabledAnalyticsSource(config)) {
    return { ok: true, configured: false, changed: false, projectId: null, projectScope: 'disabled', needsUserInput: false };
  }

  const configuredProjectId = normalizeString(config.project?.analyticsProjectId);
  if (configuredProjectId) {
    const changed = await configureAnalyticsProject(configPath, configuredProjectId);
    return { ok: true, configured: true, changed, projectId: configuredProjectId, projectScope: 'single_project', needsUserInput: false };
  }

  const projectList = await listAnalyticsProjects();
  if (!projectList.ok) {
    return {
      ok: true,
      configured: false,
      changed: false,
      projectId: null,
      projectScope: 'all_accessible_projects',
      projectCount: null,
      needsUserInput: false,
      warning: truncate(projectList.error, 800),
    };
  }

  return {
    ok: true,
    configured: false,
    changed: false,
    projectId: null,
    projectScope: 'all_accessible_projects',
    projectCount: projectList.projects.length,
    projects: projectList.projects,
    needsUserInput: false,
  };
}

async function buildProjectSelectionResponse({ configCreated, configPath, projectConfigured, rawError }) {
  const projectList = await listAnalyticsProjects();
  const projects = projectList.projects;
  return {
    ok: false,
    phase: 'analytics_project_scope_error',
    setupComplete: false,
    configCreated,
    configPath,
    projectConfigured,
    needsUserInput: false,
    question: null,
    message: 'An AnalyticsCLI command still requires a project pin, but connector setup should use all accessible projects by default.',
    projects,
    suggestedProjectId: null,
    nextCommand: `node scripts/openclaw-growth-start.mjs --config ${quote(configPath)}`,
    alternatePersistCommand: null,
    retryCommand: `node scripts/openclaw-growth-start.mjs --config ${quote(configPath)}`,
    rawError: truncate(rawError, 800),
    projectListError: projectList.ok ? null : truncate(projectList.error, 800),
  };
}

function remediationForCheck(checkName, configPath) {
  if (checkName === 'dependency:analyticscli') {
    return 'Run AnalyticsCLI CLI with `npx -y @analyticscli/cli@preview --help`, or use `@analyticscli/cli` after stable release.';
  }
  if (checkName === 'project:github-repo') {
    return `Set \`project.githubRepo\` in ${configPath} (owner/repo).`;
  }
  if (checkName.startsWith('secret:GITHUB_TOKEN')) {
    return 'Set `GITHUB_TOKEN` (fine-grained PAT with repository `Issues: Read/Write` and `Contents: Read`).';
  }
  if (checkName === 'source:analytics:file') {
    return 'Write `data/openclaw-growth-engineer/analytics_summary.json` via your analytics refresh step (API-key based source command/file generation).';
  }
  if (checkName === 'connection:analytics') {
    return 'Run `node scripts/openclaw-growth-wizard.mjs --connectors analytics` and paste a fresh AnalyticsCLI readonly CLI token into the local terminal wizard.';
  }
  if (checkName === 'connection:github') {
    return 'Verify `GITHUB_TOKEN` and repo access to `/repos/<owner>/<repo>` + issues API.';
  }
  if (checkName === 'connection:github-pull-requests') {
    return 'Verify `GITHUB_TOKEN` and repo access to `/repos/<owner>/<repo>/pulls`, plus `Pull requests: Read/Write` and `Contents: Read/Write` scopes.';
  }
  if (checkName === 'connection:asc_cli') {
    return 'ASC setup should list App Store Connect apps and persist the selected app automatically. Rerun the connector wizard; if this repeats, update the skill/CLI rather than setting ASC_APP_ID by hand.';
  }
  return 'Fix this blocker and rerun start.';
}

function isInvalidAscPrivateKeyError(error) {
  return /invalid private key|failed to parse|asn1|sequence truncated|malformed/i.test(String(error || ''));
}

function describeAscAppSetupFailure(error) {
  if (isInvalidAscPrivateKeyError(error)) {
    return 'Stored ASC .p8 private key is invalid or truncated. The connector wizard must reject this before saving; rerun the updated wizard and paste the full .p8 file content from BEGIN PRIVATE KEY to END PRIVATE KEY.';
  }
  return `Could not list App Store Connect apps (${error || 'unknown error'})`;
}

function remediateAscAppSetupFailure(error) {
  if (isInvalidAscPrivateKeyError(error)) {
    return 'Rerun the updated connector wizard and paste the full downloaded .p8 file content. The wizard validates it before saving ASC_PRIVATE_KEY_PATH.';
  }
  return 'Verify ASC credentials, key role access, and `asc apps list --output json`.';
}

async function runPreflight(configPath, testConnections, progressJson = false, onlyConnectors = []) {
  const commandParts = [
    nodeRuntimeScriptCommand('openclaw-growth-preflight.mjs'),
    '--config',
    quote(configPath),
  ];
  if (testConnections) {
    commandParts.push('--test-connections');
  }
  if (onlyConnectors.length > 0) {
    commandParts.push('--only-connectors', quote(onlyConnectors.join(',')));
  }
  if (progressJson) {
    commandParts.push('--progress-json');
  }
  const command = commandParts.join(' ');
  const result = await runShellCommand(command, 180_000, {
    onStderrLine: progressJson
      ? (line) => {
          if (line.startsWith('OPENCLAW_PROGRESS ')) {
            process.stderr.write(`${line}\n`);
          }
        }
      : undefined,
  });
  const payload = parseJsonFromStdout(result.stdout);
  return {
    shell: result,
    payload,
  };
}

async function runFirstPass(configPath) {
  const command = `${nodeRuntimeScriptCommand('openclaw-growth-runner.mjs')} --config ${quote(configPath)}`;
  return runShellCommand(command, 300_000);
}

async function main() {
  await loadOpenClawGrowthSecrets();
  const args = parseArgs(process.argv.slice(2));
  const configPath = path.resolve(args.config);

  const configResult = await ensureConfig(configPath);
  const initialConfig = await readJson(configPath);
  await applyOpenClawSecretRefs(initialConfig);
  const heartbeat = await ensureGrowthHeartbeat(configPath, initialConfig);
  const projectConfigured = await configureAnalyticsProject(configPath, args.project);
  const ascAppConfiguredFromArg = await configureAscApp(configPath, args.ascApp);
  const analyticscliEnsure = await ensureAnalyticsCliInstalled();
  if (!analyticscliEnsure.ok) {
    process.stdout.write(
      `${JSON.stringify(
        {
          ok: false,
          phase: 'dependency_setup',
          configCreated: configResult.created,
          configPath,
          heartbeat,
          projectConfigured,
          ascAppConfigured: ascAppConfiguredFromArg,
          blockers: [
            {
              check: 'dependency:analyticscli',
              detail: analyticscliEnsure.detail,
              remediation: `Install the npm package with \`npm install -g ${ANALYTICSCLI_PACKAGE_SPEC}\` or set ANALYTICSCLI_NPM_PREFIX to a writable prefix.`,
            },
          ],
        },
        null,
        2,
      )}\n`,
    );
    process.exitCode = 1;
    return;
  }

  if (args.connectors.length > 0) {
    emitProgress(args.progressJson, {
      phase: 'start',
      key: 'connectorSetup',
      label: 'Connector helpers',
      detail: 'installing and enabling selected helpers',
    });
  }
  const connectorSetup = args.connectors.length > 0 ? await installConnectorHelpers(configPath, args.connectors) : [];
  const failedConnectors = connectorSetup.filter((entry) => !entry.ok);
  if (args.connectors.length > 0) {
    emitProgress(args.progressJson, {
      phase: 'finish',
      key: 'connectorSetup',
      label: 'Connector helpers',
      detail: failedConnectors.length > 0
        ? `${failedConnectors.length} helper setup step(s) need attention`
        : 'selected helpers enabled',
      status: failedConnectors.length > 0 ? 'fail' : 'pass',
    });
  }
  if (failedConnectors.length > 0 && !args.run) {
    process.stdout.write(
      `${JSON.stringify(
        {
          ok: false,
          phase: 'connector_setup',
          configCreated: configResult.created,
          configPath,
          heartbeat,
          projectConfigured,
          ascAppConfigured: ascAppConfiguredFromArg,
          connectorSetup,
          blockers: failedConnectors.map((entry) => ({
            check: `connector:${entry.connector}`,
            detail: entry.detail,
            remediation:
              entry.connector === 'analytics'
                ? 'Paste a fresh AnalyticsCLI readonly token into the connector wizard so it can store ANALYTICSCLI_ACCESS_TOKEN.'
                : entry.connector === 'github'
                ? 'Provide a GitHub token through the connector wizard for code access.'
                : entry.connector === 'asc'
                  ? 'Install the ASC CLI and provide ASC_KEY_ID, ASC_ISSUER_ID, and ASC_PRIVATE_KEY_PATH or ASC_PRIVATE_KEY. Resolve the app after auth succeeds.'
                  : entry.connector === 'sentry'
                    ? 'Set SENTRY_AUTH_TOKEN plus SENTRY_ORG in the connector wizard. Defer project scope to app/repo context, or configure sources.sentry.accounts[].projects[] only when a fixed mapping is known.'
                    : 'Set REVENUECAT_API_KEY and rerun connector setup to write RevenueCat MCP config.',
          })),
        },
        null,
        2,
      )}\n`,
    );
    process.exitCode = 1;
    return;
  }

  emitProgress(args.progressJson, {
    phase: 'start',
    key: 'analyticsProject',
    label: 'AnalyticsCLI scope',
    detail: 'checking accessible analytics projects',
  });
  const analyticsProjectSetup = await ensureAnalyticsProjectConfigured(configPath, args.project);
  emitProgress(args.progressJson, {
    phase: 'finish',
    key: 'analyticsProject',
    label: 'AnalyticsCLI scope',
    detail: analyticsProjectSetup.ok
      ? analyticsProjectSetup.projectId
        ? `using ${analyticsProjectSetup.projectId}`
        : analyticsProjectSetup.projectScope === 'all_accessible_projects'
          ? `using all accessible projects (${analyticsProjectSetup.projectCount ?? 'unknown'} found)`
          : 'no project pin needed'
      : 'analytics scope check failed',
    status: analyticsProjectSetup.ok ? 'pass' : 'fail',
  });
  emitProgress(args.progressJson, {
    phase: 'start',
    key: 'ascApp',
    label: 'ASC app scope',
    detail: 'resolving App Store Connect app scope',
  });
  const ascAppSetup = await ensureAscAppConfigured(configPath, args.ascApp);
  emitProgress(args.progressJson, {
    phase: 'finish',
    key: 'ascApp',
    label: 'ASC app scope',
    detail: ascAppSetup.ok
      ? ascAppSetup.appId
        ? `using app ${ascAppSetup.appId}`
        : ascAppSetup.appScope === 'all_accessible_apps'
          ? `using all accessible apps (${ascAppSetup.appCount || 0} found)`
          : 'not enabled'
      : describeAscAppSetupFailure(ascAppSetup.error),
    status: ascAppSetup.ok ? 'pass' : 'fail',
  });
  if (!ascAppSetup.ok) {
    process.stdout.write(
      `${JSON.stringify(
        {
          ok: false,
          phase: 'asc_app_setup',
          configCreated: configResult.created,
          configPath,
          heartbeat,
          projectConfigured: projectConfigured || analyticsProjectSetup.configured,
          analyticsProjectId: analyticsProjectSetup.projectId || null,
          ascAppConfigured: false,
          connectorSetup,
          needsUserInput: false,
          question: null,
          apps: [],
          nextCommand: null,
          blockers: [
            {
              check: 'connection:asc_app',
              detail: describeAscAppSetupFailure(ascAppSetup.error),
              remediation: remediateAscAppSetupFailure(ascAppSetup.error),
            },
          ],
        },
        null,
        2,
      )}\n`,
    );
    process.exitCode = 1;
    return;
  }

  const preflightResult = await runPreflight(configPath, args.testConnections, args.progressJson, args.onlyConnectors);
  const preflightPayload = preflightResult.payload;

  if (!preflightPayload) {
    throw new Error(
      `Preflight returned invalid output.\nstdout:\n${preflightResult.shell.stdout}\nstderr:\n${preflightResult.shell.stderr}`,
    );
  }

  const failures = Array.isArray(preflightPayload.checks)
    ? preflightPayload.checks.filter((check) => check.status === 'fail')
    : [];

  if (failures.length > 0) {
    const blockers = failures.map((check) => ({
      check: check.name,
      detail: check.detail,
      remediation: remediationForCheck(check.name, configPath),
    }));
    process.stdout.write(
      `${JSON.stringify(
        {
          ok: false,
          phase: 'preflight',
          configCreated: configResult.created,
          configPath,
          heartbeat,
          projectConfigured: projectConfigured || analyticsProjectSetup.configured,
          analyticsProjectId: analyticsProjectSetup.projectId || null,
          ascAppConfigured: ascAppSetup.configured,
          ascAppId: ascAppSetup.appId || null,
          ascAppScope: ascAppSetup.appScope || null,
          githubRepo: configResult.githubRepo,
          connectorSetup,
          checks: preflightPayload.checks || [],
          blockers,
        },
        null,
        2,
      )}\n`,
    );
    process.exitCode = 1;
    return;
  }

  if (!args.run) {
    process.stdout.write(
      `${JSON.stringify(
        {
          ok: true,
          phase: 'setup_complete',
          configCreated: configResult.created,
          configPath,
          heartbeat,
          projectConfigured: projectConfigured || analyticsProjectSetup.configured,
          analyticsProjectId: analyticsProjectSetup.projectId || null,
          ascAppConfigured: ascAppSetup.configured,
          ascAppId: ascAppSetup.appId || null,
          ascAppScope: ascAppSetup.appScope || null,
          connectorSetup,
          message: 'Preflight passed. First run skipped due to --setup-only.',
        },
        null,
        2,
      )}\n`,
    );
    return;
  }

  const runResult = await runFirstPass(configPath);
  if (!runResult.ok) {
    const rawError = runResult.stderr || `exit ${runResult.code}`;
    if (isMissingProjectSelection(rawError)) {
      process.stdout.write(
        `${JSON.stringify(
          await buildProjectSelectionResponse({
            configCreated: configResult.created,
            configPath,
            projectConfigured,
            rawError,
          }),
          null,
          2,
        )}\n`,
      );
      return;
    }

    process.stdout.write(
      `${JSON.stringify(
        {
          ok: false,
          phase: 'first_run',
          configCreated: configResult.created,
          configPath,
          heartbeat,
          projectConfigured,
          error: rawError,
        },
        null,
        2,
      )}\n`,
    );
    process.exitCode = 1;
    return;
  }

  const actionMode = getActionMode(await readJson(configPath));
  process.stdout.write(
    `${JSON.stringify(
      {
        ok: true,
        phase: 'first_run_complete',
        configCreated: configResult.created,
        configPath,
        heartbeat,
        projectConfigured,
        actionMode,
        runnerOutput: runResult.stdout.trim(),
      },
      null,
      2,
    )}\n`,
  );
}

main().catch((error) => {
  process.stderr.write(`${error instanceof Error ? error.message : String(error)}\n`);
  process.exitCode = 1;
});
