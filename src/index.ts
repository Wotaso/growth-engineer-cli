#!/usr/bin/env node

import { mkdir, readFile, writeFile } from 'node:fs/promises';
import { appendFileSync, existsSync, mkdirSync, readFileSync, writeFileSync } from 'node:fs';
import { createHash } from 'node:crypto';
import { basename, delimiter, dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { Command } from 'commander';
import {
  fileExists,
  parseOpenClawConfig,
  readJsonFile,
  readOpenClawConfig,
  toLegacyGrowthConfig,
  writeJsonFile,
  writeOpenClawConfig,
  type OpenClawConfig,
} from './config.js';
import { isCommandAvailable, runCommand, runCommandInherited } from './shell.js';

type DeliveryState = {
  lastDeliveredFingerprint?: string;
  lastDeliveredAt?: string;
};

type RuntimePreflightResult = {
  ok: boolean;
  summary: {
    pass: number;
    warn: number;
    fail: number;
  };
  checks: Array<{
    name: string;
    status: 'pass' | 'warn' | 'fail';
    detail: string;
  }>;
};

type IssueDraft = {
  signal_id: string;
  title: string;
  body: string;
  files?: string[];
  priority?: string;
  area?: string;
  source?: string;
  expected_impact?: string;
  confidence?: string;
};

type IssueDraftPayload = {
  generated_at: string;
  repo_root: string;
  issue_count: number;
  issues: IssueDraft[];
};

const moduleDir = dirname(fileURLToPath(import.meta.url));
const packageRoot = resolve(moduleDir, '..');
const localAnalyticsCliDir = resolve(packageRoot, '../cli');
const analyticsCliPackageSpec = process.env.ANALYTICSCLI_CLI_PACKAGE || '@analyticscli/cli@preview';
const analyticsCliNpmPrefix =
  process.env.ANALYTICSCLI_NPM_PREFIX ||
  (process.env.HOME ? join(process.env.HOME, '.local') : resolve(process.cwd(), '.analyticscli-npm'));
const program = new Command();

type ConnectorKey = 'github' | 'asc' | 'revenuecat';

type ConnectorInstallResult = {
  connector: ConnectorKey;
  ok: boolean;
  detail: string;
};

const shellQuote = (value: string): string => {
  if (/^[a-zA-Z0-9_./:@-]+$/.test(value)) {
    return value;
  }
  return `'${value.replace(/'/g, `'\\''`)}'`;
};

const truncate = (value: string, max = 240): string =>
  value.length <= max ? value : `${value.slice(0, max)}...`;

const resolveCommandPath = (command: string): string | null => {
  const result = runCommand('sh', ['-c', `command -v ${shellQuote(command)}`], {
    timeoutMs: 10_000,
  });
  return result.ok ? result.stdout.trim() : null;
};

const commandExists = (command: string): boolean => resolveCommandPath(command) !== null;

const appendDetail = (details: string[], label: string, result: ReturnType<typeof runCommand>): void => {
  if (result.ok) {
    details.push(`${label}: ok`);
    return;
  }

  const output = truncate(`${result.stderr}\n${result.stdout}`.trim() || `exit code ${result.code ?? 'unknown'}`);
  details.push(`${label}: ${result.timedOut ? 'timed out' : output}`);
};

const isPermissionFailure = (output: string): boolean =>
  /EACCES|permission denied|access denied|operation not permitted/i.test(output);

const isClawHubSuspiciousSkillFailure = (output: string): boolean =>
  /Use --force to install suspicious skills in non-interactive mode|Already installed: .*use --force/i.test(output);

const prependToPath = (binDir: string): void => {
  process.env.PATH = `${binDir}${delimiter}${process.env.PATH || ''}`;
};

const getPathProfileEntries = (binDir: string): string[] => {
  const entries = [binDir];
  if (process.env.HOME && resolve(binDir) === resolve(process.env.HOME, '.local', 'bin')) {
    entries.push(join(process.env.HOME, '.local', 'analyticscli-npm', 'bin'));
  }
  return entries;
};

const renderProfilePathEntries = (binDir: string): string =>
  getPathProfileEntries(binDir)
    .map((entry) => {
      const home = process.env.HOME ? resolve(process.env.HOME) : null;
      const resolved = resolve(entry);
      if (home && (resolved === home || resolved.startsWith(`${home}/`))) {
        return `$HOME/${resolved.slice(home.length + 1)}`;
      }
      return entry;
    })
    .join(':');

const ensureProfilePath = (binDir: string): boolean => {
  if (process.env.ANALYTICSCLI_SKIP_PROFILE_UPDATE === 'true' || !process.env.HOME) {
    return false;
  }

  const line = `export PATH="${renderProfilePathEntries(binDir)}:$PATH"`;
  const profiles = ['.profile', '.bashrc', '.bash_profile', '.zshrc', '.zprofile'].map((name) =>
    join(process.env.HOME!, name),
  );
  let wrote = false;

  for (const profile of profiles) {
    let current = '';
    if (existsSync(profile)) {
      current = readFileSync(profile, 'utf8');
    }
    if (!current.includes(line)) {
      appendFileSync(profile, `\n# AnalyticsCLI CLI user-local npm bin\n${line}\n`, 'utf8');
      wrote = true;
    }
  }

  return wrote;
};

const verifyFreshShellProfile = (): boolean => {
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

  return probes.some((probe) => {
    if (!existsSync(probe.shell)) {
      return false;
    }
    const result = runCommand(
      'sh',
      [
        '-c',
        `env HOME=${shellQuote(process.env.HOME!)} PATH=${shellQuote(cleanPath)} ${shellQuote(probe.shell)} -lc ${shellQuote(probe.command)}`,
      ],
      { timeoutMs: 30_000 },
    );
    return result.ok;
  });
};

const isUserLocalBin = (binDir: string): boolean => {
  if (!process.env.HOME) {
    return false;
  }
  const home = resolve(process.env.HOME);
  const resolved = resolve(binDir);
  return resolved === home || resolved.startsWith(`${home}/`);
};

const ensureAnalyticsCliPackage = (): { ok: boolean; detail: string } => {
  const beforePath = resolveCommandPath('analyticscli');
  if (!isCommandAvailable('npm')) {
    return beforePath
      ? {
          ok: true,
          detail: `analyticscli binary found at ${beforePath}; npm unavailable, so package update was skipped`,
        }
      : {
          ok: false,
          detail: `analyticscli binary missing and npm is unavailable; install ${analyticsCliPackageSpec}`,
        };
  }

  const globalInstall = runCommand('npm', ['install', '-g', analyticsCliPackageSpec], {
    timeoutMs: 180_000,
  });
  if (!globalInstall.ok) {
    const installOutput = `${globalInstall.stderr}\n${globalInstall.stdout}`;
    if (isPermissionFailure(installOutput)) {
      mkdirSync(analyticsCliNpmPrefix, { recursive: true });
      const localInstall = runCommand(
        'npm',
        ['install', '-g', '--prefix', analyticsCliNpmPrefix, analyticsCliPackageSpec],
        {
          timeoutMs: 180_000,
        },
      );
      if (!localInstall.ok) {
        return beforePath
          ? {
              ok: true,
              detail: `analyticscli binary found at ${beforePath}; update failed globally and in user-local prefix (${truncate(localInstall.stderr || localInstall.stdout)})`,
            }
          : {
              ok: false,
              detail: `npm install failed globally and in user-local prefix ${analyticsCliNpmPrefix}: ${truncate(localInstall.stderr || localInstall.stdout)}`,
            };
      }
      const localBinDir = join(analyticsCliNpmPrefix, 'bin');
      prependToPath(localBinDir);
      ensureProfilePath(localBinDir);
    } else {
      return beforePath
        ? {
            ok: true,
            detail: `analyticscli binary found at ${beforePath}; package update failed (${truncate(installOutput)})`,
          }
        : {
            ok: false,
            detail: `npm install -g ${analyticsCliPackageSpec} failed: ${truncate(installOutput)}`,
          };
    }
  }

  const afterPath = resolveCommandPath('analyticscli');
  if (afterPath) {
    const helpCheck = runCommand('sh', ['-c', 'analyticscli --help >/dev/null 2>&1'], {
      timeoutMs: 30_000,
    });
    if (!helpCheck.ok) {
      return {
        ok: false,
        detail: `analyticscli binary found at ${afterPath}, but --help failed: ${truncate(helpCheck.stderr || helpCheck.stdout)}`,
      };
    }

    const binDir = dirname(afterPath);
    if (isUserLocalBin(binDir)) {
      ensureProfilePath(binDir);
      if (!verifyFreshShellProfile()) {
        return {
          ok: false,
          detail: `analyticscli works at ${afterPath}, but a fresh shell still cannot resolve it after profile update; add ${renderProfilePathEntries(binDir)} to PATH`,
        };
      }
      return {
        ok: true,
        detail: `analyticscli package ensured via ${analyticsCliPackageSpec}; binary found at ${afterPath}; shell profiles updated and fresh shell verification passed`,
      };
    }
  }

  return afterPath
    ? {
        ok: true,
        detail: `analyticscli package ensured via ${analyticsCliPackageSpec}; binary found at ${afterPath}`,
      }
    : {
        ok: false,
        detail: `Installed ${analyticsCliPackageSpec}, but analyticscli is still not on PATH`,
      };
};

const resolveRuntimeInvocation = (scriptName: string): { command: string; args: string[] } => {
  const sourcePath = resolve(packageRoot, 'src', 'runtime', `${scriptName}.mts`);
  if (existsSync(sourcePath)) {
    return {
      command: 'tsx',
      args: [sourcePath],
    };
  }

  const distPath = resolve(packageRoot, 'dist', 'runtime', `${scriptName}.mjs`);
  if (existsSync(distPath)) {
    return {
      command: 'node',
      args: [distPath],
    };
  }

  throw new Error(`Runtime script not found: ${scriptName}`);
};

const runRuntime = (
  scriptName: string,
  args: string[],
  options?: { cwd?: string; input?: string; timeoutMs?: number },
) => {
  const runtime = resolveRuntimeInvocation(scriptName);
  return runCommand(runtime.command, [...runtime.args, ...args], options);
};

const runRuntimeInteractive = (scriptName: string, args: string[], options?: { cwd?: string; timeoutMs?: number }) => {
  const runtime = resolveRuntimeInvocation(scriptName);
  return runCommandInherited(runtime.command, [...runtime.args, ...args], options);
};

const resolveTemplatePath = () => resolve(packageRoot, 'templates', 'config.example.json');
const forwardedArgsAfter = (needle: string): string[] => {
  const index = process.argv.indexOf(needle);
  return index >= 0 ? process.argv.slice(index + 1) : [];
};

const parseGitHubRepoFromRemote = (remoteUrl: string): string | null => {
  const value = remoteUrl.trim();
  if (!value) {
    return null;
  }

  const sshMatch = value.match(/^git@github\.com:([^/]+\/[^/]+?)(?:\.git)?$/i);
  if (sshMatch?.[1]) {
    return sshMatch[1];
  }

  const httpsMatch = value.match(/^https?:\/\/github\.com\/([^/]+\/[^/]+?)(?:\.git)?$/i);
  if (httpsMatch?.[1]) {
    return httpsMatch[1];
  }

  return null;
};

const detectGitHubRepo = (cwd: string): string | null => {
  const result = runCommand('git', ['config', '--get', 'remote.origin.url'], {
    cwd,
    timeoutMs: 10_000,
  });
  if (!result.ok) {
    return null;
  }
  return parseGitHubRepoFromRemote(result.stdout.trim());
};

const resolveDefaultSourceCommand = (repoRoot: string, command: 'analytics' | 'asc' | 'feedback'): string => {
  if (command === 'analytics') {
    const repoScript = resolve(repoRoot, 'scripts', 'export-analytics-summary.mjs');
    return fileExists(repoScript)
      ? 'node scripts/export-analytics-summary.mjs'
      : 'openclaw exporters analytics-summary';
  }

  if (command === 'feedback') {
    return 'analyticscli feedback summary --format json';
  }

  const repoScript = resolve(repoRoot, 'scripts', 'export-asc-summary.mjs');
  return fileExists(repoScript) ? 'node scripts/export-asc-summary.mjs' : 'openclaw exporters asc-summary';
};

const resolvePaths = (configPath: string) => {
  const baseDir = dirname(resolve(configPath));
  const openclawDir = resolve(baseDir, '.openclaw');
  const runtimeDir = resolve(openclawDir, 'runtime');
  return {
    baseDir,
    openclawDir,
    runtimeDir,
    legacyConfigPath: resolve(runtimeDir, 'legacy-config.json'),
    legacyStatePath: resolve(runtimeDir, 'legacy-state.json'),
    orchestratorStatePath: resolve(openclawDir, 'state.json'),
  };
};

const loadTemplateConfig = async (): Promise<OpenClawConfig> => {
  const raw = await readFile(resolveTemplatePath(), 'utf8');
  return parseOpenClawConfig(JSON.parse(raw));
};

const createInitialConfig = async (configPath: string, repoRoot: string, force: boolean): Promise<void> => {
  const targetPath = resolve(configPath);
  if (!force && fileExists(targetPath)) {
    throw Object.assign(new Error(`Config already exists: ${targetPath}`), { exitCode: 2 });
  }

  const template = await loadTemplateConfig();
  const resolvedRepoRoot = resolve(repoRoot);
  const configBaseDir = dirname(targetPath);
  const repoRootForConfig = resolve(configBaseDir) === resolvedRepoRoot ? '.' : resolvedRepoRoot;
  const githubRepo = detectGitHubRepo(repoRoot) ?? template.project.githubRepo;
  const nextConfig: OpenClawConfig = {
    ...template,
    version: 7,
    generatedAt: new Date().toISOString(),
    project: {
      ...template.project,
      githubRepo,
      repoRoot: repoRootForConfig,
      outFile: 'data/openclaw-growth-engineer/issues.generated.json',
    },
    sources: {
      ...template.sources,
      analytics: {
        ...template.sources.analytics,
        enabled: true,
        mode: 'command',
        command: resolveDefaultSourceCommand(repoRoot, 'analytics'),
      },
      revenuecat: {
        ...template.sources.revenuecat,
        enabled: false,
      },
      sentry: {
        ...template.sources.sentry,
        enabled: false,
      },
      feedback: {
        ...template.sources.feedback,
        enabled: true,
        mode: 'command',
        command: resolveDefaultSourceCommand(repoRoot, 'feedback'),
        cursorMode: 'auto_since_last_fetch',
        initialLookback: '30d',
      },
      extra: (template.sources.extra ?? []).map((source) => {
        if (source.service === 'asc-cli') {
          return {
            ...source,
            command: resolveDefaultSourceCommand(repoRoot, 'asc'),
          };
        }
        return source;
      }),
    },
    strategy: template.strategy,
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
    },
  };

  await writeOpenClawConfig(targetPath, nextConfig);
};

const runSharedAnalyticsSetup = (): { ok: boolean; detail: string } => {
  const sharedArgs = ['setup', '--skip-login', '--agents', 'all', '--no-auto-skill-update'];
  const cliInstall = ensureAnalyticsCliPackage();
  if (!cliInstall.ok) {
    return cliInstall;
  }

  const interpretSetupResult = (
    result: ReturnType<typeof runCommand>,
    fallbackDetail: string,
  ): { ok: boolean; detail: string } => {
    const detail = result.stderr.trim() || result.stdout.trim() || fallbackDetail;
    if (!result.ok) {
      return {
        ok: false,
        detail,
      };
    }

    const jsonStart = result.stdout.indexOf('{');
    if (jsonStart >= 0) {
      try {
        const payload = JSON.parse(result.stdout.slice(jsonStart)) as {
          ok?: boolean;
          skillSetup?: Array<{ target?: string; ok?: boolean; skipped?: boolean; detail?: string }>;
        };
        const failedSkills = (payload.skillSetup ?? []).filter((entry) => entry.ok === false);
        if (payload.ok === false || failedSkills.length > 0) {
          return {
            ok: false,
            detail: failedSkills.length
              ? `AnalyticsCLI setup reported failed skill setup: ${failedSkills
                  .map((entry) => `${entry.target ?? 'unknown'}: ${entry.detail ?? 'failed'}`)
                  .join('; ')}`
              : detail,
          };
        }
      } catch {
        // Keep the command result as the source of truth when stdout is not JSON.
      }
    }

    return {
      ok: true,
      detail,
    };
  };

  if (fileExists(resolve(localAnalyticsCliDir, 'src/index.ts')) && isCommandAvailable('pnpm')) {
    const result = runCommand(
      'pnpm',
      ['--filter', '@analyticscli/cli', 'dev', ...sharedArgs],
      {
        cwd: resolve(packageRoot, '../..'),
        timeoutMs: 10 * 60_000,
      },
    );
    return interpretSetupResult(result, 'analyticscli local setup finished');
  }

  if (isCommandAvailable('analyticscli')) {
    const result = runCommand('analyticscli', sharedArgs, {
      timeoutMs: 10 * 60_000,
    });
    return interpretSetupResult(result, 'analyticscli setup finished');
  }

  const result = runCommand('npx', ['-y', '@analyticscli/cli@preview', ...sharedArgs], {
    timeoutMs: 10 * 60_000,
  });
  return interpretSetupResult(result, 'analyticscli preview setup finished');
};

const normalizeConnectorKey = (value: string): ConnectorKey | 'all' | null => {
  const normalized = value.trim().toLowerCase().replace(/[_\s]+/g, '-');
  if (!normalized) {
    return null;
  }

  if (normalized === 'all') {
    return 'all';
  }

  if (['github', 'gh', 'github-code', 'codebase', 'code-access'].includes(normalized)) {
    return 'github';
  }

  if (['asc', 'asc-cli', 'app-store-connect', 'appstoreconnect', 'app-store'].includes(normalized)) {
    return 'asc';
  }

  if (['revenuecat', 'revenue-cat', 'rc', 'revenuecat-mcp'].includes(normalized)) {
    return 'revenuecat';
  }

  return null;
};

const parseConnectorList = (value?: string): ConnectorKey[] => {
  if (!value?.trim()) {
    return [];
  }

  const connectors = new Set<ConnectorKey>();
  for (const entry of value.split(',')) {
    const connector = normalizeConnectorKey(entry);
    if (!connector) {
      throw Object.assign(
        new Error(`Unknown connector "${entry.trim()}". Use github, asc, revenuecat, or all.`),
        { exitCode: 2 },
      );
    }

    if (connector === 'all') {
      connectors.add('github');
      connectors.add('asc');
      connectors.add('revenuecat');
    } else {
      connectors.add(connector);
    }
  }

  return [...connectors];
};

const installClawHubSkill = (skillName: string, details: string[]): boolean => {
  const invoker = isCommandAvailable('clawhub')
    ? { command: 'clawhub', prefix: [] as string[] }
    : isCommandAvailable('npx')
      ? { command: 'npx', prefix: ['-y', 'clawhub'] }
      : null;
  if (!invoker) {
    details.push(`ClawHub skill ${skillName}: skipped because neither clawhub nor npx is available`);
    return false;
  }

  let install = runCommand(invoker.command, [...invoker.prefix, 'install', skillName], {
    timeoutMs: 120_000,
  });
  const installOutput = `${install.stderr}\n${install.stdout}`;
  if (!install.ok && isClawHubSuspiciousSkillFailure(installOutput)) {
    install = runCommand(invoker.command, [...invoker.prefix, 'install', skillName, '--force'], {
      timeoutMs: 120_000,
    });
  }
  appendDetail(details, `ClawHub skill ${skillName}`, install);
  return install.ok;
};

const installCodexClaudeSkill = (repo: string, details: string[]): boolean => {
  if (!isCommandAvailable('npx')) {
    details.push(`Agent skill ${repo}: skipped because npx is unavailable`);
    return false;
  }

  const install = runCommand('npx', ['-y', 'skills', 'add', repo], {
    timeoutMs: 180_000,
  });
  appendDetail(details, `Agent skill ${repo}`, install);
  return install.ok;
};

const installGitHubConnector = (): ConnectorInstallResult => {
  const details: string[] = [];
  installClawHubSkill('github', details);

  const beforePath = resolveCommandPath('gh');
  if (beforePath) {
    details.push(`gh binary found at ${beforePath}`);
    return {
      connector: 'github',
      ok: true,
      detail: details.join('; '),
    };
  }

  if (commandExists('brew')) {
    const brewInstall = runCommand('brew', ['install', 'gh'], {
      timeoutMs: 10 * 60_000,
    });
    appendDetail(details, 'brew install gh', brewInstall);
  } else if (commandExists('winget')) {
    const wingetInstall = runCommand('winget', ['install', '--id', 'GitHub.cli', '-e', '--silent'], {
      timeoutMs: 10 * 60_000,
    });
    appendDetail(details, 'winget install GitHub.cli', wingetInstall);
  } else {
    details.push('No supported non-interactive gh installer found; install GitHub CLI via Homebrew, winget, or the official package for this OS');
  }

  const afterPath = resolveCommandPath('gh');
  return {
    connector: 'github',
    ok: Boolean(afterPath),
    detail: afterPath
      ? `${details.join('; ')}; gh binary found at ${afterPath}; next run gh auth status or gh auth login`
      : `${details.join('; ')}; GitHub CLI is still missing`,
  };
};

const installAscConnector = (): ConnectorInstallResult => {
  const details: string[] = [];
  installCodexClaudeSkill('rorkai/app-store-connect-cli-skills', details);

  const beforePath = resolveCommandPath('asc');
  if (beforePath) {
    details.push(`asc binary found at ${beforePath}`);
    return {
      connector: 'asc',
      ok: true,
      detail: details.join('; '),
    };
  }

  if (commandExists('brew')) {
    const brewInstall = runCommand('brew', ['install', 'asc'], {
      timeoutMs: 10 * 60_000,
    });
    appendDetail(details, 'brew install asc', brewInstall);
  }

  if (!resolveCommandPath('asc') && commandExists('curl')) {
    const installScript = runCommand('sh', ['-c', 'curl -fsSL https://asccli.sh/install | bash'], {
      timeoutMs: 10 * 60_000,
    });
    appendDetail(details, 'asc install script', installScript);
  }

  const afterPath = resolveCommandPath('asc');
  return {
    connector: 'asc',
    ok: Boolean(afterPath),
    detail: afterPath
      ? `${details.join('; ')}; asc binary found at ${afterPath}; next run asc auth status --validate or asc auth login`
      : `${details.join('; ')}; asc CLI is still missing`,
  };
};

const escapeTomlString = (value: string): string => value.replace(/\\/g, '\\\\').replace(/"/g, '\\"');

const resolveMcpNpmCacheDir = (): string =>
  process.env.OPENCLAW_MCP_NPM_CACHE ||
  (process.env.HOME ? join(process.env.HOME, '.cache', 'openclaw-mcp-npm') : resolve(process.cwd(), '.openclaw-mcp-npm-cache'));

const upsertRevenueCatCodexMcpConfig = (apiKey: string): string | null => {
  if (!process.env.HOME) {
    return null;
  }

  const configDir = join(process.env.HOME, '.codex');
  const configFile = join(configDir, 'config.toml');
  mkdirSync(configDir, { recursive: true });
  const existing = existsSync(configFile) ? readFileSync(configFile, 'utf8') : '';
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
  writeFileSync(configFile, `${next.trimEnd()}\n`, 'utf8');
  return configFile;
};

const installRevenueCatConnector = (): ConnectorInstallResult => {
  const details: string[] = [];
  if (!isCommandAvailable('npx')) {
    return {
      connector: 'revenuecat',
      ok: false,
      detail: 'npx is required for the RevenueCat MCP transport (mcp-remote), but npx is unavailable',
    };
  }

  const mcpRemoteCheck = runCommand('npx', ['--yes', '--cache', resolveMcpNpmCacheDir(), 'mcp-remote'], {
    timeoutMs: 120_000,
  });
  const mcpRemoteOutput = `${mcpRemoteCheck.stderr}\n${mcpRemoteCheck.stdout}`;
  const mcpRemoteAvailable = mcpRemoteCheck.ok || /Usage: .*mcp-remote|Usage: .*proxy\.ts/i.test(mcpRemoteOutput);
  if (mcpRemoteAvailable) {
    details.push(`RevenueCat MCP transport mcp-remote is available via npx cache ${resolveMcpNpmCacheDir()}`);
  } else {
    appendDetail(details, 'npx mcp-remote availability check', mcpRemoteCheck);
  }
  if (!mcpRemoteAvailable) {
    return {
      connector: 'revenuecat',
      ok: false,
      detail: details.join('; '),
    };
  }

  const revenuecatApiKey = process.env.REVENUECAT_API_KEY?.trim();
  if (revenuecatApiKey) {
    const configFile = upsertRevenueCatCodexMcpConfig(revenuecatApiKey);
    details.push(
      configFile
        ? `RevenueCat MCP configured in ${configFile} using REVENUECAT_API_KEY`
        : 'RevenueCat MCP transport is available; HOME is missing so MCP client config was not written',
    );
  } else {
    details.push(
      'RevenueCat MCP transport is available; set REVENUECAT_API_KEY, then rerun this command to write the MCP client config',
    );
  }

  return {
    connector: 'revenuecat',
    ok: true,
    detail: details.join('; '),
  };
};

const enableConnectorConfig = async (configPath: string, connectors: ConnectorKey[], repoRoot: string): Promise<void> => {
  if (connectors.length === 0 || !fileExists(resolve(configPath))) {
    return;
  }

  const config = await readOpenClawConfig(resolve(configPath));
  const nextConfig: OpenClawConfig = {
    ...config,
    sources: {
      ...config.sources,
      revenuecat: connectors.includes('revenuecat')
        ? {
            ...config.sources.revenuecat,
            enabled: true,
          }
        : config.sources.revenuecat,
      extra: config.sources.extra.map((source) => {
        if (connectors.includes('asc') && source.service === 'asc-cli') {
          return {
            ...source,
            enabled: true,
            mode: 'command',
            command: source.command || resolveDefaultSourceCommand(repoRoot, 'asc'),
          };
        }
        return source;
      }),
    },
  };

  await writeOpenClawConfig(resolve(configPath), nextConfig);
};

const installConnectorHelpers = async (
  configPath: string,
  connectors: ConnectorKey[],
  repoRoot: string,
): Promise<ConnectorInstallResult[]> => {
  await enableConnectorConfig(configPath, connectors, repoRoot);

  return connectors.map((connector) => {
    if (connector === 'github') {
      return installGitHubConnector();
    }
    if (connector === 'asc') {
      return installAscConnector();
    }
    return installRevenueCatConnector();
  });
};

const writeLegacyConfig = async (configPath: string, config: OpenClawConfig): Promise<ReturnType<typeof resolvePaths>> => {
  const paths = resolvePaths(configPath);
  await mkdir(paths.runtimeDir, { recursive: true });
  await writeJsonFile(paths.legacyConfigPath, toLegacyGrowthConfig(resolve(configPath), config));
  return paths;
};

const runPreflight = async (configPath: string, options: { testConnections: boolean }) => {
  const config = await readOpenClawConfig(resolve(configPath));
  const paths = await writeLegacyConfig(configPath, config);
  const runtimeArgs = ['--config', paths.legacyConfigPath, '--json'];
  if (options.testConnections) {
    runtimeArgs.push('--test-connections');
  }
  const runtime = runRuntime('openclaw-growth-preflight', runtimeArgs, {
    cwd: dirname(resolve(configPath)),
    timeoutMs: 120_000,
  });

  let runtimePayload: RuntimePreflightResult = {
    ok: false,
    summary: {
      pass: 0,
      warn: 0,
      fail: 1,
    },
    checks: [
      {
        name: 'runtime',
        status: 'fail',
        detail: runtime.stderr.trim() || runtime.stdout.trim() || 'preflight failed',
      },
    ],
  };

  if (runtime.stdout.trim()) {
    runtimePayload = JSON.parse(runtime.stdout) as RuntimePreflightResult;
  }

  const extraChecks: RuntimePreflightResult['checks'] = [];
  if (config.deliveries.openclawChat.enabled) {
    extraChecks.push({
      name: 'delivery:openclaw-chat',
      status: 'pass',
      detail: `writes ${config.deliveries.openclawChat.markdownPath} and ${config.deliveries.openclawChat.jsonPath}`,
    });
  }

  if (config.deliveries.slack.enabled) {
    const webhookEnv = config.deliveries.slack.webhookEnv;
    extraChecks.push({
      name: `secret:${webhookEnv}`,
      status: process.env[webhookEnv] ? 'pass' : 'fail',
      detail: process.env[webhookEnv] ? 'set' : 'missing (required for Slack delivery)',
    });
  }

  if (config.deliveries.webhook.enabled) {
    const urlEnv = config.deliveries.webhook.urlEnv;
    extraChecks.push({
      name: `secret:${urlEnv}`,
      status: process.env[urlEnv] ? 'pass' : 'fail',
      detail: process.env[urlEnv] ? 'set' : 'missing (required for webhook delivery)',
    });
  }

  const checks = [...runtimePayload.checks, ...extraChecks];
  const summary = checks.reduce(
    (acc, check) => {
      acc[check.status] += 1;
      return acc;
    },
    { pass: 0, warn: 0, fail: 0 },
  );

  return {
    ok: summary.fail === 0,
    summary,
    checks,
  };
};

const renderPreflightChecklist = (result: Awaited<ReturnType<typeof runPreflight>>): string => {
  const blocking = result.checks.filter((check) => check.status === 'fail');
  const warnings = result.checks.filter((check) => check.status === 'warn');
  const lines = [
    `Preflight failed: ${result.summary.pass} pass, ${result.summary.warn} warn, ${result.summary.fail} fail`,
    '',
  ];

  const analyticsFailures = blocking.filter((check) => check.name.startsWith('connection:analytics'));
  const feedbackAuthFailure = blocking.find(
    (check) => check.name === 'connection:feedback' && /UNAUTHORIZED|Authentication required/i.test(check.detail),
  );
  if (analyticsFailures.length > 0 || feedbackAuthFailure) {
    lines.push('Next required input: AnalyticsCLI analytics baseline');
    lines.push('- Why: growth proposals need project analytics and feedback data before the first run can generate useful work.');
    lines.push(
      '- Minimum access: a readonly AnalyticsCLI CLI token with access to the target project, stored via `analyticscli login` or provided as `ANALYTICSCLI_ACCESS_TOKEN` from a secret store.',
    );
    lines.push('- Where: dash.analyticscli.com -> API Keys.');
    lines.push('');
  }

  const remainingBlocking = blocking.filter(
    (check) =>
      !check.name.startsWith('connection:analytics') &&
      !(check.name === 'connection:feedback' && /UNAUTHORIZED|Authentication required/i.test(check.detail)),
  );
  if (remainingBlocking.length > 0) {
    lines.push('Other blockers:');
    for (const check of remainingBlocking) {
      lines.push(`- ${check.name}: ${check.detail}`);
    }
    lines.push('');
  }

  if (warnings.length > 0) {
    lines.push('Warnings:');
    for (const check of warnings.slice(0, 5)) {
      lines.push(`- ${check.name}: ${check.detail}`);
    }
  }

  return `${lines.join('\n').trimEnd()}\n`;
};

const computeIssuesFingerprint = (payload: IssueDraftPayload): string => {
  const normalized = payload.issues
    .map((issue) => `${issue.title}|${issue.priority ?? 'medium'}|${issue.area ?? 'general'}`)
    .sort()
    .join('\n');
  return createHash('sha256').update(normalized).digest('hex');
};

const buildSlackText = (payload: IssueDraftPayload): string => {
  const lines = [
    `OpenClaw generated ${payload.issue_count} proposal(s) for ${payload.repo_root}.`,
    ...payload.issues.slice(0, 5).map((issue, index) => `${index + 1}. ${issue.title}`),
  ];
  return lines.join('\n');
};

const buildOpenClawChatMarkdown = (payload: IssueDraftPayload): string => {
  const sections = [
    '# OpenClaw Proposal Outbox',
    '',
    `Generated: ${payload.generated_at}`,
    `Repo: ${payload.repo_root}`,
    `Proposals: ${payload.issue_count}`,
    '',
    'Use this file as the chat handoff for OpenClaw. Ask OpenClaw to inspect the generated proposals and either summarize them, create a GitHub issue/PR, or implement one of them.',
  ];

  for (const [index, issue] of payload.issues.entries()) {
    sections.push('');
    sections.push(`## ${index + 1}. ${issue.title}`);
    sections.push(`- Priority: ${issue.priority ?? 'medium'}`);
    sections.push(`- Area: ${issue.area ?? 'general'}`);
    if (issue.source) {
      sections.push(`- Source: ${issue.source}`);
    }
    if (issue.expected_impact) {
      sections.push(`- Expected impact: ${issue.expected_impact}`);
    }
    if (issue.confidence) {
      sections.push(`- Confidence: ${issue.confidence}`);
    }
    if (issue.files?.length) {
      sections.push(`- Candidate files: ${issue.files.map((file) => `\`${file}\``).join(', ')}`);
    }
    sections.push('');
    sections.push(issue.body.trim());
  }

  return `${sections.join('\n')}\n`;
};

const writeOpenClawChatOutbox = async (
  configPath: string,
  config: OpenClawConfig,
  payload: IssueDraftPayload,
  fingerprint: string,
) => {
  const baseDir = dirname(resolve(configPath));
  const markdownPath = resolve(baseDir, config.deliveries.openclawChat.markdownPath);
  const jsonPath = resolve(baseDir, config.deliveries.openclawChat.jsonPath);

  await mkdir(dirname(markdownPath), { recursive: true });
  await mkdir(dirname(jsonPath), { recursive: true });
  await writeFile(markdownPath, buildOpenClawChatMarkdown(payload), 'utf8');
  await writeJsonFile(jsonPath, {
    channel: 'openclaw_chat',
    generatedAt: payload.generated_at,
    fingerprint,
    repoRoot: payload.repo_root,
    issueCount: payload.issue_count,
    issues: payload.issues,
  });

  return {
    markdownPath,
    jsonPath,
  };
};

const sendSlackMessage = async (config: OpenClawConfig, payload: IssueDraftPayload) => {
  const webhookEnv = config.deliveries.slack.webhookEnv;
  const webhookUrl = process.env[webhookEnv];
  if (!webhookUrl) {
    throw new Error(`Missing ${webhookEnv} for Slack delivery.`);
  }

  const response = await fetch(webhookUrl, {
    method: 'POST',
    headers: {
      'content-type': 'application/json',
    },
    body: JSON.stringify({
      text: buildSlackText(payload),
      username: config.deliveries.slack.username,
    }),
  });

  if (!response.ok) {
    throw new Error(`Slack delivery failed (${response.status})`);
  }
};

const sendWebhook = async (config: OpenClawConfig, payload: IssueDraftPayload) => {
  const urlEnv = config.deliveries.webhook.urlEnv;
  const webhookUrl = process.env[urlEnv];
  if (!webhookUrl) {
    throw new Error(`Missing ${urlEnv} for webhook delivery.`);
  }

  const response = await fetch(webhookUrl, {
    method: config.deliveries.webhook.method,
    headers: {
      'content-type': 'application/json',
      ...config.deliveries.webhook.headers,
    },
    body: JSON.stringify({
      generatedAt: payload.generated_at,
      repoRoot: payload.repo_root,
      issues: payload.issues,
    }),
  });

  if (!response.ok) {
    throw new Error(`Webhook delivery failed (${response.status})`);
  }
};

const deliverArtifacts = async (configPath: string, config: OpenClawConfig, payload: IssueDraftPayload) => {
  const fingerprint = computeIssuesFingerprint(payload);
  const paths = resolvePaths(configPath);
  const state = fileExists(paths.orchestratorStatePath)
    ? await readJsonFile<DeliveryState>(paths.orchestratorStatePath)
    : {};
  const deliveryTargets: string[] = [];

  if (config.schedule.skipIfIssueSetUnchanged && state.lastDeliveredFingerprint === fingerprint) {
    return {
      delivered: false,
      fingerprint,
      skippedReason: 'issue_set_unchanged',
      deliveryTargets,
    };
  }

  if (config.deliveries.openclawChat.enabled) {
    await writeOpenClawChatOutbox(configPath, config, payload, fingerprint);
    deliveryTargets.push('openclaw_chat');
  }

  if (config.deliveries.slack.enabled) {
    await sendSlackMessage(config, payload);
    deliveryTargets.push('slack');
  }

  if (config.deliveries.webhook.enabled) {
    await sendWebhook(config, payload);
    deliveryTargets.push('webhook');
  }

  await writeJsonFile(paths.orchestratorStatePath, {
    lastDeliveredFingerprint: fingerprint,
    lastDeliveredAt: new Date().toISOString(),
  } satisfies DeliveryState);

  return {
    delivered: true,
    fingerprint,
    skippedReason: null,
    deliveryTargets,
  };
};

const runOnce = async (configPath: string) => {
  const config = await readOpenClawConfig(resolve(configPath));
  const paths = await writeLegacyConfig(configPath, config);
  const runtime = runRuntime(
    'openclaw-growth-runner',
    ['--config', paths.legacyConfigPath, '--state', paths.legacyStatePath],
    {
      cwd: dirname(resolve(configPath)),
      timeoutMs: 20 * 60_000,
    },
  );
  if (!runtime.ok) {
    throw new Error(runtime.stderr.trim() || runtime.stdout.trim() || 'runner failed');
  }

  const issuesPath = resolve(dirname(resolve(configPath)), config.project.outFile);
  const issuesPayload = await readJsonFile<IssueDraftPayload>(issuesPath);
  const deliveryResult = await deliverArtifacts(configPath, config, issuesPayload);

  return {
    runtimeOutput: runtime.stdout.trim(),
    issuesPath,
    issueCount: issuesPayload.issue_count,
    deliveryResult,
  };
};

const sleep = async (ms: number) => new Promise((resolvePromise) => setTimeout(resolvePromise, ms));

const resolvePackageName = (): string | null => {
  try {
    const packageJson = JSON.parse(readFileSync(resolve(packageRoot, 'package.json'), 'utf8')) as { name?: string };
    return packageJson.name || null;
  } catch {
    return null;
  }
};

const cliName =
  resolvePackageName() === '@analyticscli/growth-engineer' ||
  basename(process.argv[1] || '').startsWith('growth-engineer')
    ? 'growth-engineer'
    : 'openclaw';
program
  .name(cliName)
  .description(
    cliName === 'growth-engineer'
      ? 'Growth Engineer CLI for connector setup, scheduling, health checks, and OpenClaw-compatible growth runs'
      : 'Standalone OpenClaw orchestration CLI',
  );

program
  .command('init')
  .description('Create an OpenClaw config file in the current repository')
  .option('--config <file>', 'Config path', 'openclaw.config.json')
  .option('--repo-root <dir>', 'Repository root', process.cwd())
  .option('--force', 'Overwrite existing config', false)
  .action(async (options: { config: string; repoRoot: string; force?: boolean }) => {
    try {
      await createInitialConfig(options.config, resolve(options.repoRoot), Boolean(options.force));
      process.stdout.write(`Created ${resolve(options.config)}\n`);
    } catch (error) {
      process.stderr.write(`${error instanceof Error ? error.message : String(error)}\n`);
      process.exitCode = 1;
    }
  });

program
  .command('wizard')
  .description('Run the interactive Growth Engineer setup wizard')
  .allowUnknownOption(true)
  .allowExcessArguments(true)
  .action(() => {
    const result = runRuntimeInteractive('openclaw-growth-wizard', forwardedArgsAfter('wizard'), {
      cwd: process.cwd(),
    });
    if (!result.ok) {
      process.exitCode = result.code ?? 1;
    }
  });

program
  .command('setup')
  .description('Initialize config and reuse the shared AnalyticsCLI skill install flow')
  .option('--config <file>', 'Config path', 'openclaw.config.json')
  .option('--repo-root <dir>', 'Repository root', process.cwd())
  .option('--force', 'Overwrite existing config', false)
  .option('--skip-config', 'Skip config initialization', false)
  .option('--skip-shared-skills', 'Skip shared skill installation', false)
  .option(
    '--connectors <list>',
    'Install/enable connector helper tooling for selected connectors (github,asc,revenuecat,all)',
  )
  .action(
    async (options: {
      config: string;
      repoRoot: string;
      force?: boolean;
      skipConfig?: boolean;
      skipSharedSkills?: boolean;
      connectors?: string;
    }) => {
      try {
        const selectedConnectors = parseConnectorList(options.connectors);
        const repoRoot = resolve(options.repoRoot);
        if (!options.skipConfig) {
          const configTarget = resolve(options.config);
          if (Boolean(options.force) || !fileExists(configTarget)) {
            await createInitialConfig(options.config, repoRoot, Boolean(options.force));
            process.stdout.write(`Config ready: ${configTarget}\n`);
          } else {
            process.stdout.write(`Config already exists: ${configTarget}\n`);
          }
        }

        if (!options.skipSharedSkills) {
          const setup = runSharedAnalyticsSetup();
          process.stdout.write(`${setup.detail}\n`);
          if (!setup.ok) {
            process.exitCode = 1;
          }
        }

        if (selectedConnectors.length > 0) {
          const results = await installConnectorHelpers(resolve(options.config), selectedConnectors, repoRoot);
          for (const result of results) {
            process.stdout.write(
              `Connector helper (${result.connector}): ${result.ok ? 'ok' : 'failed'} — ${result.detail}\n`,
            );
          }
          if (results.some((result) => !result.ok)) {
            process.exitCode = 1;
          }
        }
      } catch (error) {
        process.stderr.write(`${error instanceof Error ? error.message : String(error)}\n`);
        process.exitCode = 1;
      }
    },
  );

program
  .command('preflight')
  .description('Validate runtime dependencies, signals, and delivery configuration')
  .option('--config <file>', 'Config path', 'openclaw.config.json')
  .option('--test-connections', 'Run live connection checks where supported', false)
  .option('--json', 'Print JSON result', false)
  .action(async (options: { config: string; testConnections?: boolean; json?: boolean }) => {
    try {
      const result = await runPreflight(options.config, {
        testConnections: Boolean(options.testConnections),
      });
      if (options.json) {
        process.stdout.write(`${JSON.stringify(result, null, 2)}\n`);
      } else {
        process.stdout.write(
          `Preflight ${result.ok ? 'passed' : 'failed'}: ${result.summary.pass} pass, ${result.summary.warn} warn, ${result.summary.fail} fail\n`,
        );
        for (const check of result.checks) {
          process.stdout.write(`- [${check.status}] ${check.name}: ${check.detail}\n`);
        }
      }
      if (!result.ok) {
        process.exitCode = 1;
      }
    } catch (error) {
      process.stderr.write(`${error instanceof Error ? error.message : String(error)}\n`);
      process.exitCode = 1;
    }
  });

program
  .command('run')
  .description('Run one OpenClaw evaluation pass or stay in interval loop')
  .option('--config <file>', 'Config path', 'openclaw.config.json')
  .option('--loop', 'Keep running using schedule.intervalMinutes', false)
  .action(async (options: { config: string; loop?: boolean }) => {
    try {
      const config = await readOpenClawConfig(resolve(options.config));
      do {
        const result = await runOnce(options.config);
        process.stdout.write(`${result.runtimeOutput}\n`);
        process.stdout.write(`Issue drafts: ${result.issueCount} (${result.issuesPath})\n`);
        if (!result.deliveryResult.delivered && result.deliveryResult.skippedReason) {
          process.stdout.write(`Deliveries skipped: ${result.deliveryResult.skippedReason}\n`);
        }
        if (!options.loop) {
          break;
        }
        await sleep(config.schedule.intervalMinutes * 60_000);
      } while (options.loop);
    } catch (error) {
      process.stderr.write(`${error instanceof Error ? error.message : String(error)}\n`);
      process.exitCode = 1;
    }
  });

program
  .command('start')
  .description('Create config if needed, run preflight, then execute one pass')
  .option('--config <file>', 'Config path', 'openclaw.config.json')
  .option('--repo-root <dir>', 'Repository root for initial config generation', process.cwd())
  .option('--test-connections', 'Run live connection checks during preflight', true)
  .option('--no-test-connections', 'Skip live connection checks during preflight')
  .action(async (options: { config: string; repoRoot: string; testConnections?: boolean }) => {
    try {
      if (!fileExists(resolve(options.config))) {
        await createInitialConfig(options.config, resolve(options.repoRoot), false);
        process.stdout.write(`Created ${resolve(options.config)}\n`);
      }

      const preflight = await runPreflight(options.config, {
        testConnections: Boolean(options.testConnections),
      });
      if (!preflight.ok) {
        process.stdout.write(renderPreflightChecklist(preflight));
        process.exitCode = 1;
        return;
      }

      const result = await runOnce(options.config);
      process.stdout.write(`${result.runtimeOutput}\n`);
      process.stdout.write(`Issue drafts: ${result.issueCount} (${result.issuesPath})\n`);
    } catch (error) {
      process.stderr.write(`${error instanceof Error ? error.message : String(error)}\n`);
      process.exitCode = 1;
    }
  });

const exportersCommand = program.command('exporters').description('Connector export helpers');

exportersCommand
  .command('analytics-summary')
  .description('Export the default analytics summary JSON')
  .allowUnknownOption(true)
  .action(() => {
    const result = runRuntime('export-analytics-summary', forwardedArgsAfter('analytics-summary'), {
      cwd: process.cwd(),
      timeoutMs: 20 * 60_000,
    });
    process.stdout.write(result.stdout);
    process.stderr.write(result.stderr);
    if (!result.ok) {
      process.exitCode = 1;
    }
  });

exportersCommand
  .command('asc-summary')
  .description('Export the default ASC summary JSON')
  .allowUnknownOption(true)
  .action(() => {
    const result = runRuntime('export-asc-summary', forwardedArgsAfter('asc-summary'), {
      cwd: process.cwd(),
      timeoutMs: 20 * 60_000,
    });
    process.stdout.write(result.stdout);
    process.stderr.write(result.stderr);
    if (!result.ok) {
      process.exitCode = 1;
    }
  });

program
  .command('feedback-api')
  .description('Run the lightweight feedback API ingestion endpoint')
  .allowUnknownOption(true)
  .action(() => {
    const result = runRuntime('openclaw-feedback-api', forwardedArgsAfter('feedback-api'), {
      cwd: process.cwd(),
      timeoutMs: 20 * 60_000,
    });
    process.stdout.write(result.stdout);
    process.stderr.write(result.stderr);
    if (!result.ok) {
      process.exitCode = 1;
    }
  });

program.parseAsync(process.argv).catch((error) => {
  process.stderr.write(`${error instanceof Error ? error.message : String(error)}\n`);
  process.exitCode = 1;
});
