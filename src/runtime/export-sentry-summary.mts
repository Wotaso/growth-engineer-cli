#!/usr/bin/env node

import { promises as fs } from 'node:fs';
import path from 'node:path';
import { writeJsonOutput, buildSentrySummary } from './openclaw-exporters-lib.mjs';

const DEFAULT_BASE_URL = 'https://sentry.io';
const DEFAULT_CONFIG_PATH = 'data/openclaw-growth-engineer/config.json';

function printHelpAndExit(exitCode, reason = null) {
  if (reason) {
    process.stderr.write(`${reason}\n\n`);
  }
  process.stdout.write(`
Export Sentry Summary

Builds an OpenClaw-compatible crash/stability summary JSON from the Sentry API.

Usage:
  node scripts/export-sentry-summary.mjs [options]

Options:
  --org <slug>           Sentry organization slug (default: SENTRY_ORG)
  --project <slug>       Sentry project slug (default: SENTRY_PROJECT)
  --environment <name>   Sentry environment filter (default: SENTRY_ENVIRONMENT or production)
  --last <duration>      Sentry statsPeriod, e.g. 24h, 7d, 30d (default: 7d)
  --query <query>        Issue search query (default: is:unresolved)
  --limit <n>            Max Sentry issues to fetch, capped at 50 (default: 20)
  --max-signals <n>      Max normalized signals/issues to emit (default: 5)
  --base-url <url>       Sentry base URL for self-hosted instances (default: SENTRY_BASE_URL or ${DEFAULT_BASE_URL})
  --config <file>        OpenClaw config with sources.sentry.accounts[] (default: ${DEFAULT_CONFIG_PATH} when present)
  --accounts-file <file> JSON file containing an accounts[] array or array
  --out <file>           Write JSON to file instead of stdout
  --help, -h             Show help
`);
  process.exit(exitCode);
}

function parseArgs(argv) {
  const args = {
    org: String(process.env.SENTRY_ORG || '').trim(),
    project: String(process.env.SENTRY_PROJECT || '').trim(),
    environment: String(process.env.SENTRY_ENVIRONMENT || 'production').trim(),
    last: '7d',
    query: 'is:unresolved',
    limit: 20,
    maxSignals: 5,
    baseUrl: String(process.env.SENTRY_BASE_URL || DEFAULT_BASE_URL).trim(),
    config: '',
    accountsFile: '',
    out: '',
  };

  for (let index = 0; index < argv.length; index += 1) {
    const token = argv[index];
    const next = argv[index + 1];
    if (token === '--') {
      continue;
    } else if (token === '--org') {
      args.org = String(next || '').trim();
      index += 1;
    } else if (token === '--project') {
      args.project = String(next || '').trim();
      index += 1;
    } else if (token === '--environment') {
      args.environment = String(next || '').trim();
      index += 1;
    } else if (token === '--last') {
      args.last = String(next || args.last).trim();
      index += 1;
    } else if (token === '--query') {
      args.query = String(next || '').trim();
      index += 1;
    } else if (token === '--limit') {
      args.limit = normalizeInteger(next, '--limit', 1, 50);
      index += 1;
    } else if (token === '--max-signals') {
      args.maxSignals = normalizeInteger(next, '--max-signals', 1, 20);
      index += 1;
    } else if (token === '--base-url') {
      args.baseUrl = String(next || '').trim();
      index += 1;
    } else if (token === '--config') {
      args.config = String(next || '').trim();
      index += 1;
    } else if (token === '--accounts-file') {
      args.accountsFile = String(next || '').trim();
      index += 1;
    } else if (token === '--out') {
      args.out = String(next || '').trim();
      index += 1;
    } else if (token === '--help' || token === '-h') {
      printHelpAndExit(0);
    } else {
      printHelpAndExit(1, `Unknown argument: ${token}`);
    }
  }

  return args;
}

async function readJsonIfPresent(filePath, required = false) {
  const normalized = String(filePath || '').trim();
  if (!normalized) return null;
  try {
    return JSON.parse(await fs.readFile(path.resolve(normalized), 'utf8'));
  } catch (error) {
    if (!required && error && typeof error === 'object' && (error as any).code === 'ENOENT') return null;
    throw error;
  }
}

function normalizeArray(value) {
  if (Array.isArray(value)) return value;
  if (Array.isArray(value?.accounts)) return value.accounts;
  return [];
}

function normalizeProjectEntries(account) {
  const projects = Array.isArray(account?.projects) ? account.projects : account?.project ? [account.project] : [];
  return projects
    .map((entry) => (typeof entry === 'string' ? { project: entry } : entry))
    .filter((entry) => entry && typeof entry === 'object' && String(entry.project || entry.slug || '').trim())
    .map((entry) => ({
      project: String(entry.project || entry.slug || '').trim(),
      org: String(entry.org || entry.organization || account.org || account.organization || '').trim(),
      environment: String(entry.environment || account.environment || process.env.SENTRY_ENVIRONMENT || 'production').trim(),
      last: String(entry.last || account.last || '').trim(),
      query: String(entry.query || account.query || '').trim(),
      limit: entry.limit || account.limit,
    }));
}

function normalizeAccountConfigs(rawAccounts, args) {
  return rawAccounts.flatMap((account, index) => {
    if (!account || typeof account !== 'object') return [];
    const id = String(account.id || account.key || account.label || `sentry_${index + 1}`).trim();
    const baseUrl = String(account.baseUrl || account.base_url || account.url || args.baseUrl || DEFAULT_BASE_URL).trim();
    const tokenEnv = String(account.tokenEnv || account.token_env || account.secretEnv || 'SENTRY_AUTH_TOKEN').trim();
    const projectEntries = normalizeProjectEntries(account);
    if (projectEntries.length > 0) {
      return projectEntries.map((projectEntry) => ({
        ...projectEntry,
        id: `${id}_${projectEntry.project}`.replace(/[^a-zA-Z0-9._-]+/g, '_'),
        accountId: id,
        label: String(account.label || account.name || id).trim(),
        baseUrl,
        tokenEnv,
        maxSignals: args.maxSignals,
      }));
    }

    const org = String(account.org || account.organization || args.org || '').trim();
    if (!org) return [];
    return [{
      id: id.replace(/[^a-zA-Z0-9._-]+/g, '_'),
      accountId: id,
      label: String(account.label || account.name || id).trim(),
      baseUrl,
      tokenEnv,
      org,
      project: '',
      environment: String(account.environment || args.environment || process.env.SENTRY_ENVIRONMENT || 'production').trim(),
      last: String(account.last || args.last || '').trim(),
      query: String(account.query || args.query || '').trim(),
      limit: account.limit || args.limit,
      maxSignals: args.maxSignals,
    }];
  });
}

async function expandDiscoveredProjects(account, token) {
  if (String(account.project || '').trim()) return [account];
  const org = requireValue(account.org, 'SENTRY_ORG');
  const url = buildUrl(account.baseUrl || DEFAULT_BASE_URL, `/api/0/organizations/${encodeURIComponent(org)}/projects/`, {
    per_page: 100,
  });
  const payload = await sentryFetchList(url, token);
  const projects = apiListItems(payload)
    .map((project) => String(project?.slug || project?.name || '').trim())
    .filter(Boolean);
  const uniqueProjects = [...new Set(projects)];
  if (uniqueProjects.length === 0) {
    throw new Error(`No Sentry projects are visible for org ${org}. Check token scopes org:read/project:read/event:read and org access.`);
  }
  return uniqueProjects.map((project) => ({
    ...account,
    project,
    id: `${account.accountId || account.id}_${project}`.replace(/[^a-zA-Z0-9._-]+/g, '_'),
  }));
}

async function loadConfiguredAccounts(args) {
  const accountPayload = args.accountsFile ? await readJsonIfPresent(args.accountsFile, true) : null;
  const accountsFromFile = normalizeArray(accountPayload);
  if (accountsFromFile.length > 0) return normalizeAccountConfigs(accountsFromFile, args);

  const configPath = args.config || DEFAULT_CONFIG_PATH;
  const config = await readJsonIfPresent(configPath, Boolean(args.config));
  const accountsFromConfig = normalizeArray(config?.sources?.sentry);
  if (accountsFromConfig.length > 0) return normalizeAccountConfigs(accountsFromConfig, args);

  const singleAccount = {
    id: 'sentry',
    label: 'Sentry',
    baseUrl: args.baseUrl,
    tokenEnv: 'SENTRY_AUTH_TOKEN',
    org: args.org,
    project: args.project,
    environment: args.environment,
    last: args.last,
    query: args.query,
    limit: args.limit,
  };
  return normalizeProjectEntries(singleAccount).length > 0 ? normalizeAccountConfigs([singleAccount], args) : [];
}

function normalizeInteger(value, label, min, max) {
  const parsed = Number.parseInt(String(value || ''), 10);
  if (!Number.isFinite(parsed) || parsed < min || parsed > max) {
    printHelpAndExit(1, `${label} must be an integer between ${min} and ${max}`);
  }
  return parsed;
}

function requireValue(value, label) {
  const normalized = String(value || '').trim();
  if (!normalized) {
    throw new Error(`${label} is required. Set it in the Sentry connector wizard or pass the flag explicitly.`);
  }
  return normalized;
}

function buildUrl(baseUrl, pathname, params) {
  const url = new URL(pathname, `${baseUrl.replace(/\/$/, '')}/`);
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
  return [];
}

async function sentryFetchJson(url, token) {
  const response = await fetch(url, {
    method: 'GET',
    headers: {
      Accept: 'application/json',
      Authorization: `Bearer ${token}`,
      'User-Agent': 'openclaw-growth-sentry-exporter',
    },
  });
  const body = await response.text();
  if (!response.ok) {
    throw new Error(`Sentry API ${response.status}: ${body.slice(0, 500) || 'request failed'}`);
  }
  return body ? JSON.parse(body) : null;
}

async function sentryFetchList(url, token) {
  const items = [];
  let nextUrl = url;
  for (let page = 0; nextUrl && page < 10; page += 1) {
    const payload = await sentryFetchJson(nextUrl, token);
    items.push(...apiListItems(payload));
    const next = payload && typeof payload === 'object' ? payload.next : null;
    nextUrl = typeof next === 'string' && next.trim() ? new URL(next, `${nextUrl.origin}/`) : null;
  }
  return items;
}

function redactString(value) {
  return String(value || '')
    .replace(/[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}/g, '[REDACTED_EMAIL]')
    .replace(/\b(?:\d{1,3}\.){3}\d{1,3}\b/g, '[REDACTED_IP]');
}

function redactData(value) {
  if (typeof value === 'string') return redactString(value);
  if (Array.isArray(value)) return value.map((entry) => redactData(entry));
  if (value && typeof value === 'object') {
    return Object.fromEntries(
      Object.entries(value).map(([key, entry]) => [
        key,
        ['email', 'ip', 'ip_address'].includes(key.toLowerCase()) ? '[REDACTED]' : redactData(entry),
      ]),
    );
  }
  return value;
}

async function listIssues(account, token) {
  const org = encodeURIComponent(requireValue(account.org, 'SENTRY_ORG'));
  const project = encodeURIComponent(requireValue(account.project, 'SENTRY_PROJECT'));
  const url = buildUrl(account.baseUrl || DEFAULT_BASE_URL, `/api/0/projects/${org}/${project}/issues/`, {
    statsPeriod: account.last,
    environment: account.environment,
    query: account.query,
    per_page: account.limit,
  });
  const payload = await sentryFetchJson(url, token);
  return Array.isArray(payload) ? payload : [];
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  const configuredAccounts = await loadConfiguredAccounts(args);
  if (configuredAccounts.length === 0) {
    throw new Error(
      `No Sentry account configured. Set SENTRY_AUTH_TOKEN plus SENTRY_ORG, pass --org, or add sources.sentry.accounts[] in ${args.config || DEFAULT_CONFIG_PATH}.`,
    );
  }
  const accounts = [];
  for (const account of configuredAccounts) {
    const token = requireValue(process.env[account.tokenEnv], account.tokenEnv);
    accounts.push(...await expandDiscoveredProjects(account, token));
  }
  const summaries = [];
  for (const account of accounts) {
    const token = requireValue(process.env[account.tokenEnv], account.tokenEnv);
    const issuesPayload = redactData(await listIssues(account, token));
    summaries.push({
      id: account.id,
      label: account.label,
      org: account.org,
      project: account.project,
      environment: account.environment,
      last: account.last || args.last,
      issuesPayload,
      maxSignals: args.maxSignals,
    });
  }
  const summary =
    summaries.length === 1
      ? buildSentrySummary(summaries[0])
      : buildSentrySummary({ accounts: summaries, last: args.last, maxSignals: args.maxSignals });
  await writeJsonOutput(args.out, summary);
}

main().catch((error) => {
  process.stderr.write(`${error instanceof Error ? error.message : String(error)}\n`);
  process.exitCode = 1;
});
