#!/usr/bin/env node

import { spawn } from 'node:child_process';
import process from 'node:process';
import { buildAscSummary, writeJsonOutput } from './openclaw-exporters-lib.mjs';
import { loadOpenClawGrowthSecrets } from './openclaw-growth-env.mjs';

function printHelpAndExit(exitCode, reason = null) {
  if (reason) {
    process.stderr.write(`${reason}\n\n`);
  }
  process.stdout.write(`
Export ASC Summary

Builds an OpenClaw-compatible store/release summary JSON from the asc CLI.

Usage:
  node scripts/export-asc-summary.mjs [options]

Options:
  --app <id>             Optional App Store Connect app ID filter (defaults to all accessible apps)
  --out <file>           Write JSON to file instead of stdout
  --start <date>         App Store Connect Analytics start date (YYYY-MM-DD, default: last 30 complete days)
  --end <date>           App Store Connect Analytics end date (YYYY-MM-DD, default: yesterday UTC)
  --skip-web-analytics   Skip experimental ASC web analytics queries
  --country <code>       Ratings country override (default: all countries)
  --reviews-limit <n>    Review summarizations limit (default: 20)
  --feedback-limit <n>   TestFlight feedback limit (default: 20)
  --max-signals <n>      Maximum signals to emit (default: 4)
  --help, -h             Show help
`);
  process.exit(exitCode);
}

function parseArgs(argv) {
  const defaultEnd = formatDate(addDays(new Date(), -1));
  const defaultStart = formatDate(addDays(parseDate(defaultEnd), -29));
  const args = {
    app: String(process.env.ASC_APP_ID || '').trim(),
    out: '',
    start: String(process.env.ASC_ANALYTICS_START || defaultStart).trim(),
    end: String(process.env.ASC_ANALYTICS_END || defaultEnd).trim(),
    webAnalytics: true,
    country: '',
    reviewsLimit: 20,
    feedbackLimit: 20,
    maxSignals: 4,
  };

  for (let index = 0; index < argv.length; index += 1) {
    const token = argv[index];
    const next = argv[index + 1];

    if (token === '--') {
      continue;
    } else if (token === '--app') {
      args.app = String(next || '').trim();
      index += 1;
    } else if (token === '--out') {
      args.out = String(next || '').trim();
      index += 1;
    } else if (token === '--start') {
      args.start = String(next || '').trim();
      index += 1;
    } else if (token === '--end') {
      args.end = String(next || '').trim();
      index += 1;
    } else if (token === '--skip-web-analytics') {
      args.webAnalytics = false;
    } else if (token === '--country') {
      args.country = String(next || '').trim();
      index += 1;
    } else if (token === '--reviews-limit') {
      const parsed = Number.parseInt(String(next || ''), 10);
      if (!Number.isFinite(parsed) || parsed <= 0) {
        printHelpAndExit(1, `Invalid value for --reviews-limit: ${String(next || '')}`);
      }
      args.reviewsLimit = parsed;
      index += 1;
    } else if (token === '--feedback-limit') {
      const parsed = Number.parseInt(String(next || ''), 10);
      if (!Number.isFinite(parsed) || parsed <= 0) {
        printHelpAndExit(1, `Invalid value for --feedback-limit: ${String(next || '')}`);
      }
      args.feedbackLimit = parsed;
      index += 1;
    } else if (token === '--max-signals') {
      const parsed = Number.parseInt(String(next || ''), 10);
      if (!Number.isFinite(parsed) || parsed <= 0) {
        printHelpAndExit(1, `Invalid value for --max-signals: ${String(next || '')}`);
      }
      args.maxSignals = parsed;
      index += 1;
    } else if (token === '--help' || token === '-h') {
      printHelpAndExit(0);
    } else {
      printHelpAndExit(1, `Unknown argument: ${token}`);
    }
  }

  return args;
}

function addDays(date, days) {
  const copy = new Date(date);
  copy.setUTCDate(copy.getUTCDate() + days);
  return copy;
}

function parseDate(value) {
  return new Date(`${value}T00:00:00Z`);
}

function formatDate(date) {
  return date.toISOString().slice(0, 10);
}

function runJsonCommand(command, commandArgs) {
  return new Promise((resolve, reject) => {
    const child = spawn(command, commandArgs, {
      stdio: ['ignore', 'pipe', 'pipe'],
      env: process.env,
    });

    let stdout = '';
    let stderr = '';

    child.stdout.on('data', (chunk) => {
      stdout += String(chunk);
    });
    child.stderr.on('data', (chunk) => {
      stderr += String(chunk);
    });
    child.on('error', reject);
    child.on('close', (code) => {
      if (code !== 0) {
        reject(Object.assign(new Error(stderr.trim() || `${command} exited with code ${code}`), { exitCode: code }));
        return;
      }

      try {
        resolve(JSON.parse(stdout));
      } catch {
        reject(new Error(`${command} returned non-JSON output`));
      }
    });
  });
}

async function runBestEffortAscQuery(label, args, warnings) {
  try {
    return await runJsonCommand('asc', args);
  } catch (error) {
    warnings.push(`${label}: ${error instanceof Error ? error.message : String(error)}`);
    return null;
  }
}

function normalizeString(value) {
  return typeof value === 'string' && value.trim() ? value.trim() : null;
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
    byId.set(id, {
      id,
      name:
        normalizeString(candidate.name) ||
        normalizeString(candidate.appName) ||
        normalizeString(candidate.displayName) ||
        normalizeString(attrs.name),
      bundleId:
        normalizeString(candidate.bundleId) ||
        normalizeString(candidate.bundle_id) ||
        normalizeString(attrs.bundleId),
    });
  }
  return [...byId.values()];
}

async function listAscApps() {
  const payload = await runJsonCommand('asc', ['apps', 'list', '--output', 'json']);
  const apps = extractAscAppChoices(payload);
  if (apps.length === 0) {
    throw new Error('asc apps list returned no accessible apps');
  }
  return apps;
}

async function buildSingleAppSummary(appId, args) {
  const warnings = [];
  const statusPayload = await runBestEffortAscQuery('ASC status query', [
    'status',
    '--app',
    appId,
    '--include',
    'builds,testflight,submission,review,appstore',
  ], warnings);

  const ratingsArgs = ['reviews', 'ratings', '--app', appId];
  if (args.country) {
    ratingsArgs.push('--country', args.country);
  } else {
    ratingsArgs.push('--all');
  }
  const ratingsPayload = await runBestEffortAscQuery('ASC ratings query', ratingsArgs, warnings);

  const reviewSummariesPayload = await runBestEffortAscQuery('ASC review summarizations query', [
    'reviews',
    'summarizations',
    '--app',
    appId,
    '--platform',
    'IOS',
    '--limit',
    String(args.reviewsLimit),
    '--fields',
    'text,createdDate,locale',
  ], warnings);

  const feedbackPayload = await runBestEffortAscQuery('ASC beta feedback query', [
    'feedback',
    '--app',
    appId,
    '--limit',
    String(args.feedbackLimit),
    '--sort',
    '-createdDate',
  ], warnings);

  const analyticsMetricsPayload = args.webAnalytics
    ? await runBestEffortAscQuery('ASC web analytics metrics query', [
        'web',
        'analytics',
        'metrics',
        '--app',
        appId,
        '--start',
        args.start,
        '--end',
        args.end,
        '--frequency',
        'day',
        '--measures',
        'units,redownloads,conversionRate,crashRate',
        '--output',
        'json',
      ], warnings)
    : null;

  const analyticsSourcesPayload = args.webAnalytics
    ? await runBestEffortAscQuery('ASC web analytics sources query', [
        'web',
        'analytics',
        'sources',
        '--app',
        appId,
        '--start',
        args.start,
        '--end',
        args.end,
        '--output',
        'json',
      ], warnings)
    : null;

  const analyticsOverviewPayload = args.webAnalytics
    ? await runBestEffortAscQuery('ASC web analytics overview query', [
        'web',
        'analytics',
        'overview',
        '--app',
        appId,
        '--start',
        args.start,
        '--end',
        args.end,
        '--output',
        'json',
      ], warnings)
    : null;

  return buildAscSummary({
    appId,
    statusPayload,
    ratingsPayload,
    reviewSummariesPayload,
    feedbackPayload,
    analyticsMetricsPayload,
    analyticsSourcesPayload,
    analyticsOverviewPayload,
    analyticsWindow: { start: args.start, end: args.end },
    analyticsWarnings: warnings,
    maxSignals: args.maxSignals,
  });
}

async function buildAllAppsSummary(args) {
  const apps = args.app ? [{ id: args.app }] : await listAscApps();
  const summaries = [];
  const warnings = [];

  for (const app of apps) {
    try {
      summaries.push(await buildSingleAppSummary(app.id, args));
    } catch (error) {
      warnings.push({
        appId: app.id,
        appName: app.name || null,
        error: error instanceof Error ? error.message : String(error),
        publicStatusHint: String(error instanceof Error ? error.message : error)
          .toLowerCase()
          .includes('403')
          ? 'app may not be public yet or ASC web analytics may not be available for this app'
          : null,
      });
    }
  }

  if (summaries.length === 0) {
    throw new Error(`ASC summary failed for every accessible app: ${JSON.stringify(warnings)}`);
  }

  if (summaries.length === 1) {
    const summary = summaries[0];
    if (warnings.length > 0) {
      summary.meta = { ...(summary.meta || {}), warnings };
    }
    return summary;
  }

  const signals = summaries
    .flatMap((summary) =>
      (Array.isArray(summary.signals) ? summary.signals : []).map((signal) => ({
        ...signal,
        id: `${summary.meta?.appId || 'app'}_${signal.id}`,
        evidence: [
          `ASC app: ${summary.meta?.appId || 'unknown'}`,
          ...(Array.isArray(signal.evidence) ? signal.evidence : []),
        ],
      })),
    )
    .sort((a, b) => {
      const priorityRank = { high: 0, medium: 1, low: 2 };
      return (priorityRank[a.priority] ?? 3) - (priorityRank[b.priority] ?? 3);
    })
    .slice(0, Math.max(1, Number(args.maxSignals) || 4));

  return {
    project: 'app-store-connect:all',
    window: 'latest',
    signals,
    meta: {
      generatedAt: new Date().toISOString(),
      source: 'asc',
      appScope: 'all',
      appCount: apps.length,
      summarizedAppCount: summaries.length,
      appIds: summaries.map((summary) => summary.meta?.appId).filter(Boolean),
      warnings,
    },
  };
}

async function main() {
  await loadOpenClawGrowthSecrets();
  const args = parseArgs(process.argv.slice(2));
  const summary = await buildAllAppsSummary(args);

  await writeJsonOutput(args.out, summary);
}

main().catch((error) => {
  process.stderr.write(`${error instanceof Error ? error.message : String(error)}\n`);
  process.exitCode = 1;
});
