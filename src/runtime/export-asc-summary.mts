#!/usr/bin/env node

import { spawn } from 'node:child_process';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
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
  --cache-dir <dir>      Cache downloaded ASC batch reports (default: data/openclaw-growth-engineer/asc-cache)
  --include-web-analytics Include experimental ASC web analytics queries only when API reports are insufficient
  --skip-web-analytics   Deprecated no-op; web analytics is skipped by default
  --country <code>       Ratings country override (default: all countries)
  --reviews-limit <n>    Review summarizations limit (default: 20)
  --feedback-limit <n>   TestFlight feedback limit (default: 20)
  --analytics-instance-limit <n> Maximum App Analytics report instances to download (default: 8)
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
    cacheDir: String(process.env.ASC_BATCH_CACHE_DIR || 'data/openclaw-growth-engineer/asc-cache').trim(),
    webAnalytics: ['1', 'true', 'yes'].includes(String(process.env.ASC_INCLUDE_WEB_ANALYTICS || '').toLowerCase()),
    country: '',
    reviewsLimit: 20,
    feedbackLimit: 20,
    analyticsInstanceLimit: 8,
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
    } else if (token === '--cache-dir') {
      args.cacheDir = String(next || '').trim();
      index += 1;
    } else if (token === '--include-web-analytics') {
      args.webAnalytics = true;
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
    } else if (token === '--analytics-instance-limit') {
      const parsed = Number.parseInt(String(next || ''), 10);
      if (!Number.isFinite(parsed) || parsed <= 0) {
        printHelpAndExit(1, `Invalid value for --analytics-instance-limit: ${String(next || '')}`);
      }
      args.analyticsInstanceLimit = parsed;
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

function normalizeKey(value) {
  return String(value || '').toLowerCase().replace(/[^a-z0-9]/g, '');
}

function coerceNumber(value) {
  if (value === null || value === undefined) return null;
  const normalized = String(value).trim().replace(/%$/, '').replace(/,/g, '');
  if (!normalized) return null;
  const parsed = Number(normalized);
  return Number.isFinite(parsed) ? parsed : null;
}

function addMetricTotals(metrics, measure, value, date = null, { rate = false } = {}) {
  if (value === null || value === undefined || !Number.isFinite(Number(value))) return;
  const key = normalizeKey(measure);
  if (!key) return;
  const existing = metrics.get(key) || {
    measure,
    total: 0,
    previousTotal: null,
    percentChange: null,
    data: [],
    count: 0,
    rate,
  };
  existing.count += 1;
  existing.total = rate ? existing.total + Number(value) : existing.total + Number(value);
  if (date) existing.data.push({ date, value: Number(value) });
  metrics.set(key, existing);
}

function finishMetricTotals(metrics) {
  return [...metrics.values()].map((metric) => ({
    measure: metric.measure,
    total: metric.rate && metric.count > 0 ? metric.total / metric.count : metric.total,
    previousTotal: metric.previousTotal,
    percentChange: metric.percentChange,
    data: metric.data,
  }));
}

function splitDelimitedLine(line, delimiter) {
  const cells = [];
  let current = '';
  let quoted = false;
  for (let index = 0; index < line.length; index += 1) {
    const char = line[index];
    if (char === '"') {
      if (quoted && line[index + 1] === '"') {
        current += '"';
        index += 1;
      } else {
        quoted = !quoted;
      }
    } else if (char === delimiter && !quoted) {
      cells.push(current);
      current = '';
    } else {
      current += char;
    }
  }
  cells.push(current);
  return cells;
}

async function parseDelimitedReport(filePath) {
  const content = await fs.readFile(filePath, 'utf8');
  const lines = content.split(/\r?\n/).filter((line) => line.trim());
  if (lines.length < 2) return [];
  const delimiter = lines[0].includes('\t') ? '\t' : ',';
  const headers = splitDelimitedLine(lines[0], delimiter).map((header) => header.trim());
  return lines.slice(1).map((line) => {
    const cells = splitDelimitedLine(line, delimiter);
    const row = {};
    headers.forEach((header, index) => {
      row[header] = cells[index] ?? '';
    });
    return row;
  });
}

function findHeader(row, candidates) {
  const byKey = new Map(Object.keys(row).map((key) => [normalizeKey(key), key]));
  for (const candidate of candidates) {
    const match = byKey.get(normalizeKey(candidate));
    if (match) return match;
  }
  return null;
}

function summarizeBatchRows(rows, appId) {
  const metrics = new Map();
  for (const row of rows) {
    const appIdHeader = findHeader(row, ['apple identifier', 'app apple identifier', 'app id', 'adam id']);
    if (appIdHeader && String(row[appIdHeader] || '').trim() && String(row[appIdHeader]).trim() !== String(appId)) {
      continue;
    }
    const dateHeader = findHeader(row, ['date', 'begin date', 'start date', 'end date']);
    const date = dateHeader ? String(row[dateHeader] || '').slice(0, 10) : null;
    const fields = [
      { measure: 'units', candidates: ['units', 'app units', 'downloads', 'first-time downloads'] },
      { measure: 'redownloads', candidates: ['redownloads', 're-downloads'] },
      { measure: 'conversionRate', candidates: ['conversion rate', 'app store conversion rate'], rate: true },
      { measure: 'crashRate', candidates: ['crash rate'], rate: true },
      { measure: 'crashes', candidates: ['crashes', 'crash count'] },
      { measure: 'pageViewUnique', candidates: ['unique product page views', 'product page views', 'page views'] },
      { measure: 'proceeds', candidates: ['developer proceeds', 'proceeds'] },
    ];
    for (const field of fields) {
      const header = findHeader(row, field.candidates);
      if (!header) continue;
      addMetricTotals(metrics, field.measure, coerceNumber(row[header]), date, { rate: Boolean(field.rate) });
    }
  }
  return { results: finishMetricTotals(metrics) };
}

function extractItems(payload) {
  if (Array.isArray(payload)) return payload;
  if (Array.isArray(payload?.data)) return payload.data;
  if (Array.isArray(payload?.items)) return payload.items;
  if (Array.isArray(payload?.requests)) return payload.requests;
  return [];
}

function extractId(item) {
  return normalizeString(item?.id) || normalizeString(item?.requestId) || normalizeString(item?.attributes?.id);
}

function extractInstances(payload) {
  const instances = [];
  const visit = (value) => {
    if (!value || typeof value !== 'object') return;
    if (Array.isArray(value)) {
      for (const item of value) visit(item);
      return;
    }
    const id = extractId(value);
    const type = normalizeString(value.type) || normalizeString(value.kind) || '';
    const attrs = value.attributes && typeof value.attributes === 'object' ? value.attributes : {};
    if (id && /instance/i.test(`${type} ${attrs.name || ''} ${attrs.category || ''}`)) {
      instances.push({ id, name: normalizeString(attrs.name) || normalizeString(value.name) });
    }
    for (const child of Object.values(value)) visit(child);
  };
  visit(payload);
  const byId = new Map();
  for (const instance of instances) byId.set(instance.id, instance);
  return [...byId.values()];
}

async function downloadAndParseReport(label, commandArgs, outputPath, warnings) {
  try {
    await fs.mkdir(path.dirname(outputPath), { recursive: true });
    await runJsonCommand('asc', [...commandArgs, '--output', outputPath, '--decompress']);
    const rows = await parseDelimitedReport(outputPath);
    return { label, outputPath, rows };
  } catch (error) {
    warnings.push(`${label}: ${error instanceof Error ? error.message : String(error)}`);
    return null;
  }
}

async function downloadBatchAnalyticsReports(appId, args, warnings) {
  const reports = [];
  const appCacheDir = path.resolve(args.cacheDir || path.join(os.tmpdir(), 'openclaw-asc-cache'), appId, args.end);

  const vendor = String(process.env.ASC_VENDOR_NUMBER || process.env.ASC_ANALYTICS_VENDOR_NUMBER || '').trim();
  if (vendor) {
    const sales = await downloadAndParseReport(
      'ASC daily sales batch report',
      [
        'analytics',
        'sales',
        '--vendor',
        vendor,
        '--type',
        'SALES',
        '--subtype',
        'SUMMARY',
        '--frequency',
        'DAILY',
        '--date',
        args.end,
      ],
      path.join(appCacheDir, 'sales-daily.tsv'),
      warnings,
    );
    if (sales) reports.push(sales);
  }

  const requestsPayload = await runBestEffortAscQuery('ASC analytics requests query', [
    'analytics',
    'requests',
    '--app',
    appId,
    '--state',
    'COMPLETED',
    '--output',
    'json',
  ], warnings);
  let requestId = extractItems(requestsPayload).map(extractId).find(Boolean);
  if (!requestId) {
    const anyRequestsPayload = await runBestEffortAscQuery('ASC analytics all requests query', [
      'analytics',
      'requests',
      '--app',
      appId,
      '--output',
      'json',
    ], warnings);
    const existingRequestId = extractItems(anyRequestsPayload).map(extractId).find(Boolean);
    if (!existingRequestId) {
      const createdRequest = await runBestEffortAscQuery('ASC analytics ongoing request creation', [
        'analytics',
        'request',
        '--app',
        appId,
        '--access-type',
        'ONGOING',
        '--output',
        'json',
      ], warnings);
      const createdRequestId = extractId(createdRequest) || extractItems(createdRequest).map(extractId).find(Boolean);
      warnings.push(
        createdRequestId
          ? `ASC App Analytics batch report: created ongoing analytics request ${createdRequestId}; report instances will be available after Apple finishes processing`
          : 'ASC App Analytics batch report: requested ongoing analytics access; report instances will be available after Apple finishes processing',
      );
      return reports;
    }
    requestId = existingRequestId;
    warnings.push(`ASC App Analytics batch report: request ${requestId} is not completed yet; using other ASC API-key surfaces for this run`);
    return reports;
  }

  const viewPayload = await runBestEffortAscQuery('ASC analytics reports view query', [
    'analytics',
    'view',
    '--request-id',
    requestId,
    '--date',
    args.end,
    '--include-segments',
    '--paginate',
    '--output',
    'json',
  ], warnings);
  const instances = extractInstances(viewPayload).slice(0, Math.max(1, Number(args.analyticsInstanceLimit) || 8));
  if (instances.length === 0) {
    warnings.push(`ASC App Analytics batch report: request ${requestId} has no downloadable instances for ${args.end}`);
    return reports;
  }

  for (const instance of instances) {
    const report = await downloadAndParseReport(
      `ASC App Analytics batch report ${instance.id}`,
      ['analytics', 'download', '--request-id', requestId, '--instance-id', instance.id],
      path.join(appCacheDir, `analytics-${instance.id}.csv`),
      warnings,
    );
    if (report) reports.push(report);
  }

  return reports;
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

  const batchReports = await downloadBatchAnalyticsReports(appId, args, warnings);
  const batchRows = batchReports.flatMap((report) => report.rows);
  const batchAnalyticsPayload = batchRows.length > 0 ? summarizeBatchRows(batchRows, appId) : null;

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
    batchAnalyticsPayload,
    analyticsWindow: { start: args.start, end: args.end },
    analyticsWarnings: warnings,
    batchReports: batchReports.map((report) => ({
      label: report.label,
      outputPath: report.outputPath,
      rowCount: report.rows.length,
    })),
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
          ? 'app may not be public yet or ASC analytics reports may not be available for this app'
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
