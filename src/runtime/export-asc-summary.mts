#!/usr/bin/env node

import { spawn } from 'node:child_process';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import process from 'node:process';
import { promisify } from 'node:util';
import { gunzip } from 'node:zlib';
import { buildAscSummary, writeJsonOutput } from './openclaw-exporters-lib.mjs';
import { loadOpenClawGrowthSecrets } from './openclaw-growth-env.mjs';

const gunzipAsync = promisify(gunzip);
const DEFAULT_ASC_TIMEOUT_SECONDS = '120';

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
  --start <date>         App Store Connect Analytics start date (YYYY-MM-DD, default: 30 complete days)
  --end <date>           App Store Connect Analytics end date (YYYY-MM-DD, default: two days ago UTC)
  --cache-dir <dir>      Cache downloaded ASC batch reports (default: data/openclaw-growth-engineer/asc-cache)
  --force-refresh        Re-download reports even when the daily cache already has them
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
  const defaultEnd = formatDate(addDays(new Date(), -2));
  const defaultStart = formatDate(addDays(parseDate(defaultEnd), -29));
  const args = {
    app: '',
    out: '',
    start: String(process.env.ASC_ANALYTICS_START || defaultStart).trim(),
    end: String(process.env.ASC_ANALYTICS_END || defaultEnd).trim(),
    cacheDir: String(process.env.ASC_BATCH_CACHE_DIR || 'data/openclaw-growth-engineer/asc-cache').trim(),
    forceRefresh: ['1', 'true', 'yes'].includes(String(process.env.ASC_FORCE_REFRESH || '').toLowerCase()),
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
    } else if (token === '--force-refresh') {
      args.forceRefresh = true;
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

function ascCommandEnv(): NodeJS.ProcessEnv {
  return {
    ...process.env,
    ASC_TIMEOUT_SECONDS: normalizeString(process.env.ASC_TIMEOUT_SECONDS) || DEFAULT_ASC_TIMEOUT_SECONDS,
  };
}

function runJsonCommand(command, commandArgs) {
  return new Promise((resolve, reject) => {
    const child = spawn(command, commandArgs, {
      stdio: ['ignore', 'pipe', 'pipe'],
      env: ascCommandEnv(),
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

function addBreakdownMetric(target, key, field, value) {
  if (!key || value === null || value === undefined || !Number.isFinite(Number(value))) return;
  const existing = target.get(key) || {
    key,
    title: key,
    impressions: 0,
    pageViewUnique: 0,
    units: 0,
    redownloads: 0,
    purchases: 0,
    proceeds: 0,
    crashes: 0,
  };
  existing[field] = (existing[field] || 0) + Number(value);
  target.set(key, existing);
}

async function fileExists(filePath) {
  try {
    await fs.access(filePath);
    return true;
  } catch {
    return false;
  }
}

async function readReportText(filePath) {
  const buffer = await fs.readFile(filePath);
  if (buffer.length >= 2 && buffer[0] === 0x1f && buffer[1] === 0x8b) {
    return (await gunzipAsync(buffer)).toString('utf8');
  }
  return buffer.toString('utf8');
}

async function materializeParsedReport(rawPath, parsedPath) {
  const content = await readReportText(rawPath);
  await fs.mkdir(path.dirname(parsedPath), { recursive: true });
  await fs.writeFile(parsedPath, content, 'utf8');
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
  const content = await readReportText(filePath);
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

function firstRowValue(row, candidates) {
  const header = findHeader(row, candidates);
  if (!header) return null;
  const value = String(row[header] || '').trim();
  return value || null;
}

function reportCachePaths(appCacheDir, baseName) {
  const safeBase = String(baseName || 'report').replace(/[^a-zA-Z0-9._-]+/g, '-');
  return {
    rawPath: path.join(appCacheDir, `${safeBase}.txt.gz`),
    parsedPath: path.join(appCacheDir, `${safeBase}.txt`),
    manifestPath: path.join(appCacheDir, `${safeBase}.manifest.json`),
  };
}

async function loadCachedReport(label, paths) {
  if (await fileExists(paths.parsedPath)) {
    const rows = await parseDelimitedReport(paths.parsedPath);
    return { label, outputPath: paths.parsedPath, rows, cacheStatus: 'hit' };
  }
  if (await fileExists(paths.rawPath)) {
    await materializeParsedReport(paths.rawPath, paths.parsedPath);
    const rows = await parseDelimitedReport(paths.parsedPath);
    return { label, outputPath: paths.parsedPath, rows, cacheStatus: 'hit_raw' };
  }
  return null;
}

async function writeReportManifest(paths, report) {
  await fs.writeFile(
    paths.manifestPath,
    `${JSON.stringify(
      {
        label: report.label,
        fetchedAt: new Date().toISOString(),
        outputPath: report.outputPath,
        rowCount: report.rows.length,
        cacheStatus: report.cacheStatus,
      },
      null,
      2,
    )}\n`,
    'utf8',
  );
}

async function loadCachedReportsFromDir(appCacheDir, warnings, reason) {
  let entries = [];
  try {
    entries = await fs.readdir(appCacheDir);
  } catch {
    return [];
  }
  const manifests = entries.filter((entry) => entry.endsWith('.manifest.json')).sort();
  const reports = [];
  for (const manifestName of manifests) {
    try {
      const manifestPath = path.join(appCacheDir, manifestName);
      const manifest = JSON.parse(await fs.readFile(manifestPath, 'utf8'));
      const parsedPath = String(manifest.outputPath || '').trim();
      if (!parsedPath || !(await fileExists(parsedPath))) continue;
      const rows = await parseDelimitedReport(parsedPath);
      reports.push({
        label: String(manifest.label || manifestName.replace(/\.manifest\.json$/, '')),
        outputPath: parsedPath,
        rows,
        cacheStatus: 'fallback_hit',
      });
    } catch {
      // Ignore malformed cache entries; a fresh download will repair them.
    }
  }
  if (reports.length > 0 && reason) {
    warnings.push(`${reason}: using ${reports.length} cached ASC report(s) from ${appCacheDir}`);
  }
  return reports;
}

async function loadLatestCachedReports(appCacheRoot, requestedEnd, warnings) {
  let entries = [];
  try {
    entries = await fs.readdir(appCacheRoot, { withFileTypes: true });
  } catch {
    return [];
  }
  const requestedTime = parseDate(requestedEnd).getTime();
  const dateDirs = entries
    .filter((entry) => entry.isDirectory() && /^\d{4}-\d{2}-\d{2}$/.test(entry.name))
    .map((entry) => entry.name)
    .filter((date) => parseDate(date).getTime() <= requestedTime)
    .sort()
    .reverse();
  for (const date of dateDirs) {
    const reports = await loadCachedReportsFromDir(
      path.join(appCacheRoot, date),
      warnings,
      `ASC App Analytics batch report unavailable for ${requestedEnd}`,
    );
    if (reports.length > 0) return reports;
  }
  return [];
}

function summarizeBatchRows(rows, appId) {
  const metrics = new Map();
  const sourceBreakdowns = new Map();
  const crashBreakdowns = new Map();
  for (const row of rows) {
    const appIdHeader = findHeader(row, ['apple identifier', 'app apple identifier', 'app id', 'adam id']);
    if (appIdHeader && String(row[appIdHeader] || '').trim() && String(row[appIdHeader]).trim() !== String(appId)) {
      continue;
    }
    const dateHeader = findHeader(row, ['date', 'begin date', 'start date', 'end date']);
    const date = dateHeader ? String(row[dateHeader] || '').slice(0, 10) : null;
    const fields = [
      { measure: 'impressions', candidates: ['impressions', 'unique impressions', 'impression count'] },
      { measure: 'pageViewUnique', candidates: ['unique product page views', 'product page views', 'page views', 'page view count', 'page views unique'] },
      { measure: 'units', candidates: ['units', 'app units', 'downloads', 'first-time downloads'] },
      { measure: 'redownloads', candidates: ['redownloads', 're-downloads'] },
      { measure: 'conversionRate', candidates: ['conversion rate', 'app store conversion rate'], rate: true },
      { measure: 'crashRate', candidates: ['crash rate'], rate: true },
      { measure: 'crashes', candidates: ['crashes', 'crash count'] },
      { measure: 'sessions', candidates: ['sessions', 'session count'] },
      { measure: 'activeDevices', candidates: ['active devices', 'active devices count', 'unique devices'] },
      { measure: 'installations', candidates: ['installations', 'installs', 'app installations'] },
      { measure: 'deletions', candidates: ['deletions', 'deletes', 'app deletions'] },
      { measure: 'purchases', candidates: ['purchases', 'purchase count', 'sales', 'paying purchases'] },
      { measure: 'payingUsers', candidates: ['paying users', 'unique paying users'] },
      { measure: 'subscriptions', candidates: ['subscriptions', 'active subscriptions', 'paid subscriptions'] },
      { measure: 'trialStarts', candidates: ['trial starts', 'free trial starts', 'introductory offer starts'] },
      { measure: 'proceeds', candidates: ['developer proceeds', 'proceeds'] },
    ];
    for (const field of fields) {
      const header = findHeader(row, field.candidates);
      if (!header) continue;
      addMetricTotals(metrics, field.measure, coerceNumber(row[header]), date, { rate: Boolean(field.rate) });
    }

    const source =
      firstRowValue(row, ['source type', 'download source type', 'source', 'source name', 'referrer', 'app referrer', 'web referrer', 'campaign']) ||
      firstRowValue(row, ['page type', 'event type']);
    if (source) {
      const sourceFields = [
        { field: 'impressions', candidates: ['impressions', 'unique impressions', 'impression count'] },
        { field: 'pageViewUnique', candidates: ['unique product page views', 'product page views', 'page views', 'page view count', 'page views unique'] },
        { field: 'units', candidates: ['units', 'app units', 'downloads', 'first-time downloads'] },
        { field: 'redownloads', candidates: ['redownloads', 're-downloads'] },
        { field: 'purchases', candidates: ['purchases', 'purchase count', 'sales'] },
        { field: 'proceeds', candidates: ['developer proceeds', 'proceeds'] },
      ];
      for (const field of sourceFields) {
        const header = findHeader(row, field.candidates);
        if (header) addBreakdownMetric(sourceBreakdowns, source, field.field, coerceNumber(row[header]));
      }
    }

    const crashesHeader = findHeader(row, ['crashes', 'crash count']);
    const crashes = crashesHeader ? coerceNumber(row[crashesHeader]) : null;
    if (crashes !== null && crashes > 0) {
      const version = firstRowValue(row, ['app version', 'version', 'app version number']) || 'Unknown app version';
      const platform = firstRowValue(row, ['platform', 'device type', 'device', 'os']) || '';
      const key = `${version}${platform ? ` (${platform})` : ''}`;
      addBreakdownMetric(crashBreakdowns, key, 'crashes', crashes);
    }
  }
  const results = finishMetricTotals(metrics);
  return {
    results,
    sourceBreakdown: [...sourceBreakdowns.values()]
      .filter((entry) => entry.impressions > 0 || entry.pageViewUnique > 0 || entry.units > 0 || entry.purchases > 0 || entry.proceeds > 0)
      .sort((a, b) => (b.pageViewUnique || b.impressions || b.units || 0) - (a.pageViewUnique || a.impressions || a.units || 0)),
    crashBreakdown: [...crashBreakdowns.values()]
      .map((entry) => ({ label: entry.title, value: entry.crashes }))
      .filter((entry) => entry.value > 0)
      .sort((a, b) => b.value - a.value),
    overviewMetricCatalog: results.map((metric) => ({
      section: 'batchReports',
      measure: metric.measure,
      total: metric.total,
      previousTotal: metric.previousTotal,
      percentChange: metric.percentChange,
      type: 'COUNT',
    })),
  };
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

async function downloadAndParseReport(label, commandArgs, appCacheDir, baseName, args, warnings) {
  const paths = reportCachePaths(appCacheDir, baseName);
  if (!args.forceRefresh) {
    const cached = await loadCachedReport(label, paths);
    if (cached) return cached;
  }

  try {
    await fs.mkdir(appCacheDir, { recursive: true });
    await runJsonCommand('asc', [...commandArgs, '--output', paths.rawPath]);
    await materializeParsedReport(paths.rawPath, paths.parsedPath);
    const rows = await parseDelimitedReport(paths.parsedPath);
    const report = { label, outputPath: paths.parsedPath, rows, cacheStatus: 'downloaded' };
    await writeReportManifest(paths, report);
    return report;
  } catch (error) {
    const compressedError = error instanceof Error ? error.message : String(error);
    try {
      await runJsonCommand('asc', [...commandArgs, '--output', paths.parsedPath, '--decompress']);
      const rows = await parseDelimitedReport(paths.parsedPath);
      const report = { label, outputPath: paths.parsedPath, rows, cacheStatus: 'downloaded_decompressed' };
      await writeReportManifest(paths, report);
      return report;
    } catch (fallbackError) {
      const cached = await loadCachedReport(label, paths);
      if (cached) {
        warnings.push(`${label}: fresh download failed; using cached report (${compressedError})`);
        return cached;
      }
      warnings.push(`${label}: ${fallbackError instanceof Error ? fallbackError.message : String(fallbackError)}`);
      return null;
    }
  }
}

async function downloadBatchAnalyticsReports(appId, args, warnings) {
  const reports = [];
  const appCacheRoot = path.resolve(args.cacheDir || path.join(os.tmpdir(), 'openclaw-asc-cache'), appId);
  const appCacheDir = path.join(appCacheRoot, args.end);

  const vendor = String(process.env.ASC_VENDOR_NUMBER || '').trim();
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
      appCacheDir,
      'sales-daily',
      args,
      warnings,
    );
    if (sales) reports.push(sales);
  }

  const requestsPayload = await runBestEffortAscQuery('ASC analytics requests query', [
    'analytics',
    'requests',
    '--app',
    appId,
    '--output',
    'json',
  ], warnings);
  let requestId = extractItems(requestsPayload).map(extractId).find(Boolean);
  if (!requestId) {
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
    return reports.length > 0 ? reports : loadLatestCachedReports(appCacheRoot, args.end, warnings);
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
    const cached = await loadCachedReportsFromDir(appCacheDir, warnings, 'ASC App Analytics batch report has no downloadable instances');
    if (reports.length > 0 || cached.length > 0) return [...reports, ...cached];
    return loadLatestCachedReports(appCacheRoot, args.end, warnings);
  }

  for (const instance of instances) {
    const report = await downloadAndParseReport(
      `ASC App Analytics batch report ${instance.id}`,
      ['analytics', 'download', '--request-id', requestId, '--instance-id', instance.id],
      appCacheDir,
      `analytics-${instance.id}`,
      args,
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

  return buildAscSummary({
    appId,
    statusPayload,
    ratingsPayload,
    reviewSummariesPayload,
    feedbackPayload,
    batchAnalyticsPayload,
    analyticsWindow: { start: args.start, end: args.end },
    analyticsWarnings: warnings,
    batchReports: batchReports.map((report) => ({
      label: report.label,
      outputPath: report.outputPath,
      rowCount: report.rows.length,
      cacheStatus: report.cacheStatus || null,
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
