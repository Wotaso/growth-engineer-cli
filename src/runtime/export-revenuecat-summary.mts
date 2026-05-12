#!/usr/bin/env node

import process from 'node:process';
import { buildRevenueCatSummary, writeJsonOutput } from './openclaw-exporters-lib.mjs';
import { loadOpenClawGrowthSecrets } from './openclaw-growth-env.mjs';

const API_BASE = 'https://api.revenuecat.com/v2';

function printHelpAndExit(exitCode, reason = null) {
  if (reason) {
    process.stderr.write(`${reason}\n\n`);
  }
  process.stdout.write(`
Export RevenueCat Summary

Builds an OpenClaw-compatible RevenueCat summary JSON from the RevenueCat API v2.

Usage:
  node scripts/export-revenuecat-summary.mjs [options]

Options:
  --project <id>       RevenueCat project ID (defaults to all visible projects, first summarized)
  --currency <code>    Currency for overview metrics (default: USD)
  --limit <n>          Maximum list entries to fetch per endpoint (default: 20)
  --out <file>         Write JSON to file instead of stdout
  --max-signals <n>    Maximum signals to emit (default: 4)
  --help, -h           Show help
`);
  process.exit(exitCode);
}

function parseArgs(argv) {
  const args = {
    project: '',
    currency: 'USD',
    limit: 20,
    out: '',
    maxSignals: 4,
  };

  for (let index = 0; index < argv.length; index += 1) {
    const token = argv[index];
    const next = argv[index + 1];

    if (token === '--') {
      continue;
    } else if (token === '--project') {
      args.project = String(next || '').trim();
      index += 1;
    } else if (token === '--currency') {
      args.currency = String(next || 'USD').trim().toUpperCase() || 'USD';
      index += 1;
    } else if (token === '--limit') {
      const parsed = Number.parseInt(String(next || ''), 10);
      if (!Number.isFinite(parsed) || parsed <= 0) {
        printHelpAndExit(1, `Invalid value for --limit: ${String(next || '')}`);
      }
      args.limit = parsed;
      index += 1;
    } else if (token === '--out') {
      args.out = String(next || '').trim();
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

function buildUrl(pathname, params = {}) {
  const url = new URL(`${API_BASE}${pathname}`);
  for (const [key, value] of Object.entries(params)) {
    if (value === undefined || value === null || value === '') continue;
    url.searchParams.set(key, String(value));
  }
  return url.toString();
}

async function fetchRevenueCatJson(pathname, params = {}) {
  const token = String(process.env.REVENUECAT_API_KEY || '').trim();
  if (!token) {
    throw new Error('Missing REVENUECAT_API_KEY. Rerun the connector wizard and paste a RevenueCat API v2 secret key.');
  }

  const response = await fetch(buildUrl(pathname, params), {
    method: 'GET',
    headers: {
      Accept: 'application/json',
      Authorization: `Bearer ${token}`,
      'User-Agent': 'openclaw-growth-revenuecat-exporter',
    },
  });
  const body = await response.text();
  if (!response.ok) {
    throw new Error(`RevenueCat ${pathname} failed with HTTP ${response.status}: ${body.slice(0, 500)}`);
  }
  return body.trim() ? JSON.parse(body) : {};
}

function listItems(payload) {
  if (Array.isArray(payload)) return payload;
  if (Array.isArray(payload?.items)) return payload.items;
  if (Array.isArray(payload?.data)) return payload.data;
  return [];
}

function projectId(project) {
  return String(project?.id || project?.project_id || '').trim();
}

async function fetchOptional(projectIdValue, label, pathname, params, warnings) {
  try {
    return await fetchRevenueCatJson(pathname, params);
  } catch (error) {
    warnings.push(`${label} for ${projectIdValue}: ${error instanceof Error ? error.message : String(error)}`);
    return null;
  }
}

async function buildSingleProjectSummary(project, args) {
  const id = projectId(project);
  if (!id) {
    throw new Error('RevenueCat project response did not contain a project id.');
  }

  const warnings = [];
  const limit = Math.max(1, Math.min(Number(args.limit) || 20, 100));
  const [overviewPayload, appsPayload, productsPayload, offeringsPayload, entitlementsPayload] = await Promise.all([
    fetchOptional(id, 'overview metrics', `/projects/${encodeURIComponent(id)}/metrics/overview`, { currency: args.currency }, warnings),
    fetchOptional(id, 'apps', `/projects/${encodeURIComponent(id)}/apps`, { limit }, warnings),
    fetchOptional(id, 'products', `/projects/${encodeURIComponent(id)}/products`, { limit }, warnings),
    fetchOptional(id, 'offerings', `/projects/${encodeURIComponent(id)}/offerings`, { limit }, warnings),
    fetchOptional(id, 'entitlements', `/projects/${encodeURIComponent(id)}/entitlements`, { limit }, warnings),
  ]);

  return buildRevenueCatSummary({
    project,
    projectId: id,
    overviewPayload,
    appsPayload,
    productsPayload,
    offeringsPayload,
    entitlementsPayload,
    warnings,
    maxSignals: args.maxSignals,
  });
}

async function main() {
  await loadOpenClawGrowthSecrets();
  const args = parseArgs(process.argv.slice(2));
  const projectsPayload = await fetchRevenueCatJson('/projects', { limit: Math.max(1, Math.min(Number(args.limit) || 20, 100)) });
  const projects = listItems(projectsPayload);
  if (projects.length === 0) {
    throw new Error('RevenueCat API returned no visible projects for this key.');
  }

  const selectedProject = args.project
    ? projects.find((project) => projectId(project) === args.project) || { id: args.project }
    : projects[0];
  const summary = await buildSingleProjectSummary(selectedProject, args);

  if (!args.project && projects.length > 1) {
    (summary.meta as Record<string, unknown>).availableProjectCount = projects.length;
    (summary.meta as Record<string, unknown>).availableProjectIds = projects.map(projectId).filter(Boolean);
  }

  await writeJsonOutput(args.out, summary);
}

main().catch((error) => {
  process.stderr.write(`${error instanceof Error ? error.message : String(error)}\n`);
  process.exitCode = 1;
});
