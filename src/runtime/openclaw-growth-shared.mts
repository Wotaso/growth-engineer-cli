const BUILTIN_SOURCE_NAMES = ['analytics', 'revenuecat', 'sentry', 'feedback'];

const SERVICE_KIND_ALIASES = {
  analytics: [
    'analytics',
    'analyticscli',
    'mixpanel',
    'amplitude',
    'firebase',
    'posthog',
    'telemetry',
  ],
  revenue: ['revenuecat', 'stripe', 'purchases', 'billing', 'adapty', 'superwall'],
  crash: ['sentry', 'glitchtip', 'crashlytics', 'bugsnag', 'datadog', 'rollbar'],
  feedback: [
    'feedback',
    'support',
    'intercom',
    'zendesk',
    'app-store-reviews',
    'app_store_reviews',
    'play-store-reviews',
    'play_console_reviews',
  ],
  store: [
    'asc',
    'asc-cli',
    'app-store-connect',
    'app_store_connect',
    'play-console',
    'play_console',
    'google-play',
    'google_play',
    'aso',
  ],
};

export function getBuiltinSourceNames() {
  return [...BUILTIN_SOURCE_NAMES];
}

export function normalizeServiceType(value) {
  return String(value || '')
    .trim()
    .toLowerCase()
    .replace(/[\s/]+/g, '-')
    .replace(/[^a-z0-9._-]/g, '');
}

export function classifyServiceKind(service) {
  const normalized = normalizeServiceType(service);
  for (const [kind, aliases] of Object.entries(SERVICE_KIND_ALIASES)) {
    if (aliases.includes(normalized)) {
      return kind;
    }
  }
  return 'custom';
}

export function normalizeSourceKey(value, fallback = 'source') {
  const normalized = normalizeServiceType(value).replace(/[.-]+/g, '_');
  return normalized || fallback;
}

export function getDefaultSourcePath(key) {
  return `data/openclaw-growth-engineer/${normalizeSourceKey(key)}_summary.json`;
}

export function getDefaultSourceHint(service) {
  const kind = classifyServiceKind(service);
  if (kind === 'analytics') {
    return '- Preferred: AnalyticsCLI bounded query/export written to JSON.\n- For command mode, output summary JSON in the shared signals[] shape.';
  }
  if (kind === 'revenue') {
    return '- Revenue provider summary with monetization deltas, package/offering signals, and churn notes.\n- Command mode should output JSON in the shared signals[] shape.';
  }
  if (kind === 'crash') {
    return '- Crash/error provider summary with top regressions, affected users, and issue evidence.\n- `issues[]` or shared `signals[]` payloads are both accepted.';
  }
  if (kind === 'feedback') {
    return '- Aggregate app reviews, support tickets, or in-app feedback into recurring themes.\n- `items[]` or shared `signals[]` payloads are both accepted.';
  }
  if (kind === 'store') {
    return '- Store/distribution summary from ASC CLI, Play Console exports, or release tooling.\n- Focus on review trends, release blockers, ratings, and ASO signals.';
  }
  return '- Any connector is supported when it can produce JSON in the shared `signals[]` shape.\n- Use `issues[]` for crash tools or `items[]` for feedback-like tools when that fits better.';
}

export function getDefaultSourceCommand(service) {
  const normalized = normalizeServiceType(service);
  if (normalized === 'analytics' || normalized === 'analyticscli') {
    return 'node scripts/export-analytics-summary.mjs';
  }
  if (normalized === 'revenuecat' || normalized === 'revenue-cat' || normalized === 'rc') {
    return 'node scripts/export-revenuecat-summary.mjs';
  }
  if (normalized === 'sentry') {
    return 'node scripts/export-sentry-summary.mjs';
  }
  if (normalized === 'feedback') {
    return 'analyticscli feedback summary --format json';
  }
  if (
    normalized === 'asc' ||
    normalized === 'asc-cli' ||
    normalized === 'app-store-connect' ||
    normalized === 'app_store_connect'
  ) {
    return 'node scripts/export-asc-summary.mjs';
  }
  return null;
}

export function getAutomationConfig(config) {
  const automation = config?.automation && typeof config.automation === 'object' ? config.automation : {};
  const openclawCron =
    automation.openclawCron && typeof automation.openclawCron === 'object' ? automation.openclawCron : {};
  return {
    ...automation,
    openclawCron: {
      enabled: openclawCron.enabled !== false,
      mode: String(openclawCron.mode || 'main').trim() || 'main',
      schedule: String(openclawCron.schedule || '*/30 * * * *').trim() || '*/30 * * * *',
      timezone: String(openclawCron.timezone || process.env.TZ || 'UTC').trim() || 'UTC',
      name: String(openclawCron.name || 'OpenClaw Growth Engineer scheduler').trim() ||
        'OpenClaw Growth Engineer scheduler',
    },
  };
}

export function buildGrowthRunnerCommand(configPath) {
  return `node scripts/openclaw-growth-runner.mjs --config ${configPath}`;
}

export function buildOpenClawGrowthSystemEvent(configPath, config = {}) {
  const command = buildGrowthRunnerCommand(configPath);
  const automation = getAutomationConfig(config);
  return [
    'Run OpenClaw Growth Engineer for this workspace.',
    `Execute: ${command}`,
    'The runner is the source of truth for connector health, daily, weekly, monthly, quarterly, six-month, and yearly cadence decisions.',
    'After the command finishes, inspect data/openclaw-growth-engineer/state.json and data/openclaw-growth-engineer/runtime/scheduler-proof.jsonl.',
    'If connector health is healthy, no production issue is found, and no actionable growth finding was generated, reply HEARTBEAT_OK.',
    `Expected OpenClaw cron schedule: ${automation.openclawCron.schedule} ${automation.openclawCron.timezone}.`,
  ].join(' ');
}

export function buildExtraSourceConfig(service, options: Record<string, any> = {}) {
  const normalizedService = normalizeServiceType(service);
  const key = normalizeSourceKey(options.key || normalizedService || `extra_${Date.now()}`);
  const defaultCommand = getDefaultSourceCommand(normalizedService || key);
  const mode = options.mode || (defaultCommand ? 'command' : 'file');
  return {
    key,
    label: options.label || normalizedService || key,
    service: normalizedService || key,
    enabled: options.enabled !== false,
    mode,
    ...(mode === 'command'
      ? { command: options.command || defaultCommand || '' }
      : { path: options.path || getDefaultSourcePath(key) }),
    hint: options.hint || getDefaultSourceHint(normalizedService || key),
    secretEnv: options.secretEnv || null,
  };
}

export function getExtraSources(config) {
  const extra = Array.isArray(config?.sources?.extra) ? config.sources.extra : [];
  const seen = new Set();
  const result = [];

  for (const [index, source] of extra.entries()) {
    if (!source || typeof source !== 'object') {
      continue;
    }
    const key = normalizeSourceKey(source.key || source.name || source.service || `extra_${index + 1}`);
    if (seen.has(key)) {
      continue;
    }
    seen.add(key);
    result.push({
      ...source,
      key,
      label: String(source.label || source.name || source.service || key),
      service: normalizeServiceType(source.service || source.name || key),
      enabled: source.enabled !== false,
      mode: String(source.mode || 'file').toLowerCase() === 'command' ? 'command' : 'file',
      secretEnv:
        typeof source.secretEnv === 'string' && source.secretEnv.trim()
          ? source.secretEnv.trim()
          : null,
      hint:
        typeof source.hint === 'string' && source.hint.trim()
          ? source.hint
          : getDefaultSourceHint(source.service || key),
    });
  }

  return result;
}

export function getAllSourceEntries(config) {
  const builtins = getBuiltinSourceNames()
    .filter((name) => Boolean(config?.sources?.[name]))
    .map((name) => {
      const source = config.sources[name];
      return {
        key: name,
        label: name,
        service: normalizeServiceType(source?.service || name),
        builtIn: true,
        ...(source || {}),
      };
    });

  return [...builtins, ...getExtraSources(config).map((source) => ({ ...source, builtIn: false }))];
}

export function getActionMode(config) {
  const configured = normalizeServiceType(config?.actions?.mode || '');
  if (configured === 'pull-request' || configured === 'pull_request' || configured === 'pr') {
    return 'pull_request';
  }
  if (config?.actions?.autoCreatePullRequests === true) {
    return 'pull_request';
  }
  return 'issue';
}

export function getGitHubArtifactModes(config) {
  const modes = [];
  const hasExplicitDestinations = Array.isArray(config?.actions?.outputDestinations);
  const destinations = hasExplicitDestinations
    ? config.actions.outputDestinations.map((value) => normalizeServiceType(value))
    : [];
  const deliveryModes = Array.isArray(config?.deliveries?.github?.modes)
    ? config.deliveries.github.modes.map((value) => normalizeServiceType(value))
    : [];
  if (
    config?.actions?.autoCreateIssues === true ||
    destinations.includes('github_issue') ||
    deliveryModes.includes('issue')
  ) {
    modes.push('issue');
  }
  if (
    config?.actions?.autoCreatePullRequests === true ||
    destinations.includes('github_pull_request') ||
    destinations.includes('github-pr') ||
    destinations.includes('draft_pr') ||
    deliveryModes.includes('pull_request') ||
    deliveryModes.includes('pull-request')
  ) {
    modes.push('pull_request');
  }
  if (modes.length === 0 && !hasExplicitDestinations) {
    modes.push(getActionMode(config));
  }
  return [...new Set(modes)];
}

export function shouldAutoCreateGitHubArtifact(config, requestedMode = null) {
  if (config?.actions?.disableAutoCreateGitHubArtifacts === true) {
    return false;
  }
  if (!requestedMode && Array.isArray(config?.actions?.outputDestinations) && getGitHubArtifactModes(config).length === 0) {
    return false;
  }
  const mode = requestedMode || getActionMode(config);
  if (mode === 'pull_request') {
    return config?.actions?.autoCreatePullRequests === true;
  }
  if (config?.actions?.autoCreateIssues === true) {
    return true;
  }

  const tokenEnv = String(config?.secrets?.githubTokenEnv || 'GITHUB_TOKEN').trim();
  const hasToken = Boolean(process.env[tokenEnv]);
  const hasRepo = Boolean(String(config?.project?.githubRepo || '').trim());
  const autoCreateWhenWritable = config?.actions?.autoCreateWhenGitHubWriteAccess !== false;
  return autoCreateWhenWritable && hasToken && hasRepo;
}

export function getGitHubRequirementText(actionMode) {
  if (actionMode === 'pull_request') {
    return 'fine-grained PAT with Pull requests: Read/Write and Contents: Read/Write';
  }
  return 'fine-grained PAT with Issues: Read/Write and Contents: Read';
}

export function getGitHubConnectionSummary(actionMode) {
  if (actionMode === 'pull_request') {
    return 'GitHub auth, repository access, pull-request API read checks, and default-branch metadata checks passed';
  }
  return 'GitHub auth, repository access, and issues API read checks passed';
}

export function getGitHubActionNoun(actionMode) {
  return actionMode === 'pull_request' ? 'pull requests' : 'issues';
}
