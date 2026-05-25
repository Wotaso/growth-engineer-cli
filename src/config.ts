import { mkdir, readFile, writeFile } from 'node:fs/promises';
import { existsSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import { z } from 'zod';

const sourceSchema = z
  .object({
    enabled: z.boolean().optional(),
    mode: z.enum(['command', 'file']).optional(),
    command: z.string().optional(),
    path: z.string().optional(),
    cursorMode: z.enum(['manual', 'auto_since_last_fetch']).optional(),
    initialLookback: z.string().optional(),
    key: z.string().optional(),
    label: z.string().optional(),
    service: z.string().optional(),
    secretEnv: z.string().nullable().optional(),
    hint: z.string().optional(),
  })
  .passthrough();

const githubDeliverySchema = z
  .object({
    enabled: z.boolean().default(false),
    mode: z.enum(['issue', 'pull_request']).default('issue'),
    autoCreate: z.boolean().default(false),
    draftPullRequests: z.boolean().default(true),
    proposalBranchPrefix: z.string().default('openclaw/proposals'),
  })
  .default({});

const openclawChatDeliverySchema = z
  .object({
    enabled: z.boolean().default(true),
    markdownPath: z.string().default('.openclaw/chat/latest.md'),
    jsonPath: z.string().default('.openclaw/chat/latest.json'),
  })
  .default({});

const slackDeliverySchema = z
  .object({
    enabled: z.boolean().default(false),
    webhookEnv: z.string().default('SLACK_WEBHOOK_URL'),
    username: z.string().optional(),
  })
  .default({});

const webhookDeliverySchema = z
  .object({
    enabled: z.boolean().default(false),
    urlEnv: z.string().default('OPENCLAW_WEBHOOK_URL'),
    method: z.enum(['POST']).default('POST'),
    headers: z.record(z.string()).default({}),
  })
  .default({});

const commandDeliverySchema = z
  .object({
    enabled: z.boolean().default(false),
    label: z.string().default('command'),
    command: z.string().default(''),
  })
  .default({});

const notificationChannelSchema = z
  .object({
    type: z.enum(['openclaw-chat', 'slack', 'webhook', 'command']).default('openclaw-chat'),
    enabled: z.boolean().default(true),
    label: z.string().optional(),
    markdownPath: z.string().optional(),
    jsonPath: z.string().optional(),
    webhookEnv: z.string().optional(),
    urlEnv: z.string().optional(),
    method: z.enum(['POST']).default('POST'),
    headers: z.record(z.string()).default({}),
    command: z.string().optional(),
  })
  .passthrough();

const notificationsSchema = z
  .object({
    connectorHealth: z
      .object({
        enabled: z.boolean().default(true),
        channels: z.array(notificationChannelSchema).default([]),
      })
      .default({}),
    growthRun: z
      .object({
        enabled: z.boolean().default(true),
        channels: z.array(notificationChannelSchema).default([]),
      })
      .default({}),
  })
  .default({});

const cadenceSchema = z
  .object({
    key: z.string(),
    title: z.string().optional(),
    intervalMinutes: z.number().int().min(1).optional(),
    intervalDays: z.number().min(1).optional(),
    criticalOnly: z.boolean().optional(),
    objective: z.string().optional(),
    instructions: z.string().optional(),
    focusAreas: z.array(z.string()).default([]),
    sourcePriorities: z.array(z.string()).default([]),
    enabled: z.boolean().default(true),
  })
  .passthrough();

const scheduleSchema = z
  .object({
    intervalMinutes: z.number().int().min(1).default(1440),
    connectorHealthCheckIntervalMinutes: z.number().int().min(1).default(360),
    skipIfNoDataChange: z.boolean().default(true),
    skipIfIssueSetUnchanged: z.boolean().default(true),
    cadences: z.array(cadenceSchema).default([]),
  })
  .default({});

const strategySchema = z
  .object({
    proposalMode: z.enum(['mandatory', 'balanced', 'creative']).default('balanced'),
  })
  .default({});

const actionsSchema = z
  .object({
    mode: z.enum(['issue', 'pull_request']).default('issue'),
    outputDestinations: z.array(z.string()).default(['openclaw_chat']),
    productionErrorMode: z.enum(['alert', 'issue', 'pull_request']).default('alert'),
    autoCreateIssues: z.boolean().default(false),
    autoCreatePullRequests: z.boolean().default(false),
    autoCreateWhenGitHubWriteAccess: z.boolean().default(true),
    disableAutoCreateGitHubArtifacts: z.boolean().default(false),
    draftPullRequests: z.boolean().default(true),
    proposalBranchPrefix: z.string().default('openclaw/proposals'),
    usageMode: z.string().default('production_autopilot'),
  })
  .passthrough()
  .default({});

const secretRefSchema = z
  .object({
    source: z.enum(['env', 'file', 'exec']).default('env'),
    provider: z.string().default('default'),
    id: z.string(),
  })
  .passthrough();

const secretsSchema = z
  .object({
    githubTokenEnv: z.string().default('GITHUB_TOKEN'),
    githubTokenRef: secretRefSchema.optional(),
    analyticsTokenEnv: z.string().default('ANALYTICSCLI_ACCESS_TOKEN'),
    analyticsTokenRef: secretRefSchema.optional(),
    revenuecatTokenEnv: z.string().default('REVENUECAT_API_KEY'),
    revenuecatTokenRef: secretRefSchema.optional(),
    paddleTokenEnv: z.string().default('PADDLE_API_KEY'),
    paddleTokenRef: secretRefSchema.optional(),
    gscTokenEnv: z.string().default('GOOGLE_SEARCH_CONSOLE_ACCESS_TOKEN'),
    gscTokenRef: secretRefSchema.optional(),
    dataforseoLoginEnv: z.string().default('DATAFORSEO_LOGIN'),
    dataforseoLoginRef: secretRefSchema.optional(),
    dataforseoPasswordEnv: z.string().default('DATAFORSEO_PASSWORD'),
    dataforseoPasswordRef: secretRefSchema.optional(),
    sentryTokenEnv: z.string().default('SENTRY_AUTH_TOKEN'),
    sentryTokenRef: secretRefSchema.optional(),
    coolifyTokenEnv: z.string().default('COOLIFY_API_TOKEN'),
    coolifyTokenRef: secretRefSchema.optional(),
  })
  .default({});

const securitySchema = z
  .object({
    connectorSecrets: z
      .object({
        mode: z.string().default('openclaw-secret-refs'),
        persisted: z.boolean().default(true),
        agentReadable: z.union([z.boolean(), z.string()]).optional(),
        secretsFile: z.string().optional(),
      })
      .passthrough()
      .optional(),
  })
  .passthrough()
  .default({});

const automationSchema = z
  .object({
    openclawCron: z
      .object({
        enabled: z.boolean().default(true),
        mode: z.enum(['main', 'isolated']).default('main'),
        schedule: z.string().default('*/30 * * * *'),
        timezone: z.string().default('UTC'),
        name: z.string().default('OpenClaw Growth Engineer scheduler'),
      })
      .passthrough()
      .default({}),
  })
  .passthrough()
  .default({});

const templateConfigSchema = z.object({
  version: z.number().int().default(7),
  generatedAt: z.string().optional(),
  project: z.object({
    githubRepo: z.string(),
    repoRoot: z.string().default('.'),
    outFile: z.string().default('data/openclaw-growth-engineer/issues.generated.json'),
    maxIssues: z.number().int().min(1).max(20).default(4),
    titlePrefix: z.string().default('[Growth]'),
    labels: z.array(z.string()).default(['ai-growth', 'autogenerated', 'product']),
  }),
  sources: z
    .object({
      analytics: sourceSchema.default({ enabled: true, mode: 'command' }),
      revenuecat: sourceSchema.default({ enabled: false, mode: 'file' }),
      paddle: sourceSchema.default({ enabled: false, mode: 'command' }),
      seo: sourceSchema.default({ enabled: false, mode: 'command' }),
      sentry: sourceSchema.default({ enabled: false, mode: 'file' }),
      coolify: sourceSchema.default({ enabled: false, mode: 'command' }),
      feedback: sourceSchema.default({
        enabled: true,
        mode: 'command',
        cursorMode: 'auto_since_last_fetch',
        initialLookback: '30d',
      }),
      extra: z.array(sourceSchema).default([]),
    })
    .default({}),
  schedule: scheduleSchema,
  strategy: strategySchema,
  actions: actionsSchema,
  deliveries: z
    .object({
      openclawChat: openclawChatDeliverySchema,
      github: githubDeliverySchema,
      slack: slackDeliverySchema,
      webhook: webhookDeliverySchema,
      command: commandDeliverySchema,
      discord: commandDeliverySchema,
    })
    .default({}),
  charting: z
    .object({
      enabled: z.boolean().default(false),
      command: z.string().nullable().default(null),
    })
    .default({}),
  notifications: notificationsSchema,
  automation: automationSchema,
  security: securitySchema,
  secrets: secretsSchema,
});

export type OpenClawConfig = z.infer<typeof templateConfigSchema>;

export type LegacyGrowthConfig = {
  version: number;
  generatedAt?: string;
  project: {
    githubRepo: string;
    repoRoot: string;
    outFile: string;
    maxIssues: number;
    titlePrefix: string;
    labels: string[];
  };
  sources: {
    analytics: z.infer<typeof sourceSchema>;
    revenuecat: z.infer<typeof sourceSchema>;
    paddle: z.infer<typeof sourceSchema>;
    seo: z.infer<typeof sourceSchema>;
    sentry: z.infer<typeof sourceSchema>;
    coolify: z.infer<typeof sourceSchema>;
    feedback: z.infer<typeof sourceSchema>;
    extra: Array<z.infer<typeof sourceSchema>>;
  };
  schedule: z.infer<typeof scheduleSchema>;
  strategy: z.infer<typeof strategySchema>;
  actions: {
    mode: 'issue' | 'pull_request';
    outputDestinations?: string[];
    productionErrorMode?: 'alert' | 'issue' | 'pull_request';
    autoCreateIssues: boolean;
    autoCreatePullRequests: boolean;
    autoCreateWhenGitHubWriteAccess?: boolean;
    disableAutoCreateGitHubArtifacts?: boolean;
    draftPullRequests: boolean;
    proposalBranchPrefix: string;
    usageMode?: string;
  };
  charting: {
    enabled: boolean;
    command: string | null;
  };
  deliveries?: OpenClawConfig['deliveries'];
  notifications?: OpenClawConfig['notifications'];
  security?: OpenClawConfig['security'];
  secrets: z.infer<typeof secretsSchema>;
};

export const parseOpenClawConfig = (value: unknown): OpenClawConfig => templateConfigSchema.parse(value);

export const readOpenClawConfig = async (configPath: string): Promise<OpenClawConfig> => {
  const raw = await readFile(configPath, 'utf8');
  return parseOpenClawConfig(JSON.parse(raw));
};

export const writeOpenClawConfig = async (configPath: string, value: OpenClawConfig): Promise<void> => {
  await mkdir(dirname(configPath), { recursive: true });
  await writeFile(configPath, `${JSON.stringify(value, null, 2)}\n`, 'utf8');
};

const resolveMaybePath = (baseDir: string, filePath: string | undefined): string | undefined => {
  if (!filePath) {
    return undefined;
  }
  return resolve(baseDir, filePath);
};

const absolutizeSource = (
  baseDir: string,
  source: z.infer<typeof sourceSchema>,
): z.infer<typeof sourceSchema> => {
  if (source.mode === 'file' && source.path) {
    return {
      ...source,
      path: resolveMaybePath(baseDir, source.path),
    };
  }
  return source;
};

export const toLegacyGrowthConfig = (
  configPath: string,
  config: OpenClawConfig,
): LegacyGrowthConfig => {
  const baseDir = dirname(configPath);
  const github = config.deliveries.github;

  return {
    version: 2,
    generatedAt: config.generatedAt,
    project: {
      githubRepo: config.project.githubRepo,
      repoRoot: resolveMaybePath(baseDir, config.project.repoRoot) ?? config.project.repoRoot,
      outFile: resolveMaybePath(baseDir, config.project.outFile) ?? config.project.outFile,
      maxIssues: config.project.maxIssues,
      titlePrefix: config.project.titlePrefix,
      labels: config.project.labels,
    },
    sources: {
      analytics: absolutizeSource(baseDir, config.sources.analytics),
      revenuecat: absolutizeSource(baseDir, config.sources.revenuecat),
      paddle: absolutizeSource(baseDir, config.sources.paddle),
      seo: absolutizeSource(baseDir, config.sources.seo),
      sentry: absolutizeSource(baseDir, config.sources.sentry),
      coolify: absolutizeSource(baseDir, config.sources.coolify),
      feedback: absolutizeSource(baseDir, config.sources.feedback),
      extra: config.sources.extra.map((source) => absolutizeSource(baseDir, source)),
    },
    schedule: config.schedule,
    strategy: config.strategy,
    actions: {
      ...config.actions,
      mode: config.actions.mode || github.mode,
      autoCreateIssues: config.actions.autoCreateIssues || (github.enabled && github.mode === 'issue' && github.autoCreate),
      autoCreatePullRequests:
        config.actions.autoCreatePullRequests || (github.enabled && github.mode === 'pull_request' && github.autoCreate),
      draftPullRequests: config.actions.draftPullRequests ?? github.draftPullRequests,
      proposalBranchPrefix: config.actions.proposalBranchPrefix || github.proposalBranchPrefix,
    },
    charting: {
      enabled: config.charting.enabled,
      command: config.charting.command,
    },
    deliveries: config.deliveries,
    notifications: config.notifications,
    security: config.security,
    secrets: config.secrets,
  };
};

export const readJsonFile = async <T>(filePath: string): Promise<T> => {
  const raw = await readFile(filePath, 'utf8');
  return JSON.parse(raw) as T;
};

export const writeJsonFile = async (filePath: string, value: unknown): Promise<void> => {
  await mkdir(dirname(filePath), { recursive: true });
  await writeFile(filePath, `${JSON.stringify(value, null, 2)}\n`, 'utf8');
};

export const fileExists = (filePath: string): boolean => existsSync(filePath);
