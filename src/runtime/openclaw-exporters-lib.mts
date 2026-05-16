import { promises as fs } from 'node:fs';
import path from 'node:path';

function coerceNumber(value) {
  if (typeof value === 'number' && Number.isFinite(value)) {
    return value;
  }
  if (typeof value === 'string' && value.trim()) {
    const parsed = Number(value);
    if (Number.isFinite(parsed)) {
      return parsed;
    }
  }
  return null;
}

function coerceRatioFromPercent(value) {
  const numeric = coerceNumber(value);
  if (numeric === null) return null;
  return numeric / 100;
}

function round(value, digits = 4) {
  if (!Number.isFinite(value)) return 0;
  return Number(value.toFixed(digits));
}

function computeDeltaPercent(currentValue, baselineValue) {
  if (!Number.isFinite(currentValue) || !Number.isFinite(baselineValue)) {
    return null;
  }
  if (Math.abs(baselineValue) < 1e-9) {
    if (Math.abs(currentValue) < 1e-9) return 0;
    return currentValue > 0 ? 100 : -100;
  }
  return round(((currentValue - baselineValue) / Math.abs(baselineValue)) * 100, 2);
}

function normalizeWindow(last) {
  const normalized = String(last || '30d')
    .trim()
    .toLowerCase();
  if (!normalized) return 'last_30d';
  if (normalized.startsWith('last_')) return normalized;
  return `last_${normalized}`;
}

function priorityRank(priority) {
  if (priority === 'high') return 3;
  if (priority === 'medium') return 2;
  return 1;
}

function sortSignals(signals) {
  return [...signals].sort((a, b) => {
    const priorityDelta =
      priorityRank(String(b.priority || 'low')) - priorityRank(String(a.priority || 'low'));
    if (priorityDelta !== 0) return priorityDelta;
    const deltaA = Math.abs(coerceNumber(a.delta_percent ?? a.deltaPercent) || 0);
    const deltaB = Math.abs(coerceNumber(b.delta_percent ?? b.deltaPercent) || 0);
    return deltaB - deltaA;
  });
}

function hasMinimumSample(value, minimum = 20) {
  const numeric = coerceNumber(value);
  return numeric !== null && numeric >= minimum;
}

function normalizeRetentionReliability(retention) {
  if (!retention?.quality || typeof retention.quality !== 'object') {
    return null;
  }
  const value = String(retention?.quality?.reliability || '')
    .trim()
    .toLowerCase();
  if (value === 'high' || value === 'medium' || value === 'low' || value === 'unknown') {
    return value;
  }
  return 'unknown';
}

function buildRetentionQualityEvidence(retention) {
  const quality = retention?.quality;
  if (!quality || typeof quality !== 'object') {
    return [];
  }

  const evidence = [`Retention reliability: ${normalizeRetentionReliability(retention)}`];
  const stableShare = coerceNumber(quality.stableIdentityShare);
  const multiSessionShare = coerceNumber(quality.multiSessionShare);
  if (stableShare !== null) {
    evidence.push(`Stable identity share: ${(stableShare * 100).toFixed(1)}%`);
  }
  if (multiSessionShare !== null) {
    evidence.push(`Multi-session identity share: ${(multiSessionShare * 100).toFixed(1)}%`);
  }
  if (Array.isArray(quality.warnings)) {
    for (const warning of quality.warnings.slice(0, 2)) {
      if (typeof warning === 'string' && warning.trim()) {
        evidence.push(`Retention caveat: ${warning.trim()}`);
      }
    }
  }
  return evidence;
}

function maybePushSignal(signals, signal) {
  if (!signal) return;
  signals.push(signal);
}

function buildAnalyticsTrendEvidence(label, trend) {
  if (!trend || typeof trend !== 'object') return null;
  const direction = String(trend.direction || '').trim();
  const percentChange = coerceNumber(trend.percentChange);
  const startValue = coerceNumber(trend.startValue);
  const currentValue = coerceNumber(trend.currentValue);
  if (!direction || percentChange === null || startValue === null || currentValue === null) {
    return null;
  }
  const signed = percentChange > 0 ? `+${percentChange}%` : `${percentChange}%`;
  return `${label} trend: ${direction} ${signed} (start=${startValue}, current=${currentValue})`;
}

export function buildAnalyticsSummary(input) {
  const last = String(input?.last || '30d');
  const onboardingJourney = input?.onboardingJourney || null;
  const retention = input?.retention || null;
  const project =
    String(
      onboardingJourney?.projectId || input?.projectId || input?.project || 'analyticscli-project',
    ).trim() || 'analyticscli-project';

  const signals = [];
  const starters = coerceNumber(onboardingJourney?.starters) || 0;
  const paywallReachedUsers = coerceNumber(onboardingJourney?.paywallReachedUsers) || 0;

  const completionRate = coerceRatioFromPercent(onboardingJourney?.completionRate);
  const paywallSkipRate = coerceRatioFromPercent(onboardingJourney?.paywallSkipRateFromPaywall);
  const purchaseRateFromPaywall = coerceRatioFromPercent(
    onboardingJourney?.purchaseRateFromPaywall,
  );

  if (hasMinimumSample(starters)) {
    const completionBaseline = 0.6;
    if (completionRate !== null && completionRate < completionBaseline) {
      maybePushSignal(signals, {
        id: 'onboarding_completion_below_target',
        title: 'Onboarding completion rate is below target',
        area: 'onboarding',
        priority: completionRate < 0.45 ? 'high' : 'medium',
        metric: 'onboarding_completion_rate',
        current_value: round(completionRate),
        baseline_value: completionBaseline,
        delta_percent: computeDeltaPercent(completionRate, completionBaseline),
        evidence: [
          `${onboardingJourney?.completedUsers || 0} of ${starters} onboarding starters completed successfully`,
          onboardingJourney?.paywallAnchorEvent
            ? `Paywall anchor event in the flow: ${onboardingJourney.paywallAnchorEvent}`
            : 'No stable paywall anchor event detected in the onboarding journey payload',
          buildAnalyticsTrendEvidence('Completion rate', onboardingJourney?.trends?.completionRate),
        ].filter(Boolean),
        suggested_actions: [
          'Shorten the onboarding path before the first value moment',
          'Delay monetization or permission friction until after the first core success event',
          'Inspect the heaviest drop-off steps in the onboarding journey and simplify one of them',
        ],
        keywords: ['onboarding', 'completion', 'dropoff', 'first_value'],
      });
    }
  }

  if (hasMinimumSample(paywallReachedUsers)) {
    const paywallSkipBaseline = 0.45;
    if (paywallSkipRate !== null && paywallSkipRate > paywallSkipBaseline) {
      maybePushSignal(signals, {
        id: 'paywall_skip_rate_above_target',
        title: 'Paywall skip rate is above target',
        area: 'paywall',
        priority: paywallSkipRate > 0.6 ? 'high' : 'medium',
        metric: 'paywall_skip_rate',
        current_value: round(paywallSkipRate),
        baseline_value: paywallSkipBaseline,
        delta_percent: computeDeltaPercent(paywallSkipRate, paywallSkipBaseline),
        evidence: [
          `${onboardingJourney?.paywallSkippedUsers || 0} users skipped after ${paywallReachedUsers} reached the paywall`,
          onboardingJourney?.paywallSkipEvent
            ? `Most visible skip event: ${onboardingJourney.paywallSkipEvent}`
            : 'No stable skip event detected in the onboarding journey payload',
          buildAnalyticsTrendEvidence(
            'Paywall reached rate',
            onboardingJourney?.trends?.paywallReachedRate,
          ),
        ].filter(Boolean),
        suggested_actions: [
          'Clarify the premium value proposition and annual-vs-monthly trade-off',
          'Reduce cognitive load on the first paywall view and tighten the CTA hierarchy',
          'Test a later paywall placement after a stronger proof-of-value moment',
        ],
        keywords: ['paywall', 'skip', 'pricing', 'conversion'],
      });
    }

    const purchaseBaseline = 0.12;
    if (purchaseRateFromPaywall !== null && purchaseRateFromPaywall < purchaseBaseline) {
      maybePushSignal(signals, {
        id: 'paywall_purchase_rate_below_target',
        title: 'Paywall-to-purchase conversion is below target',
        area: 'conversion',
        priority: purchaseRateFromPaywall < 0.06 ? 'high' : 'medium',
        metric: 'purchase_rate_from_paywall',
        current_value: round(purchaseRateFromPaywall),
        baseline_value: purchaseBaseline,
        delta_percent: computeDeltaPercent(purchaseRateFromPaywall, purchaseBaseline),
        evidence: [
          `${onboardingJourney?.purchasedUsers || 0} purchases from ${paywallReachedUsers} paywall exposures`,
          onboardingJourney?.purchaseEvent
            ? `Purchase success event observed: ${onboardingJourney.purchaseEvent}`
            : 'No stable purchase success event detected in the onboarding journey payload',
          buildAnalyticsTrendEvidence('Purchase rate', onboardingJourney?.trends?.purchaseRate),
        ].filter(Boolean),
        suggested_actions: [
          'Simplify the paywall package comparison and highlight the default recommended offer',
          'Reduce ambiguity around trial terms, pricing cadence, and restore flow',
          'Test a stronger trust/benefit section near the purchase CTA',
        ],
        keywords: ['purchase', 'paywall', 'subscription', 'conversion'],
      });
    }
  }

  const retentionByDay = new Map<number, number>(
    Array.isArray(retention?.days)
      ? retention.days
          .map((entry) => {
            const day = coerceNumber(entry?.day);
            const rate = coerceNumber(entry?.retentionRate);
            if (day === null || rate === null) return null;
            return [day, rate] as [number, number];
          })
          .filter((entry): entry is [number, number] => entry !== null)
      : [],
  );

  const retentionTargets = [
    { day: 7, baseline: 0.1 },
    { day: 3, baseline: 0.2 },
    { day: 1, baseline: 0.35 },
  ];
  const retentionReliability = normalizeRetentionReliability(retention);
  const retentionHasLowConfidence =
    retentionReliability === 'low' || retentionReliability === 'unknown';
  const retentionQualityEvidence = buildRetentionQualityEvidence(retention);

  if (hasMinimumSample(retention?.cohortSize)) {
    for (const target of retentionTargets) {
      const actual = retentionByDay.get(target.day);
      if (actual === undefined || actual >= target.baseline) {
        continue;
      }

      maybePushSignal(signals, {
        id: `retention_d${target.day}_below_target`,
        title: retentionHasLowConfidence
          ? `Day-${target.day} retention appears below target, but identity quality is low`
          : `Day-${target.day} retention is below target`,
        area: 'retention',
        priority: retentionHasLowConfidence ? 'medium' : target.day >= 3 ? 'high' : 'medium',
        metric: `d${target.day}_retention`,
        current_value: round(actual),
        baseline_value: target.baseline,
        delta_percent: computeDeltaPercent(actual, target.baseline),
        evidence: [
          `Retention cohort size: ${retention.cohortSize}`,
          `Observed D${target.day} retention: ${(actual * 100).toFixed(2)}%`,
          ...retentionQualityEvidence,
          retention?.avgActiveDays !== undefined
            ? `Average active days in the cohort: ${retention.avgActiveDays}`
            : null,
        ].filter(Boolean),
        suggested_actions: [
          retentionHasLowConfidence
            ? 'Verify SDK identity persistence and rerun retention with stable identity filtering before treating D1/D7 as a product fact'
            : null,
          'Revisit the first-session value loop and ensure the core action completes quickly',
          'Add targeted re-entry prompts or reminders after the first session',
          'Instrument the major early-session drop-off points to isolate which step drives the retention loss',
        ].filter(Boolean),
        keywords: ['retention', 'engagement', 'activation', `d${target.day}`],
      });
      break;
    }
  }

  return {
    project,
    window: normalizeWindow(last),
    signals: sortSignals(signals).slice(0, Math.max(1, Number(input?.maxSignals) || 4)),
    meta: {
      generatedAt: new Date().toISOString(),
      source: 'analyticscli',
      starters,
      paywallReachedUsers,
      retentionCohortSize: coerceNumber(retention?.cohortSize) || 0,
      retentionReliability: retentionReliability || 'unreported',
      retentionStableIdentityShare: coerceNumber(retention?.quality?.stableIdentityShare) || 0,
    },
  };
}

function isObject(value) {
  return Boolean(value) && typeof value === 'object' && !Array.isArray(value);
}

function walk(value, visitor, pathParts = []) {
  if (Array.isArray(value)) {
    value.forEach((entry, index) => {
      walk(entry, visitor, [...pathParts, String(index)]);
    });
    return;
  }

  if (!isObject(value)) {
    visitor(value, pathParts);
    return;
  }

  for (const [key, entry] of Object.entries(value)) {
    const nextPath = [...pathParts, key];
    visitor(entry, nextPath, key);
    walk(entry, visitor, nextPath);
  }
}

function collectStatusEntries(payload) {
  const entries = [];
  walk(payload, (value, pathParts, key) => {
    if (typeof value !== 'string') return;
    const normalizedKey = String(key || '').toLowerCase();
    if (!['state', 'status', 'processingstate', 'reviewstate'].includes(normalizedKey)) {
      return;
    }
    entries.push({
      path: pathParts.join('.'),
      value: value.trim(),
    });
  });
  return entries;
}

function classifyAscStatus(value) {
  const normalized = String(value || '')
    .trim()
    .toLowerCase();
  if (!normalized) return null;

  if (
    /(reject|rejected|fail|failed|error|invalid|missing|remove|blocked|denied|cancel)/.test(
      normalized,
    )
  ) {
    return 'blocking';
  }

  if (
    /(processing|pending|waiting|prepare_for_submission|ready_for_review|in_review)/.test(
      normalized,
    )
  ) {
    return 'watch';
  }

  if (/(ready_for_sale|approved|active|available|complete|passed|ok)/.test(normalized)) {
    return 'healthy';
  }

  return null;
}

function findNumbersByCandidateKeys(payload, candidateKeys) {
  const matches = [];
  walk(payload, (value, pathParts, key) => {
    if (!key) return;
    const normalizedKey = String(key).toLowerCase();
    if (!candidateKeys.includes(normalizedKey)) return;
    const numeric = coerceNumber(value);
    if (numeric === null) return;
    matches.push({ path: pathParts.join('.'), value: numeric });
  });
  return matches;
}

function extractReviewTexts(payload) {
  const texts = [];
  walk(payload, (value, pathParts, key) => {
    if (typeof value !== 'string') return;
    const normalizedKey = String(key || '').toLowerCase();
    if (!['text', 'comment', 'summary', 'body', 'title', 'feedback'].includes(normalizedKey)) {
      return;
    }
    const trimmed = value.trim();
    if (!trimmed) return;
    texts.push({
      path: pathParts.join('.'),
      text: trimmed,
    });
  });
  return texts;
}

function rankKeywordThemes(texts) {
  const themeDefinitions = [
    {
      id: 'stability',
      area: 'stability',
      keywords: ['crash', 'crashes', 'crashing', 'freeze', 'frozen', 'bug', 'broken'],
      suggestedActions: [
        'Review recent crash and review signals together to isolate the highest-impact regression',
        'Prioritize the failing flow in the next patch release and add deterministic regression coverage',
      ],
    },
    {
      id: 'pricing',
      area: 'paywall',
      keywords: [
        'subscription',
        'subscribe',
        'paywall',
        'price',
        'pricing',
        'trial',
        'premium',
        'restore',
      ],
      suggestedActions: [
        'Clarify package differences and restore messaging in the paywall flow',
        'Use review phrasing directly to rewrite confusing pricing copy',
      ],
    },
    {
      id: 'auth',
      area: 'authentication',
      keywords: ['login', 'log in', 'sign in', 'account', 'password'],
      suggestedActions: [
        'Audit authentication entry points and reduce avoidable sign-in friction',
        'Surface clearer account state and recovery messaging in the first-session path',
      ],
    },
    {
      id: 'onboarding',
      area: 'onboarding',
      keywords: ['onboarding', 'tutorial', 'signup', 'sign up', 'permission', 'too long'],
      suggestedActions: [
        'Trim the onboarding path and move optional steps later',
        'Match onboarding copy more closely to the first-value promise from the store listing',
      ],
    },
    {
      id: 'performance',
      area: 'performance',
      keywords: ['slow', 'lag', 'loading', 'stuck', 'wait'],
      suggestedActions: [
        'Measure the slowest startup and primary interaction paths that users mention',
        'Ship a focused performance pass on the worst-loading user journeys',
      ],
    },
  ];

  return themeDefinitions
    .map((theme) => {
      let hits = 0;
      for (const entry of texts) {
        const normalized = entry.text.toLowerCase();
        for (const keyword of theme.keywords) {
          if (normalized.includes(keyword)) {
            hits += 1;
          }
        }
      }
      return { ...theme, hits };
    })
    .filter((theme) => theme.hits > 0)
    .sort((a, b) => b.hits - a.hits);
}

function collectAscMetricEntries(payload) {
  if (Array.isArray(payload?.result?.results)) return payload.result.results;
  if (Array.isArray(payload?.results)) return payload.results;
  if (Array.isArray(payload?.acquisition)) return payload.acquisition;
  if (Array.isArray(payload)) return payload;
  return [];
}

function findAscMetric(payload, measure) {
  const normalized = String(measure || '').toLowerCase();
  return collectAscMetricEntries(payload).find(
    (entry) => String(entry?.measure || '').toLowerCase() === normalized,
  ) || null;
}

function normalizeMetricData(entry) {
  return Array.isArray(entry?.data)
    ? entry.data
        .map((point) => ({
          date: String(point?.date || '').slice(0, 10),
          value: coerceNumber(point?.value),
        }))
        .filter((point) => point.date && point.value !== null)
    : [];
}

function summarizeSourceBreakdown(payload) {
  const entries = Array.isArray(payload?.result?.results)
    ? payload.result.results
    : Array.isArray(payload?.results)
      ? payload.results
      : [];
  return entries
    .map((entry) => {
      const data = Array.isArray(entry?.data) ? entry.data : [];
      const total = data.reduce(
        (sum, point) => sum + (coerceNumber(point?.pageViewUnique ?? point?.value) || 0),
        0,
      );
      return {
        key: String(entry?.group?.key || entry?.key || entry?.source || 'unknown'),
        title: String(entry?.group?.title || entry?.title || entry?.label || entry?.group?.key || 'Unknown'),
        pageViewUnique: total,
      };
    })
    .filter((entry) => entry.pageViewUnique > 0)
    .sort((a, b) => b.pageViewUnique - a.pageViewUnique);
}

function extractAscCrashBreakdowns(payload) {
  const breakdowns = Array.isArray(payload?.appUsageBreakdowns) ? payload.appUsageBreakdowns : [];
  return breakdowns
    .filter((breakdown) => String(breakdown?.measure || '').toLowerCase() === 'crashes')
    .flatMap((breakdown) =>
      Array.isArray(breakdown?.items)
        ? breakdown.items.map((item) => ({
            label: String(item?.label || item?.key || 'Unknown app version'),
            value: coerceNumber(item?.value) || 0,
          }))
        : [],
    )
    .filter((entry) => entry.value > 0)
    .sort((a, b) => b.value - a.value);
}

function totalAscCrashes(payload) {
  const breakdowns = Array.isArray(payload?.appUsageBreakdowns) ? payload.appUsageBreakdowns : [];
  const directTotal = breakdowns
    .filter((breakdown) => String(breakdown?.measure || '').toLowerCase() === 'crashes')
    .reduce((sum, breakdown) => sum + (coerceNumber(breakdown?.total) || 0), 0);
  if (directTotal > 0) return directTotal;
  return extractAscCrashBreakdowns(payload).reduce((sum, entry) => sum + entry.value, 0);
}

function isLikelyAscWebAuthMissing(warnings) {
  return warnings.some((warning) => {
    const normalized = String(warning || '').toLowerCase();
    return (
      normalized.includes('asc web auth login') ||
      normalized.includes('web session is unauthorized') ||
      normalized.includes('web session is expired')
    );
  });
}

function collectAscOverviewMetricCatalog(payload) {
  const sections = ['acquisition', 'sales', 'subscriptions'];
  const metrics = [];
  for (const section of sections) {
    const entries = Array.isArray(payload?.[section]) ? payload[section] : [];
    for (const entry of entries) {
      const measure = String(entry?.measure || '').trim();
      if (!measure) continue;
      metrics.push({
        section,
        measure,
        total: coerceNumber(entry?.total),
        previousTotal: coerceNumber(entry?.previousTotal),
        percentChange: coerceNumber(entry?.percentChange),
        type: String(entry?.type || '').trim() || null,
      });
    }
  }

  const breakdownSections = ['featureBreakdowns', 'appUsageBreakdowns'];
  for (const section of breakdownSections) {
    const entries = Array.isArray(payload?.[section]) ? payload[section] : [];
    for (const entry of entries) {
      const measure = String(entry?.measure || entry?.name || '').trim();
      if (!measure) continue;
      metrics.push({
        section,
        measure,
        total: coerceNumber(entry?.total),
        previousTotal: coerceNumber(entry?.previousTotal),
        percentChange: coerceNumber(entry?.percentChange),
        type: 'BREAKDOWN',
      });
    }
  }

  const planTimeline = Array.isArray(payload?.planTimeline) ? payload.planTimeline : [];
  for (const entry of planTimeline) {
    const totals = entry?.totals && typeof entry.totals === 'object' ? entry.totals : null;
    const measure = String(totals?.key || '').trim();
    if (!measure) continue;
    metrics.push({
      section: 'planTimeline',
      measure,
      total: coerceNumber(totals?.value),
      previousTotal: null,
      percentChange: null,
      type: String(totals?.type || 'COUNT'),
    });
  }

  const byKey = new Map();
  for (const metric of metrics) {
    const key = `${metric.section}:${metric.measure}`;
    if (!byKey.has(key)) byKey.set(key, metric);
  }
  return [...byKey.values()];
}

function formatMetricMovement(metric) {
  const total = metric.total === null ? 'unknown' : round(metric.total);
  const previous = metric.previousTotal === null ? null : ` previous ${round(metric.previousTotal)}`;
  const change = metric.percentChange === null ? null : ` change ${formatPercent(metric.percentChange)}`;
  return `${metric.section}.${metric.measure}: ${total}${previous || ''}${change || ''}`;
}

function formatPercent(value) {
  if (value === null || value === undefined || !Number.isFinite(Number(value))) return 'unknown';
  return `${round(Number(value) * 100)}%`;
}

export function buildAscSummary(input) {
  const appId = String(input?.appId || 'ASC_APP_ID').trim() || 'ASC_APP_ID';
  const statusEntries = collectStatusEntries(input?.statusPayload);
  const blockingStatuses = statusEntries.filter(
    (entry) => classifyAscStatus(entry.value) === 'blocking',
  );
  const watchStatuses = statusEntries.filter((entry) => classifyAscStatus(entry.value) === 'watch');

  const averageRatingCandidates = findNumbersByCandidateKeys(input?.ratingsPayload, [
    'averagerating',
    'averageuserrating',
    'ratingaverage',
    'avgrating',
  ]).filter((entry) => entry.value >= 0 && entry.value <= 5);
  const ratingCountCandidates = findNumbersByCandidateKeys(input?.ratingsPayload, [
    'ratingcount',
    'userratingcount',
    'ratingscount',
    'count',
  ]).filter((entry) => entry.value >= 0);

  const averageRating = averageRatingCandidates[0]?.value ?? null;
  const ratingCount = ratingCountCandidates[0]?.value ?? null;

  const reviewTexts = [
    ...extractReviewTexts(input?.reviewSummariesPayload),
    ...extractReviewTexts(input?.feedbackPayload),
  ];
  const topThemes = rankKeywordThemes(reviewTexts).slice(0, 2);
  const analyticsMetricsPayload =
    input?.batchAnalyticsPayload || input?.analyticsMetricsPayload || input?.analyticsOverviewPayload;
  const unitsMetric = findAscMetric(analyticsMetricsPayload, 'units');
  const redownloadsMetric = findAscMetric(analyticsMetricsPayload, 'redownloads');
  const conversionRateMetric = findAscMetric(
    analyticsMetricsPayload || input?.analyticsOverviewPayload,
    'conversionRate',
  );
  const crashRateMetric = findAscMetric(analyticsMetricsPayload, 'crashRate');
  const sourceBreakdown = summarizeSourceBreakdown(input?.analyticsSourcesPayload);
  const totalSourcePageViews = sourceBreakdown.reduce((sum, source) => sum + source.pageViewUnique, 0);
  const topSource = sourceBreakdown[0] || null;
  const crashBreakdown = extractAscCrashBreakdowns(input?.analyticsOverviewPayload);
  const totalCrashes = totalAscCrashes(input?.analyticsOverviewPayload);
  const analyticsWarnings = Array.isArray(input?.analyticsWarnings) ? input.analyticsWarnings : [];
  const webAuthMissing = isLikelyAscWebAuthMissing(analyticsWarnings);
  const batchReports = Array.isArray(input?.batchReports) ? input.batchReports : [];
  const analyticsAvailability = webAuthMissing
    ? 'web_auth_missing'
    : analyticsWarnings.some((warning) => String(warning).includes('403'))
    ? 'not_public_or_not_analytics_ready'
    : unitsMetric || conversionRateMetric || sourceBreakdown.length > 0 || totalCrashes > 0 || batchReports.length > 0
      ? 'available'
      : 'unknown';
  const overviewMetricCatalog = collectAscOverviewMetricCatalog(input?.analyticsOverviewPayload);
  const notableOverviewMetrics = overviewMetricCatalog
    .filter((metric) => {
      const measure = metric.measure.toLowerCase();
      if (['units', 'redownloads', 'conversionrate'].includes(measure)) return false;
      if (measure === 'crashrate' || measure === 'crashes') return false;
      const percentChange = Math.abs(coerceNumber(metric.percentChange) || 0);
      const total = Math.abs(coerceNumber(metric.total) || 0);
      return percentChange >= 0.1 || total > 0;
    })
    .sort((a, b) => {
      const aChange = Math.abs(coerceNumber(a.percentChange) || 0);
      const bChange = Math.abs(coerceNumber(b.percentChange) || 0);
      if (aChange !== bChange) return bChange - aChange;
      return Math.abs(coerceNumber(b.total) || 0) - Math.abs(coerceNumber(a.total) || 0);
    })
    .slice(0, 6);
  const nonZeroCrashRateDays = normalizeMetricData(crashRateMetric)
    .filter((point) => Number(point.value) > 0)
    .slice(-5);

  const signals = [];

  if (webAuthMissing) {
    maybePushSignal(signals, {
      id: 'asc_web_analytics_access_missing',
      title: 'ASC web analytics access needs login refresh',
      area: 'connector',
      priority: 'high',
      metric: 'asc_web_analytics_access',
      current_value: 0,
      baseline_value: 1,
      delta_percent: -100,
      evidence: analyticsWarnings.slice(0, 3),
      suggested_actions: [
        'Ask the OpenClaw user whether to enable experimental ASC web analytics for the specific missing metric that API-key batch reports could not provide',
        'If the user accepts, set ASC_WEB_APPLE_ID in the host terminal and run: asc web auth login --apple-id "$ASC_WEB_APPLE_ID"',
        'Continue using API-key ASC batch reports if the user declines or the Apple Account web session expires again',
      ],
      keywords: ['asc', 'web_analytics', 'login', 'connector'],
    });
  }

  if (blockingStatuses.length > 0) {
    maybePushSignal(signals, {
      id: 'asc_release_blockers_detected',
      title: 'App Store Connect reports blocking release states',
      area: 'release',
      priority: 'high',
      metric: 'asc_release_blockers',
      current_value: blockingStatuses.length,
      baseline_value: 0,
      delta_percent: blockingStatuses.length > 0 ? 100 : 0,
      evidence: blockingStatuses.slice(0, 5).map((entry) => `${entry.path}: ${entry.value}`),
      suggested_actions: [
        'Open the failing ASC section and resolve the blocking review, submission, or build issue',
        'Link the blocking ASC state to the corresponding release checklist item before the next submission',
      ],
      keywords: ['asc', 'review', 'submission', 'release', 'blocker'],
    });
  } else if (watchStatuses.length > 0) {
    maybePushSignal(signals, {
      id: 'asc_release_in_progress',
      title: 'App Store Connect still shows in-progress release states',
      area: 'release',
      priority: 'medium',
      metric: 'asc_release_watch_states',
      current_value: watchStatuses.length,
      baseline_value: 0,
      delta_percent: watchStatuses.length > 0 ? 100 : 0,
      evidence: watchStatuses.slice(0, 5).map((entry) => `${entry.path}: ${entry.value}`),
      suggested_actions: [
        'Monitor build processing and review transitions until they reach a terminal healthy state',
        'Avoid scheduling a coordinated release action until ASC processing has finished',
      ],
      keywords: ['asc', 'processing', 'review', 'submission'],
    });
  }

  if (averageRating !== null && ratingCount !== null && ratingCount >= 20 && averageRating < 4.2) {
    const ratingBaseline = 4.2;
    maybePushSignal(signals, {
      id: 'asc_rating_below_target',
      title: 'App Store rating is below target',
      area: 'store',
      priority: averageRating < 3.8 ? 'high' : 'medium',
      metric: 'app_store_average_rating',
      current_value: round(averageRating),
      baseline_value: ratingBaseline,
      delta_percent: computeDeltaPercent(averageRating, ratingBaseline),
      evidence: [
        `Average rating: ${averageRating.toFixed(2)} from ${Math.round(ratingCount)} ratings`,
        'Ratings came from the ASC review ratings command output',
      ],
      suggested_actions: [
        'Read recent review summaries to identify the dominant complaint before changing store copy',
        'Tie the next release notes and onboarding/paywall adjustments to the main rating complaint themes',
      ],
      keywords: ['app_store', 'rating', 'reviews', 'aso'],
    });
  }

  for (const theme of topThemes) {
    maybePushSignal(signals, {
      id: `asc_review_theme_${theme.id}`,
      title: `Store and beta feedback repeatedly mention ${theme.area} issues`,
      area: theme.area,
      priority: theme.hits >= 4 ? 'high' : 'medium',
      metric: `feedback_theme_${theme.id}`,
      current_value: theme.hits,
      baseline_value: 0,
      delta_percent: theme.hits > 0 ? 100 : 0,
      evidence: reviewTexts
        .slice(0, 3)
        .map((entry) => entry.text)
        .filter(Boolean),
      suggested_actions: theme.suggestedActions,
      keywords: ['reviews', 'feedback', theme.area, ...theme.keywords.slice(0, 3)],
    });
  }

  if (totalCrashes > 0 || nonZeroCrashRateDays.length > 0) {
    maybePushSignal(signals, {
      id: 'asc_production_crashes_detected',
      title: 'ASC reports production crashes',
      area: 'crash',
      priority: totalCrashes >= 10 || nonZeroCrashRateDays.length >= 3 ? 'high' : 'medium',
      metric: 'asc_total_crashes',
      current_value: totalCrashes,
      baseline_value: 0,
      delta_percent: totalCrashes > 0 ? 100 : null,
      evidence: [
        totalCrashes > 0 ? `ASC total crashes: ${totalCrashes}` : null,
        ...crashBreakdown
          .slice(0, 3)
          .map((entry) => `Crashes by app version: ${entry.label} = ${entry.value}`),
        ...nonZeroCrashRateDays.map((point) => `Crash rate ${point.date}: ${point.value}`),
      ].filter(Boolean),
      suggested_actions: [
        'Notify the OpenClaw user through the connected chat or social delivery channel before growth traffic is scaled',
        'Compare ASC total crashes with Sentry production issues for the same app version and date range',
        'If GitHub issue/PR write access is configured in OpenClaw, create the tracking issue or implementation PR automatically',
      ],
      keywords: ['asc', 'crash', 'production', 'sentry', 'release'],
    });
  }

  if (notableOverviewMetrics.length > 0) {
    maybePushSignal(signals, {
      id: 'asc_overview_metric_movements_detected',
      title: 'ASC overview metrics have movement worth comparing',
      area: 'analytics',
      priority: notableOverviewMetrics.some((metric) => Math.abs(coerceNumber(metric.percentChange) || 0) >= 0.5)
        ? 'medium'
        : 'low',
      metric: 'asc_overview_metrics',
      current_value: notableOverviewMetrics.length,
      baseline_value: overviewMetricCatalog.length,
      delta_percent: null,
      evidence: notableOverviewMetrics.map(formatMetricMovement),
      suggested_actions: [
        'Analyze every available ASC batch-report metric together with units, conversion, sources, AnalyticsCLI funnels, Sentry stability, and reviews before choosing a recommendation',
        'Keep financial metrics secondary unless the user asks, but still use them as validation for acquisition and conversion quality',
      ],
      keywords: ['asc', 'analytics', 'overview_metrics', 'conversion', 'handlungsempfehlung'],
    });
  }

  if (unitsMetric && coerceNumber(unitsMetric.total) !== null) {
    const units = coerceNumber(unitsMetric.total) || 0;
    const percentChange = coerceNumber(unitsMetric.percentChange);
    if (units >= 10 && percentChange !== null && percentChange <= -0.2) {
      maybePushSignal(signals, {
        id: 'asc_units_declining',
        title: 'App Store downloads are down versus the previous comparable period',
        area: 'acquisition',
        priority: percentChange <= -0.4 ? 'high' : 'medium',
        metric: 'asc_units',
        current_value: units,
        baseline_value: coerceNumber(unitsMetric.previousTotal),
        delta_percent: round(percentChange * 100),
        evidence: [
          `Units: ${units}`,
          `Previous units: ${coerceNumber(unitsMetric.previousTotal) ?? 'unknown'}`,
          `Change: ${formatPercent(percentChange)}`,
          redownloadsMetric ? `Redownloads: ${coerceNumber(redownloadsMetric.total) ?? 0}` : null,
        ].filter(Boolean),
        suggested_actions: [
          'Compare the download drop with source traffic, store impressions, page views, ranking/search changes, and recent releases',
          'Segment the recommendation into ASO, web/referrer, browse, or app-referrer work based on the source mix',
        ],
        keywords: ['asc', 'units', 'downloads', 'acquisition', 'sources'],
      });
    }
  }

  if (conversionRateMetric && coerceNumber(conversionRateMetric.total) !== null) {
    const conversionRate = coerceNumber(conversionRateMetric.total) || 0;
    const percentChange = coerceNumber(conversionRateMetric.percentChange);
    if (percentChange !== null && percentChange <= -0.1) {
      maybePushSignal(signals, {
        id: 'asc_conversion_rate_declining',
        title: 'App Store conversion rate is declining',
        area: 'conversion',
        priority: percentChange <= -0.25 ? 'high' : 'medium',
        metric: 'asc_conversion_rate',
        current_value: round(conversionRate),
        baseline_value: coerceNumber(conversionRateMetric.previousTotal),
        delta_percent: round(percentChange * 100),
        evidence: [
          `Conversion rate: ${round(conversionRate)}`,
          `Previous conversion rate: ${coerceNumber(conversionRateMetric.previousTotal) ?? 'unknown'}`,
          `Change: ${formatPercent(percentChange)}`,
          topSource ? `Top source by unique product page views: ${topSource.title} (${topSource.pageViewUnique})` : null,
        ].filter(Boolean),
        suggested_actions: [
          'Review store listing screenshots, subtitle, keywords, and price/paywall promise for the source that changed most',
          'Compare ASC conversion movement with AnalyticsCLI onboarding and paywall conversion before changing app code',
        ],
        keywords: ['asc', 'conversion', 'store_listing', 'sources', 'aso'],
      });
    }
  }

  if (topSource && totalSourcePageViews > 0) {
    const share = topSource.pageViewUnique / totalSourcePageViews;
    if (share >= 0.5 || sourceBreakdown.length >= 2) {
      maybePushSignal(signals, {
        id: 'asc_source_mix_available',
        title: 'ASC source traffic is available for acquisition recommendations',
        area: 'acquisition',
        priority: share >= 0.7 ? 'medium' : 'low',
        metric: 'asc_source_page_view_unique',
        current_value: topSource.pageViewUnique,
        baseline_value: totalSourcePageViews,
        delta_percent: round(share * 100),
        evidence: [
          `Top source: ${topSource.title} (${topSource.pageViewUnique} unique product page views)`,
          `Source mix: ${sourceBreakdown
            .slice(0, 5)
            .map((source) => `${source.title} ${source.pageViewUnique}`)
            .join(', ')}`,
          'ASC sources are product page views by unique devices, not source-level download units',
        ],
        suggested_actions: [
          'Turn the dominant source into a specific Handlungsempfehlung: Search -> ASO/keywords, Web Referrer -> landing pages/UTMs, Browse -> creative/category positioning, App Referrer -> cross-promo/deep links',
          'Compare source movement with units, redownloads, conversion rate, AnalyticsCLI activation, and Sentry crashes before recommending more spend or traffic',
        ],
        keywords: ['asc', 'sources', 'traffic', 'page_views', 'handlungsempfehlung'],
      });
    }
  }

  return {
    project: `app-store-connect:${appId}`,
    window: 'latest',
    signals: sortSignals(signals).slice(0, Math.max(1, Number(input?.maxSignals) || 4)),
    meta: {
      generatedAt: new Date().toISOString(),
      source: 'asc',
      appId,
      ratingCount: ratingCount ?? 0,
      feedbackTextCount: reviewTexts.length,
      analyticsWindow: input?.analyticsWindow || null,
      analyticsAvailability,
      analyticsWarnings,
      batchReports,
      analytics: {
        units: unitsMetric
          ? {
              total: coerceNumber(unitsMetric.total) ?? 0,
              previousTotal: coerceNumber(unitsMetric.previousTotal),
              percentChange: coerceNumber(unitsMetric.percentChange),
            }
          : null,
        redownloads: redownloadsMetric
          ? {
              total: coerceNumber(redownloadsMetric.total) ?? 0,
              previousTotal: coerceNumber(redownloadsMetric.previousTotal),
              percentChange: coerceNumber(redownloadsMetric.percentChange),
            }
          : null,
        conversionRate: conversionRateMetric
          ? {
              total: coerceNumber(conversionRateMetric.total) ?? 0,
              previousTotal: coerceNumber(conversionRateMetric.previousTotal),
              percentChange: coerceNumber(conversionRateMetric.percentChange),
            }
          : null,
        crashRate: crashRateMetric
          ? {
              total: coerceNumber(crashRateMetric.total) ?? 0,
              previousTotal: coerceNumber(crashRateMetric.previousTotal),
              percentChange: coerceNumber(crashRateMetric.percentChange),
              nonZeroDays: nonZeroCrashRateDays,
            }
          : null,
        totalCrashes,
        crashBreakdown,
        sourceBreakdown,
        overviewMetricCatalog,
      },
    },
  };
}

function extractListItems(payload) {
  if (Array.isArray(payload)) return payload;
  if (Array.isArray(payload?.items)) return payload.items;
  if (Array.isArray(payload?.data)) return payload.data;
  return [];
}

function displayName(value) {
  return String(
    value?.display_name ||
      value?.displayName ||
      value?.name ||
      value?.store_identifier ||
      value?.lookup_key ||
      value?.id ||
      '',
  ).trim();
}

function metricValueById(metrics, candidateIds) {
  const candidates = new Set(candidateIds.map((id) => String(id).toLowerCase()));
  for (const metric of metrics) {
    const id = String(metric?.id || metric?.name || '').toLowerCase();
    if (!candidates.has(id)) continue;
    const value = coerceNumber(metric?.value);
    if (value !== null) return { id, value, metric };
  }
  return null;
}

export function buildRevenueCatSummary(input) {
  const projectId =
    String(input?.projectId || input?.project?.id || 'revenuecat-project').trim() ||
    'revenuecat-project';
  const projectName = displayName(input?.project) || projectId;
  const apps = extractListItems(input?.appsPayload);
  const products = extractListItems(input?.productsPayload);
  const offerings = extractListItems(input?.offeringsPayload);
  const entitlements = extractListItems(input?.entitlementsPayload);
  const metrics = Array.isArray(input?.overviewPayload?.metrics)
    ? input.overviewPayload.metrics
    : [];
  const warnings = Array.isArray(input?.warnings) ? input.warnings.filter(Boolean) : [];

  const signals = [];
  const revenueMetric = metricValueById(metrics, [
    'revenue',
    'mrr',
    'arr',
    'new_revenue',
    'monthly_recurring_revenue',
  ]);
  const activeTrialsMetric = metricValueById(metrics, ['active_trials']);
  const activeSubscriptionsMetric = metricValueById(metrics, ['active_subscriptions', 'actives']);
  const churnMetric = metricValueById(metrics, ['churn', 'churn_rate']);

  if (revenueMetric || activeSubscriptionsMetric || activeTrialsMetric) {
    maybePushSignal(signals, {
      id: 'revenuecat_overview_metrics_available',
      title: 'RevenueCat overview metrics are connected',
      area: 'revenue',
      priority: 'medium',
      metric:
        revenueMetric?.id ||
        activeSubscriptionsMetric?.id ||
        activeTrialsMetric?.id ||
        'revenuecat_metrics',
      current_value:
        revenueMetric?.value ?? activeSubscriptionsMetric?.value ?? activeTrialsMetric?.value ?? 0,
      baseline_value: null,
      delta_percent: null,
      evidence: [
        revenueMetric
          ? `${revenueMetric.metric?.name || revenueMetric.id}: ${revenueMetric.value}`
          : null,
        activeSubscriptionsMetric
          ? `${activeSubscriptionsMetric.metric?.name || activeSubscriptionsMetric.id}: ${activeSubscriptionsMetric.value}`
          : null,
        activeTrialsMetric
          ? `${activeTrialsMetric.metric?.name || activeTrialsMetric.id}: ${activeTrialsMetric.value}`
          : null,
      ].filter(Boolean),
      suggested_actions: [
        'Compare RevenueCat movement with AnalyticsCLI paywall and purchase funnel signals',
        'Use product and entitlement metadata to verify the paid path users see in the app',
      ],
      keywords: ['revenuecat', 'revenue', 'subscription', 'metrics'],
    });
  }

  if (churnMetric && churnMetric.value > 0) {
    maybePushSignal(signals, {
      id: 'revenuecat_churn_visible',
      title: 'RevenueCat reports churn movement',
      area: 'retention',
      priority: churnMetric.value >= 10 ? 'high' : 'medium',
      metric: churnMetric.id,
      current_value: churnMetric.value,
      baseline_value: 0,
      delta_percent: 100,
      evidence: [`${churnMetric.metric?.name || churnMetric.id}: ${churnMetric.value}`],
      suggested_actions: [
        'Inspect cancellation timing against onboarding and first-week retention signals',
        'Prioritize paywall promise and subscription value alignment if churn clusters after trial or first renewal',
      ],
      keywords: ['revenuecat', 'churn', 'subscription', 'retention'],
    });
  }

  if (products.length === 0 || offerings.length === 0 || entitlements.length === 0) {
    maybePushSignal(signals, {
      id: 'revenuecat_catalog_incomplete',
      title: 'RevenueCat product catalog looks incomplete',
      area: 'paywall',
      priority: products.length === 0 || offerings.length === 0 ? 'high' : 'medium',
      metric: 'revenuecat_catalog_entities',
      current_value: products.length + offerings.length + entitlements.length,
      baseline_value: 3,
      delta_percent: computeDeltaPercent(
        products.length + offerings.length + entitlements.length,
        3,
      ),
      evidence: [
        `Products: ${products.length}`,
        `Offerings: ${offerings.length}`,
        `Entitlements: ${entitlements.length}`,
      ],
      suggested_actions: [
        'Verify the app has at least one active product, entitlement, and offering in RevenueCat',
        'Check that App Store Connect product identifiers match the RevenueCat products used by the app',
      ],
      keywords: ['revenuecat', 'products', 'offerings', 'entitlements', 'paywall'],
    });
  } else {
    maybePushSignal(signals, {
      id: 'revenuecat_catalog_summary',
      title: 'RevenueCat catalog is available for monetization analysis',
      area: 'paywall',
      priority: 'low',
      metric: 'revenuecat_products',
      current_value: products.length,
      baseline_value: 1,
      delta_percent: computeDeltaPercent(products.length, 1),
      evidence: [
        `Apps: ${apps.length}`,
        `Products: ${products.slice(0, 5).map(displayName).filter(Boolean).join(', ') || products.length}`,
        `Offerings: ${offerings.slice(0, 5).map(displayName).filter(Boolean).join(', ') || offerings.length}`,
        `Entitlements: ${entitlements.slice(0, 5).map(displayName).filter(Boolean).join(', ') || entitlements.length}`,
      ],
      suggested_actions: [
        'Use this catalog context when evaluating paywall copy, package order, and entitlement naming',
        'Cross-check product availability with ASC if users report unavailable purchases',
      ],
      keywords: ['revenuecat', 'catalog', 'products', 'offerings', 'entitlements'],
    });
  }

  return {
    project: `revenuecat:${projectId}`,
    window: 'latest',
    signals: sortSignals(signals).slice(0, Math.max(1, Number(input?.maxSignals) || 4)),
    meta: {
      generatedAt: new Date().toISOString(),
      source: 'revenuecat',
      projectId,
      projectName,
      appsCount: apps.length,
      productsCount: products.length,
      offeringsCount: offerings.length,
      entitlementsCount: entitlements.length,
      metricsCount: metrics.length,
      warnings,
    },
  };
}

function normalizeSentryIssueCount(issue) {
  return coerceNumber(issue?.count ?? issue?.events ?? issue?.eventCount ?? issue?.stats?.sum) || 0;
}

function normalizeSentryUserCount(issue) {
  return coerceNumber(issue?.userCount ?? issue?.users ?? issue?.affectedUsers) || 0;
}

function normalizeSentryIssueTitle(issue) {
  return String(issue?.title || issue?.metadata?.title || issue?.culprit || 'Untitled Sentry issue').trim();
}

function normalizeSentryPriority(issue) {
  const level = String(issue?.level || issue?.priority || '').toLowerCase();
  const events = normalizeSentryIssueCount(issue);
  const users = normalizeSentryUserCount(issue);
  if (level === 'fatal' || events >= 100 || users >= 25) return 'high';
  if (level === 'error' || events >= 20 || users >= 5) return 'medium';
  return 'low';
}

function normalizeSentryEvidence(issue, environment) {
  return [
    issue?.shortId ? `Sentry issue: ${issue.shortId}` : issue?.id ? `Sentry issue id: ${issue.id}` : null,
    issue?.permalink ? `Permalink: ${issue.permalink}` : null,
    issue?.level ? `Level: ${issue.level}` : null,
    issue?.status ? `Status: ${issue.status}` : null,
    issue?.firstSeen ? `First seen: ${issue.firstSeen}` : null,
    issue?.lastSeen ? `Last seen: ${issue.lastSeen}` : null,
    environment ? `Environment: ${environment}` : null,
    normalizeSentryIssueCount(issue) ? `Events: ${normalizeSentryIssueCount(issue)}` : null,
    normalizeSentryUserCount(issue) ? `Affected users: ${normalizeSentryUserCount(issue)}` : null,
    issue?.culprit ? `Culprit: ${issue.culprit}` : null,
  ].filter(Boolean);
}

function buildCombinedSentrySummary(input) {
  const accounts = Array.isArray(input?.accounts) ? input.accounts : [];
  const maxSignals = Math.max(1, Number(input?.maxSignals) || 5);
  const summaries = accounts
    .filter((account) => account && typeof account === 'object')
    .map((account, index) => {
      const accountId = String(account.id || account.key || account.label || `sentry_${index + 1}`).trim();
      const label = String(account.label || accountId).trim();
      const summary = buildSentrySummary({
        ...account,
        accounts: undefined,
        maxSignals: account.maxSignals || maxSignals,
      });
      return { accountId, label, summary };
    });
  const issues = summaries.flatMap(({ accountId, label, summary }) =>
    (Array.isArray(summary.issues) ? summary.issues : []).map((issue) => ({
      ...issue,
      id: `${accountId}:${issue.id}`,
      accountId,
      accountLabel: label,
      sourceProject: summary.project,
    })),
  );
  const signals = summaries
    .flatMap(({ accountId, label, summary }) =>
      (Array.isArray(summary.signals) ? summary.signals : []).map((signal) => ({
        ...signal,
        id: `${accountId}:${signal.id}`,
        evidence: [`Sentry account: ${label}`, `Sentry project: ${summary.project}`, ...(signal.evidence || [])],
        keywords: [accountId, ...(signal.keywords || [])],
      })),
    )
    .sort((a, b) => {
      const priorityDelta = priorityRank(b.priority) - priorityRank(a.priority);
      if (priorityDelta !== 0) return priorityDelta;
      return (Number(b.current_value) || 0) - (Number(a.current_value) || 0);
    })
    .slice(0, maxSignals);

  return {
    project: 'sentry:multiple',
    window: normalizeWindow(input?.last || input?.window || '24h'),
    issues,
    signals,
    meta: {
      generatedAt: new Date().toISOString(),
      source: 'sentry',
      multiAccount: true,
      accountCount: summaries.length,
      accounts: summaries.map(({ accountId, label, summary }) => ({
        id: accountId,
        label,
        project: summary.project,
        issuesReturned: summary.meta?.issuesReturned ?? 0,
        environment: summary.meta?.environment ?? null,
      })),
      issuesReturned: issues.length,
    },
  };
}

export function buildSentrySummary(input) {
  if (Array.isArray(input?.accounts)) {
    return buildCombinedSentrySummary(input);
  }

  const issues = Array.isArray(input?.issuesPayload)
    ? input.issuesPayload
    : Array.isArray(input?.issues)
      ? input.issues
      : [];
  const org = String(input?.org || input?.organization || process.env.SENTRY_ORG || '').trim();
  const project = String(input?.project || process.env.SENTRY_PROJECT || 'sentry-project').trim();
  const environment = String(input?.environment || process.env.SENTRY_ENVIRONMENT || '').trim();
  const last = String(input?.last || input?.window || '24h');
  const maxSignals = Math.max(1, Number(input?.maxSignals) || 5);
  const normalizedIssues = issues
    .filter((issue) => issue && typeof issue === 'object')
    .map((issue, index) => ({
      id: String(issue.id || issue.shortId || `sentry_${index + 1}`),
      title: normalizeSentryIssueTitle(issue),
      priority: normalizeSentryPriority(issue),
      impact:
        normalizeSentryUserCount(issue) > 0
          ? `${normalizeSentryUserCount(issue)} affected users in ${last}`
          : `Production stability issue observed in ${last}`,
      events: normalizeSentryIssueCount(issue),
      users: normalizeSentryUserCount(issue),
      area: 'crash',
      metric: 'sentry_unresolved_issues',
      stack_keywords: [
        issue.level,
        issue.type,
        issue.platform,
        issue.metadata?.type,
        issue.culprit,
      ]
        .filter(Boolean)
        .map((value) => String(value).slice(0, 80)),
      evidence: normalizeSentryEvidence(issue, environment),
      suggested_actions: [
        'Map this Sentry issue to the current production release and affected user journey',
        'Check whether the crash intersects onboarding, paywall, purchase, or first value events',
        'Fix or mitigate the highest-user-impact issue before running new growth experiments that send more traffic into the broken path',
      ],
      confidence: 'high',
    }))
    .sort((a, b) => {
      const priorityDelta = priorityRank(b.priority) - priorityRank(a.priority);
      if (priorityDelta !== 0) return priorityDelta;
      const usersDelta = (b.users || 0) - (a.users || 0);
      if (usersDelta !== 0) return usersDelta;
      return (b.events || 0) - (a.events || 0);
    })
    .slice(0, maxSignals);

  return {
    project: org ? `sentry:${org}/${project}` : `sentry:${project}`,
    window: normalizeWindow(last),
    issues: normalizedIssues,
    signals: normalizedIssues.map((issue) => ({
      id: issue.id,
      title: issue.title,
      area: issue.area,
      priority: issue.priority,
      metric: issue.metric,
      current_value: issue.events,
      evidence: issue.evidence,
      suggested_actions: issue.suggested_actions,
      keywords: issue.stack_keywords,
      confidence: issue.confidence,
    })),
    meta: {
      generatedAt: new Date().toISOString(),
      source: 'sentry',
      org,
      project,
      environment: environment || null,
      issuesReturned: normalizedIssues.length,
    },
  };
}

export async function writeJsonOutput(outPath, payload) {
  const serialized = `${JSON.stringify(payload, null, 2)}\n`;
  if (outPath) {
    const resolved = path.resolve(String(outPath));
    await fs.mkdir(path.dirname(resolved), { recursive: true });
    await fs.writeFile(resolved, serialized, 'utf8');
    return resolved;
  }

  process.stdout.write(serialized);
  return null;
}
