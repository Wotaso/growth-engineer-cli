# Growth Engineer CLI

User-facing CLI for the AnalyticsCLI Growth Engineer workflow.

Start setup from the app workspace on a VPS, Mac mini, or other host:

```bash
npx -y @analyticscli/growth-engineer@preview wizard --connectors
```

The wizard lets you choose connectors, intervals, output mode, and notification delivery. Connector secrets are collected only in the local terminal.

Common commands:

```bash
growth-engineer wizard --connectors
growth-engineer setup --config openclaw.config.json
growth-engineer preflight --config openclaw.config.json --test-connections
growth-engineer run --config openclaw.config.json
growth-engineer start --config openclaw.config.json
```

The ClawHub skill remains the agent instruction layer:

https://clawhub.ai/wotaso-dev/growth-engineer

