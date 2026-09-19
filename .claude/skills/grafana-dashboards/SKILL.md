---
name: grafana-dashboards
description: Create and manage production Grafana dashboards for real-time visualization of system and application metrics. Use when building monitoring dashboards, visualizing metrics, or creating operational observability interfaces.
---

# Grafana Dashboards

Create and manage production-ready Grafana dashboards for comprehensive system observability.

## Purpose

Design effective Grafana dashboards for monitoring applications, infrastructure, and business metrics.

## When to Use

- Visualize Prometheus metrics
- Create custom dashboards
- Implement SLO dashboards
- Monitor infrastructure
- Track business KPIs

## In this repo (deCDN) — read first

Generic k8s / `node_exporter` / `http_requests_total` examples below are patterns, not
targets. This repo exports ~200 of its own `decdn_*` series and already ships a
four-dashboard suite, alerting rules, and logs and traces to go with them.

**The files:**

- `monitoring/grafana-dashboard.json` — "deCDN — fleet overview", uid `decdn-poc-overview`.
  Fleet status, delivery funnel, slash safety, logs and traces.
- `monitoring/dashboard-delivery.json` — uid `decdn-delivery`. Serve leg, paying pull leg,
  cache, origin, warming.
- `monitoring/dashboard-chain.json` — uid `decdn-chain`. Watcher liveness, registries,
  seller and buyer payments.
- `monitoring/dashboard-node.json` — uid `decdn-node`. Single-node drilldown: host,
  process, iroh transport, DHT and probe, logs, traces.
- `monitoring/prometheus-alerts.yml` — three groups: `decdn-slash-safety`, `decdn-liveness`,
  `decdn-delivery`.

Add a row to an existing dashboard before starting a fifth one.

**All three signals are live.** Metrics reach Grafana Cloud Prometheus as `job="decdn-node"`;
logs reach Loki as `{service_name="decdn-node", unit="decdn-node.service"}`; traces
reach Tempo as `resource.service.name="decdn-node"`. The log streams keep
`job="integrations/node_exporter"` because Grafana Cloud's Linux Server integration joins logs to
host metrics on `job` + `instance`, so never select daemon logs by `job`. Dashboard log panels
select on `unit` + `instance`. Grafana Alloy on each host does all three — its config lives in
`internal-devops`, not here.

**Metric names are gated by tests, not by promtool.** No CI workflow references `monitoring/`.
The only thing tying these files to the code is the test module in `crates/node/src/metrics.rs`:

- `monitoring_selectors_are_exported` — sweeps **every `.yml` and `.json`** in `monitoring/`,
  so a new dashboard is gated the day it lands. Every `decdn_*` name in each file must resolve
  to a series the exporter emits, and each file must yield at least 15 distinct names.
  Whole-line `#` comments are stripped first, so a retired name can still be explained in prose
  — but JSON has no comments, so a name in a panel `description` is checked like any other.
  The exported set includes the `decdn_iroh_*` sub-registry, registered in the test from an
  `EndpointMetrics::default()` rather than a live socket.
- `alert_and_dashboard_selectors_match_the_exported_series` — asserts the `expr:` / `"expr":`
  *query line* for `decdn_probe_hold_unavailable_total{reason="exhausted"}` in
  `prometheus-alerts.yml` and `grafana-dashboard.json` specifically, not the file, so prose in
  `description` / `legendFormat` cannot mask a rename. Keep that panel on the overview, and
  keep `reason` first inside the braces.
- the `adr/appendix-observability.md` registry gate — every row with Status `live` must be
  exported; `planned` rows are skipped.

Run them after any edit to any file under `monitoring/`:

```bash
cargo nextest run -p decdn-node metrics
```

**Rules that follow from that:**

1. Any exported series may be used, not only the ones `adr/appendix-observability.md` lists —
   the registry is a curated subset of roughly 180 series and says so. But check the **Status**
   column before using a name you found *there*: a `planned` row emits nothing, so a panel
   built on it is permanently empty. The exporter is the source of truth; the appendix is a
   view of it.
2. Build the exported-name set from an actual scrape, not from `# TYPE` lines:
   the gate compares against sample lines.
3. `reason`-style splits are usually **sibling counters, not labels** — one unlabeled counter per
   reason. `decdn_probe_hold_unavailable{reason}` is the single documented exception (#1475).
   Do not write a `by (reason)` query against a series that has no such label.
4. A name that resolves can still sit at a permanent zero because nothing increments it. The gate
   checks the name, not the wiring.
5. Adding a panel or alert that names a *new* metric is a code change too — the metric has to
   exist in `crates/node/src/metrics.rs` first.

**`rate()` and `increase()` drop `__name__`.** This is the trap in every family panel here:

```promql
# does not evaluate — the per-reason series collapse to identical label sets
sum by (__name__) (rate({__name__=~"decdn_serve_stream_rejected_.+_total"}[5m]))
```

Prometheus answers `vector cannot contain metrics with the same labelset`, and wrapping it in
`sum by (instance)` does not help — the duplicate exists before the aggregation runs. So:

- **Rate panels name each series explicitly**, one target per metric. Verbose, and correct, and
  it puts every name under the gate; the regex form scans as the wildcard token
  `decdn_serve_stream_rejected_` and is skipped entirely.
- **Instant panels over a family use `label_replace` on the raw selector**, before any operator
  strips the name:

```promql
time() - label_replace(
  {__name__=~"decdn_.+_watcher_last_tick_timestamp_seconds"} > 0,
  "watcher", "$1", "__name__", "decdn_(.+)_watcher_last_tick_timestamp_seconds")
```

**No histograms exist.** The exporter registers none, so there is no `_bucket` series anywhere
and `histogram_quantile` is not available for anything. The only latency distribution in the
stack is TraceQL: `{...} | quantile_over_time(duration, .5, .95, .99)`.

**Logs need `| json`.** The daemon writes tracing JSON to stdout and journald stamps every line
priority `info`, so Loki's `level` stream label and `detected_level` are both useless. Parse the
body instead, naming the fields so nothing collides with a stream label:

```logql
{unit="decdn-node.service", instance=~"$instance"}
  | json lvl="level", tgt="target", msg="fields.message"
  | lvl=~"WARN|ERROR"
```

**Publishing.** `GRAFANA_SERVICE_ACCOUNT_TOKEN` is in the environment; dashboards go to
`POST /api/dashboards/db` with `overwrite: true`, into folder uid `dfykix7ln0gsgc` ("deCDN").
The Grafana Cloud **ruler proxy rejects writes** (`400 bad request data`) even for a valid
group, so alerts are provisioned as Grafana-managed rules through
`/api/v1/provisioning/alert-rules` instead.

## Dashboard Design Principles

### 1. Hierarchy of Information

```
┌─────────────────────────────────────┐
│  Critical Metrics (Big Numbers)     │
├─────────────────────────────────────┤
│  Key Trends (Time Series)           │
├─────────────────────────────────────┤
│  Detailed Metrics (Tables/Heatmaps) │
└─────────────────────────────────────┘
```

### 2. RED Method (Services)

- **Rate** - Requests per second
- **Errors** - Error rate
- **Duration** - Latency/response time

### 3. USE Method (Resources)

- **Utilization** - % time resource is busy
- **Saturation** - Queue length/wait time
- **Errors** - Error count

## Dashboard Structure

### API Monitoring Dashboard

```json
{
  "dashboard": {
    "title": "API Monitoring",
    "tags": ["api", "production"],
    "timezone": "browser",
    "refresh": "30s",
    "panels": [
      {
        "title": "Request Rate",
        "type": "graph",
        "targets": [
          {
            "expr": "sum(rate(http_requests_total[5m])) by (service)",
            "legendFormat": "{{service}}"
          }
        ],
        "gridPos": { "x": 0, "y": 0, "w": 12, "h": 8 }
      },
      {
        "title": "Error Rate %",
        "type": "graph",
        "targets": [
          {
            "expr": "(sum(rate(http_requests_total{status=~\"5..\"}[5m])) / sum(rate(http_requests_total[5m]))) * 100",
            "legendFormat": "Error Rate"
          }
        ],
        "alert": {
          "conditions": [
            {
              "evaluator": { "params": [5], "type": "gt" },
              "operator": { "type": "and" },
              "query": { "params": ["A", "5m", "now"] },
              "type": "query"
            }
          ]
        },
        "gridPos": { "x": 12, "y": 0, "w": 12, "h": 8 }
      },
      {
        "title": "P95 Latency",
        "type": "graph",
        "targets": [
          {
            "expr": "histogram_quantile(0.95, sum(rate(http_request_duration_seconds_bucket[5m])) by (le, service))",
            "legendFormat": "{{service}}"
          }
        ],
        "gridPos": { "x": 0, "y": 8, "w": 24, "h": 8 }
      }
    ]
  }
}
```

**Reference:** in this repo, see `monitoring/grafana-dashboard.json` and the three
dashboards beside it.

## Panel Types

### 1. Stat Panel (Single Value)

```json
{
  "type": "stat",
  "title": "Total Requests",
  "targets": [
    {
      "expr": "sum(http_requests_total)"
    }
  ],
  "options": {
    "reduceOptions": {
      "values": false,
      "calcs": ["lastNotNull"]
    },
    "orientation": "auto",
    "textMode": "auto",
    "colorMode": "value"
  },
  "fieldConfig": {
    "defaults": {
      "thresholds": {
        "mode": "absolute",
        "steps": [
          { "value": 0, "color": "green" },
          { "value": 80, "color": "yellow" },
          { "value": 90, "color": "red" }
        ]
      }
    }
  }
}
```

### 2. Time Series Graph

```json
{
  "type": "graph",
  "title": "CPU Usage",
  "targets": [
    {
      "expr": "100 - (avg by (instance) (rate(node_cpu_seconds_total{mode=\"idle\"}[5m])) * 100)"
    }
  ],
  "yaxes": [
    { "format": "percent", "max": 100, "min": 0 },
    { "format": "short" }
  ]
}
```

### 3. Table Panel

```json
{
  "type": "table",
  "title": "Service Status",
  "targets": [
    {
      "expr": "up",
      "format": "table",
      "instant": true
    }
  ],
  "transformations": [
    {
      "id": "organize",
      "options": {
        "excludeByName": { "Time": true },
        "indexByName": {},
        "renameByName": {
          "instance": "Instance",
          "job": "Service",
          "Value": "Status"
        }
      }
    }
  ]
}
```

### 4. Heatmap

```json
{
  "type": "heatmap",
  "title": "Latency Heatmap",
  "targets": [
    {
      "expr": "sum(rate(http_request_duration_seconds_bucket[5m])) by (le)",
      "format": "heatmap"
    }
  ],
  "dataFormat": "tsbuckets",
  "yAxis": {
    "format": "s"
  }
}
```

## Variables

### Query Variables

```json
{
  "templating": {
    "list": [
      {
        "name": "namespace",
        "type": "query",
        "datasource": "Prometheus",
        "query": "label_values(kube_pod_info, namespace)",
        "refresh": 1,
        "multi": false
      },
      {
        "name": "service",
        "type": "query",
        "datasource": "Prometheus",
        "query": "label_values(kube_service_info{namespace=\"$namespace\"}, service)",
        "refresh": 1,
        "multi": true
      }
    ]
  }
}
```

### Use Variables in Queries

```
sum(rate(http_requests_total{namespace="$namespace", service=~"$service"}[5m]))
```

## Alerts in Dashboards

```json
{
  "alert": {
    "name": "High Error Rate",
    "conditions": [
      {
        "evaluator": {
          "params": [5],
          "type": "gt"
        },
        "operator": { "type": "and" },
        "query": {
          "params": ["A", "5m", "now"]
        },
        "reducer": { "type": "avg" },
        "type": "query"
      }
    ],
    "executionErrorState": "alerting",
    "for": "5m",
    "frequency": "1m",
    "message": "Error rate is above 5%",
    "noDataState": "no_data",
    "notifications": [{ "uid": "slack-channel" }]
  }
}
```

## Dashboard Provisioning

**dashboards.yml:**

```yaml
apiVersion: 1

providers:
  - name: "default"
    orgId: 1
    folder: "General"
    type: file
    disableDeletion: false
    updateIntervalSeconds: 10
    allowUiUpdates: true
    options:
      path: /etc/grafana/dashboards
```

## Common Dashboard Patterns

### Infrastructure Dashboard

**Key Panels:**

- CPU utilization per node
- Memory usage per node
- Disk I/O
- Network traffic
- Pod count by namespace
- Node status

**Reference:** in this repo, host panels live in `monitoring/dashboard-node.json`, which
reads `node_exporter` series under the same `instance` and `region` labels the `decdn_*`
series carry — one node selector drives both.

### Database Dashboard

**Key Panels:**

- Queries per second
- Connection pool usage
- Query latency (P50, P95, P99)
- Active connections
- Database size
- Replication lag
- Slow queries

**Reference:** not applicable in this repo — deCDN has no database tier.

### Application Dashboard

**Key Panels:**

- Request rate
- Error rate
- Response time (percentiles)
- Active users/sessions
- Cache hit rate
- Queue length

## Best Practices

1. **Start with templates** (Grafana community dashboards)
2. **Use consistent naming** for panels and variables
3. **Group related metrics** in rows
4. **Set appropriate time ranges** (default: Last 6 hours)
5. **Use variables** for flexibility
6. **Add panel descriptions** for context
7. **Configure units** correctly
8. **Set meaningful thresholds** for colors
9. **Use consistent colors** across dashboards
10. **Test with different time ranges**

## Dashboard as Code

### Terraform Provisioning

```hcl
resource "grafana_dashboard" "api_monitoring" {
  config_json = file("${path.module}/dashboards/api-monitoring.json")
  folder      = grafana_folder.monitoring.id
}

resource "grafana_folder" "monitoring" {
  title = "Production Monitoring"
}
```

### Ansible Provisioning

```yaml
- name: Deploy Grafana dashboards
  copy:
    src: "{{ item }}"
    dest: /etc/grafana/dashboards/
  with_fileglob:
    - "dashboards/*.json"
  notify: restart grafana
```

## Related references

- `adr/appendix-observability.md` — canonical metric registry (name, type, tier, Status).
- `crates/node/src/metrics.rs` — the exporter and the tests that gate `monitoring/`.
- `docs/runbook.md` — operational responses the alerts point at.
