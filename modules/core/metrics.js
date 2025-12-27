const METRIC_NAME_REGEX = /^[a-zA-Z_:][a-zA-Z0-9_:]*$/;
const MAX_LABEL_COMBINATIONS = Number(process.env.METRICS_MAX_LABEL_COMBINATIONS) || 10000;
const MAX_HISTOGRAM_VALUES = Number(process.env.METRICS_MAX_HISTOGRAM_VALUES) || 1000;
const MAX_TOTAL_HISTOGRAM_MEMORY = Number(process.env.METRICS_MAX_HISTOGRAM_MEMORY) || 100000;
const METRIC_TTL_MS = Number(process.env.METRICS_TTL_MS) || 24 * 60 * 60 * 1000;
const HISTOGRAM_BUCKETS = [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10];
const SUMMARY_QUANTILES = [0.5, 0.9, 0.95, 0.99];
const SUMMARY_MAX_AGE_MS = 600000;
class Metrics {
  constructor() {
    this.counters = new Map();
    this.gauges = new Map();
    this.histograms = new Map();
    this.summaries = new Map();
    this.metadataMap = new Map();
    this.startTime = Date.now();
    this.updateLock = false;
    this.totalHistogramValues = 0;
    this.labelCombinationCount = 0;
  }
  _acquireLock() {
    while (this.updateLock) {}
    this.updateLock = true;
  }
  _releaseLock() {
    this.updateLock = false;
  }
  _validateMetricName(name) {
    if (!name || typeof name !== 'string') return false;
    return METRIC_NAME_REGEX.test(name);
  }
  _escapeLabelValue(value) {
    if (value == null) return '';
    return String(value)
      .replace(/\\/g, '\\\\')
      .replace(/\n/g, '\\n')
      .replace(/"/g, '\\"');
  }
  _makeKey(name, labels) {
    if (!labels || Object.keys(labels).length === 0) return name;
    const labelStr = Object.entries(labels)
      .sort(([a], [b]) => a.localeCompare(b))
      .map(([k, v]) => `${k}="${this._escapeLabelValue(v)}"`)
      .join(',');
    return `${name}{${labelStr}}`;
  }
  _checkLabelLimit() {
    if (this.labelCombinationCount >= MAX_LABEL_COMBINATIONS) {
      return false;
    }
    return true;
  }
  _expireOldMetrics() {
    const now = Date.now();
    const expireGauges = [];
    for (const [key, gauge] of this.gauges.entries()) {
      if (now - gauge.timestamp > METRIC_TTL_MS) {
        expireGauges.push(key);
      }
    }
    expireGauges.forEach(key => {
      this.gauges.delete(key);
      this.labelCombinationCount = Math.max(0, this.labelCombinationCount - 1);
    });
  }
  setMetadata(name, help, type) {
    if (!this._validateMetricName(name)) return;
    this.metadataMap.set(name, { help: help || '', type: type || 'untyped' });
  }
  incrementCounter(name, value = 1, labels = {}) {
    if (!this._validateMetricName(name)) return;
    this._acquireLock();
    try {
      const key = this._makeKey(name, labels);
      if (!this.counters.has(key)) {
        if (!this._checkLabelLimit()) {
          this._releaseLock();
          return;
        }
        this.counters.set(key, { name, labels, value: 0 });
        this.labelCombinationCount++;
        if (!this.metadataMap.has(name)) {
          this.setMetadata(name, `Counter ${name}`, 'counter');
        }
      }
      const counter = this.counters.get(key);
      counter.value += value;
    } finally {
      this._releaseLock();
    }
  }
  setGauge(name, value, labels = {}) {
    if (!this._validateMetricName(name)) return;
    this._acquireLock();
    try {
      const key = this._makeKey(name, labels);
      if (!this.gauges.has(key)) {
        if (!this._checkLabelLimit()) {
          this._releaseLock();
          return;
        }
        this.labelCombinationCount++;
        if (!this.metadataMap.has(name)) {
          this.setMetadata(name, `Gauge ${name}`, 'gauge');
        }
      }
      this.gauges.set(key, { name, labels, value, timestamp: Date.now() });
    } finally {
      this._releaseLock();
    }
  }
  recordHistogram(name, value, labels = {}) {
    if (!this._validateMetricName(name)) return;
    this._acquireLock();
    try {
      const key = this._makeKey(name, labels);
      if (!this.histograms.has(key)) {
        if (!this._checkLabelLimit()) {
          this._releaseLock();
          return;
        }
        this.histograms.set(key, { name, labels, values: [], buckets: {} });
        this.labelCombinationCount++;
        if (!this.metadataMap.has(name)) {
          this.setMetadata(name, `Histogram ${name}`, 'histogram');
        }
      }
      const hist = this.histograms.get(key);
      if (this.totalHistogramValues >= MAX_TOTAL_HISTOGRAM_MEMORY) {
        this._releaseLock();
        return;
      }
      if (hist.values.length >= MAX_HISTOGRAM_VALUES) {
        hist.values.shift();
        this.totalHistogramValues = Math.max(0, this.totalHistogramValues - 1);
      }
      hist.values.push(value);
      this.totalHistogramValues++;
      for (const bucket of HISTOGRAM_BUCKETS) {
        if (value <= bucket) {
          hist.buckets[bucket] = (hist.buckets[bucket] || 0) + 1;
        }
      }
      hist.buckets['+Inf'] = (hist.buckets['+Inf'] || 0) + 1;
    } finally {
      this._releaseLock();
    }
  }
  recordSummary(name, value, labels = {}) {
    if (!this._validateMetricName(name)) return;
    this._acquireLock();
    try {
      const key = this._makeKey(name, labels);
      if (!this.summaries.has(key)) {
        if (!this._checkLabelLimit()) {
          this._releaseLock();
          return;
        }
        this.summaries.set(key, { name, labels, values: [], timestamps: [] });
        this.labelCombinationCount++;
        if (!this.metadataMap.has(name)) {
          this.setMetadata(name, `Summary ${name}`, 'summary');
        }
      }
      const summary = this.summaries.get(key);
      const now = Date.now();
      summary.values.push(value);
      summary.timestamps.push(now);
      const cutoff = now - SUMMARY_MAX_AGE_MS;
      while (summary.timestamps.length > 0 && summary.timestamps[0] < cutoff) {
        summary.timestamps.shift();
        summary.values.shift();
      }
      if (summary.values.length > MAX_HISTOGRAM_VALUES) {
        summary.values.shift();
        summary.timestamps.shift();
      }
    } finally {
      this._releaseLock();
    }
  }
  onStateTransition(from, to, _data) {
    this.incrementCounter('cluster_state_transitions_total', 1, { from, to });
  }
  _calculatePercentile(sortedValues, percentile) {
    if (sortedValues.length === 0) return 0;
    const index = Math.ceil(sortedValues.length * percentile) - 1;
    return sortedValues[Math.max(0, index)];
  }
  _calculateQuantile(sortedValues, quantile) {
    if (sortedValues.length === 0) return 0;
    const pos = (sortedValues.length - 1) * quantile;
    const base = Math.floor(pos);
    const rest = pos - base;
    if (sortedValues[base + 1] !== undefined) {
      return sortedValues[base] + rest * (sortedValues[base + 1] - sortedValues[base]);
    }
    return sortedValues[base];
  }
  toPrometheus() {
    this._expireOldMetrics();
    const lines = [];
    const processedTypes = new Set();
    const metricsByName = new Map();
    for (const counter of this.counters.values()) {
      if (!metricsByName.has(counter.name)) {
        metricsByName.set(counter.name, []);
      }
      metricsByName.get(counter.name).push({ type: 'counter', data: counter });
    }
    for (const gauge of this.gauges.values()) {
      if (!metricsByName.has(gauge.name)) {
        metricsByName.set(gauge.name, []);
      }
      metricsByName.get(gauge.name).push({ type: 'gauge', data: gauge });
    }
    for (const hist of this.histograms.values()) {
      if (!metricsByName.has(hist.name)) {
        metricsByName.set(hist.name, []);
      }
      metricsByName.get(hist.name).push({ type: 'histogram', data: hist });
    }
    for (const summary of this.summaries.values()) {
      if (!metricsByName.has(summary.name)) {
        metricsByName.set(summary.name, []);
      }
      metricsByName.get(summary.name).push({ type: 'summary', data: summary });
    }
    for (const [metricName, metrics] of metricsByName.entries()) {
      if (metrics.length === 0) continue;
      const metricType = metrics[0].type;
      const metadata = this.metadataMap.get(metricName);
      if (metadata && metadata.help) {
        lines.push(`# HELP ${metricName} ${metadata.help}`);
      }
      lines.push(`# TYPE ${metricName} ${metricType}`);
      processedTypes.add(metricName);
      for (const metric of metrics) {
        if (metric.type === 'counter') {
          const counter = metric.data;
          const labelStr = Object.entries(counter.labels || {})
            .map(([k, v]) => `${k}="${this._escapeLabelValue(v)}"`)
            .join(',');
          const nameWithLabels = labelStr ? `${counter.name}{${labelStr}}` : counter.name;
          lines.push(`${nameWithLabels} ${counter.value}`);
        } else if (metric.type === 'gauge') {
          const gauge = metric.data;
          const labelStr = Object.entries(gauge.labels || {})
            .map(([k, v]) => `${k}="${this._escapeLabelValue(v)}"`)
            .join(',');
          const nameWithLabels = labelStr ? `${gauge.name}{${labelStr}}` : gauge.name;
          lines.push(`${nameWithLabels} ${gauge.value}`);
        } else if (metric.type === 'histogram') {
          const hist = metric.data;
          const values = hist.values;
          if (values.length === 0) continue;
          const sum = values.reduce((acc, v) => acc + v, 0);
          const count = values.length;
          const labelStr = Object.entries(hist.labels || {})
            .map(([k, v]) => `${k}="${this._escapeLabelValue(v)}"`)
            .join(',');
          const baseLabels = labelStr ? `${labelStr},` : '';
          for (const bucket of HISTOGRAM_BUCKETS) {
            const bucketCount = hist.buckets[bucket] || 0;
            lines.push(`${hist.name}_bucket{${baseLabels}le="${bucket}"} ${bucketCount}`);
          }
          lines.push(`${hist.name}_bucket{${baseLabels}le="+Inf"} ${count}`);
          const sumName = labelStr ? `${hist.name}_sum{${labelStr}}` : `${hist.name}_sum`;
          const countName = labelStr ? `${hist.name}_count{${labelStr}}` : `${hist.name}_count`;
          lines.push(`${sumName} ${sum}`);
          lines.push(`${countName} ${count}`);
        } else if (metric.type === 'summary') {
          const summary = metric.data;
          const values = [...summary.values].sort((a, b) => a - b);
          if (values.length === 0) continue;
          const sum = values.reduce((acc, v) => acc + v, 0);
          const count = values.length;
          const labelStr = Object.entries(summary.labels || {})
            .map(([k, v]) => `${k}="${this._escapeLabelValue(v)}"`)
            .join(',');
          const baseLabels = labelStr ? `${labelStr},` : '';
          for (const quantile of SUMMARY_QUANTILES) {
            const value = this._calculateQuantile(values, quantile);
            lines.push(`${summary.name}{${baseLabels}quantile="${quantile}"} ${value}`);
          }
          const sumName = labelStr ? `${summary.name}_sum{${labelStr}}` : `${summary.name}_sum`;
          const countName = labelStr ? `${summary.name}_count{${labelStr}}` : `${summary.name}_count`;
          lines.push(`${sumName} ${sum}`);
          lines.push(`${countName} ${count}`);
        }
      }
    }
    if (!processedTypes.has('process_uptime_seconds')) {
      lines.push('# HELP process_uptime_seconds Process uptime in seconds');
      lines.push('# TYPE process_uptime_seconds gauge');
    }
    lines.push(`process_uptime_seconds ${Math.floor((Date.now() - this.startTime) / 1000)}`);
    return lines.join('\n') + '\n';
  }
  getMetrics() {
    this._expireOldMetrics();
    const histograms = [];
    for (const hist of this.histograms.values()) {
      const sortedValues = [...hist.values].sort((a, b) => a - b);
      histograms.push({
        name: hist.name,
        labels: hist.labels,
        count: hist.values.length,
        sum: hist.values.reduce((acc, v) => acc + v, 0),
        avg: hist.values.length > 0 ? hist.values.reduce((acc, v) => acc + v, 0) / hist.values.length : 0,
        min: hist.values.length > 0 ? Math.min(...hist.values) : 0,
        max: hist.values.length > 0 ? Math.max(...hist.values) : 0,
        p50: this._calculatePercentile(sortedValues, 0.5),
        p90: this._calculatePercentile(sortedValues, 0.9),
        p95: this._calculatePercentile(sortedValues, 0.95),
        p99: this._calculatePercentile(sortedValues, 0.99),
      });
    }
    const summaries = [];
    for (const summary of this.summaries.values()) {
      const sortedValues = [...summary.values].sort((a, b) => a - b);
      summaries.push({
        name: summary.name,
        labels: summary.labels,
        count: summary.values.length,
        sum: summary.values.reduce((acc, v) => acc + v, 0),
        quantiles: {
          0.5: this._calculateQuantile(sortedValues, 0.5),
          0.9: this._calculateQuantile(sortedValues, 0.9),
          0.95: this._calculateQuantile(sortedValues, 0.95),
          0.99: this._calculateQuantile(sortedValues, 0.99),
        },
      });
    }
    return {
      uptime_seconds: Math.floor((Date.now() - this.startTime) / 1000),
      counters: Array.from(this.counters.values()),
      gauges: Array.from(this.gauges.values()),
      histograms,
      summaries,
      stats: {
        labelCombinations: this.labelCombinationCount,
        maxLabelCombinations: MAX_LABEL_COMBINATIONS,
        totalHistogramValues: this.totalHistogramValues,
        maxHistogramMemory: MAX_TOTAL_HISTOGRAM_MEMORY,
      },
    };
  }
  reset() {
    this._acquireLock();
    try {
      this.counters.clear();
      this.gauges.clear();
      this.histograms.clear();
      this.summaries.clear();
      this.metadataMap.clear();
      this.startTime = Date.now();
      this.totalHistogramValues = 0;
      this.labelCombinationCount = 0;
    } finally {
      this._releaseLock();
    }
  }
}
const globalMetrics = new Metrics();
export default globalMetrics;
export { Metrics };
