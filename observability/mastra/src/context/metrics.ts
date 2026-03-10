/**
 * MetricsContextImpl - Metric emission with cardinality protection.
 *
 * Provides counter, gauge, and histogram instrument creation.
 * All metrics pass through cardinality filtering before emission.
 * Context labels are snapshotted at construction time.
 */

import type {
  MetricsContext,
  Counter,
  Gauge,
  Histogram,
  ExportedMetric,
  MetricEvent,
} from '@mastra/core/observability';

import type { ObservabilityBus } from '../bus';
import type { CardinalityFilter } from '../metrics/cardinality';

export interface MetricsContextConfig {
  /** Base labels merged into every emitted metric (entity context, model, provider, serviceName, etc.) */
  labels?: Record<string, string>;

  /** Bus for event emission */
  observabilityBus: ObservabilityBus;

  /** Cardinality filter applied to all labels */
  cardinalityFilter: CardinalityFilter;
}

export class MetricsContextImpl implements MetricsContext {
  private config: MetricsContextConfig;

  /**
   * Create a metrics context. Base labels are defensively copied so
   * mutations after construction do not affect emitted metrics.
   */
  constructor(config: MetricsContextConfig) {
    this.config = {
      ...config,
      labels: config.labels ? { ...config.labels } : undefined,
    };
  }

  /** Emit a metric observation. */
  emit(name: string, value: number, labels?: Record<string, string>): void {
    this.emitInternal(name, value, labels);
  }

  /** @deprecated Use `emit()` instead. */
  counter(name: string): Counter {
    return {
      add: (value: number, additionalLabels?: Record<string, string>) => {
        this.emitInternal(name, value, additionalLabels);
      },
    };
  }

  /** @deprecated Use `emit()` instead. */
  gauge(name: string): Gauge {
    return {
      set: (value: number, additionalLabels?: Record<string, string>) => {
        this.emitInternal(name, value, additionalLabels);
      },
    };
  }

  /** @deprecated Use `emit()` instead. */
  histogram(name: string): Histogram {
    return {
      record: (value: number, additionalLabels?: Record<string, string>) => {
        this.emitInternal(name, value, additionalLabels);
      },
    };
  }

  /** Merge base + additional labels, apply cardinality filtering, and emit a MetricEvent. Non-finite values are silently dropped. */
  private emitInternal(name: string, value: number, additionalLabels?: Record<string, string>): void {
    if (!Number.isFinite(value)) return;

    const allLabels = {
      ...this.config.labels,
      ...additionalLabels,
    };
    const filteredLabels = this.config.cardinalityFilter.filterLabels(allLabels);

    const exportedMetric: ExportedMetric = {
      timestamp: new Date(),
      name,
      value,
      labels: filteredLabels,
    };

    const event: MetricEvent = { type: 'metric', metric: exportedMetric };
    this.config.observabilityBus.emit(event);
  }
}
