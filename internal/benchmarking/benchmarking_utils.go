package benchmarking

import (
	"runtime/metrics"
	"testing"
)

type mutexMetricsCollector struct {
	baseValue float64
}

func NewMutexMetricsCollector() *mutexMetricsCollector {
	return &mutexMetricsCollector{baseValue: getCurrentTotalWaits()}
}

func (m *mutexMetricsCollector) Reset() {
	m.baseValue = getCurrentTotalWaits()
}

func (m *mutexMetricsCollector) Report(b *testing.B) {
	value := getCurrentTotalWaits() - m.baseValue
	b.ReportMetric(value/float64(b.N), "mutex_wait_ns/op")
}

func getCurrentTotalWaits() float64 {
	sample := make([]metrics.Sample, 1)
	sample[0].Name = "/sync/mutex/wait/total:seconds"
	metrics.Read(sample)
	mutexTotalWaits := sample[0].Value.Float64()
	return mutexTotalWaits * 1000000
}
