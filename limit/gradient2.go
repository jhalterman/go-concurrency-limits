package limit

import (
	"fmt"
	"math"
	"sync"

	"github.com/platinummonkey/go-concurrency-limits/core"
	"github.com/platinummonkey/go-concurrency-limits/measurements"
)

// Gradient2Limit implements a concurrency limit algorithm that adjust the limits based on the gradient of change current
// average RTT and a long term exponentially smoothed average RTT. Unlike traditional congestion control algorithms we
// use average instead of minimum since RPC methods can be very bursty due to various factors such as non-homogenous
// request processing complexity as well as a wide distribution of data size.  We have also found that using minimum can
// result in an bias towards an impractically low base RTT resulting in excessive load shedding.  An exponential decay is
// applied to the base RTT so that the value is kept stable yet is allowed to adapt to long term changes in latency
// characteristics.
//
// The core algorithm re-calculates the limit every sampling window (ex. 1 second) using the formula
//     // Calculate the gradient limiting to the range [0.5, 1.0] to filter outliers
//     gradient = max(0.5, min(1.0, longtermRtt / currentRtt));
//
//     // Calculate the new limit by applying the gradient and allowing for some queuing
//     newLimit = gradient * currentLimit + queueSize;
//
//     // Update the limit using a smoothing factor (default 0.2)
//     newLimit = currentLimit * (1-smoothing) + newLimit * smoothing
//
// The limit can be in one of three main states
//
// 1. Steady state
//    In this state the average RTT is very stable and the current measurement whipsaws around this value, sometimes
//    reducing the limit, sometimes increasing it.
// 2. Transition from steady state to load
//    In this state either the RPS to latency has spiked. The gradient is < 1.0 due to a growing request queue that
//    cannot be handled by the system. Excessive requests and rejected due to the low limit. The baseline RTT grows using
//    exponential decay but lags the current measurement, which keeps the gradient < 1.0 and limit low.
// 3. Transition from load to steady state
//    In this state the system goes back to steady state after a prolonged period of excessive load.  Requests aren't
//    rejected and the sample RTT remains low. During this state the long term RTT may take some time to go back to
//    normal and could potentially be several multiples higher than the current RTT.
type Gradient2Limit struct {
	// Estimated concurrency limit based on our algorithm
	estimatedLimit float64
	// Tracks a measurement of the short time, and more volatile, RTT meant to represent the current system latency
	shortRTT core.MeasurementInterface
	// Tracks a measurement of the long term, less volatile, RTT meant to represent the baseline latency.  When the system
	// is under load this number is expect to trend higher.
	longRTT core.MeasurementInterface
	// Maximum allowed limit providing an upper bound failsafe
	maxLimit int
	// Minimum allowed limit providing a lower bound failsafe
	minLimit                int
	queueSizeFunc           func(limit int) int
	smoothing               float64
	commonSampler           *core.CommonMetricSampler
	longRTTSampleListener   core.MetricSampleListener
	shortRTTSampleListener  core.MetricSampleListener
	queueSizeSampleListener core.MetricSampleListener

	mu        sync.RWMutex
	listeners []core.LimitChangeListener
	logger    Logger
	registry  core.MetricRegistry

	stablePeriods   int // Number of consecutive periods with stable latency
	stableThreshold int // Number of stable periods needed before a growth spurt
	growthStep      int // Amount to increase limit in each spurt

	inflightHistory  []float64
	rttHistory       []float64
	covarianceWindow int
	covariance       *CovarianceCalculator
}

// NewDefaultGradient2Limit create a default Gradient2Limit
func NewDefaultGradient2Limit(
	name string,
	logger Logger,
	registry core.MetricRegistry,
	tags ...string,
) *Gradient2Limit {
	l, _ := NewGradient2Limit(
		name,
		20,
		200,
		20,
		func(limit int) int { return 4 },
		0.2,
		600,
		logger,
		registry,
		tags...,
	)
	return l
}

// NewGradient2Limit will create a new Gradient2Limit
// @param initialLimit: Initial limit used by the limiter.
// @param maxConcurrency: Maximum allowable concurrency.  Any estimated concurrency will be capped.
// @param minLimit: Minimum concurrency limit allowed.  The minimum helps prevent the algorithm from adjust the limit
//                  too far down.  Note that this limit is not desirable when use as backpressure for batch apps.
// @param queueSizeFunc: Function to dynamically determine the amount the estimated limit can grow while
//                       latencies remain low as a function of the current limit.
// @param smoothing: Smoothing factor to limit how aggressively the estimated limit can shrink when queuing has been
//                   detected.  Value of 0.0 to 1.0 where 1.0 means the limit is completely replicated by the new estimate.
// @param longWindow: long time window for the exponential avg recordings.
// @param registry: metric registry to publish metrics
func NewGradient2Limit(
	name string,
	initialLimit int, // Initial limit used by the limiter
	maxConurrency int,
	minLimit int,
	queueSizeFunc func(limit int) int,
	smoothing float64,
	longWindow int,
	logger Logger,
	registry core.MetricRegistry,
	tags ...string,
) (*Gradient2Limit, error) {
	if smoothing > 1.0 || smoothing < 0 {
		smoothing = 0.2
	}
	if maxConurrency <= 0 {
		maxConurrency = 1000
	}
	if minLimit <= 0 {
		minLimit = 4
	}
	if longWindow < 0 {
		longWindow = 100
	}
	if logger == nil {
		logger = NoopLimitLogger{}
	}
	if registry == nil {
		registry = core.EmptyMetricRegistryInstance
	}

	if minLimit > maxConurrency {
		return nil, fmt.Errorf("minLimit must be <= maxConcurrency")
	}
	if queueSizeFunc == nil {
		// set the default
		queueSizeFunc = func(limit int) int { return 4 }
	}
	if initialLimit <= 0 {
		initialLimit = 4
	}

	l := &Gradient2Limit{
		estimatedLimit:          float64(initialLimit),
		maxLimit:                maxConurrency,
		minLimit:                minLimit,
		queueSizeFunc:           queueSizeFunc,
		smoothing:               smoothing,
		shortRTT:                &measurements.SingleMeasurement{},
		longRTT:                 measurements.NewExponentialAverageMeasurement(longWindow, 10),
		longRTTSampleListener:   registry.RegisterDistribution(core.PrefixMetricWithName(core.MetricMinRTT, name), tags...),
		shortRTTSampleListener:  registry.RegisterDistribution(core.PrefixMetricWithName(core.MetricWindowMinRTT, name), tags...),
		queueSizeSampleListener: registry.RegisterDistribution(core.PrefixMetricWithName(core.MetricWindowQueueSize, name), tags...),
		listeners:               make([]core.LimitChangeListener, 0),
		logger:                  logger,
		registry:                registry,

		stableThreshold: 3,
		growthStep:      5,

		covarianceWindow: 20,
		covariance:       NewCovarianceCalculator(50),
	}

	l.commonSampler = core.NewCommonMetricSamplerOrNil(registry, l, name, tags...)

	return l, nil
}

// EstimatedLimit returns the current estimated limit.
func (l *Gradient2Limit) EstimatedLimit() int {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return int(l.estimatedLimit)
}

// NotifyOnChange will register a callback to receive notification whenever the limit is updated to a new value.
func (l *Gradient2Limit) NotifyOnChange(consumer core.LimitChangeListener) {
	l.mu.Lock()
	l.listeners = append(l.listeners, consumer)
	l.mu.Unlock()
}

// notifyListeners will call the callbacks on limit changes
func (l *Gradient2Limit) notifyListeners(newLimit int) {
	for _, listener := range l.listeners {
		listener(newLimit)
	}
}

// OnSample the concurrency limit using a new rtt sample.
func (l *Gradient2Limit) OnSample(startTime int64, rtt int64, inFlight int, didDrop bool) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.commonSampler.Sample(rtt, inFlight, didDrop)
	shortRTT, _ := l.shortRTT.Add(float64(rtt))
	longRTT, _ := l.longRTT.Add(float64(rtt))

	l.shortRTTSampleListener.AddSample(shortRTT)
	l.longRTTSampleListener.AddSample(longRTT)

	// If the long RTT is substantially larger than the short RTT then reduce the long RTT measurement.
	// This can happen when latency returns to normal after a prolonged prior of excessive load.  Reducing the
	// long RTT without waiting for the exponential smoothing helps bring the system back to steady state.
	// if (longRTT / shortRTT) > 2 {
	// 	l.longRTT.Update(func(value float64) float64 {
	// 		return value * 0.9
	// 	})
	// }

	// Rtt could be higher than rtt_noload because of smoothing rtt noload updates
	// so set to 1.0 to indicate no queuing.  Otherwise calculate the slope and don't
	// allow it to be reduced by more than half to avoid aggressive load-shedding due to
	// outliers.
	// including a tolerance of 1.5 just means gradient is more likely to be 1, so if we include a queue, the limit increases faster
	initialGradient := math.Max(0.5, math.Min(1.5, longRTT/shortRTT))

	// Covariance calculation
	l.inflightHistory = append(l.inflightHistory, float64(inFlight))
	l.rttHistory = append(l.rttHistory, shortRTT)

	if len(l.inflightHistory) > l.covarianceWindow {
		l.inflightHistory = l.inflightHistory[1:]
		l.rttHistory = l.rttHistory[1:]
	}

	// covariance := calculateCovariance(l.inflightHistory, l.rttHistory)
	// covariance := l.covariance.Update(float64(inFlight), shortRTT)
	covariance := calculateCovariance(l.inflightHistory, l.rttHistory)


	// Use covariance to adjust gradient
	gradient := initialGradient
	if covariance < 0 {
		// Allow limit to increase if limit has correlated with stable/improved latency
		gradient = min(1.5, gradient*1.1)
	} else if covariance > 0 {
		// Decrease limit more aggressively if limit correlates with worse latency
		gradient = max(.5, gradient*0.9)
	}

	queueSize := 0
	// if gradient >= 1.0 {
	// Don't grow the limit if we not necessary
	if float64(inFlight) > l.estimatedLimit*10 {
		l.logger.Debugf("old limit=%0.2f, inflight=%d, covariance=%0.2f, shortRTT=%0.2f ms, longRTT=%0.2f ms",
			l.estimatedLimit, inFlight, covariance, shortRTT/1e6, longRTT/1e6)
		return
	}
	queueSize = l.queueSizeFunc(int(l.estimatedLimit))
	queueSize = 0
	l.queueSizeSampleListener.AddSample(float64(queueSize))

	newLimit := l.estimatedLimit*gradient + float64(queueSize)
	newLimit = l.estimatedLimit*(1-l.smoothing) + newLimit*l.smoothing
	newLimit = math.Max(float64(l.minLimit), math.Min(float64(l.maxLimit), newLimit))

	l.logger.Debugf("new limit=%0.2f, inflight=%d, covariance=%0.2f, shortRTT=%0.2f ms, longRTT=%0.2f ms, queueSize=%d, initialGradient=%0.2f, gradient=%0.2f",
		newLimit, inFlight, covariance, shortRTT/1e6, longRTT/1e6, queueSize, initialGradient, gradient)

	l.estimatedLimit = newLimit
	l.notifyListeners(int(l.estimatedLimit))
}

type CovarianceCalculator struct {
	sumX       float64
	sumY       float64
	sumXY      float64
	sumX2      float64
	sumY2      float64
	n          int
	windowSize int
	samples    []Sample
}

// Sample represents an individual (x, y) data pair.
type Sample struct {
	x float64
	y float64
}

// NewCovarianceCalculator initializes a new CovarianceCalculator with a specified window size.
func NewCovarianceCalculator(windowSize int) *CovarianceCalculator {
	return &CovarianceCalculator{
		windowSize: windowSize,
		samples:    make([]Sample, 0, windowSize),
	}
}

// Update updates the covariance with a new pair of (x, y) values, maintaining the window size.
func (c *CovarianceCalculator) Update(x, y float64) float64 {
	// Add the new sample
	c.samples = append(c.samples, Sample{x, y})
	c.sumX += x
	c.sumY += y
	c.sumXY += x * y
	c.sumX2 += x * x
	c.sumY2 += y * y
	c.n++

	// If the window size exceeds, remove the oldest sample
	if c.n > c.windowSize {
		oldSample := c.samples[0]
		c.samples = c.samples[1:] // Remove the oldest sample
		c.sumX -= oldSample.x
		c.sumY -= oldSample.y
		c.sumXY -= oldSample.x * oldSample.y
		c.sumX2 -= oldSample.x * oldSample.x
		c.sumY2 -= oldSample.y * oldSample.y
		c.n-- // Decrease count to maintain window size
	}

	// Calculate covariance
	covariance := (c.sumXY - (c.sumX * c.sumY / float64(c.n))) / float64(c.n)
	return covariance
}

func calculateCovariance(x, y []float64) float64 {
	if len(x) != len(y) || len(x) < 2 {
		return 0
	}

	var sumX, sumY, sumXY, sumX2, sumY2 float64
	n := float64(len(x))

	for i := 0; i < len(x); i++ {
		sumX += x[i]
		sumY += y[i]
		sumXY += x[i] * y[i]
		sumX2 += x[i] * x[i]
		sumY2 += y[i] * y[i]
	}

	covariance := (sumXY - (sumX * sumY / n)) / n
	return covariance
}

func (l *Gradient2Limit) String() string {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return fmt.Sprintf("Gradient2Limit{limit=%d}", int(l.estimatedLimit))
}
