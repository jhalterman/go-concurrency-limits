package measurements

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestExponentialAverageMeasurement(t *testing.T) {
	t.Parallel()
	asrt := assert.New(t)
	m := NewExponentialAverageMeasurement(100, 10)

	expected := []float64{10, 10.5, 11, 11.5, 12, 12.5, 13, 13.5, 14, 14.5}
	for i := 0; i < 10; i++ {
		result, _ := m.Add(float64(i + 10))
		asrt.Equal(expected[i], result)
	}

	m.Add(100)
	asrt.InDelta(float64(16.2), m.Get(), 0.01)

	m.Update(func(value float64) float64 {
		return value - 1
	})
	asrt.InDelta(float64(15.19), m.Get(), 0.01)

	m.Reset()
	asrt.Equal(float64(0), m.Get())

	asrt.Equal(
		"ExponentialAverageMeasurement{value=0.00000, count=0, window=100, warmupWindow=10}",
		m.String(),
	)
}

func TestEWMA_AddAndValue(t *testing.T) {
	tests := []struct {
		name          string
		windowSize    int
		warmupSamples int
		values        []float64
		expected      float64
	}{
		{
			name:          "average during warmup",
			windowSize:    5,
			warmupSamples: 3,
			values:        []float64{10, 20, 30},
			expected:      20.0,
		},
		{
			name:          "average after warmup",
			windowSize:    3,
			warmupSamples: 2,
			values:        []float64{10, 20, 30, 40},
			expected:      31,
		},
		{
			name:          "average without warmup",
			windowSize:    5,
			warmupSamples: 0,
			values:        []float64{100, 50},
			expected:      39,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ma := NewExponentialAverageMeasurement(tc.windowSize, tc.warmupSamples)
			for _, v := range tc.values {
				ma.Add(v)
			}
			assert.Equal(t, tc.expected, math.Round(ma.Get()))
		})
	}
}
