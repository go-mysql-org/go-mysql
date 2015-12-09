package replication

import (
	"testing"
)

func TestDecodeDecimal(t *testing.T) {
	testcases := []struct {
		Data        []byte
		Precision   int
		Decimals    int
		Expected    float64
		ExpectedPos int
		ExpectedErr error
	}{
		{
			Data:        []byte{127, 255, 255, 248, 99, 241},
			Precision:   13,
			Decimals:    2,
			Expected:    float64(-1948.14),
			ExpectedPos: 6,
		},
		{
			Data:        []byte{128, 0, 0, 67, 222, 96},
			Precision:   13,
			Decimals:    2,
			Expected:    float64(17374.96),
			ExpectedPos: 6,
		},
		{
			Data:        []byte{128, 0, 0, 0, 0, 0},
			Precision:   13,
			Decimals:    2,
			Expected:    float64(0.0),
			ExpectedPos: 6,
		},
		{
			Data:        []byte{127, 255, 255, 253, 205, 197},
			Precision:   13,
			Decimals:    2,
			Expected:    float64(-562.58),
			ExpectedPos: 6,
		},
		{
			Data:        []byte{127, 255, 255, 241, 140, 254},
			Precision:   13,
			Decimals:    2,
			Expected:    float64(-3699.01),
			ExpectedPos: 6,
		},
	}
	for i, tc := range testcases {
		aFloat, aPos, aErr := decodeDecimal(tc.Data, tc.Precision, tc.Decimals)
		if aErr != tc.ExpectedErr {
			t.Errorf("For test %v : Did not get expected error(%v), got %v instead.", i, tc.ExpectedErr, aErr)
			continue
		}
		if tc.ExpectedErr != nil {
			continue
		}
		if aPos != tc.ExpectedPos {
			t.Errorf("For test %v : Did not get expected Pos(%v), got %v instead.", i, tc.ExpectedPos, aPos)
		}
		if aFloat != tc.Expected {
			t.Errorf("For test %v : Did not get expected Value(%v), got %v instead.", i, tc.Expected, aFloat)
		}
	}

}
