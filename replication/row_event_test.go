package replication

import (
	"fmt"

	. "gopkg.in/check.v1"
)

type testDecodeSuite struct{}

var _ = Suite(&testDecodeSuite{})

type decodeDecimalChecker struct {
	*CheckerInfo
}

func (_ *decodeDecimalChecker) Check(params []interface{}, names []string) (bool, string) {
	var test int
	val := struct {
		Value  float64
		Pos    int
		Err    error
		EValue float64
		EPos   int
		EErr   error
	}{}

	for i, name := range names {
		switch name {
		case "obtainedValue":
			val.Value, _ = params[i].(float64)
		case "obtainedPos":
			val.Pos, _ = params[i].(int)
		case "obtainedErr":
			val.Err, _ = params[i].(error)
		case "expectedValue":
			val.EValue, _ = params[i].(float64)
		case "expectedPos":
			val.EPos, _ = params[i].(int)
		case "expectedErr":
			val.EErr, _ = params[i].(error)
		case "caseNumber":
			test = params[i].(int)
		}
	}
	errorMsgFmt := fmt.Sprintf("For Test %v: ", test) + "Did not get expected %v(%v), got %v instead."
	if val.Err != val.EErr {
		return false, fmt.Sprintf(errorMsgFmt, "error", val.EErr, val.Err)
	}
	if val.Pos != val.EPos {
		return false, fmt.Sprintf(errorMsgFmt, "position", val.EPos, val.Pos)
	}
	if val.Value != val.EValue {
		return false, fmt.Sprintf(errorMsgFmt, "value", val.EValue, val.Value)
	}
	return true, ""
}

var DecodeDecimalsEquals = &decodeDecimalChecker{
	&CheckerInfo{Name: "Equals", Params: []string{"obtainedValue", "obtainedPos", "obtainedErr", "expectedValue", "expectedPos", "expectedErr", "caseNumber"}},
}

func (_ *testDecodeSuite) TestDecodeDecimal(c *C) {
	// _PLACEHOLDER_ := 0
	testcases := []struct {
		Data        []byte
		Precision   int
		Decimals    int
		Expected    float64
		ExpectedPos int
		ExpectedErr error
	}{
		/*
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
		*/
		// These are cases from the mysql test cases
		// id = 1
		/*
			mysql> select * from decodedecimal;
			+----+--------+-------+-----------+-------------+-------------+----------------+-------------------+-----------------------+---------------------+---------------------------------+---------------------------------+------+-------+
			| id | v4_2   | v5_0  | v7_3      | v10_2       | v10_3       | v13_2          | v15_14            | v20_10                | v30_5               | v30_20                          | v30_25                          | prec | scale |
			+----+--------+-------+-----------+-------------+-------------+----------------+-------------------+-----------------------+---------------------+---------------------------------+---------------------------------+------+-------+
			|  1 | -10.55 |   -11 |   -10.550 |      -10.55 |     -10.550 |         -10.55 | -9.99999999999999 |        -10.5500000000 |           -10.55000 |        -10.55000000000000000000 |   -10.5500000000000000000000000 |    4 |     2 |
			|  2 |   0.01 |     0 |     0.012 |        0.01 |       0.012 |           0.01 |  0.01234567890123 |          0.0123456789 |             0.01235 |          0.01234567890123456789 |     0.0123456789012345678912345 |   30 |    25 |
			|  3 |  99.99 | 12345 |  9999.999 |    12345.00 |   12345.000 |       12345.00 |  9.99999999999999 |      12345.0000000000 |         12345.00000 |      12345.00000000000000000000 | 12345.0000000000000000000000000 |    5 |     0 |
			|  4 |  99.99 | 12345 |  9999.999 |    12345.00 |   12345.000 |       12345.00 |  9.99999999999999 |      12345.0000000000 |         12345.00000 |      12345.00000000000000000000 | 12345.0000000000000000000000000 |   10 |     3 |
			|  5 |  99.99 |   123 |   123.450 |      123.45 |     123.450 |         123.45 |  9.99999999999999 |        123.4500000000 |           123.45000 |        123.45000000000000000000 |   123.4500000000000000000000000 |   10 |     3 |
			|  6 | -99.99 |  -123 |  -123.450 |     -123.45 |    -123.450 |        -123.45 | -9.99999999999999 |       -123.4500000000 |          -123.45000 |       -123.45000000000000000000 |  -123.4500000000000000000000000 |   20 |    10 |
			|  7 |   0.00 |     0 |     0.000 |        0.00 |       0.000 |           0.00 |  0.00012345000099 |          0.0001234500 |             0.00012 |          0.00012345000098765000 |     0.0001234500009876500000000 |   15 |    14 |
			|  8 |   0.00 |     0 |     0.000 |        0.00 |       0.000 |           0.00 |  0.00012345000099 |          0.0001234500 |             0.00012 |          0.00012345000098765000 |     0.0001234500009876500000000 |   22 |    20 |
			|  9 |   0.12 |     0 |     0.123 |        0.12 |       0.123 |           0.12 |  0.12345000098765 |          0.1234500010 |             0.12345 |          0.12345000098765000000 |     0.1234500009876500000000000 |   30 |    20 |
			| 10 |   0.00 |     0 |     0.000 |        0.00 |       0.000 |           0.00 | -0.00000001234500 |         -0.0000000123 |             0.00000 |         -0.00000001234500009877 |    -0.0000000123450000987650000 |   30 |    20 |
			| 11 |  99.99 | 99999 |  9999.999 | 99999999.99 | 9999999.999 | 99999999999.99 |  9.99999999999999 | 9999999999.9999999999 | 1234500009876.50000 | 9999999999.99999999999999999999 | 99999.9999999999999999999999999 |   30 |     5 |
			| 12 |  99.99 | 99999 |  9999.999 | 99999999.99 | 9999999.999 |   111111111.11 |  9.99999999999999 |  111111111.1100000000 |     111111111.11000 |  111111111.11000000000000000000 | 99999.9999999999999999999999999 |   10 |     2 |
			| 13 |   0.01 |     0 |     0.010 |        0.01 |       0.010 |           0.01 |  0.01000000000000 |          0.0100000000 |             0.01000 |          0.01000000000000000000 |     0.0100000000000000000000000 |    7 |     3 |
			| 14 |  99.99 |   123 |   123.400 |      123.40 |     123.400 |         123.40 |  9.99999999999999 |        123.4000000000 |           123.40000 |        123.40000000000000000000 |   123.4000000000000000000000000 |   10 |     2 |
			| 15 | -99.99 |  -563 |  -562.580 |     -562.58 |    -562.580 |        -562.58 | -9.99999999999999 |       -562.5800000000 |          -562.58000 |       -562.58000000000000000000 |  -562.5800000000000000000000000 |   13 |     2 |
			| 16 | -99.99 | -3699 | -3699.010 |    -3699.01 |   -3699.010 |       -3699.01 | -9.99999999999999 |      -3699.0100000000 |         -3699.01000 |      -3699.01000000000000000000 | -3699.0100000000000000000000000 |   13 |     2 |
			| 17 | -99.99 | -1948 | -1948.140 |    -1948.14 |   -1948.140 |       -1948.14 | -9.99999999999999 |      -1948.1400000000 |         -1948.14000 |      -1948.14000000000000000000 | -1948.1400000000000000000000000 |   13 |     2 |
			+----+--------+-------+-----------+-------------+-------------+----------------+-------------------+-----------------------+---------------------+---------------------------------+---------------------------------+------+-------+
		*/
		{
			Data:        []byte{117, 200, 127, 255},
			Precision:   4,
			Decimals:    2,
			Expected:    float64(-10.55),
			ExpectedPos: 2,
		},
		{
			Data:        []byte{127, 255, 244, 127, 245},
			Precision:   5,
			Decimals:    0,
			Expected:    float64(-11),
			ExpectedPos: 3,
		},
		{
			Data:        []byte{127, 245, 253, 217, 127, 255},
			Precision:   7,
			Decimals:    3,
			Expected:    float64(-10.550),
			ExpectedPos: 4,
		},
		{
			Data:        []byte{127, 255, 255, 245, 200, 127, 255},
			Precision:   10,
			Decimals:    2,
			Expected:    float64(-10.55),
			ExpectedPos: 5,
		},
		{
			Data:        []byte{127, 255, 255, 245, 253, 217, 127, 255},
			Precision:   10,
			Decimals:    3,
			Expected:    float64(-10.550),
			ExpectedPos: 6,
		},
		{
			Data:        []byte{127, 255, 255, 255, 245, 200, 118, 196},
			Precision:   13,
			Decimals:    2,
			Expected:    float64(-10.55),
			ExpectedPos: 6,
		},
		{
			Data:        []byte{118, 196, 101, 54, 0, 254, 121, 96, 127, 255},
			Precision:   15,
			Decimals:    14,
			Expected:    float64(-9.99999999999999),
			ExpectedPos: 8,
		},
		{
			Data:        []byte{127, 255, 255, 255, 245, 223, 55, 170, 127, 255, 127, 255},
			Precision:   20,
			Decimals:    10,
			Expected:    float64(-10.5500000000),
			ExpectedPos: 10,
		},
		{
			Data:        []byte{127, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 245, 255, 41, 39, 127, 255},
			Precision:   30,
			Decimals:    5,
			Expected:    float64(-10.55000),
			ExpectedPos: 15,
		},
		{
			Data:        []byte{127, 255, 255, 255, 245, 223, 55, 170, 127, 255, 255, 255, 255, 255, 127, 255},
			Precision:   30,
			Decimals:    20,
			Expected:    float64(-10.55000000000000000000),
			ExpectedPos: 14,
		},
		{
			Data:        []byte{127, 255, 245, 223, 55, 170, 127, 255, 255, 255, 255, 255, 255, 255, 255, 4, 0},
			Precision:   30,
			Decimals:    25,
			Expected:    float64(-10.5500000000000000000000000),
			ExpectedPos: 15,
		},
		{
			Data:        []byte{128, 1, 128, 0},
			Precision:   4,
			Decimals:    2,
			Expected:    float64(0.01),
			ExpectedPos: 2,
		},
		{
			Data:        []byte{128, 0, 0, 128, 0},
			Precision:   5,
			Decimals:    0,
			Expected:    float64(0),
			ExpectedPos: 3,
		},
		{
			Data:        []byte{128, 0, 0, 12, 128, 0},
			Precision:   7,
			Decimals:    3,
			Expected:    float64(0.012),
			ExpectedPos: 4,
		},
		{
			Data:        []byte{128, 0, 0, 0, 1, 128, 0},
			Precision:   10,
			Decimals:    2,
			Expected:    float64(0.01),
			ExpectedPos: 5,
		},
		{
			Data:        []byte{128, 0, 0, 0, 0, 12, 128, 0},
			Precision:   10,
			Decimals:    3,
			Expected:    float64(0.012),
			ExpectedPos: 6,
		},
		{
			Data:        []byte{128, 0, 0, 0, 0, 1, 128, 0},
			Precision:   13,
			Decimals:    2,
			Expected:    float64(0.01),
			ExpectedPos: 6,
		},
		{
			Data:        []byte{128, 0, 188, 97, 78, 1, 96, 11, 128, 0},
			Precision:   15,
			Decimals:    14,
			Expected:    float64(0.01234567890123),
			ExpectedPos: 8,
		},
		{
			Data:        []byte{128, 0, 0, 0, 0, 0, 188, 97, 78, 9, 128, 0},
			Precision:   20,
			Decimals:    10,
			Expected:    float64(0.0123456789),
			ExpectedPos: 10,
		},
		{
			Data:        []byte{128, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 4, 211, 128, 0},
			Precision:   30,
			Decimals:    5,
			Expected:    float64(0.01235),
			ExpectedPos: 15,
		},
		{
			Data:        []byte{128, 0, 0, 0, 0, 0, 188, 97, 78, 53, 183, 191, 135, 89, 128, 0},
			Precision:   30,
			Decimals:    20,
			Expected:    float64(0.01234567890123456789),
			ExpectedPos: 14,
		},
		{
			Data:        []byte{128, 0, 0, 0, 188, 97, 78, 53, 183, 191, 135, 0, 135, 253, 217, 30, 0},
			Precision:   30,
			Decimals:    25,
			Expected:    float64(0.0123456789012345678912345),
			ExpectedPos: 15,
		},
		{
			Data:        []byte{227, 99, 128, 48},
			Precision:   4,
			Decimals:    2,
			Expected:    float64(99.99),
			ExpectedPos: 2,
		},
		{
			Data:        []byte{128, 48, 57, 167, 15},
			Precision:   5,
			Decimals:    0,
			Expected:    float64(12345),
			ExpectedPos: 3,
		},
		{
			Data:        []byte{167, 15, 3, 231, 128, 0},
			Precision:   7,
			Decimals:    3,
			Expected:    float64(9999.999),
			ExpectedPos: 4,
		},
		{
			Data:        []byte{128, 0, 48, 57, 0, 128, 0},
			Precision:   10,
			Decimals:    2,
			Expected:    float64(12345.00),
			ExpectedPos: 5,
		},
		{
			Data:        []byte{128, 0, 48, 57, 0, 0, 128, 0},
			Precision:   10,
			Decimals:    3,
			Expected:    float64(12345.000),
			ExpectedPos: 6,
		},
		{
			Data:        []byte{128, 0, 0, 48, 57, 0, 137, 59},
			Precision:   13,
			Decimals:    2,
			Expected:    float64(12345.00),
			ExpectedPos: 6,
		},
		{
			Data:        []byte{137, 59, 154, 201, 255, 1, 134, 159, 128, 0},
			Precision:   15,
			Decimals:    14,
			Expected:    float64(9.99999999999999),
			ExpectedPos: 8,
		},
		{
			Data:        []byte{128, 0, 0, 48, 57, 0, 0, 0, 0, 0, 128, 0},
			Precision:   20,
			Decimals:    10,
			Expected:    float64(12345.0000000000),
			ExpectedPos: 10,
		},
		{
			Data:        []byte{128, 0, 0, 0, 0, 0, 0, 0, 0, 0, 48, 57, 0, 0, 0, 128, 0},
			Precision:   30,
			Decimals:    5,
			Expected:    float64(12345.00000),
			ExpectedPos: 15,
		},
		{
			Data:        []byte{128, 0, 0, 48, 57, 0, 0, 0, 0, 0, 0, 0, 0, 0, 128, 48},
			Precision:   30,
			Decimals:    20,
			Expected:    float64(12345.00000000000000000000),
			ExpectedPos: 14,
		},
		{
			Data:        []byte{128, 48, 57, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 5, 0},
			Precision:   30,
			Decimals:    25,
			Expected:    float64(12345.0000000000000000000000000),
			ExpectedPos: 15,
		},
		{
			Data:        []byte{227, 99, 128, 48},
			Precision:   4,
			Decimals:    2,
			Expected:    float64(99.99),
			ExpectedPos: 2,
		},
		{
			Data:        []byte{128, 48, 57, 167, 15},
			Precision:   5,
			Decimals:    0,
			Expected:    float64(12345),
			ExpectedPos: 3,
		},
		{
			Data:        []byte{167, 15, 3, 231, 128, 0},
			Precision:   7,
			Decimals:    3,
			Expected:    float64(9999.999),
			ExpectedPos: 4,
		},
		{
			Data:        []byte{128, 0, 48, 57, 0, 128, 0},
			Precision:   10,
			Decimals:    2,
			Expected:    float64(12345.00),
			ExpectedPos: 5,
		},
		{
			Data:        []byte{128, 0, 48, 57, 0, 0, 128, 0},
			Precision:   10,
			Decimals:    3,
			Expected:    float64(12345.000),
			ExpectedPos: 6,
		},
		{
			Data:        []byte{128, 0, 0, 48, 57, 0, 137, 59},
			Precision:   13,
			Decimals:    2,
			Expected:    float64(12345.00),
			ExpectedPos: 6,
		},
		{
			Data:        []byte{137, 59, 154, 201, 255, 1, 134, 159, 128, 0},
			Precision:   15,
			Decimals:    14,
			Expected:    float64(9.99999999999999),
			ExpectedPos: 8,
		},
		{
			Data:        []byte{128, 0, 0, 48, 57, 0, 0, 0, 0, 0, 128, 0},
			Precision:   20,
			Decimals:    10,
			Expected:    float64(12345.0000000000),
			ExpectedPos: 10,
		},
		{
			Data:        []byte{128, 0, 0, 0, 0, 0, 0, 0, 0, 0, 48, 57, 0, 0, 0, 128, 0},
			Precision:   30,
			Decimals:    5,
			Expected:    float64(12345.00000),
			ExpectedPos: 15,
		},
		{
			Data:        []byte{128, 0, 0, 48, 57, 0, 0, 0, 0, 0, 0, 0, 0, 0, 128, 48},
			Precision:   30,
			Decimals:    20,
			Expected:    float64(12345.00000000000000000000),
			ExpectedPos: 14,
		},
		{
			Data:        []byte{128, 48, 57, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 10, 0},
			Precision:   30,
			Decimals:    25,
			Expected:    float64(12345.0000000000000000000000000),
			ExpectedPos: 15,
		},
		{
			Data:        []byte{227, 99, 128, 0},
			Precision:   4,
			Decimals:    2,
			Expected:    float64(99.99),
			ExpectedPos: 2,
		},
		{
			Data:        []byte{128, 0, 123, 128, 123},
			Precision:   5,
			Decimals:    0,
			Expected:    float64(123),
			ExpectedPos: 3,
		},
		{
			Data:        []byte{128, 123, 1, 194, 128, 0},
			Precision:   7,
			Decimals:    3,
			Expected:    float64(123.450),
			ExpectedPos: 4,
		},
		{
			Data:        []byte{128, 0, 0, 123, 45, 128, 0},
			Precision:   10,
			Decimals:    2,
			Expected:    float64(123.45),
			ExpectedPos: 5,
		},
		{
			Data:        []byte{128, 0, 0, 123, 1, 194, 128, 0},
			Precision:   10,
			Decimals:    3,
			Expected:    float64(123.450),
			ExpectedPos: 6,
		},
		{
			Data:        []byte{128, 0, 0, 0, 123, 45, 137, 59},
			Precision:   13,
			Decimals:    2,
			Expected:    float64(123.45),
			ExpectedPos: 6,
		},
		{
			Data:        []byte{137, 59, 154, 201, 255, 1, 134, 159, 128, 0},
			Precision:   15,
			Decimals:    14,
			Expected:    float64(9.99999999999999),
			ExpectedPos: 8,
		},
		{
			Data:        []byte{128, 0, 0, 0, 123, 26, 210, 116, 128, 0, 128, 0},
			Precision:   20,
			Decimals:    10,
			Expected:    float64(123.4500000000),
			ExpectedPos: 10,
		},
		{
			Data:        []byte{128, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 123, 0, 175, 200, 128, 0},
			Precision:   30,
			Decimals:    5,
			Expected:    float64(123.45000),
			ExpectedPos: 15,
		},
		{
			Data:        []byte{128, 0, 0, 0, 123, 26, 210, 116, 128, 0, 0, 0, 0, 0, 128, 0},
			Precision:   30,
			Decimals:    20,
			Expected:    float64(123.45000000000000000000),
			ExpectedPos: 14,
		},
		{
			Data:        []byte{128, 0, 123, 26, 210, 116, 128, 0, 0, 0, 0, 0, 0, 0, 0, 10, 0},
			Precision:   30,
			Decimals:    25,
			Expected:    float64(123.4500000000000000000000000),
			ExpectedPos: 15,
		},
		{
			Data:        []byte{28, 156, 127, 255},
			Precision:   4,
			Decimals:    2,
			Expected:    float64(-99.99),
			ExpectedPos: 2,
		},
		{
			Data:        []byte{127, 255, 132, 127, 132},
			Precision:   5,
			Decimals:    0,
			Expected:    float64(-123),
			ExpectedPos: 3,
		},
		{
			Data:        []byte{127, 132, 254, 61, 127, 255},
			Precision:   7,
			Decimals:    3,
			Expected:    float64(-123.450),
			ExpectedPos: 4,
		},
		{
			Data:        []byte{127, 255, 255, 132, 210, 127, 255},
			Precision:   10,
			Decimals:    2,
			Expected:    float64(-123.45),
			ExpectedPos: 5,
		},
		{
			Data:        []byte{127, 255, 255, 132, 254, 61, 127, 255},
			Precision:   10,
			Decimals:    3,
			Expected:    float64(-123.450),
			ExpectedPos: 6,
		},
		{
			Data:        []byte{127, 255, 255, 255, 132, 210, 118, 196},
			Precision:   13,
			Decimals:    2,
			Expected:    float64(-123.45),
			ExpectedPos: 6,
		},
		{
			Data:        []byte{118, 196, 101, 54, 0, 254, 121, 96, 127, 255},
			Precision:   15,
			Decimals:    14,
			Expected:    float64(-9.99999999999999),
			ExpectedPos: 8,
		},
		{
			Data:        []byte{127, 255, 255, 255, 132, 229, 45, 139, 127, 255, 127, 255},
			Precision:   20,
			Decimals:    10,
			Expected:    float64(-123.4500000000),
			ExpectedPos: 10,
		},
		{
			Data:        []byte{127, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 132, 255, 80, 55, 127, 255},
			Precision:   30,
			Decimals:    5,
			Expected:    float64(-123.45000),
			ExpectedPos: 15,
		},
		{
			Data:        []byte{127, 255, 255, 255, 132, 229, 45, 139, 127, 255, 255, 255, 255, 255, 127, 255},
			Precision:   30,
			Decimals:    20,
			Expected:    float64(-123.45000000000000000000),
			ExpectedPos: 14,
		},
		{
			Data:        []byte{127, 255, 132, 229, 45, 139, 127, 255, 255, 255, 255, 255, 255, 255, 255, 20, 0},
			Precision:   30,
			Decimals:    25,
			Expected:    float64(-123.4500000000000000000000000),
			ExpectedPos: 15,
		},
		{
			Data:        []byte{128, 0, 128, 0},
			Precision:   4,
			Decimals:    2,
			Expected:    float64(0.00),
			ExpectedPos: 2,
		},
		{
			Data:        []byte{128, 0, 0, 128, 0},
			Precision:   5,
			Decimals:    0,
			Expected:    float64(0),
			ExpectedPos: 3,
		},
		{
			Data:        []byte{128, 0, 0, 0, 128, 0},
			Precision:   7,
			Decimals:    3,
			Expected:    float64(0.000),
			ExpectedPos: 4,
		},
		{
			Data:        []byte{128, 0, 0, 0, 0, 128, 0},
			Precision:   10,
			Decimals:    2,
			Expected:    float64(0.00),
			ExpectedPos: 5,
		},
		{
			Data:        []byte{128, 0, 0, 0, 0, 0, 128, 0},
			Precision:   10,
			Decimals:    3,
			Expected:    float64(0.000),
			ExpectedPos: 6,
		},
		{
			Data:        []byte{128, 0, 0, 0, 0, 0, 128, 0},
			Precision:   13,
			Decimals:    2,
			Expected:    float64(0.00),
			ExpectedPos: 6,
		},
		{
			Data:        []byte{128, 0, 1, 226, 58, 0, 0, 99, 128, 0},
			Precision:   15,
			Decimals:    14,
			Expected:    float64(0.00012345000099),
			ExpectedPos: 8,
		},
		{
			Data:        []byte{128, 0, 0, 0, 0, 0, 1, 226, 58, 0, 128, 0},
			Precision:   20,
			Decimals:    10,
			Expected:    float64(0.0001234500),
			ExpectedPos: 10,
		},
		{
			Data:        []byte{128, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 12, 128, 0},
			Precision:   30,
			Decimals:    5,
			Expected:    float64(0.00012),
			ExpectedPos: 15,
		},
		{
			Data:        []byte{128, 0, 0, 0, 0, 0, 1, 226, 58, 0, 15, 18, 2, 0, 128, 0},
			Precision:   30,
			Decimals:    20,
			Expected:    float64(0.00012345000098765000),
			ExpectedPos: 14,
		},
		{
			Data:        []byte{128, 0, 0, 0, 1, 226, 58, 0, 15, 18, 2, 0, 0, 0, 0, 15, 0},
			Precision:   30,
			Decimals:    25,
			Expected:    float64(0.0001234500009876500000000),
			ExpectedPos: 15,
		},
		{
			Data:        []byte{128, 0, 128, 0},
			Precision:   4,
			Decimals:    2,
			Expected:    float64(0.00),
			ExpectedPos: 2,
		},
		{
			Data:        []byte{128, 0, 0, 128, 0},
			Precision:   5,
			Decimals:    0,
			Expected:    float64(0),
			ExpectedPos: 3,
		},
		{
			Data:        []byte{128, 0, 0, 0, 128, 0},
			Precision:   7,
			Decimals:    3,
			Expected:    float64(0.000),
			ExpectedPos: 4,
		},
		{
			Data:        []byte{128, 0, 0, 0, 0, 128, 0},
			Precision:   10,
			Decimals:    2,
			Expected:    float64(0.00),
			ExpectedPos: 5,
		},
		{
			Data:        []byte{128, 0, 0, 0, 0, 0, 128, 0},
			Precision:   10,
			Decimals:    3,
			Expected:    float64(0.000),
			ExpectedPos: 6,
		},
		{
			Data:        []byte{128, 0, 0, 0, 0, 0, 128, 0},
			Precision:   13,
			Decimals:    2,
			Expected:    float64(0.00),
			ExpectedPos: 6,
		},
		{
			Data:        []byte{128, 0, 1, 226, 58, 0, 0, 99, 128, 0},
			Precision:   15,
			Decimals:    14,
			Expected:    float64(0.00012345000099),
			ExpectedPos: 8,
		},
		{
			Data:        []byte{128, 0, 0, 0, 0, 0, 1, 226, 58, 0, 128, 0},
			Precision:   20,
			Decimals:    10,
			Expected:    float64(0.0001234500),
			ExpectedPos: 10,
		},
		{
			Data:        []byte{128, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 12, 128, 0},
			Precision:   30,
			Decimals:    5,
			Expected:    float64(0.00012),
			ExpectedPos: 15,
		},
		{
			Data:        []byte{128, 0, 0, 0, 0, 0, 1, 226, 58, 0, 15, 18, 2, 0, 128, 0},
			Precision:   30,
			Decimals:    20,
			Expected:    float64(0.00012345000098765000),
			ExpectedPos: 14,
		},
		{
			Data:        []byte{128, 0, 0, 0, 1, 226, 58, 0, 15, 18, 2, 0, 0, 0, 0, 22, 0},
			Precision:   30,
			Decimals:    25,
			Expected:    float64(0.0001234500009876500000000),
			ExpectedPos: 15,
		},
		{
			Data:        []byte{128, 12, 128, 0},
			Precision:   4,
			Decimals:    2,
			Expected:    float64(0.12),
			ExpectedPos: 2,
		},
		{
			Data:        []byte{128, 0, 0, 128, 0},
			Precision:   5,
			Decimals:    0,
			Expected:    float64(0),
			ExpectedPos: 3,
		},
		{
			Data:        []byte{128, 0, 0, 123, 128, 0},
			Precision:   7,
			Decimals:    3,
			Expected:    float64(0.123),
			ExpectedPos: 4,
		},
		{
			Data:        []byte{128, 0, 0, 0, 12, 128, 0},
			Precision:   10,
			Decimals:    2,
			Expected:    float64(0.12),
			ExpectedPos: 5,
		},
		{
			Data:        []byte{128, 0, 0, 0, 0, 123, 128, 0},
			Precision:   10,
			Decimals:    3,
			Expected:    float64(0.123),
			ExpectedPos: 6,
		},
		{
			Data:        []byte{128, 0, 0, 0, 0, 12, 128, 7},
			Precision:   13,
			Decimals:    2,
			Expected:    float64(0.12),
			ExpectedPos: 6,
		},
		{
			Data:        []byte{128, 7, 91, 178, 144, 1, 129, 205, 128, 0},
			Precision:   15,
			Decimals:    14,
			Expected:    float64(0.12345000098765),
			ExpectedPos: 8,
		},
		{
			Data:        []byte{128, 0, 0, 0, 0, 7, 91, 178, 145, 0, 128, 0},
			Precision:   20,
			Decimals:    10,
			Expected:    float64(0.1234500010),
			ExpectedPos: 10,
		},
		{
			Data:        []byte{128, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 48, 57, 128, 0},
			Precision:   30,
			Decimals:    5,
			Expected:    float64(0.12345),
			ExpectedPos: 15,
		},
		{
			Data:        []byte{128, 0, 0, 0, 0, 7, 91, 178, 144, 58, 222, 87, 208, 0, 128, 0},
			Precision:   30,
			Decimals:    20,
			Expected:    float64(0.12345000098765000000),
			ExpectedPos: 14,
		},
		{
			Data:        []byte{128, 0, 0, 7, 91, 178, 144, 58, 222, 87, 208, 0, 0, 0, 0, 30, 0},
			Precision:   30,
			Decimals:    25,
			Expected:    float64(0.1234500009876500000000000),
			ExpectedPos: 15,
		},
		{
			Data:        []byte{128, 0, 128, 0},
			Precision:   4,
			Decimals:    2,
			Expected:    float64(0.00),
			ExpectedPos: 2,
		},
		{
			Data:        []byte{128, 0, 0, 128, 0},
			Precision:   5,
			Decimals:    0,
			Expected:    float64(0),
			ExpectedPos: 3,
		},
		{
			Data:        []byte{128, 0, 0, 0, 128, 0},
			Precision:   7,
			Decimals:    3,
			Expected:    float64(0.000),
			ExpectedPos: 4,
		},
		{
			Data:        []byte{128, 0, 0, 0, 0, 128, 0},
			Precision:   10,
			Decimals:    2,
			Expected:    float64(0.00),
			ExpectedPos: 5,
		},
		{
			Data:        []byte{128, 0, 0, 0, 0, 0, 128, 0},
			Precision:   10,
			Decimals:    3,
			Expected:    float64(0.000),
			ExpectedPos: 6,
		},
		{
			Data:        []byte{128, 0, 0, 0, 0, 0, 127, 255},
			Precision:   13,
			Decimals:    2,
			Expected:    float64(0.00),
			ExpectedPos: 6,
		},
		{
			Data:        []byte{127, 255, 255, 255, 243, 255, 121, 59, 127, 255},
			Precision:   15,
			Decimals:    14,
			Expected:    float64(-0.00000001234500),
			ExpectedPos: 8,
		},
		{
			Data:        []byte{127, 255, 255, 255, 255, 255, 255, 255, 243, 252, 128, 0},
			Precision:   20,
			Decimals:    10,
			Expected:    float64(-0.0000000123),
			ExpectedPos: 10,
		},
		{
			Data:        []byte{128, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 127, 255},
			Precision:   30,
			Decimals:    5,
			Expected:    float64(0.00000),
			ExpectedPos: 15,
		},
		{
			Data:        []byte{127, 255, 255, 255, 255, 255, 255, 255, 243, 235, 111, 183, 93, 178, 127, 255},
			Precision:   30,
			Decimals:    20,
			Expected:    float64(-0.00000001234500009877),
			ExpectedPos: 14,
		},
		{
			Data:        []byte{127, 255, 255, 255, 255, 255, 243, 235, 111, 183, 93, 255, 139, 69, 47, 30, 0},
			Precision:   30,
			Decimals:    25,
			Expected:    float64(-0.0000000123450000987650000),
			ExpectedPos: 15,
		},
		{
			Data:        []byte{227, 99, 129, 134},
			Precision:   4,
			Decimals:    2,
			Expected:    float64(99.99),
			ExpectedPos: 2,
		},
		{
			Data:        []byte{129, 134, 159, 167, 15},
			Precision:   5,
			Decimals:    0,
			Expected:    float64(99999),
			ExpectedPos: 3,
		},
		{
			Data:        []byte{167, 15, 3, 231, 133, 245},
			Precision:   7,
			Decimals:    3,
			Expected:    float64(9999.999),
			ExpectedPos: 4,
		},
		{
			Data:        []byte{133, 245, 224, 255, 99, 128, 152},
			Precision:   10,
			Decimals:    2,
			Expected:    float64(99999999.99),
			ExpectedPos: 5,
		},
		{
			Data:        []byte{128, 152, 150, 127, 3, 231, 227, 59},
			Precision:   10,
			Decimals:    3,
			Expected:    float64(9999999.999),
			ExpectedPos: 6,
		},
		{
			Data:        []byte{227, 59, 154, 201, 255, 99, 137, 59},
			Precision:   13,
			Decimals:    2,
			Expected:    float64(99999999999.99),
			ExpectedPos: 6,
		},
		{
			Data:        []byte{137, 59, 154, 201, 255, 1, 134, 159, 137, 59},
			Precision:   15,
			Decimals:    14,
			Expected:    float64(9.99999999999999),
			ExpectedPos: 8,
		},
		{
			Data:        []byte{137, 59, 154, 201, 255, 59, 154, 201, 255, 9, 128, 0},
			Precision:   20,
			Decimals:    10,
			Expected:    float64(9999999999.9999999999),
			ExpectedPos: 10,
		},
		{
			Data:        []byte{128, 0, 0, 0, 0, 0, 4, 210, 29, 205, 139, 148, 0, 195, 80, 137, 59},
			Precision:   30,
			Decimals:    5,
			Expected:    float64(1234500009876.50000),
			ExpectedPos: 15,
		},
		{
			Data:        []byte{137, 59, 154, 201, 255, 59, 154, 201, 255, 59, 154, 201, 255, 99, 129, 134},
			Precision:   30,
			Decimals:    20,
			Expected:    float64(9999999999.99999999999999999999),
			ExpectedPos: 14,
		},
		{
			Data:        []byte{129, 134, 159, 59, 154, 201, 255, 59, 154, 201, 255, 0, 152, 150, 127, 30, 0},
			Precision:   30,
			Decimals:    25,
			Expected:    float64(99999.9999999999999999999999999),
			ExpectedPos: 15,
		},
		{
			Data:        []byte{227, 99, 129, 134},
			Precision:   4,
			Decimals:    2,
			Expected:    float64(99.99),
			ExpectedPos: 2,
		},
		{
			Data:        []byte{129, 134, 159, 167, 15},
			Precision:   5,
			Decimals:    0,
			Expected:    float64(99999),
			ExpectedPos: 3,
		},
		{
			Data:        []byte{167, 15, 3, 231, 133, 245},
			Precision:   7,
			Decimals:    3,
			Expected:    float64(9999.999),
			ExpectedPos: 4,
		},
		{
			Data:        []byte{133, 245, 224, 255, 99, 128, 152},
			Precision:   10,
			Decimals:    2,
			Expected:    float64(99999999.99),
			ExpectedPos: 5,
		},
		{
			Data:        []byte{128, 152, 150, 127, 3, 231, 128, 6},
			Precision:   10,
			Decimals:    3,
			Expected:    float64(9999999.999),
			ExpectedPos: 6,
		},
		{
			Data:        []byte{128, 6, 159, 107, 199, 11, 137, 59},
			Precision:   13,
			Decimals:    2,
			Expected:    float64(111111111.11),
			ExpectedPos: 6,
		},
		{
			Data:        []byte{137, 59, 154, 201, 255, 1, 134, 159, 128, 6},
			Precision:   15,
			Decimals:    14,
			Expected:    float64(9.99999999999999),
			ExpectedPos: 8,
		},
		{
			Data:        []byte{128, 6, 159, 107, 199, 6, 142, 119, 128, 0, 128, 0},
			Precision:   20,
			Decimals:    10,
			Expected:    float64(111111111.1100000000),
			ExpectedPos: 10,
		},
		{
			Data:        []byte{128, 0, 0, 0, 0, 0, 0, 0, 6, 159, 107, 199, 0, 42, 248, 128, 6},
			Precision:   30,
			Decimals:    5,
			Expected:    float64(111111111.11000),
			ExpectedPos: 15,
		},
		{
			Data:        []byte{128, 6, 159, 107, 199, 6, 142, 119, 128, 0, 0, 0, 0, 0, 129, 134},
			Precision:   30,
			Decimals:    20,
			Expected:    float64(111111111.11000000000000000000),
			ExpectedPos: 14,
		},
		{
			Data:        []byte{129, 134, 159, 59, 154, 201, 255, 59, 154, 201, 255, 0, 152, 150, 127, 10, 0},
			Precision:   30,
			Decimals:    25,
			Expected:    float64(99999.9999999999999999999999999),
			ExpectedPos: 15,
		},
		{
			Data:        []byte{128, 1, 128, 0},
			Precision:   4,
			Decimals:    2,
			Expected:    float64(0.01),
			ExpectedPos: 2,
		},
		{
			Data:        []byte{128, 0, 0, 128, 0},
			Precision:   5,
			Decimals:    0,
			Expected:    float64(0),
			ExpectedPos: 3,
		},
		{
			Data:        []byte{128, 0, 0, 10, 128, 0},
			Precision:   7,
			Decimals:    3,
			Expected:    float64(0.010),
			ExpectedPos: 4,
		},
		{
			Data:        []byte{128, 0, 0, 0, 1, 128, 0},
			Precision:   10,
			Decimals:    2,
			Expected:    float64(0.01),
			ExpectedPos: 5,
		},
		{
			Data:        []byte{128, 0, 0, 0, 0, 10, 128, 0},
			Precision:   10,
			Decimals:    3,
			Expected:    float64(0.010),
			ExpectedPos: 6,
		},
		{
			Data:        []byte{128, 0, 0, 0, 0, 1, 128, 0},
			Precision:   13,
			Decimals:    2,
			Expected:    float64(0.01),
			ExpectedPos: 6,
		},
		{
			Data:        []byte{128, 0, 152, 150, 128, 0, 0, 0, 128, 0},
			Precision:   15,
			Decimals:    14,
			Expected:    float64(0.01000000000000),
			ExpectedPos: 8,
		},
		{
			Data:        []byte{128, 0, 0, 0, 0, 0, 152, 150, 128, 0, 128, 0},
			Precision:   20,
			Decimals:    10,
			Expected:    float64(0.0100000000),
			ExpectedPos: 10,
		},
		{
			Data:        []byte{128, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3, 232, 128, 0},
			Precision:   30,
			Decimals:    5,
			Expected:    float64(0.01000),
			ExpectedPos: 15,
		},
		{
			Data:        []byte{128, 0, 0, 0, 0, 0, 152, 150, 128, 0, 0, 0, 0, 0, 128, 0},
			Precision:   30,
			Decimals:    20,
			Expected:    float64(0.01000000000000000000),
			ExpectedPos: 14,
		},
		{
			Data:        []byte{128, 0, 0, 0, 152, 150, 128, 0, 0, 0, 0, 0, 0, 0, 0, 7, 0},
			Precision:   30,
			Decimals:    25,
			Expected:    float64(0.0100000000000000000000000),
			ExpectedPos: 15,
		},
		{
			Data:        []byte{227, 99, 128, 0},
			Precision:   4,
			Decimals:    2,
			Expected:    float64(99.99),
			ExpectedPos: 2,
		},
		{
			Data:        []byte{128, 0, 123, 128, 123},
			Precision:   5,
			Decimals:    0,
			Expected:    float64(123),
			ExpectedPos: 3,
		},
		{
			Data:        []byte{128, 123, 1, 144, 128, 0},
			Precision:   7,
			Decimals:    3,
			Expected:    float64(123.400),
			ExpectedPos: 4,
		},
		{
			Data:        []byte{128, 0, 0, 123, 40, 128, 0},
			Precision:   10,
			Decimals:    2,
			Expected:    float64(123.40),
			ExpectedPos: 5,
		},
		{
			Data:        []byte{128, 0, 0, 123, 1, 144, 128, 0},
			Precision:   10,
			Decimals:    3,
			Expected:    float64(123.400),
			ExpectedPos: 6,
		},
		{
			Data:        []byte{128, 0, 0, 0, 123, 40, 137, 59},
			Precision:   13,
			Decimals:    2,
			Expected:    float64(123.40),
			ExpectedPos: 6,
		},
		{
			Data:        []byte{137, 59, 154, 201, 255, 1, 134, 159, 128, 0},
			Precision:   15,
			Decimals:    14,
			Expected:    float64(9.99999999999999),
			ExpectedPos: 8,
		},
		{
			Data:        []byte{128, 0, 0, 0, 123, 23, 215, 132, 0, 0, 128, 0},
			Precision:   20,
			Decimals:    10,
			Expected:    float64(123.4000000000),
			ExpectedPos: 10,
		},
		{
			Data:        []byte{128, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 123, 0, 156, 64, 128, 0},
			Precision:   30,
			Decimals:    5,
			Expected:    float64(123.40000),
			ExpectedPos: 15,
		},
		{
			Data:        []byte{128, 0, 0, 0, 123, 23, 215, 132, 0, 0, 0, 0, 0, 0, 128, 0},
			Precision:   30,
			Decimals:    20,
			Expected:    float64(123.40000000000000000000),
			ExpectedPos: 14,
		},
		{
			Data:        []byte{128, 0, 123, 23, 215, 132, 0, 0, 0, 0, 0, 0, 0, 0, 0, 10, 0},
			Precision:   30,
			Decimals:    25,
			Expected:    float64(123.4000000000000000000000000),
			ExpectedPos: 15,
		},
		{
			Data:        []byte{28, 156, 127, 253},
			Precision:   4,
			Decimals:    2,
			Expected:    float64(-99.99),
			ExpectedPos: 2,
		},
		{
			Data:        []byte{127, 253, 204, 125, 205},
			Precision:   5,
			Decimals:    0,
			Expected:    float64(-563),
			ExpectedPos: 3,
		},
		{
			Data:        []byte{125, 205, 253, 187, 127, 255},
			Precision:   7,
			Decimals:    3,
			Expected:    float64(-562.580),
			ExpectedPos: 4,
		},
		{
			Data:        []byte{127, 255, 253, 205, 197, 127, 255},
			Precision:   10,
			Decimals:    2,
			Expected:    float64(-562.58),
			ExpectedPos: 5,
		},
		{
			Data:        []byte{127, 255, 253, 205, 253, 187, 127, 255},
			Precision:   10,
			Decimals:    3,
			Expected:    float64(-562.580),
			ExpectedPos: 6,
		},
		{
			Data:        []byte{127, 255, 255, 253, 205, 197, 118, 196},
			Precision:   13,
			Decimals:    2,
			Expected:    float64(-562.58),
			ExpectedPos: 6,
		},
		{
			Data:        []byte{118, 196, 101, 54, 0, 254, 121, 96, 127, 255},
			Precision:   15,
			Decimals:    14,
			Expected:    float64(-9.99999999999999),
			ExpectedPos: 8,
		},
		{
			Data:        []byte{127, 255, 255, 253, 205, 221, 109, 230, 255, 255, 127, 255},
			Precision:   20,
			Decimals:    10,
			Expected:    float64(-562.5800000000),
			ExpectedPos: 10,
		},
		{
			Data:        []byte{127, 255, 255, 255, 255, 255, 255, 255, 255, 255, 253, 205, 255, 29, 111, 127, 255},
			Precision:   30,
			Decimals:    5,
			Expected:    float64(-562.58000),
			ExpectedPos: 15,
		},
		{
			Data:        []byte{127, 255, 255, 253, 205, 221, 109, 230, 255, 255, 255, 255, 255, 255, 127, 253},
			Precision:   30,
			Decimals:    20,
			Expected:    float64(-562.58000000000000000000),
			ExpectedPos: 14,
		},
		{
			Data:        []byte{127, 253, 205, 221, 109, 230, 255, 255, 255, 255, 255, 255, 255, 255, 255, 13, 0},
			Precision:   30,
			Decimals:    25,
			Expected:    float64(-562.5800000000000000000000000),
			ExpectedPos: 15,
		},
		{
			Data:        []byte{28, 156, 127, 241},
			Precision:   4,
			Decimals:    2,
			Expected:    float64(-99.99),
			ExpectedPos: 2,
		},
		{
			Data:        []byte{127, 241, 140, 113, 140},
			Precision:   5,
			Decimals:    0,
			Expected:    float64(-3699),
			ExpectedPos: 3,
		},
		{
			Data:        []byte{113, 140, 255, 245, 127, 255},
			Precision:   7,
			Decimals:    3,
			Expected:    float64(-3699.010),
			ExpectedPos: 4,
		},
		{
			Data:        []byte{127, 255, 241, 140, 254, 127, 255},
			Precision:   10,
			Decimals:    2,
			Expected:    float64(-3699.01),
			ExpectedPos: 5,
		},
		{
			Data:        []byte{127, 255, 241, 140, 255, 245, 127, 255},
			Precision:   10,
			Decimals:    3,
			Expected:    float64(-3699.010),
			ExpectedPos: 6,
		},
		{
			Data:        []byte{127, 255, 255, 241, 140, 254, 118, 196},
			Precision:   13,
			Decimals:    2,
			Expected:    float64(-3699.01),
			ExpectedPos: 6,
		},
		{
			Data:        []byte{118, 196, 101, 54, 0, 254, 121, 96, 127, 255},
			Precision:   15,
			Decimals:    14,
			Expected:    float64(-9.99999999999999),
			ExpectedPos: 8,
		},
		{
			Data:        []byte{127, 255, 255, 241, 140, 255, 103, 105, 127, 255, 127, 255},
			Precision:   20,
			Decimals:    10,
			Expected:    float64(-3699.0100000000),
			ExpectedPos: 10,
		},
		{
			Data:        []byte{127, 255, 255, 255, 255, 255, 255, 255, 255, 255, 241, 140, 255, 252, 23, 127, 255},
			Precision:   30,
			Decimals:    5,
			Expected:    float64(-3699.01000),
			ExpectedPos: 15,
		},
		{
			Data:        []byte{127, 255, 255, 241, 140, 255, 103, 105, 127, 255, 255, 255, 255, 255, 127, 241},
			Precision:   30,
			Decimals:    20,
			Expected:    float64(-3699.01000000000000000000),
			ExpectedPos: 14,
		},
		{
			Data:        []byte{127, 241, 140, 255, 103, 105, 127, 255, 255, 255, 255, 255, 255, 255, 255, 13, 0},
			Precision:   30,
			Decimals:    25,
			Expected:    float64(-3699.0100000000000000000000000),
			ExpectedPos: 15,
		},
		{
			Data:        []byte{28, 156, 127, 248},
			Precision:   4,
			Decimals:    2,
			Expected:    float64(-99.99),
			ExpectedPos: 2,
		},
		{
			Data:        []byte{127, 248, 99, 120, 99},
			Precision:   5,
			Decimals:    0,
			Expected:    float64(-1948),
			ExpectedPos: 3,
		},
		{
			Data:        []byte{120, 99, 255, 115, 127, 255},
			Precision:   7,
			Decimals:    3,
			Expected:    float64(-1948.140),
			ExpectedPos: 4,
		},
		{
			Data:        []byte{127, 255, 248, 99, 241, 127, 255},
			Precision:   10,
			Decimals:    2,
			Expected:    float64(-1948.14),
			ExpectedPos: 5,
		},
		{
			Data:        []byte{127, 255, 248, 99, 255, 115, 127, 255},
			Precision:   10,
			Decimals:    3,
			Expected:    float64(-1948.140),
			ExpectedPos: 6,
		},
		{
			Data:        []byte{127, 255, 255, 248, 99, 241, 118, 196},
			Precision:   13,
			Decimals:    2,
			Expected:    float64(-1948.14),
			ExpectedPos: 6,
		},
		{
			Data:        []byte{118, 196, 101, 54, 0, 254, 121, 96, 127, 255},
			Precision:   15,
			Decimals:    14,
			Expected:    float64(-9.99999999999999),
			ExpectedPos: 8,
		},
		{
			Data:        []byte{127, 255, 255, 248, 99, 247, 167, 196, 255, 255, 127, 255},
			Precision:   20,
			Decimals:    10,
			Expected:    float64(-1948.1400000000),
			ExpectedPos: 10,
		},
		{
			Data:        []byte{127, 255, 255, 255, 255, 255, 255, 255, 255, 255, 248, 99, 255, 201, 79, 127, 255},
			Precision:   30,
			Decimals:    5,
			Expected:    float64(-1948.14000),
			ExpectedPos: 15,
		},
		{
			Data:        []byte{127, 255, 255, 248, 99, 247, 167, 196, 255, 255, 255, 255, 255, 255, 127, 248},
			Precision:   30,
			Decimals:    20,
			Expected:    float64(-1948.14000000000000000000),
			ExpectedPos: 14,
		},
		{
			Data:        []byte{127, 248, 99, 247, 167, 196, 255, 255, 255, 255, 255, 255, 255, 255, 255, 13, 0},
			Precision:   30,
			Decimals:    25,
			Expected:    float64(-1948.1400000000000000000000000),
			ExpectedPos: 15,
		},
	}
	for i, tc := range testcases {
		value, pos, err := decodeDecimal(tc.Data, tc.Precision, tc.Decimals)
		c.Check(value, DecodeDecimalsEquals, pos, err, tc.Expected, tc.ExpectedPos, tc.ExpectedErr, i)
		//c.Assert(value, DecodeDecimalsEquals, pos, err, tc.Expected, tc.ExpectedPos, tc.ExpectedErr, i)
	}

}

// vim macro:
// 0dt[vf]:s/\%V /,/gi{ Data: []bytef[mar{f]r}lr,li Precision:1f i, Decimals:t-2dt i, Expected: float64(_PLACEHOLDER_), ExpectedPos: ldf 1f DF: "by$A "bp2Af,dt}F l"lyf}F C,},mb'af{@l'bj
// 'y0"yy$@a@yl"pyt|ndt)h"pp'y
// 'yk0"ay$11@u'yctf2k
// 'y22j
// 2f|
