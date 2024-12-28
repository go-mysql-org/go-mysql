//go:build !linux && !darwin

package utils

import "time"

var Now = time.Now
