package db_table_filter

import (
	"fmt"
	"strings"

	"github.com/dlclark/regexp2"
)

func containGlob(p string) bool {
	return strings.Contains(p, "*") ||
		strings.Contains(p, "?") ||
		strings.Contains(p, "%")
}

func cleanIt(s []string) []string {
	var r []string
	for _, e := range s {
		te := strings.TrimSpace(e)
		if len(te) > 0 {
			r = append(r, strings.TrimSpace(e))
		}
	}
	return r
}

// ReplaceGlob 通配符替换为正则
// todo . -> \. ?
func ReplaceGlob(p string) string {
	return strings.Replace(
		strings.Replace(
			strings.Replace(p, "*", ".*", -1),
			"%", ".*", -1,
		),
		"?", ".", -1,
	)
}

// HasGlobPattern 是否有通配符
func HasGlobPattern(patterns []string) bool {
	for _, p := range patterns {
		if strings.Contains(p, "%") || strings.Contains(p, "?") || strings.Contains(p, "*") {
			return true
		}
	}
	return false
}

func buildIncludeRegexp(parts []string) string {
	return buildRegexp(parts, `(?=(?:(%s)))`)
}

func buildExcludeRegexp(parts []string) string {
	return buildRegexp(parts, `(?!(?:(%s)))`)
}

func buildRegexp(parts []string, template string) string {
	var res string

	if len(parts) > 0 {
		res += fmt.Sprintf(template, strings.Join(parts, "|"))
	}
	return res
}

func globCheck(patterns []string) error {
	r1 := regexp2.MustCompile(`^[%?]+$`, regexp2.None)
	r2 := regexp2.MustCompile(`^\*+$`, regexp2.None)
	for _, p := range patterns {
		if containGlob(p) {
			if len(patterns) > 1 {
				return fmt.Errorf("%s: multi patterns not allowed if has glob", patterns)
			}

			m1, _ := r1.MatchString(p)
			if (strings.Contains(p, "%") || strings.Contains(p, "?")) && m1 {
				return fmt.Errorf(`%% ? can't be used alone`)
			}

			m2, _ := r2.MatchString(p)
			if strings.Contains(p, "*") && !m2 {
				return fmt.Errorf("* must used alone")
			}
		}
	}
	return nil
}
