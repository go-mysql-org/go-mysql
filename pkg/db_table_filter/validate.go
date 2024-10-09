package db_table_filter

import (
	"fmt"
	"slices"
	"strings"

	"github.com/dlclark/regexp2"
)

/*
ToDo
*/
func (c *DbTableFilter) validate() error {
	if len(c.IncludeDbPatterns) == 0 {
		return fmt.Errorf("include databases patterns can't be empty")
	}
	if len(c.IncludeTablePatterns) == 0 {
		return fmt.Errorf("include tables patterns can't be empty")
	}

	if slices.Index(c.ExcludeDbPatterns, "*") >= 0 {
		return fmt.Errorf("exclude databases patterns can't be *")

	}
	if slices.Index(c.ExcludeTablePatterns, "*") >= 0 {
		return fmt.Errorf("exclude tables patterns can't be *")
	}

	if err := globCheck(c.IncludeDbPatterns); err != nil {
		return err
	}
	if err := globCheck(c.IncludeTablePatterns); err != nil {
		return err
	}
	if err := globCheck(c.ExcludeDbPatterns); err != nil {
		return err
	}
	if err := globCheck(c.ExcludeTablePatterns); err != nil {
		return err
	}
	return nil
}

func globCheck(patterns []string) error {
	r1 := regexp2.MustCompile(`^[%?]+$`, regexp2.None)
	r2 := regexp2.MustCompile(`^\*+$`, regexp2.None)
	for _, p := range patterns {
		if ContainGlob(p) {
			//if len(patterns) > 1 {
			//	return fmt.Errorf("%s: multi patterns not allowed if has glob", patterns)
			//}

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
