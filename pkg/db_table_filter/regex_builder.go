package db_table_filter

import (
	"fmt"
	"strings"
)

func buildIncludeRegexp(parts []string) string {
	return buildRegexp(parts, `(?=(?:(%s)))`)
}

func buildExcludeRegexp(parts []string) string {
	return buildRegexp(parts, `(?!(?:(%s)))`)
}

func buildRegexp(parts []string, template string) string {
	var res string

	var np []string
	for _, part := range parts {
		np = append(np, fmt.Sprintf("%s$", part))
	}
	if len(parts) > 0 {
		res += fmt.Sprintf(template, strings.Join(np, "|"))
	}
	return res
}
