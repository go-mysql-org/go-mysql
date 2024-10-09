package db_table_filter

import "strings"

func ReplaceGlob(p string) string {
	p1 := strings.Replace(p, `.`, `\.`, -1)
	p2 := strings.Replace(p1, `*`, `.*`, -1)
	p3 := strings.Replace(p2, `%`, `.*`, -1)
	p4 := strings.Replace(p3, `?`, `.`, -1)
	return p4
}

func ReplaceGlobs(ps []string) []string {
	var res []string
	for _, p := range ps {
		res = append(res, ReplaceGlob(p))
	}
	return res
}
