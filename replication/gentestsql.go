// +build ignore
package main

import (
	"fmt"
	"strings"
)

type decv struct {
	p int
	s int
}

func (v decv) Name() string {
	return fmt.Sprintf("v%v_%v", v.p, v.s)
}
func (v decv) Type() string {
	return fmt.Sprintf("decimal(%v,%v)", v.p, v.s)
}
func (v decv) ColDef() string {
	return v.Name() + " " + v.Type()
}

func coldefs(s []decv) (strs []string) {
	for _, v := range s {
		strs = append(strs, v.ColDef())
	}
	return
}

func colnames(s []decv) (strs []string) {
	for _, v := range s {
		strs = append(strs, v.Name())
	}
	return
}

func placeholders(s []decv, i int) (str string) {
	var strs []string
	for _ = range s {
		strs = append(strs, fmt.Sprintf("\"%%[%v]v\"", i))
	}
	return strings.Join(strs, ",")
}

func main() {
	deccol := []decv{
		{4, 2},
		{5, 0},
		{7, 3},
		{10, 2},
		{10, 3},
		{13, 2},
		{15, 14},
		{20, 10},
		{30, 5},
		{30, 20},
		{30, 25},
	}
	fmt.Printf("-- Generated with foo.go --\n")
	fmt.Printf(`DROP TABLE IF EXISTS decodedecimal;
CREATE TABLE decodedecimal (
    id     int(11) not null auto_increment,
    %v,
    prec   int(11),
    scale  int(11),
    PRIMARY KEY(id)
) engine=InnoDB;`,
		strings.Join(coldefs(deccol), ",\n    "),
	)
	fmt.Println()
	values := []struct {
		value     string
		precision int
		scale     int
	}{
		// values from https://github.com/mysql/mysql-server/blob/a2757a60a7527407d08115e44e889a25f22c96c6/unittest/gunit/decimal-t.cc#L670
		{"-10.55", 4, 2},
		{"0.0123456789012345678912345", 30, 25},
		{"12345", 5, 0},
		{"12345", 10, 3},
		{"123.45", 10, 3},
		{"-123.45", 20, 10},
		{".00012345000098765", 15, 14},
		{".00012345000098765", 22, 20},
		{".12345000098765", 30, 20},
		{"-.000000012345000098765", 30, 20},
		{"1234500009876.5", 30, 5},
		{"111111111.11", 10, 2},
		{"000000000.01", 7, 3},
		{"123.4", 10, 2},
		// Values from error I was seeing in my data set.
		{"-562.58", 13, 2},
		{"-3699.01", 13, 2},
		{"-1948.14", 13, 2},
	}

	fmt.Printf("INSERT INTO decodedecimal (%v,prec,scale) VALUES \n",
		strings.Join(colnames(deccol), ","),
	)
	var strs []string
	fmtStr := fmt.Sprintf("(%v,%%v,%%v)", placeholders(deccol, 1))
	for _, v := range values {
		strs = append(strs, fmt.Sprintf(
			fmtStr,
			v.value,
			v.precision,
			v.scale,
		))
	}
	fmt.Println(strings.Join(strs, ",\n"))
	fmt.Println(";")
}
