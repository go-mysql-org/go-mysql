package pkg

import (
	"fmt"
	"strings"

	"github.com/spf13/cast"
)

// GetSequenceFromFilename get number suffix from binlog file name
func GetSequenceFromFilename(binlogFileName string) int {
	file0Arr := strings.Split(binlogFileName, ".")
	return cast.ToInt(strings.TrimLeft(file0Arr[1], "0"))
}

// ConstructBinlogFilename build new binlog filename with number sufffix
func ConstructBinlogFilename(fileNameTmpl string, sequenceSuffix int) string {
	file0Arr := strings.Split(fileNameTmpl, ".")
	file0Arr1Len := cast.ToString(len(file0Arr[1]))
	fileNameFmt := "%s." + "%0" + file0Arr1Len + "d"
	newFileName := fmt.Sprintf(fileNameFmt, file0Arr[0], sequenceSuffix)
	return newFileName
}
