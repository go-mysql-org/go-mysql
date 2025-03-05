package main

import (
	"os"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var rootCmd = &cobra.Command{
	Use:          "gomysqlbinlog",
	Short:        "gomysqlbinlog",
	Long:         "gomysqlbinlog replace mysqlbinlog",
	Version:      "1.0.0",
	SilenceUsage: true,
	RunE: func(cmd *cobra.Command, args []string) (err error) {
		return parseBinlogFile()
	},
}

var logFile string

func init() {
	// rootCmd
	rootCmd.PersistentFlags().String("start-file", "", "binlog start file name")
	rootCmd.PersistentFlags().String("stop-file", "", "binlog stop file name")
	rootCmd.PersistentFlags().String("binlog-dir", "", "binlog dir")
	rootCmd.PersistentFlags().Int("threads", 1, "parse binlog threads")
	_ = viper.BindPFlag("start-file", rootCmd.PersistentFlags().Lookup("start-file"))
	_ = viper.BindPFlag("stop-file", rootCmd.PersistentFlags().Lookup("stop-file"))
	_ = viper.BindPFlag("binlog-dir", rootCmd.PersistentFlags().Lookup("binlog-dir"))
	_ = viper.BindPFlag("threads", rootCmd.PersistentFlags().Lookup("threads"))

	rootCmd.PersistentFlags().StringSliceP("file", "f", nil, "binlog file name")
	rootCmd.PersistentFlags().String("start-datetime", "", "start datetime")
	rootCmd.PersistentFlags().String("stop-datetime", "", "stop datetime")
	rootCmd.PersistentFlags().Int("start-position", 4, "start position for --file or --start-file")
	rootCmd.PersistentFlags().Int("stop-position", 0, "stop position for --file or --stop-file")
	_ = viper.BindPFlag("file", rootCmd.PersistentFlags().Lookup("file"))
	_ = viper.BindPFlag("start-datetime", rootCmd.PersistentFlags().Lookup("start-datetime"))
	_ = viper.BindPFlag("stop-datetime", rootCmd.PersistentFlags().Lookup("stop-datetime"))
	_ = viper.BindPFlag("start-position", rootCmd.PersistentFlags().Lookup("start-position"))
	_ = viper.BindPFlag("stop-position", rootCmd.PersistentFlags().Lookup("stop-position"))

	rootCmd.PersistentFlags().StringSliceP("databases", "B", nil, "databases")
	rootCmd.PersistentFlags().StringSliceP("tables", "T", nil, "tables")
	rootCmd.PersistentFlags().StringSlice("exclude-databases", nil, "exclude databases")
	rootCmd.PersistentFlags().StringSlice("exclude-tables", nil, "exclude tables")
	rootCmd.PersistentFlags().Bool("flashback", false, "flashback")
	rootCmd.PersistentFlags().BoolP("idempotent", "i", false, "idempotent mode")
	rootCmd.PersistentFlags().Bool("disable-log-bin", false, "disable sql_log_bin")
	rootCmd.PersistentFlags().Bool("disable-foreign-key-checks", false, "set session foreign_key_checks=0")

	//rootCmd.PersistentFlags().Bool("rows-strict", false, "no statement query allowed")
	_ = viper.BindPFlag("databases", rootCmd.PersistentFlags().Lookup("databases"))
	_ = viper.BindPFlag("tables", rootCmd.PersistentFlags().Lookup("tables"))
	_ = viper.BindPFlag("exclude-databases", rootCmd.PersistentFlags().Lookup("exclude-databases"))
	_ = viper.BindPFlag("exclude-tables", rootCmd.PersistentFlags().Lookup("exclude-tables"))
	_ = viper.BindPFlag("flashback", rootCmd.PersistentFlags().Lookup("flashback"))
	_ = viper.BindPFlag("idempotent", rootCmd.PersistentFlags().Lookup("idempotent"))
	_ = viper.BindPFlag("disable-log-bin", rootCmd.PersistentFlags().Lookup("disable-log-bin"))

	rootCmd.PersistentFlags().Int("server-id", 0, "Extract only binlog entries created by the server having the given id")
	rootCmd.PersistentFlags().String("rows-filter", "", "col[0] == 'abc'")
	rootCmd.PersistentFlags().String("rows-filter-from-csv", "", "file csv format like:col[0],col[1]\nxxx,100\nyyy,200")
	rootCmd.PersistentFlags().StringSlice("rows-event-type", nil, "insert,update,delete")
	//rootCmd.PersistentFlags().String("query-event-handler", "", "keep | ignore | error | safe")
	//rootCmd.PersistentFlags().String("statement-match-error", "", "Decide how to handle the query events like statement or ddl.")
	//rootCmd.PersistentFlags().String("statement-match-ignore", "", "Decide how to handle the query events like statement or ddl.")
	//rootCmd.PersistentFlags().String("statement-match-ignore-force", "", "Decide how to handle the query events like statement or ddl.")
	_ = viper.BindPFlag("server-id", rootCmd.PersistentFlags().Lookup("server-id"))
	_ = viper.BindPFlag("rows-filter", rootCmd.PersistentFlags().Lookup("rows-filter"))
	_ = viper.BindPFlag("rows-filter-from-csv", rootCmd.PersistentFlags().Lookup("rows-filter-from-csv"))
	_ = viper.BindPFlag("rows-event-type", rootCmd.PersistentFlags().Lookup("rows-event-type"))
	//_ = viper.BindPFlag("query-event-handler", rootCmd.PersistentFlags().Lookup("query-event-handler"))

	rootCmd.PersistentFlags().StringSlice("rewrite-db", nil, "Rewrite the row event to point so that it can be applied to a new database")
	rootCmd.PersistentFlags().Bool("conv-rows-update-to-write", false, "change update event to write")
	_ = viper.BindPFlag("rewrite-db", rootCmd.PersistentFlags().Lookup("rewrite-db"))
	_ = viper.BindPFlag("conv-rows-update-to-write", rootCmd.PersistentFlags().Lookup("conv-rows-update-to-write"))

	rootCmd.PersistentFlags().String("parallel-type", "mysqlbinlog", "database | table | database_hash | table_hash | key_hash")
	rootCmd.PersistentFlags().Int("binlog-row-event-max-size", 0, "binlog-row-event-max-size")

	rootCmd.PersistentFlags().StringP("result-file", "r", "", "Direct output to a given file")
	rootCmd.PersistentFlags().Int("result-file-max-size-mb", 128, "result-file-max-size-mb")
	rootCmd.PersistentFlags().String("set-charset", "", "Add 'SET NAMES character_set' to the output, | utf8 | utf8mb4 | latin1 | gbk")
	_ = viper.BindPFlag("result-file", rootCmd.PersistentFlags().Lookup("result-file"))
	_ = viper.BindPFlag("result-file-max-size-mb", rootCmd.PersistentFlags().Lookup("result-file-max-size-mb"))
	_ = viper.BindPFlag("set-charset", rootCmd.PersistentFlags().Lookup("set-charset"))

	rootCmd.PersistentFlags().IntP("verbose", "v", 0, "verbose, 0, 1, 2")
	_ = viper.BindPFlag("verbose", rootCmd.PersistentFlags().Lookup("verbose"))
	rootCmd.PersistentFlags().BoolP("short", "s", true, "short will not print un-matched event header")
	rootCmd.PersistentFlags().BoolP("autocommit", "c", true, "set auto_commit=1 to output")
	_ = viper.BindPFlag("short", rootCmd.PersistentFlags().Lookup("short"))
	_ = viper.BindPFlag("autocommit", rootCmd.PersistentFlags().Lookup("autocommit"))

	// overwrite -h option
	rootCmd.PersistentFlags().BoolP("help", "", false, "help for this command")
	rootCmd.PersistentFlags().StringVar(&logFile, "log-dir", "",
		"log file path. default empty will log files to dir dbbackup/logs/")

	rootCmd.MarkFlagsOneRequired("file", "start-file")
	rootCmd.MarkFlagsMutuallyExclusive("file", "start-file")
	rootCmd.MarkFlagsRequiredTogether("start-file", "stop-file")
	rootCmd.MarkFlagsMutuallyExclusive("stop-file", "result-file") // 多文件解析不允许指定 result-file
}

func main() {
	err := rootCmd.Execute()
	if err != nil {
		// todo try to kill child process(mydumper / myloader / xtrabackup)
		os.Exit(1)
	}
}
