package main

import (
	"os"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var rootCmd = &cobra.Command{
	Use:     "gomysqlbinlog",
	Short:   "gomysqlbinlog",
	Long:    "gomysqlbinlog replace mysqlbinlog",
	Version: "1.0.0",
	RunE: func(cmd *cobra.Command, args []string) (err error) {
		return parseBinlogFile()
	},
}

var logFile string

func init() {
	// rootCmd
	rootCmd.PersistentFlags().StringP("file", "f", "", "binlog file name")
	rootCmd.PersistentFlags().String("start-datetime", "", "start datetime")
	rootCmd.PersistentFlags().String("stop-datetime", "", "stop datetime")
	rootCmd.PersistentFlags().Int("start-position", 0, "start position")
	rootCmd.PersistentFlags().Int("stop-position", 0, "stop position")
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
	_ = viper.BindPFlag("databases", rootCmd.PersistentFlags().Lookup("databases"))
	_ = viper.BindPFlag("tables", rootCmd.PersistentFlags().Lookup("tables"))
	_ = viper.BindPFlag("exclude-databases", rootCmd.PersistentFlags().Lookup("exclude-databases"))
	_ = viper.BindPFlag("exclude-tables", rootCmd.PersistentFlags().Lookup("exclude-tables"))
	_ = viper.BindPFlag("flashback", rootCmd.PersistentFlags().Lookup("flashback"))

	rootCmd.PersistentFlags().String("rows-filter", "", "col[0] == 'abc'")
	rootCmd.PersistentFlags().String("rows-event-type", "", "insert | update | delete")
	rootCmd.PersistentFlags().String("query-event-handler", "", "keep | ignore | error | safe")
	rootCmd.PersistentFlags().String("statement-match-error", "", "Decide how to handle the query events like statement or ddl.")
	rootCmd.PersistentFlags().String("statement-match-ignore", "", "Decide how to handle the query events like statement or ddl.")
	rootCmd.PersistentFlags().String("statement-match-ignore-force", "", "Decide how to handle the query events like statement or ddl.")
	_ = viper.BindPFlag("rows-filter", rootCmd.PersistentFlags().Lookup("rows-filter"))
	_ = viper.BindPFlag("rows-event-type", rootCmd.PersistentFlags().Lookup("rows-event-type"))
	_ = viper.BindPFlag("query-event-handler", rootCmd.PersistentFlags().Lookup("query-event-handler"))

	rootCmd.PersistentFlags().String("rewrite-db", "", "Rewrite the row event to point so that it can be applied to a new database")
	rootCmd.PersistentFlags().Bool("conv-event-update-to-write", false, "Filter string or file to filter rows from event")
	_ = viper.BindPFlag("rewrite-db", rootCmd.PersistentFlags().Lookup("rewrite-db"))
	_ = viper.BindPFlag("conv-event-update-to-write", rootCmd.PersistentFlags().Lookup("conv-event-update-to-write"))

	rootCmd.PersistentFlags().StringP("result-file", "r", "", "Direct output to a given file")
	rootCmd.PersistentFlags().String("parallel-type", "mysqlbinlog", "database | table | database_hash | table_hash | key_hash")
	rootCmd.PersistentFlags().Int("binlog-row-event-max-size", 0, "binlog-row-event-max-size")
	rootCmd.PersistentFlags().Int("result-file-max-size", 0, "result-file-max-size")

	rootCmd.PersistentFlags().IntP("verbose", "v", 1, "1 2 3")

	// overwrite -h option
	rootCmd.PersistentFlags().BoolP("help", "", false, "help for this command")
	rootCmd.PersistentFlags().StringVar(&logFile, "log-dir", "",
		"log file path. default empty will log files to dir dbbackup/logs/")
}

func main() {
	err := rootCmd.Execute()
	if err != nil {
		// todo try to kill child process(mydumper / myloader / xtrabackup)
		os.Exit(1)
	}
}
