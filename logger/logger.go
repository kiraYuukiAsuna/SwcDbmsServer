package logger

import (
	"log"
	"os"
	"time"
)

var globalLogger *log.Logger

func InitializeLogger() {
	//创建输出日志文件
	logFile, err := os.Create("./" + time.Now().Format("20060102") + ".txt")
	if err != nil {
		log.Println(err)
	}
	//创建一个Logger
	//参数1：日志写入目的地
	//参数2：每条日志的前缀
	//参数3：日志属性
	globalLogger = log.New(logFile, "test_", log.Ldate|log.Ltime|log.Lshortfile)
}

func GetLogger() *log.Logger {
	return globalLogger
}
