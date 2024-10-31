package logger

import (
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"
)

var (
	globalLogger *log.Logger
	logFile      *os.File
	mu           sync.Mutex
)

func InitializeLogger() {
	// 创建日志文件
	file, err := createLogFile()
	if err != nil {
		log.Fatalf("Failed to create log file: %v", err)
	}

	logFile = file

	// 创建一个多重写入器，同时写入文件和标准输出
	mw := io.MultiWriter(os.Stdout, logFile)

	// 创建Logger
	globalLogger = log.New(mw, "test_", log.Ldate|log.Ltime|log.Lshortfile)

	// 启动一个goroutine来每天更新日志文件
	go rotateLogFile()
}

func createLogFile() (*os.File, error) {
	now := time.Now()
	fileName := fmt.Sprintf("%s.log", now.Format("20060102 15-04-05"))

	logDir := "./logs"
	if err := os.MkdirAll(logDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create log directory: %v", err)
	}

	filePath := filepath.Join(logDir, fileName)
	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		return nil, err
	}

	fmt.Printf("Created new log file: %s\n", filePath)
	return file, nil
}

func rotateLogFile() {
	ticker := time.NewTicker(24 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		mu.Lock()

		// 关闭旧的日志文件
		if logFile != nil {
			logFile.Close()
		}

		// 创建新的日志文件
		newLogFile, err := createLogFile()
		if err != nil {
			globalLogger.Printf("Failed to create new log file: %v", err)
			mu.Unlock()
			continue
		}

		// 更新Logger的输出
		mw := io.MultiWriter(os.Stdout, newLogFile)
		globalLogger.SetOutput(mw)

		// 更新logFile
		logFile = newLogFile

		mu.Unlock()
	}
}

func GetLogger() *log.Logger {
	return globalLogger
}
