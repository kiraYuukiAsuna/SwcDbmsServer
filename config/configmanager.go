package config

import (
	"DBMS/logger"
	"encoding/json"
	"io"
	"os"
	"strconv"
)

const ApiVersion = "2024.05.06"

const ServerAppVersion = "2024.05.06"

type Config struct {
	GrpcIP           string
	GrpcPort         int32
	ReverseProxyPort int32
	MongodbIP        string
	MongodbPort      int32
	MongodbUser      string
	MongodbPassword  string
}

var AppConfig Config

func SetDafaultAppConfig() {
	AppConfig.GrpcIP = "127.0.0.1"
	AppConfig.GrpcPort = 8088
	AppConfig.ReverseProxyPort = 8089
	AppConfig.MongodbIP = "127.0.0.1"
	AppConfig.MongodbPort = 27017
	AppConfig.MongodbUser = "defaultuser"
	AppConfig.MongodbPassword = "defaultpassword"
}

func ReadConfig() bool {
	jsonFile, err := os.Open("config_dev.json")

	if err != nil {
		logger.GetLogger().Println(err)
		os.Exit(-1)
	}
	logger.GetLogger().Println("Successfully Opened config.json")

	byteValue, _ := io.ReadAll(jsonFile)

	err = json.Unmarshal(byteValue, &AppConfig)
	if err != nil {
		logger.GetLogger().Println(err)
		os.Exit(-1)
	}

	defer func(jsonFile *os.File) {
		err := jsonFile.Close()
		if err != nil {
			logger.GetLogger().Println(err)
			os.Exit(-1)
		}
	}(jsonFile)

	logger.GetLogger().Println("GrpcIP:" + AppConfig.GrpcIP)
	logger.GetLogger().Println("GrpcPort:" + strconv.Itoa(int(AppConfig.GrpcPort)))
	logger.GetLogger().Println("ReverseProxyPort:" + strconv.Itoa(int(AppConfig.ReverseProxyPort)))
	logger.GetLogger().Println("MongodbIP:" + AppConfig.MongodbIP)
	logger.GetLogger().Println("MongodbPort:" + strconv.Itoa(int(AppConfig.MongodbPort)))
	logger.GetLogger().Println("MongodbUser:" + AppConfig.MongodbUser)
	logger.GetLogger().Println("MongodbPassword:" + AppConfig.MongodbPassword)
	logger.GetLogger().Println("ApiVersion:" + ApiVersion)
	logger.GetLogger().Println("ServerAppVersion:" + ServerAppVersion)

	return true
}
