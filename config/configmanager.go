package config

import (
	"encoding/json"
	"io"
	"log"
	"os"
	"strconv"
)

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
	jsonFile, err := os.Open("config.json")

	if err != nil {
		log.Println(err)
		os.Exit(-1)
	}
	log.Println("Successfully Opened config.json")

	byteValue, _ := io.ReadAll(jsonFile)

	err = json.Unmarshal(byteValue, &AppConfig)
	if err != nil {
		log.Println(err)
		os.Exit(-1)
	}

	defer func(jsonFile *os.File) {
		err := jsonFile.Close()
		if err != nil {
			log.Println(err)
			os.Exit(-1)
		}
	}(jsonFile)

	log.Println("GrpcIP:" + AppConfig.GrpcIP)
	log.Println("GrpcPort:" + strconv.Itoa(int(AppConfig.GrpcPort)))
	log.Println("ReverseProxyPort:" + strconv.Itoa(int(AppConfig.ReverseProxyPort)))
	log.Println("MongodbIP:" + AppConfig.MongodbIP)
	log.Println("MongodbPort:" + strconv.Itoa(int(AppConfig.MongodbPort)))
	log.Println("MongodbUser:" + AppConfig.MongodbUser)
	log.Println("MongodbPassword:" + AppConfig.MongodbPassword)

	return true
}
