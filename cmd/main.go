package main

import (
	"DBMS/SwcDbmsCommon/Generated/go/proto/service"
	"DBMS/UnitTest"
	"DBMS/apihandler"
	"DBMS/bll"
	"DBMS/config"
	"DBMS/logger"
	"context"
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/grpclog"
)

func startHttpReverseProxyServer() error {
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Register gRPC server endpoint
	// Note: Make sure the gRPC server is running properly and accessible

	mux := runtime.NewServeMux()
	opts := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(1024 * 1024 * 256))} // 256MB
	address := config.AppConfig.GrpcIP + ":" + strconv.Itoa(int(config.AppConfig.GrpcPort))
	err := service.RegisterDBMSHandlerFromEndpoint(ctx, mux, address, opts)
	if err != nil {
		return err
	}

	// Start HTTP server (and proxy calls to gRPC server endpoint)
	httpAddress := ":" + strconv.Itoa(int(config.AppConfig.ReverseProxyPort))
	return http.ListenAndServe(httpAddress, mux)
}

func main() {
	logger.InitializeLogger()
	config.SetDafaultAppConfig()
	config.ReadConfig()

	go func() {
		err := startHttpReverseProxyServer()
		if err != nil {
			grpclog.Fatal(err)

		}
	}()

	bll.Initialize()
	bll.CronAutoSaveDailyStatistics()
	bll.CronHeartBeatValidationAndRefresh()
	bll.NewGrpcServer()

	return
	config.SetDafaultAppConfig()
	config.ReadConfig()
	bll.Initialize()
	bll.CronAutoSaveDailyStatistics()
	bll.CronHeartBeatValidationAndRefresh()
	bll.NewGrpcServer()
	return

	UnitTest.InitializeDb()
	UnitTest.TestUserInfo()
	UnitTest.TestProjectInfo()
	UnitTest.TestPermissionGroupInfo()
	UnitTest.TestSwcInfo()
	UnitTest.TestDailyStatisticsInfo()
	UnitTest.TestSwcData()

	return

	r := gin.Default()
	r.GET("/ping", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{
			"message": "pong",
		})
	})

	r.GET("/InitializeNewDataBaseIfNotExist", apihandler.InitializeNewDataBaseIfNotExistHandler)
	r.GET("/CreateUser", apihandler.CreateUserHandler)

	err := r.Run("0.0.0.0:8088")
	if err != nil {
		return
	}
}
