package bll

import (
	"DBMS/SwcDbmsCommon/Generated/go/proto/service"
	"DBMS/config"
	"DBMS/dal"
	"DBMS/dbmodel"
	"DBMS/logger"
	"github.com/google/uuid"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"google.golang.org/grpc"
	_ "google.golang.org/grpc/encoding/gzip" // Install the gzip compressor
	"google.golang.org/grpc/reflection"
	"net"
	"strconv"
	"time"
)

func Initialize() {
	dataBaseNameInfo := dal.DataBaseNameInfo{
		MetaInfoDataBaseName:              dal.DefaultMetaInfoDataBaseName,
		SwcDataBaseName:                   dal.DefaultSwcDataBaseName,
		SwcSnapshotDataBaseName:           dal.DefaultSwcSnapshotDataBaseName,
		SwcIncrementOperationDataBaseName: dal.DefaultSwcIncrementOperationDataBaseName,
		SwcAttachmentDataBaseName:         dal.DefaultSwcAttachmentDataBaseName,
	}

	dal.InitializeNewDataBaseIfNotExist(dataBaseNameInfo)

	var createInfo dal.MongoDbConnectionCreateInfo
	createInfo.Host = config.AppConfig.MongodbIP
	createInfo.Port = config.AppConfig.MongodbPort
	createInfo.User = config.AppConfig.MongodbUser
	createInfo.Password = config.AppConfig.MongodbPassword
	connectionInfo := dal.ConnectToMongoDb(createInfo)

	if connectionInfo.Err != nil {
		logger.GetLogger().Fatal(connectionInfo.Err)
	}

	databaseInstance := dal.ConnectToDataBase(connectionInfo, dataBaseNameInfo)

	dal.SetDbInstance(databaseInstance)

	adminPermissionGroup := dbmodel.PermissionGroupMetaInfoV1{
		Name: dal.PermissionGroupAdmin,
	}
	if result := dal.QueryPermissionGroupByName(&adminPermissionGroup, dal.GetDbInstance()); !result.Status {

	}

	_, userId := dal.GetNewUserIdAndIncrease(databaseInstance)
	var serverUser = dbmodel.UserMetaInfoV1{
		Base: dbmodel.MetaInfoBase{
			Id:                     primitive.NewObjectID(),
			DataAccessModelVersion: "V1",
			Uuid:                   uuid.NewString(),
		},
		Name:                "server",
		Password:            "123456",
		Description:         "",
		CreateTime:          time.Now(),
		HeadPhotoBinData:    nil,
		PermissionGroupUuid: adminPermissionGroup.Base.Uuid,
		UserId:              userId,
	}
	dal.CreateUser(serverUser, databaseInstance)
}

func NewGrpcServer() {
	address := config.AppConfig.GrpcIP + ":" + strconv.Itoa(int(config.AppConfig.GrpcPort))
	listener, err := net.Listen("tcp", address)
	if err != nil {
		logger.GetLogger().Fatal(err)
	}

	s := grpc.NewServer(grpc.MaxRecvMsgSize(1024*1024*256), grpc.MaxSendMsgSize(1024*1024*256)) // 256mb, 256mb

	var instanceDBMSServerController DBMSServerController
	service.RegisterDBMSServer(s, instanceDBMSServerController)
	reflection.Register(s)

	err = s.Serve(listener)
	if err != nil {
		logger.GetLogger().Fatal(err)
	}

}
