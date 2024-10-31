package dal

import (
	"DBMS/config"
	"DBMS/dbmodel"
	"DBMS/logger"
	"context"
	"github.com/google/uuid"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var globalMongodbdatabaseinfo MongoDbDataBaseInfo

func SetDbInstance(instance MongoDbDataBaseInfo) {
	globalMongodbdatabaseinfo = instance
}

func GetDbInstance() MongoDbDataBaseInfo {
	return globalMongodbdatabaseinfo
}

func InitializeNewDataBaseIfNotExist(dataBaseNameInfo DataBaseNameInfo) {
	createInfo := MongoDbConnectionCreateInfo{
		Host:     config.AppConfig.MongodbIP,
		Port:     config.AppConfig.MongodbPort,
		User:     config.AppConfig.MongodbUser,
		Password: config.AppConfig.MongodbPassword,
	}

	connectionInfo := ConnectToMongoDb(createInfo)

	if connectionInfo.Err != nil {
		logger.GetLogger().Fatal(connectionInfo.Err)
	}

	var dbInfo MongoDbDataBaseInfo
	dbInfo.MetaInfoDb = connectionInfo.Client.Database(dataBaseNameInfo.MetaInfoDataBaseName)
	dbInfo.SwcDb = connectionInfo.Client.Database(dataBaseNameInfo.SwcDataBaseName)
	dbInfo.SnapshotDb = connectionInfo.Client.Database(dataBaseNameInfo.SwcSnapshotDataBaseName)
	dbInfo.IncrementOperationDb = connectionInfo.Client.Database(dataBaseNameInfo.SwcIncrementOperationDataBaseName)
	dbInfo.AttachmentDb = connectionInfo.Client.Database(dataBaseNameInfo.SwcAttachmentDataBaseName)

	var deleteIfExist bool
	deleteIfExist = false

	databaseNames, err := connectionInfo.Client.ListDatabaseNames(context.TODO(), bson.M{})
	if err != nil {
		logger.GetLogger().Fatal(err)
	}

	databaseMetaInfoExists := false
	for _, dbName := range databaseNames {
		if dbName == dataBaseNameInfo.MetaInfoDataBaseName {
			databaseMetaInfoExists = true
			break
		}
	}
	if databaseMetaInfoExists && !deleteIfExist {
		logger.GetLogger().Printf("Database %s exists! Do not create a new one!\n", dataBaseNameInfo.MetaInfoDataBaseName)

	} else {
		if databaseMetaInfoExists && deleteIfExist {
			err := dbInfo.MetaInfoDb.Drop(context.TODO())
			if err != nil {
				logger.GetLogger().Fatal("Delete exist meta info database failed")
			}
		}
		logger.GetLogger().Printf("Database %s does not exist. Start to create a new one!\n", dataBaseNameInfo.MetaInfoDataBaseName)

		var err error

		err = dbInfo.MetaInfoDb.CreateCollection(context.TODO(), MetaInfoDbStatusCollectonString)
		if err != nil {
			logger.GetLogger().Fatal(err)
		}

		err = dbInfo.MetaInfoDb.CreateCollection(context.TODO(), ProjectMetaInfoCollectionString)
		if err != nil {
			logger.GetLogger().Fatal(err)
		}

		err = dbInfo.MetaInfoDb.CreateCollection(context.TODO(), UserMetaInfoCollectionString)
		if err != nil {
			logger.GetLogger().Fatal(err)
		}

		err = dbInfo.MetaInfoDb.CreateCollection(context.TODO(), PermissionGroupMetaInfoCollectioString)
		if err != nil {
			logger.GetLogger().Fatal(err)
		}

		var permissionGroupAdmin = dbmodel.PermissionGroupMetaInfoV1{
			Base: dbmodel.MetaInfoBase{
				Id:                     primitive.NewObjectID(),
				DataAccessModelVersion: "V1",
				Uuid:                   uuid.NewString(),
			},
			Name:        PermissionGroupAdmin,
			Description: "Admin Permission Group",
			Ace: dbmodel.PermissionGroupAceV1{
				AllPermissionGroupManagementPermission: true,
				AllUserManagementPermission:            true,
				AllProjectManagementPermission:         true,
				AllSwcManagementPermission:             true,
				AllDailyStatisticsManagementPermission: true,
				CreateProjectPermission:                true,
				CreateSwcPermission:                    true,
			},
		}
		CreatePermissionGroup(permissionGroupAdmin, dbInfo)

		var permissionGroupDefault = dbmodel.PermissionGroupMetaInfoV1{
			Base: dbmodel.MetaInfoBase{
				Id:                     primitive.NewObjectID(),
				DataAccessModelVersion: "V1",
				Uuid:                   uuid.NewString(),
			},
			Name:        PermissionGroupDefault,
			Description: "Default Permission Group",
			Ace: dbmodel.PermissionGroupAceV1{
				AllPermissionGroupManagementPermission: false,
				AllUserManagementPermission:            false,
				AllProjectManagementPermission:         false,
				AllSwcManagementPermission:             false,
				AllDailyStatisticsManagementPermission: false,
				CreateProjectPermission:                false,
				CreateSwcPermission:                    false,
			},
		}
		CreatePermissionGroup(permissionGroupDefault, dbInfo)

		var permissionGroupGroupLeader = dbmodel.PermissionGroupMetaInfoV1{
			Base: dbmodel.MetaInfoBase{
				Id:                     primitive.NewObjectID(),
				DataAccessModelVersion: "V1",
				Uuid:                   uuid.NewString(),
			},
			Name:        PermissionGroupGroupLeader,
			Description: "GroupLeader Permission Group",
			Ace: dbmodel.PermissionGroupAceV1{
				AllPermissionGroupManagementPermission: false,
				AllUserManagementPermission:            false,
				AllProjectManagementPermission:         false,
				AllSwcManagementPermission:             false,
				AllDailyStatisticsManagementPermission: false,
				CreateProjectPermission:                false,
				CreateSwcPermission:                    false,
			},
		}
		CreatePermissionGroup(permissionGroupGroupLeader, dbInfo)

		var permissionGroupGroupNormalUser = dbmodel.PermissionGroupMetaInfoV1{
			Base: dbmodel.MetaInfoBase{
				Id:                     primitive.NewObjectID(),
				DataAccessModelVersion: "V1",
				Uuid:                   uuid.NewString(),
			},
			Name:        PermissionGroupNormalUser,
			Description: "NormalUser Permission Group",
			Ace: dbmodel.PermissionGroupAceV1{
				AllPermissionGroupManagementPermission: false,
				AllUserManagementPermission:            false,
				AllProjectManagementPermission:         false,
				AllSwcManagementPermission:             false,
				AllDailyStatisticsManagementPermission: false,
				CreateProjectPermission:                false,
				CreateSwcPermission:                    false,
			},
		}
		CreatePermissionGroup(permissionGroupGroupNormalUser, dbInfo)

		var permissionGroupGroupGuest = dbmodel.PermissionGroupMetaInfoV1{
			Base: dbmodel.MetaInfoBase{
				Id:                     primitive.NewObjectID(),
				DataAccessModelVersion: "V1",
				Uuid:                   uuid.NewString(),
			},
			Name:        PermissionGroupGuest,
			Description: "Guest Permission Group",
			Ace: dbmodel.PermissionGroupAceV1{
				AllPermissionGroupManagementPermission: false,
				AllUserManagementPermission:            false,
				AllProjectManagementPermission:         false,
				AllSwcManagementPermission:             false,
				AllDailyStatisticsManagementPermission: false,
				CreateProjectPermission:                false,
				CreateSwcPermission:                    false,
			},
		}
		CreatePermissionGroup(permissionGroupGroupGuest, dbInfo)

		err = dbInfo.MetaInfoDb.CreateCollection(context.TODO(), SwcMetaInfoCollectionString)
		if err != nil {
			logger.GetLogger().Fatal(err)
		}

		opts := options.CreateCollection().SetCapped(true).SetMaxDocuments(1000).SetSizeInBytes(100 * 1024 * 1025)
		err = dbInfo.MetaInfoDb.CreateCollection(context.TODO(), DailyStatisticsMetaInfoCollectionString, opts)
		if err != nil {
			logger.GetLogger().Fatal(err)
		}
	}

	databaseSwcDataExists := false
	for _, dbName := range databaseNames {
		if dbName == dataBaseNameInfo.SwcDataBaseName {
			databaseSwcDataExists = true
			break
		}
	}
	if databaseSwcDataExists && !deleteIfExist {
		logger.GetLogger().Printf("Database %s exists! Do not create a new one!\n", dataBaseNameInfo.SwcDataBaseName)
	} else {
		if databaseSwcDataExists && deleteIfExist {
			err := dbInfo.SwcDb.Drop(context.TODO())
			if err != nil {
				logger.GetLogger().Fatal("Delete exist swc database failed")
			}
		}
		logger.GetLogger().Printf("Database %s does not exist. Will create new one when needed!\n", dataBaseNameInfo.SwcDataBaseName)
	}

	databaseSwcSnapshotExists := false
	for _, dbName := range databaseNames {
		if dbName == dataBaseNameInfo.SwcSnapshotDataBaseName {
			databaseSwcSnapshotExists = true
			break
		}
	}
	if databaseSwcSnapshotExists && !deleteIfExist {
		logger.GetLogger().Printf("Database %s exists! Do not create a new one!\n", dataBaseNameInfo.SwcSnapshotDataBaseName)
	} else {
		if databaseSwcSnapshotExists && deleteIfExist {
			err := dbInfo.SwcDb.Drop(context.TODO())
			if err != nil {
				logger.GetLogger().Fatal("Delete exist swc snapshot database failed")
			}
		}
		logger.GetLogger().Printf("Database %s does not exist. Will create new one when needed!\n", dataBaseNameInfo.SwcSnapshotDataBaseName)
	}

	databaseSwcIncrementOperationExists := false
	for _, dbName := range databaseNames {
		if dbName == dataBaseNameInfo.SwcIncrementOperationDataBaseName {
			databaseSwcIncrementOperationExists = true
			break
		}
	}
	if databaseSwcIncrementOperationExists && !deleteIfExist {
		logger.GetLogger().Printf("Database %s exists! Do not create a new one!\n", dataBaseNameInfo.SwcIncrementOperationDataBaseName)
	} else {
		if databaseSwcIncrementOperationExists && deleteIfExist {
			err := dbInfo.SwcDb.Drop(context.TODO())
			if err != nil {
				logger.GetLogger().Fatal("Delete exist swc increment operation database failed")
			}
		}
		logger.GetLogger().Printf("Database %s does not exist. Will create new one when needed!\n", dataBaseNameInfo.SwcIncrementOperationDataBaseName)
	}

	databaseSwcAttachmentExists := false
	for _, dbName := range databaseNames {
		if dbName == dataBaseNameInfo.SwcAttachmentDataBaseName {
			databaseSwcAttachmentExists = true
			break
		}
	}
	if databaseSwcAttachmentExists && !deleteIfExist {
		logger.GetLogger().Printf("Database %s exists! Do not create a new one!\n", dataBaseNameInfo.SwcAttachmentDataBaseName)
	} else {
		if databaseSwcAttachmentExists && deleteIfExist {
			err := dbInfo.SwcDb.Drop(context.TODO())
			if err != nil {
				logger.GetLogger().Fatal("Delete exist swc increment operation database failed")
			}
		}
		logger.GetLogger().Printf("Database %s does not exist. Will create new one when needed!\n", dataBaseNameInfo.SwcAttachmentDataBaseName)
	}
}
