package dal

import (
	"DBMS/config"
	"DBMS/dbmodel"
	"context"
	"github.com/google/uuid"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo/options"
	"log"
	"time"
)

var globalMongodbdatabaseinfo MongoDbDataBaseInfo

func SetDbInstance(instance MongoDbDataBaseInfo) {
	globalMongodbdatabaseinfo = instance
}

func GetDbInstance() MongoDbDataBaseInfo {
	return globalMongodbdatabaseinfo
}

func InitializeNewDataBaseIfNotExist(metaInfoDataBaseName string, swcDataBaseName string, swcSnapshotDataBaseName string, swcIncrementOperationDataBaseName string, swcAttachmentDataBaseName string) {
	createInfo := MongoDbConnectionCreateInfo{
		Host:     config.AppConfig.MongodbIP,
		Port:     config.AppConfig.MongodbPort,
		User:     config.AppConfig.MongodbUser,
		Password: config.AppConfig.MongodbPassword,
	}

	connectionInfo := ConnectToMongoDb(createInfo)

	if connectionInfo.Err != nil {
		log.Fatal(connectionInfo.Err)
	}

	var dbInfo MongoDbDataBaseInfo
	dbInfo.MetaInfoDb = connectionInfo.Client.Database(metaInfoDataBaseName)
	dbInfo.SwcDb = connectionInfo.Client.Database(swcDataBaseName)
	dbInfo.SnapshotDb = connectionInfo.Client.Database(swcSnapshotDataBaseName)
	dbInfo.IncrementOperationDb = connectionInfo.Client.Database(swcIncrementOperationDataBaseName)

	var deleteIfExist bool
	deleteIfExist = false

	databaseNames, err := connectionInfo.Client.ListDatabaseNames(context.TODO(), bson.M{})
	if err != nil {
		log.Fatal(err)
	}

	databaseMetaInfoExists := false
	for _, dbName := range databaseNames {
		if dbName == metaInfoDataBaseName {
			databaseMetaInfoExists = true
			break
		}
	}
	if databaseMetaInfoExists && !deleteIfExist {
		log.Printf("Database %s exists! Do not create a new one!\n", metaInfoDataBaseName)

	} else {
		if databaseMetaInfoExists && deleteIfExist {
			err := dbInfo.MetaInfoDb.Drop(context.TODO())
			if err != nil {
				log.Fatal("Delete exist meta info database failed")
			}
		}
		log.Printf("Database %s does not exist. Start to create a new one!\n", metaInfoDataBaseName)

		var err error

		err = dbInfo.MetaInfoDb.CreateCollection(context.TODO(), MetaInfoDbStatusCollectonString)
		if err != nil {
			log.Fatal(err)
		}

		err = dbInfo.MetaInfoDb.CreateCollection(context.TODO(), ProjectMetaInfoCollectionString)
		if err != nil {
			log.Fatal(err)
		}

		err = dbInfo.MetaInfoDb.CreateCollection(context.TODO(), UserMetaInfoCollectionString)
		if err != nil {
			log.Fatal(err)
		}

		_, userId := GetNewUserIdAndIncrease(dbInfo)
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
			UserPermissionGroup: PermissionGroupAdmin,
			UserId:              userId,
		}
		CreateUser(serverUser, dbInfo)

		err = dbInfo.MetaInfoDb.CreateCollection(context.TODO(), PermissionGroupMetaInfoCollectioString)
		if err != nil {
			log.Fatal(err)
		}

		var permissionGroupAdmin = dbmodel.PermissionGroupMetaInfoV1{
			Base: dbmodel.MetaInfoBase{
				Id:                     primitive.NewObjectID(),
				DataAccessModelVersion: "V1",
				Uuid:                   uuid.NewString(),
			},
			Name:        PermissionGroupAdmin,
			Description: "Admin Permission Group",
			Global: dbmodel.GlobalPermissionMetaInfoV1{
				WritePermissionCreateProject: true,
				WritePermissionModifyProject: true,
				WritePermissionDeleteProject: true,
				ReadPerimissionQuery:         true,
			},
			Project: dbmodel.ProjectPermissionMetaInfoV1{
				WritePermissionAddData:    true,
				WritePermissionModifyData: true,
				WritePermissionDeleteData: true,
				ReadPerimissionQuery:      true,
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
			Global: dbmodel.GlobalPermissionMetaInfoV1{
				WritePermissionCreateProject: false,
				WritePermissionModifyProject: false,
				WritePermissionDeleteProject: false,
				ReadPerimissionQuery:         true,
			},
			Project: dbmodel.ProjectPermissionMetaInfoV1{
				WritePermissionAddData:    true,
				WritePermissionModifyData: true,
				WritePermissionDeleteData: true,
				ReadPerimissionQuery:      true,
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
			Global: dbmodel.GlobalPermissionMetaInfoV1{
				WritePermissionCreateProject: true,
				WritePermissionModifyProject: true,
				WritePermissionDeleteProject: true,
				ReadPerimissionQuery:         true,
			},
			Project: dbmodel.ProjectPermissionMetaInfoV1{
				WritePermissionAddData:    true,
				WritePermissionModifyData: true,
				WritePermissionDeleteData: true,
				ReadPerimissionQuery:      true,
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
			Global: dbmodel.GlobalPermissionMetaInfoV1{
				WritePermissionCreateProject: false,
				WritePermissionModifyProject: false,
				WritePermissionDeleteProject: false,
				ReadPerimissionQuery:         true,
			},
			Project: dbmodel.ProjectPermissionMetaInfoV1{
				WritePermissionAddData:    true,
				WritePermissionModifyData: true,
				WritePermissionDeleteData: true,
				ReadPerimissionQuery:      true,
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
			Global: dbmodel.GlobalPermissionMetaInfoV1{
				WritePermissionCreateProject: false,
				WritePermissionModifyProject: false,
				WritePermissionDeleteProject: false,
				ReadPerimissionQuery:         true,
			},
			Project: dbmodel.ProjectPermissionMetaInfoV1{
				WritePermissionAddData:    false,
				WritePermissionModifyData: false,
				WritePermissionDeleteData: false,
				ReadPerimissionQuery:      true,
			},
		}
		CreatePermissionGroup(permissionGroupGroupGuest, dbInfo)

		err = dbInfo.MetaInfoDb.CreateCollection(context.TODO(), SwcMetaInfoCollectionString)
		if err != nil {
			log.Fatal(err)
		}

		opts := options.CreateCollection().SetCapped(true).SetMaxDocuments(1000).SetSizeInBytes(100 * 1024 * 1025)
		err = dbInfo.MetaInfoDb.CreateCollection(context.TODO(), DailyStatisticsMetaInfoCollectionString, opts)
		if err != nil {
			log.Fatal(err)
		}
	}

	databaseSwcDataExists := false
	for _, dbName := range databaseNames {
		if dbName == swcDataBaseName {
			databaseSwcDataExists = true
			break
		}
	}
	if databaseSwcDataExists && !deleteIfExist {
		log.Printf("Database %s exists! Do not create a new one!\n", swcDataBaseName)
	} else {
		if databaseSwcDataExists && deleteIfExist {
			err := dbInfo.SwcDb.Drop(context.TODO())
			if err != nil {
				log.Fatal("Delete exist swc database failed")
			}
		}
		log.Printf("Database %s does not exist. Will create new one when needed!\n", swcDataBaseName)
	}

	databaseSwcSnapshotExists := false
	for _, dbName := range databaseNames {
		if dbName == swcSnapshotDataBaseName {
			databaseSwcSnapshotExists = true
			break
		}
	}
	if databaseSwcSnapshotExists && !deleteIfExist {
		log.Printf("Database %s exists! Do not create a new one!\n", swcSnapshotDataBaseName)
	} else {
		if databaseSwcSnapshotExists && deleteIfExist {
			err := dbInfo.SwcDb.Drop(context.TODO())
			if err != nil {
				log.Fatal("Delete exist swc snapshot database failed")
			}
		}
		log.Printf("Database %s does not exist. Will create new one when needed!\n", swcSnapshotDataBaseName)
	}

	databaseSwcIncrementOperationExists := false
	for _, dbName := range databaseNames {
		if dbName == swcIncrementOperationDataBaseName {
			databaseSwcIncrementOperationExists = true
			break
		}
	}
	if databaseSwcIncrementOperationExists && !deleteIfExist {
		log.Printf("Database %s exists! Do not create a new one!\n", swcIncrementOperationDataBaseName)
	} else {
		if databaseSwcIncrementOperationExists && deleteIfExist {
			err := dbInfo.SwcDb.Drop(context.TODO())
			if err != nil {
				log.Fatal("Delete exist swc increment operation database failed")
			}
		}
		log.Printf("Database %s does not exist. Will create new one when needed!\n", swcIncrementOperationDataBaseName)
	}

	databaseSwcAttachmentExists := false
	for _, dbName := range databaseNames {
		if dbName == swcAttachmentDataBaseName {
			databaseSwcAttachmentExists = true
			break
		}
	}
	if databaseSwcAttachmentExists && !deleteIfExist {
		log.Printf("Database %s exists! Do not create a new one!\n", swcAttachmentDataBaseName)
	} else {
		if databaseSwcAttachmentExists && deleteIfExist {
			err := dbInfo.SwcDb.Drop(context.TODO())
			if err != nil {
				log.Fatal("Delete exist swc increment operation database failed")
			}
		}
		log.Printf("Database %s does not exist. Will create new one when needed!\n", swcAttachmentDataBaseName)
	}

}
