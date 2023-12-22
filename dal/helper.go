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
)

var g_MongoDbDataBaseInfo MongoDbDataBaseInfo

func SetDbInstance(instance MongoDbDataBaseInfo) {
	g_MongoDbDataBaseInfo = instance
}

func GetDbInstance() MongoDbDataBaseInfo {
	return g_MongoDbDataBaseInfo
}

func InitializeNewDataBaseIfNotExist(metaInfoDataBaseName string, swcDataBaseName string) {
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
		err = dbInfo.MetaInfoDb.CreateCollection(context.TODO(), dbmodel.ProjectMetaInfoCollectionString)
		if err != nil {
			log.Fatal(err)
		}

		err = dbInfo.MetaInfoDb.CreateCollection(context.TODO(), dbmodel.UserMetaInfoCollectionString)
		if err != nil {
			log.Fatal(err)
		}

		err = dbInfo.MetaInfoDb.CreateCollection(context.TODO(), dbmodel.PermissionGroupMetaInfoCollectioString)
		if err != nil {
			log.Fatal(err)
		}

		var permissionGroupAdmin = dbmodel.PermissionGroupMetaInfoV1{
			Base: dbmodel.MetaInfoBase{
				Id:         primitive.NewObjectID(),
				ApiVersion: "V1",
				Uuid:       uuid.NewString(),
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
				Id:         primitive.NewObjectID(),
				ApiVersion: "V1",
				Uuid:       uuid.NewString(),
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
				Id:         primitive.NewObjectID(),
				ApiVersion: "V1",
				Uuid:       uuid.NewString(),
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
				Id:         primitive.NewObjectID(),
				ApiVersion: "V1",
				Uuid:       uuid.NewString(),
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
				Id:         primitive.NewObjectID(),
				ApiVersion: "V1",
				Uuid:       uuid.NewString(),
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

		err = dbInfo.MetaInfoDb.CreateCollection(context.TODO(), dbmodel.SwcMetaInfoCollectionString)
		if err != nil {
			log.Fatal(err)
		}

		opts := options.CreateCollection().SetCapped(true).SetMaxDocuments(1000).SetSizeInBytes(100 * 1024 * 1025)
		err = dbInfo.MetaInfoDb.CreateCollection(context.TODO(), dbmodel.DailyStatisticsMetaInfoCollectionString, opts)
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

}
