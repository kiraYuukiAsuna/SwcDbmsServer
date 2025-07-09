package dal

import (
	"DBMS/dbmodel"
	"DBMS/logger"
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func ConnectToMongoDb(createInfo MongoDbConnectionCreateInfo) MongoDbConnectionInfo {
	//mongodb://defaultuser:defaultpassword@localhost:27017/?authMechanism=DEFAULT
	//url := "mongodb://" + createInfo.user + ":" + createInfo.password + "@" + createInfo.host + ":" + string(createInfo.port) + "/?authMechanism=DEFAULT"
	url := "mongodb://" + createInfo.Host + ":" + strconv.Itoa(int(createInfo.Port))
	var connectionInfo MongoDbConnectionInfo

	credential := options.Credential{
		Username: createInfo.User,
		Password: createInfo.Password,
	}
	clientOpts := options.Client().ApplyURI(url).
		SetAuth(credential).SetConnectTimeout(10 * time.Second)

	connectionInfo.Client, connectionInfo.Err = mongo.Connect(context.TODO(), clientOpts)
	if connectionInfo.Err != nil {
		logger.GetLogger().Fatal(connectionInfo.Err)
		return connectionInfo
	}

	var err = connectionInfo.Client.Ping(context.TODO(), nil)

	if err != nil {
		logger.GetLogger().Fatal(err)
		return connectionInfo
	}

	return connectionInfo
}

func ConnectToDataBase(connectionInfo MongoDbConnectionInfo, dataBaseNameInfo DataBaseNameInfo) MongoDbDataBaseInfo {
	if connectionInfo.Err != nil {
		logger.GetLogger().Fatal(connectionInfo.Err)
		return MongoDbDataBaseInfo{}
	}

	var dbInfo MongoDbDataBaseInfo

	dbInfo.MetaInfoDb = connectionInfo.Client.Database(dataBaseNameInfo.MetaInfoDataBaseName)
	dbInfo.SwcDb = connectionInfo.Client.Database(dataBaseNameInfo.SwcDataBaseName)
	dbInfo.SnapshotDb = connectionInfo.Client.Database(dataBaseNameInfo.SwcSnapshotDataBaseName)
	dbInfo.IncrementOperationDb = connectionInfo.Client.Database(dataBaseNameInfo.SwcIncrementOperationDataBaseName)
	dbInfo.AttachmentDb = connectionInfo.Client.Database(dataBaseNameInfo.SwcAttachmentDataBaseName)

	return dbInfo
}

func EnsureUniqueUUIDIndex(collection *mongo.Collection) error {
	indexName := "uuid_unique"
	ctx := context.Background()

	// 列出所有现有索引
	cursor, err := collection.Indexes().List(ctx)
	if err != nil {
		return fmt.Errorf("failed to list indexes: %v", err)
	}
	defer cursor.Close(ctx)

	// 检查是否已存在所需的索引
	for cursor.Next(ctx) {
		var index bson.M
		if err := cursor.Decode(&index); err != nil {
			return fmt.Errorf("failed to decode index: %v", err)
		}

		if indexInfo, ok := index["name"].(string); ok && indexInfo == indexName {
			// logger.GetLogger().Printf("Index '%s' already exists", indexName)
			return nil
		}
	}

	// 如果索引不存在，创建它
	indexModel := mongo.IndexModel{
		Keys: bson.D{{"uuid", 1}},
		Options: options.Index().
			SetName(indexName).
			SetUnique(true),
	}

	_, err = collection.Indexes().CreateOne(ctx, indexModel)
	if err != nil {
		return fmt.Errorf("failed to create index: %v", err)
	}

	logger.GetLogger().Printf("Successfully created unique index '%s' on 'uuid' field. Collection Name '%s'", indexName, collection.Name())
	return nil
}

func CreateProject(projectMetaInfo dbmodel.ProjectMetaInfoV1, databaseInfo MongoDbDataBaseInfo) ReturnWrapper {
	var projectCollection = databaseInfo.MetaInfoDb.Collection(ProjectMetaInfoCollectionString)

	result := projectCollection.FindOne(context.TODO(), bson.D{
		{"Name", projectMetaInfo.Name}})

	if result.Err() != nil {
		if errors.Is(result.Err(), mongo.ErrNoDocuments) {
			_, err := projectCollection.InsertOne(context.TODO(), projectMetaInfo)
			if err != nil {
				return ReturnWrapper{false, "Create user failed! Error:" + err.Error()}
			}
			return ReturnWrapper{true, "Create project successfully!"}
		}
		return ReturnWrapper{false, "Unknown error!"}
	} else {
		// find one means already exist
		return ReturnWrapper{false, "Project already exist!"}
	}

}

func DeleteProject(projectMetaInfo dbmodel.ProjectMetaInfoV1, databaseInfo MongoDbDataBaseInfo) ReturnWrapper {
	var projectCollection = databaseInfo.MetaInfoDb.Collection(ProjectMetaInfoCollectionString)
	_ = EnsureUniqueUUIDIndex(projectCollection)

	result := projectCollection.FindOneAndDelete(context.TODO(), bson.D{
		{"uuid", projectMetaInfo.Base.Uuid}})

	if result.Err() != nil {
		return ReturnWrapper{false, result.Err().Error()}
	} else {
		return ReturnWrapper{true, "Delete successfully!"}
	}
}

func ModifyProject(projectMetaInfo dbmodel.ProjectMetaInfoV1, databaseInfo MongoDbDataBaseInfo) ReturnWrapper {
	var projectCollection = databaseInfo.MetaInfoDb.Collection(ProjectMetaInfoCollectionString)
	_ = EnsureUniqueUUIDIndex(projectCollection)

	result := projectCollection.FindOneAndReplace(
		context.TODO(),
		bson.D{{"uuid", projectMetaInfo.Base.Uuid}},
		projectMetaInfo)

	if result.Err() != nil {
		return ReturnWrapper{false, "Update project info failed! Error:" + result.Err().Error()}
	} else {
		return ReturnWrapper{true, "Update project info success!"}
	}

}

func QueryProject(projectMetaInfo *dbmodel.ProjectMetaInfoV1, databaseInfo MongoDbDataBaseInfo) ReturnWrapper {
	var projectCollection = databaseInfo.MetaInfoDb.Collection(ProjectMetaInfoCollectionString)
	_ = EnsureUniqueUUIDIndex(projectCollection)

	result := projectCollection.FindOne(
		context.TODO(),
		bson.D{{"uuid", projectMetaInfo.Base.Uuid}})

	if result.Err() != nil {
		return ReturnWrapper{false, "Cannot find target project!"}
	} else {
		err := result.Decode(projectMetaInfo)
		if err != nil {
			return ReturnWrapper{false, err.Error()}
		} else {
			return ReturnWrapper{true, ""}
		}
	}
}

func QueryAllProject(projectMetaInfoList *[]dbmodel.ProjectMetaInfoV1, databaseInfo MongoDbDataBaseInfo) ReturnWrapper {
	var projectCollection = databaseInfo.MetaInfoDb.Collection(ProjectMetaInfoCollectionString)

	cursor, err := projectCollection.Find(
		context.TODO(),
		bson.D{})

	if err != nil {
		return ReturnWrapper{false, "Query all Project failed!"}
	}

	if err = cursor.All(context.TODO(), projectMetaInfoList); err != nil {
		logger.GetLogger().Println(err.Error())
		return ReturnWrapper{false, "Query all Project failed!"}
	}

	return ReturnWrapper{true, "Query all Project Success"}
}

func CreateUser(userMetaInfo dbmodel.UserMetaInfoV1, databaseInfo MongoDbDataBaseInfo) ReturnWrapper {
	var userCollection = databaseInfo.MetaInfoDb.Collection(UserMetaInfoCollectionString)

	result := userCollection.FindOne(context.TODO(), bson.D{
		{"Name", userMetaInfo.Name},
	})

	if result.Err() != nil {
		if errors.Is(result.Err(), mongo.ErrNoDocuments) {
			_, err := userCollection.InsertOne(context.TODO(), userMetaInfo)
			if err != nil {
				return ReturnWrapper{false, "Create user failed! Error:" + err.Error()}
			}
			return ReturnWrapper{true, "Create user successfully!"}
		}
		return ReturnWrapper{false, "Unknown error!"}
	} else {
		// find one means already exist
		return ReturnWrapper{false, "User already exist!"}
	}

}

func DeleteUser(userMetaInfo dbmodel.UserMetaInfoV1, databaseInfo MongoDbDataBaseInfo) ReturnWrapper {
	var userCollection = databaseInfo.MetaInfoDb.Collection(UserMetaInfoCollectionString)
	_ = EnsureUniqueUUIDIndex(userCollection)

	result := userCollection.FindOneAndDelete(context.TODO(), bson.D{
		{"Name", userMetaInfo.Name},
	})

	if result.Err() != nil {
		return ReturnWrapper{false, result.Err().Error()}
	} else {
		return ReturnWrapper{true, "Delete successfully!"}
	}
}

func ModifyUser(userMetaInfo dbmodel.UserMetaInfoV1, databaseInfo MongoDbDataBaseInfo) ReturnWrapper {
	var userCollection = databaseInfo.MetaInfoDb.Collection(UserMetaInfoCollectionString)
	_ = EnsureUniqueUUIDIndex(userCollection)

	result := userCollection.FindOneAndReplace(
		context.TODO(),
		bson.D{{"Name", userMetaInfo.Name}},
		userMetaInfo)

	if result.Err() != nil {
		return ReturnWrapper{false, "Update user info failed! Error:" + result.Err().Error()}
	} else {
		return ReturnWrapper{true, "Update user info success!"}
	}

}

func QueryUserByUuid(userMetaInfo *dbmodel.UserMetaInfoV1, databaseInfo MongoDbDataBaseInfo) ReturnWrapper {
	var userCollection = databaseInfo.MetaInfoDb.Collection(UserMetaInfoCollectionString)
	_ = EnsureUniqueUUIDIndex(userCollection)

	result := userCollection.FindOne(
		context.TODO(),
		bson.D{{"uuid", userMetaInfo.Base.Uuid}})

	if result.Err() != nil {
		return ReturnWrapper{false, "Cannot find target user!"}
	} else {
		err := result.Decode(userMetaInfo)
		if err != nil {
			return ReturnWrapper{false, err.Error()}
		} else {
			return ReturnWrapper{true, ""}
		}
	}
}

func QueryUserByName(userMetaInfo *dbmodel.UserMetaInfoV1, databaseInfo MongoDbDataBaseInfo) ReturnWrapper {
	var userCollection = databaseInfo.MetaInfoDb.Collection(UserMetaInfoCollectionString)
	_ = EnsureUniqueUUIDIndex(userCollection)

	result := userCollection.FindOne(
		context.TODO(),
		bson.D{{"Name", userMetaInfo.Name}})

	if result.Err() != nil {
		return ReturnWrapper{false, "Cannot find target user!"}
	} else {
		err := result.Decode(userMetaInfo)
		if err != nil {
			return ReturnWrapper{false, err.Error()}
		} else {
			return ReturnWrapper{true, ""}
		}
	}
}

func QueryAllUser(userMetaInfoList *[]dbmodel.UserMetaInfoV1, databaseInfo MongoDbDataBaseInfo) ReturnWrapper {
	var userCollection = databaseInfo.MetaInfoDb.Collection(UserMetaInfoCollectionString)

	cursor, err := userCollection.Find(
		context.TODO(),
		bson.D{})

	if err != nil {
		return ReturnWrapper{false, "Query all user failed!"}
	}

	if err = cursor.All(context.TODO(), userMetaInfoList); err != nil {
		logger.GetLogger().Println(err.Error())
		return ReturnWrapper{false, "Query all user failed!"}
	}

	return ReturnWrapper{true, "Query all user Success"}
}

func CreatePermissionGroup(permissionGroupMetaInfo dbmodel.PermissionGroupMetaInfoV1, databaseInfo MongoDbDataBaseInfo) ReturnWrapper {
	var permissionGroupCollection = databaseInfo.MetaInfoDb.Collection(PermissionGroupMetaInfoCollectioString)

	result := permissionGroupCollection.FindOne(context.TODO(), bson.D{
		{"Name", permissionGroupMetaInfo.Name},
	})

	if result.Err() != nil {
		if errors.Is(result.Err(), mongo.ErrNoDocuments) {
			_, err := permissionGroupCollection.InsertOne(context.TODO(), permissionGroupMetaInfo)
			if err != nil {
				return ReturnWrapper{false, "Create user failed! Error:" + err.Error()}
			}
			return ReturnWrapper{true, "Create permission group successfully!"}
		}
		return ReturnWrapper{false, "Unknown error!"}
	} else {
		// find one means already exist
		return ReturnWrapper{false, "Permission group already exist!"}
	}

}

func DeletePermissionGroup(permissionGroupMetaInfo dbmodel.PermissionGroupMetaInfoV1, databaseInfo MongoDbDataBaseInfo) ReturnWrapper {
	var permissionGroupCollection = databaseInfo.MetaInfoDb.Collection(PermissionGroupMetaInfoCollectioString)
	_ = EnsureUniqueUUIDIndex(permissionGroupCollection)

	result := permissionGroupCollection.FindOneAndDelete(context.TODO(), bson.D{
		{"uuid", permissionGroupMetaInfo.Base.Uuid},
	})

	if result.Err() != nil {
		return ReturnWrapper{false, result.Err().Error()}
	} else {
		return ReturnWrapper{true, "Delete successfully!"}
	}
}

func ModifyPermissionGroup(permissionGroupMetaInfo dbmodel.PermissionGroupMetaInfoV1, databaseInfo MongoDbDataBaseInfo) ReturnWrapper {
	var permissionGroupCollection = databaseInfo.MetaInfoDb.Collection(PermissionGroupMetaInfoCollectioString)
	_ = EnsureUniqueUUIDIndex(permissionGroupCollection)

	result := permissionGroupCollection.FindOneAndReplace(
		context.TODO(),
		bson.D{{"uuid", permissionGroupMetaInfo.Base.Uuid}},
		permissionGroupMetaInfo)

	if result.Err() != nil {
		return ReturnWrapper{false, "Update permission group failed! Error:" + result.Err().Error()}
	} else {
		return ReturnWrapper{true, "Update permission group success!"}
	}

}

func QueryPermissionGroupByUuid(permissionGroupMetaInfo *dbmodel.PermissionGroupMetaInfoV1, databaseInfo MongoDbDataBaseInfo) ReturnWrapper {
	var permissionGroupCollection = databaseInfo.MetaInfoDb.Collection(PermissionGroupMetaInfoCollectioString)
	_ = EnsureUniqueUUIDIndex(permissionGroupCollection)

	result := permissionGroupCollection.FindOne(
		context.TODO(),
		bson.D{{"uuid", permissionGroupMetaInfo.Base.Uuid}})

	if result.Err() != nil {
		return ReturnWrapper{false, "Cannot find target permission group!"}
	} else {
		err := result.Decode(permissionGroupMetaInfo)
		if err != nil {
			return ReturnWrapper{false, err.Error()}
		} else {
			return ReturnWrapper{true, ""}
		}
	}
}

func QueryPermissionGroupByName(permissionGroupMetaInfo *dbmodel.PermissionGroupMetaInfoV1, databaseInfo MongoDbDataBaseInfo) ReturnWrapper {
	var permissionGroupCollection = databaseInfo.MetaInfoDb.Collection(PermissionGroupMetaInfoCollectioString)
	_ = EnsureUniqueUUIDIndex(permissionGroupCollection)

	result := permissionGroupCollection.FindOne(
		context.TODO(),
		bson.D{{"Name", permissionGroupMetaInfo.Name}})

	if result.Err() != nil {
		return ReturnWrapper{false, "Cannot find target permission group!"}
	} else {
		err := result.Decode(permissionGroupMetaInfo)
		if err != nil {
			return ReturnWrapper{false, err.Error()}
		} else {
			return ReturnWrapper{true, ""}
		}
	}
}

func QueryAllPermissionGroup(permissionGroupList *[]dbmodel.PermissionGroupMetaInfoV1, databaseInfo MongoDbDataBaseInfo) ReturnWrapper {
	var permissionGroupCollection = databaseInfo.MetaInfoDb.Collection(PermissionGroupMetaInfoCollectioString)

	cursor, err := permissionGroupCollection.Find(
		context.TODO(),
		bson.D{})

	if err != nil {
		return ReturnWrapper{false, "Query all PermissionGroup failed!"}
	}

	if err = cursor.All(context.TODO(), permissionGroupList); err != nil {
		logger.GetLogger().Println(err.Error())
		return ReturnWrapper{false, "Query all PermissionGroup failed!"}
	}

	return ReturnWrapper{true, "Query all PermissionGroup Success"}
}

func CreateSwc(swcMetaInfo dbmodel.SwcMetaInfoV1, databaseInfo MongoDbDataBaseInfo) ReturnWrapper {
	var swcCollection = databaseInfo.MetaInfoDb.Collection(SwcMetaInfoCollectionString)

	result := swcCollection.FindOne(context.TODO(), bson.D{
		{"uuid", swcMetaInfo.Base.Uuid},
	})

	if result.Err() != nil {
		if errors.Is(result.Err(), mongo.ErrNoDocuments) {
			_, err := swcCollection.InsertOne(context.TODO(), swcMetaInfo)
			if err != nil {
				return ReturnWrapper{false, "Create swc failed! Error:" + err.Error()}
			}
			return ReturnWrapper{true, "Create swc successfully!"}
		}
		return ReturnWrapper{false, "Unknown error!"}
	} else {
		// find one means already exist
		return ReturnWrapper{false, "Swc already exist!"}
	}

}

func DeleteSwc(swcMetaInfo dbmodel.SwcMetaInfoV1, databaseInfo MongoDbDataBaseInfo) ReturnWrapper {
	var swcCollection = databaseInfo.MetaInfoDb.Collection(SwcMetaInfoCollectionString)
	_ = EnsureUniqueUUIDIndex(swcCollection)

	result := swcCollection.FindOneAndDelete(context.TODO(), bson.D{
		{"uuid", swcMetaInfo.Base.Uuid},
	})

	if result.Err() != nil {
		return ReturnWrapper{false, result.Err().Error()}
	} else {
		return ReturnWrapper{true, "Delete successfully!"}
	}
}

func ModifySwc(swcMetaInfo dbmodel.SwcMetaInfoV1, databaseInfo MongoDbDataBaseInfo) ReturnWrapper {
	var swcCollection = databaseInfo.MetaInfoDb.Collection(SwcMetaInfoCollectionString)
	_ = EnsureUniqueUUIDIndex(swcCollection)

	result := swcCollection.FindOneAndReplace(
		context.TODO(),
		bson.D{{"uuid", swcMetaInfo.Base.Uuid}},
		swcMetaInfo)

	if result.Err() != nil {
		return ReturnWrapper{false, "Update swc failed! Error:" + result.Err().Error()}
	} else {
		return ReturnWrapper{true, "Update swc success!"}
	}

}

func QuerySwc(swcMetaInfo *dbmodel.SwcMetaInfoV1, databaseInfo MongoDbDataBaseInfo) ReturnWrapper {
	var swcCollection = databaseInfo.MetaInfoDb.Collection(SwcMetaInfoCollectionString)
	_ = EnsureUniqueUUIDIndex(swcCollection)

	result := swcCollection.FindOne(
		context.TODO(),
		bson.D{{"uuid", swcMetaInfo.Base.Uuid}})

	if result.Err() != nil {
		return ReturnWrapper{false, "Cannot find target swc!"}
	} else {
		err := result.Decode(swcMetaInfo)
		if err != nil {
			return ReturnWrapper{false, err.Error()}
		} else {
			return ReturnWrapper{true, ""}
		}
	}
}

func QueryAllSwc(swcMetaInfoList *[]dbmodel.SwcMetaInfoV1, databaseInfo MongoDbDataBaseInfo) ReturnWrapper {
	var swcCollection = databaseInfo.MetaInfoDb.Collection(SwcMetaInfoCollectionString)

	cursor, err := swcCollection.Find(
		context.TODO(),
		bson.D{})

	if err != nil {
		return ReturnWrapper{false, "Query all swc failed!"}
	}

	if err = cursor.All(context.TODO(), swcMetaInfoList); err != nil {
		logger.GetLogger().Println(err.Error())
		return ReturnWrapper{false, "Query all swc failed!"}
	}

	return ReturnWrapper{true, "Query all swc Success"}
}

func QueryAllFreeSwc(swcMetaInfoList *[]dbmodel.SwcMetaInfoV1, databaseInfo MongoDbDataBaseInfo) ReturnWrapper {
	var swcCollection = databaseInfo.MetaInfoDb.Collection(SwcMetaInfoCollectionString)

	cursor, err := swcCollection.Find(
		context.TODO(),
		bson.M{"BelongingProjectUuid": ""})

	if err != nil {
		return ReturnWrapper{false, "Query all free swc failed!"}
	}

	if err = cursor.All(context.TODO(), swcMetaInfoList); err != nil {
		logger.GetLogger().Println(err.Error())
		return ReturnWrapper{false, "Query all free swc failed!"}
	}

	return ReturnWrapper{true, "Query all free swc Success"}
}

func CreateDailyStatistics(dailyStatisticsMetaInfo dbmodel.DailyStatisticsMetaInfoV1, databaseInfo MongoDbDataBaseInfo) ReturnWrapper {
	var dailyStatisticsCollection = databaseInfo.MetaInfoDb.Collection(DailyStatisticsMetaInfoCollectionString)

	result := dailyStatisticsCollection.FindOne(context.TODO(), bson.D{
		{"Name", dailyStatisticsMetaInfo.Name},
	})

	if result.Err() != nil {
		if errors.Is(result.Err(), mongo.ErrNoDocuments) {
			_, err := dailyStatisticsCollection.InsertOne(context.TODO(), dailyStatisticsMetaInfo)
			if err != nil {
				return ReturnWrapper{false, "Create daily statistics failed! Error:" + err.Error()}
			}
			return ReturnWrapper{true, "Create daily statistics successfully!"}
		}
		return ReturnWrapper{false, "Unknown error!"}
	} else {
		// find one means already exist
		return ReturnWrapper{false, "Daily statistics already exist!"}
	}

}

func DeleteDailyStatistics(dailyStatisticsMetaInfo dbmodel.DailyStatisticsMetaInfoV1, databaseInfo MongoDbDataBaseInfo) ReturnWrapper {
	var dailyStatisticsCollection = databaseInfo.MetaInfoDb.Collection(DailyStatisticsMetaInfoCollectionString)
	_ = EnsureUniqueUUIDIndex(dailyStatisticsCollection)

	result := dailyStatisticsCollection.FindOneAndDelete(context.TODO(), bson.D{
		{"Name", dailyStatisticsMetaInfo.Name},
	})

	if result.Err() != nil {
		return ReturnWrapper{false, result.Err().Error()}
	} else {
		return ReturnWrapper{true, "Delete successfully!"}
	}
}

func ModifyDailyStatistics(dailyStatisticsMetaInfo dbmodel.DailyStatisticsMetaInfoV1, databaseInfo MongoDbDataBaseInfo) ReturnWrapper {
	var dailyStatisticsCollection = databaseInfo.MetaInfoDb.Collection(DailyStatisticsMetaInfoCollectionString)
	_ = EnsureUniqueUUIDIndex(dailyStatisticsCollection)

	result := dailyStatisticsCollection.FindOneAndReplace(
		context.TODO(),
		bson.D{{"Name", dailyStatisticsMetaInfo.Name}},
		dailyStatisticsMetaInfo)

	if result.Err() != nil {
		return ReturnWrapper{false, "Update daily statistics failed! Error:" + result.Err().Error()}
	} else {
		return ReturnWrapper{true, "Update daily statistics success!"}
	}

}

func QueryDailyStatistics(permissionGroupMetaInfo *dbmodel.DailyStatisticsMetaInfoV1, databaseInfo MongoDbDataBaseInfo) ReturnWrapper {
	var dailyStatisticsCollection = databaseInfo.MetaInfoDb.Collection(DailyStatisticsMetaInfoCollectionString)
	_ = EnsureUniqueUUIDIndex(dailyStatisticsCollection)

	result := dailyStatisticsCollection.FindOne(
		context.TODO(),
		bson.D{{"Name", permissionGroupMetaInfo.Name}})

	if result.Err() != nil {
		return ReturnWrapper{false, "Cannot find target daily statistics!"}
	} else {
		err := result.Decode(permissionGroupMetaInfo)
		if err != nil {
			return ReturnWrapper{false, err.Error()}
		} else {
			return ReturnWrapper{true, ""}
		}
	}
}

func QueryAllDailyStatistics(dailyStatisticsList *[]dbmodel.DailyStatisticsMetaInfoV1, databaseInfo MongoDbDataBaseInfo) ReturnWrapper {
	var dailyStatisticsCollection = databaseInfo.MetaInfoDb.Collection(DailyStatisticsMetaInfoCollectionString)

	cursor, err := dailyStatisticsCollection.Find(
		context.TODO(),
		bson.D{})

	if err != nil {
		return ReturnWrapper{false, "Query all DailyStatistics failed!"}
	}

	if err = cursor.All(context.TODO(), dailyStatisticsList); err != nil {
		logger.GetLogger().Println(err.Error())
		return ReturnWrapper{false, "Query all DailyStatistics failed!"}
	}

	return ReturnWrapper{true, "Query all DailyStatistics Success"}
}

func CreateSwcData(swcUuid string, swcData *dbmodel.SwcDataV1, databaseInfo MongoDbDataBaseInfo) ReturnWrapper {
	collection := databaseInfo.SwcDb.Collection(swcUuid)

	var interfaceSlice []interface{}
	for _, v := range *swcData {
		interfaceSlice = append(interfaceSlice, v)
	}
	logger.GetLogger().Println("Inserting ", len(interfaceSlice), " nodes into ", swcUuid)
	result, err := collection.InsertMany(context.TODO(), interfaceSlice)
	if err != nil {
		if result != nil {
			return ReturnWrapper{false,
				"Insert many node failed! Inserted:" + strconv.Itoa(len(result.InsertedIDs)) +
					" , Error:" + strconv.Itoa(len(interfaceSlice)-len(result.InsertedIDs)) +
					" Total:" + strconv.Itoa(len(interfaceSlice))}
		} else {
			return ReturnWrapper{false, "Insert many node failed!"}
		}
	}

	logger.GetLogger().Println("Real Craete nodes in DB: " + strconv.Itoa(len(result.InsertedIDs)))

	return ReturnWrapper{true, "Create many node Success"}
}

func DeleteSwcDataCollection(swcUuid string, databaseInfo MongoDbDataBaseInfo) ReturnWrapper {
	collection := databaseInfo.SwcDb.Collection(swcUuid)

	err := collection.Drop(context.TODO())
	if err != nil {
		return ReturnWrapper{false, err.Error()}
	}
	return ReturnWrapper{true, "Delete swcdata collection " + swcUuid + " successfully!"}
}

func DeleteSwcData(swcUuid string, swcData dbmodel.SwcDataV1, databaseInfo MongoDbDataBaseInfo) ReturnWrapper {
	collection := databaseInfo.SwcDb.Collection(swcUuid)
	_ = EnsureUniqueUUIDIndex(collection)

	uuidList := bson.A{}

	for _, v := range swcData {
		uuidList = append(uuidList, bson.D{{"uuid", v.Base.Uuid}})
	}

	filterInterface := bson.D{
		{"$or",
			uuidList},
	}

	//// Check if all nodes exist
	//count, err := collection.CountDocuments(context.TODO(), filterInterface)
	//if err != nil {
	//	return ReturnWrapper{false, "Failed to count nodes: " + err.Error()}
	//}
	//
	//if int(count) != len(swcData) {
	//	return ReturnWrapper{false, "Not all nodes exist in the database"}
	//}

	logger.GetLogger().Println("Delete ", len(filterInterface), " nodes at ", swcUuid)
	result, err := collection.DeleteMany(context.TODO(), filterInterface)
	if err != nil {
		logger.GetLogger().Println(err.Error())
		if result != nil {
			return ReturnWrapper{false,
				"Delete many node failed! Deleted:" + strconv.Itoa(int(result.DeletedCount)) +
					" , Error:" + strconv.Itoa(len(filterInterface)-int(result.DeletedCount)) +
					" Total:" + strconv.Itoa(len(filterInterface))}
		} else {
			return ReturnWrapper{false, "Delete many node failed with error " + err.Error()}
		}
	}

	logger.GetLogger().Println("Real Delete nodes in DB: " + strconv.Itoa(int(result.DeletedCount)))

	//logger.GetLogger().Println("Start adjuest n and parent")

	//// adjust remaining node's n parent
	//cur, err := collection.Find(context.TODO(), bson.D{{}})
	//if err != nil {
	//	logger.GetLogger().Fatal(err)
	//}
	//
	//var lastNode *dbmodel.SwcNodeDataV1
	//counter := 1
	//
	//// Prepare a slice to hold the write models for the bulk operation
	//var writes []mongo.WriteModel
	//
	//logger.GetLogger().Println("For loop start")
	//
	//// Iterate over the cursor and update the documents
	//for cur.Next(context.TODO()) {
	//	var node dbmodel.SwcNodeDataV1
	//	err := cur.Decode(&node)
	//	if err != nil {
	//		logger.GetLogger().Fatal(err)
	//	}
	//
	//	// Update the node
	//	node.SwcNodeInternalData.N = int32(counter)
	//	counter++
	//
	//	// Update the current node's n
	//	update := bson.D{
	//		{"$set", bson.D{
	//			{"SwcData.n", node.SwcNodeInternalData.N},
	//		}},
	//	}
	//	model := mongo.NewUpdateOneModel().SetFilter(bson.M{"_id": node.Base.Id}).SetUpdate(update)
	//	writes = append(writes, model)
	//
	//	// Update the last node's parent to the current node's n
	//	if lastNode != nil {
	//		if lastNode.SwcNodeInternalData.Parent != -1 {
	//			update1 := bson.D{
	//				{"$set", bson.D{
	//					{"SwcData.parent", node.SwcNodeInternalData.N},
	//				}},
	//			}
	//			model1 := mongo.NewUpdateOneModel().SetFilter(bson.M{"_id": lastNode.Base.Id}).SetUpdate(update1)
	//			writes = append(writes, model1)
	//		}
	//	}
	//
	//	// Save the current node for the next iteration
	//	lastNode = &node
	//}
	//
	//logger.GetLogger().Println("For loop end")
	//
	//// Update the last node's parent to -1
	//if lastNode != nil {
	//	logger.GetLogger().Println("lastNode != nil")
	//	update := bson.D{
	//		{"$set", bson.D{
	//			{"SwcData.parent", -1},
	//		}},
	//	}
	//	model := mongo.NewUpdateOneModel().SetFilter(bson.M{"_id": lastNode.Base.Id}).SetUpdate(update)
	//	writes = append(writes, model)
	//}
	//
	//logger.GetLogger().Println("Execute the bulk operation")
	//
	//if len(writes) == 0 {
	//	return ReturnWrapper{true, "Delete many node Success! BulkWrite is empty."}
	//}
	//
	//// Execute the bulk operation
	//_, err = collection.BulkWrite(context.TODO(), writes)
	//if err != nil {
	//	logger.GetLogger().Println("Execute the bulk operation Failed")
	//	logger.GetLogger().Fatal(err)
	//}
	//
	//logger.GetLogger().Println("Execute the bulk operation Success")
	//
	//// Close the cursor
	//if cur != nil {
	//	_ = cur.Close(context.TODO())
	//}
	//
	//logger.GetLogger().Println("Close the cursor And Return")

	return ReturnWrapper{true, "Delete many node Success! BulkWrite is not empty."}
}

func ModifySwcData(swcUuid string, swcData *dbmodel.SwcDataV1, databaseInfo MongoDbDataBaseInfo) ReturnWrapper {
	collection := databaseInfo.SwcDb.Collection(swcUuid)
	_ = EnsureUniqueUUIDIndex(collection)

	logger.GetLogger().Printf("Modifying %d nodes at %s", len(*swcData), swcUuid)

	var operations []mongo.WriteModel
	for _, v := range *swcData {
		updateData := bson.D{
			{"SwcData.type", v.SwcNodeInternalData.Type},
			{"SwcData.x", v.SwcNodeInternalData.X},
			{"SwcData.y", v.SwcNodeInternalData.Y},
			{"SwcData.z", v.SwcNodeInternalData.Z},
			{"SwcData.radius", v.SwcNodeInternalData.Radius},
			{"SwcData.seg_id", v.SwcNodeInternalData.Seg_id},
			{"SwcData.level", v.SwcNodeInternalData.Level},
			{"SwcData.mode", v.SwcNodeInternalData.Mode},
			{"SwcData.timestamp", v.SwcNodeInternalData.Timestamp},
			{"SwcData.feature_value", v.SwcNodeInternalData.Feature_value},
			{"Creator", v.Creator},
			{"LastModifiedTime", v.LastModifiedTime},
			{"CheckerUserUuid", v.CheckerUserUuid},
			{"DeviceType", v.DeviceType},
		}

		if v.SwcNodeInternalData.N != 0 {
			updateData = append(updateData, bson.E{Key: "SwcData.n", Value: v.SwcNodeInternalData.N})
		}

		if v.SwcNodeInternalData.Parent != 0 {
			updateData = append(updateData, bson.E{Key: "SwcData.parent", Value: v.SwcNodeInternalData.Parent})
		}

		operation := mongo.NewUpdateOneModel().
			SetFilter(bson.D{{"uuid", v.Base.Uuid}}).
			SetUpdate(bson.D{{"$set", updateData}})

		operations = append(operations, operation)
	}

	if len(operations) == 0 {
		return ReturnWrapper{true, "No nodes to modify"}
	}

	// 执行批量写入操作
	opts := options.BulkWrite().SetOrdered(false)
	result, err := collection.BulkWrite(context.TODO(), operations, opts)
	if err != nil {
		logger.GetLogger().Printf("Bulk write error: %v", err)
		return ReturnWrapper{false, "Modify swc node failed! Error during bulk write."}
	}

	// 检查结果
	matchedCount := result.MatchedCount
	if matchedCount != int64(len(*swcData)) {
		logger.GetLogger().Printf("Warning: Expected to modify %d documents, but actually modified %d", len(*swcData), matchedCount)
		return ReturnWrapper{false, "Warning: Expected to match " + strconv.Itoa(len(*swcData)) + " documents, but actually matched " + strconv.Itoa(int(matchedCount))}
	}

	logger.GetLogger().Printf("Successfully updated %d nodes in DB", matchedCount)

	return ReturnWrapper{true, fmt.Sprintf("Modified %d swc nodes successfully", matchedCount)}

}

func QuerySwcData(swcUuid string, swcData *dbmodel.SwcDataV1, databaseInfo MongoDbDataBaseInfo) ReturnWrapper {
	collection := databaseInfo.SwcDb.Collection(swcUuid)
	_ = EnsureUniqueUUIDIndex(collection)

	uuidList := bson.A{}

	for _, v := range *swcData {
		uuidList = append(uuidList, bson.D{{"uuid", v.Base.Uuid}})
	}

	filterInterface := bson.D{
		{"$or",
			uuidList},
	}

	cursor, err := collection.Find(context.TODO(), filterInterface)
	if err != nil {
		return ReturnWrapper{false, "Query many node failed!"}
	}

	if err = cursor.All(context.TODO(), swcData); err != nil {
		return ReturnWrapper{false, "Query many node failed!"}
	}

	logger.GetLogger().Println("Query ", len(*swcData), " node at ", swcUuid)

	return ReturnWrapper{true, "Query many node Success"}
}

func QuerySwcDataByUserAndTime(
	swcUuid string,
	userName string,
	startTime time.Time,
	endTime time.Time,
	swcData *dbmodel.SwcDataV1,
	databaseInfo MongoDbDataBaseInfo) ReturnWrapper {

	collection := databaseInfo.SwcDb.Collection(swcUuid)

	filterInterface := bson.D{}

	if userName == "" {
		filterInterface = append(filterInterface, bson.E{Key: "$and",
			Value: bson.A{
				bson.M{"CreateTime": bson.M{"$gte": startTime}},
				bson.M{"CreateTime": bson.M{"$lte": endTime}},
			}})
	} else {
		filterInterface = append(filterInterface, bson.E{Key: "$and",
			Value: bson.A{
				bson.M{"Creator": userName},
				bson.M{"CreateTime": bson.M{"$gte": startTime}},
				bson.M{"CreateTime": bson.M{"$lte": endTime}},
			}})
	}

	cursor, err := collection.Find(context.TODO(), filterInterface)
	if err != nil {
		return ReturnWrapper{false, "QuerySwcDataByUserAndTime Error"}
	}

	if err = cursor.All(context.TODO(), swcData); err != nil {
		return ReturnWrapper{false, "QuerySwcDataByUserAndTime failed!"}
	}

	logger.GetLogger().Println("QueryByCondition ", len(*swcData), " nodes at ", swcUuid)

	return ReturnWrapper{true, "QuerySwcDataByUserAndTime Success"}
}

func QueryAllSwcData(swcUuid string, swcData *dbmodel.SwcDataV1, databaseInfo MongoDbDataBaseInfo) ReturnWrapper {
	collection := databaseInfo.SwcDb.Collection(swcUuid)

	cursor, err := collection.Find(context.TODO(), bson.D{})
	if err != nil {
		return ReturnWrapper{false, "Query many node failed!"}
	}

	if err = cursor.All(context.TODO(), swcData); err != nil {
		return ReturnWrapper{false, "Query many node failed!"}
	}

	logger.GetLogger().Println("QueryAll ", len(*swcData), " nodes at ", swcUuid)

	return ReturnWrapper{true, "Query many node Success"}
}

func CreateSnapshot(swcUuid string, snapshotName string, databaseInfo MongoDbDataBaseInfo) ReturnWrapper {
	srcCollection := databaseInfo.SwcDb.Collection(swcUuid)
	dstCollection := databaseInfo.SnapshotDb.Collection(snapshotName)

	cursor, err := srcCollection.Find(context.Background(), bson.D{{}})
	if err != nil {
		return ReturnWrapper{
			Status:  false,
			Message: err.Error(),
		}
	}

	var results []interface{}
	batchSize := 100000

	for cursor.Next(context.Background()) {
		var result bson.D
		err := cursor.Decode(&result)
		if err != nil {
			return ReturnWrapper{
				Status:  false,
				Message: err.Error(),
			}
		}
		results = append(results, result)

		if len(results) >= batchSize {
			_, err = dstCollection.InsertMany(context.Background(), results)
			if err != nil {
				return ReturnWrapper{
					Status:  false,
					Message: err.Error(),
				}
			}
			results = results[:0] // 清空切片，准备下一批次
		}
	}

	// 插入剩余的文档
	if len(results) > 0 {
		_, err = dstCollection.InsertMany(context.Background(), results)
		if err != nil {
			return ReturnWrapper{
				Status:  false,
				Message: err.Error(),
			}
		}
	}

	return ReturnWrapper{true, "Create Snapshot Success!"}
}

func CreateIncrementOperation(incrementOperationCollectionName string, operation dbmodel.SwcIncrementOperationV1, databaseInfo MongoDbDataBaseInfo) ReturnWrapper {
	currentCollection := databaseInfo.IncrementOperationDb.Collection(incrementOperationCollectionName)

	_, err := currentCollection.InsertOne(context.TODO(), operation)
	if err != nil {
		return ReturnWrapper{false, err.Error()}
	}

	return ReturnWrapper{true, "Insert Increment operation success!"}
}

func QuerySwcSnapshot(snapshotName string, swcData *dbmodel.SwcDataV1, databaseInfo MongoDbDataBaseInfo) ReturnWrapper {
	collection := databaseInfo.SnapshotDb.Collection(snapshotName)

	cursor, err := collection.Find(context.TODO(), bson.D{})
	if err != nil {
		return ReturnWrapper{false, "Query many node failed!"}
	}

	if err = cursor.All(context.TODO(), swcData); err != nil {
		return ReturnWrapper{false, "Query many node failed!"}
	}

	return ReturnWrapper{true, "Query many node Success"}
}

func QuerySwcIncrementOperation(incrementOperationCollectionName string, operations *dbmodel.SwcIncrementOperationListV1, databaseInfo MongoDbDataBaseInfo) ReturnWrapper {
	collection := databaseInfo.IncrementOperationDb.Collection(incrementOperationCollectionName)

	cursor, err := collection.Find(context.TODO(), bson.D{})
	if err != nil {
		return ReturnWrapper{false, "Query many node failed!"}
	}

	if err = cursor.All(context.TODO(), operations); err != nil {
		return ReturnWrapper{false, "Query many node failed!"}
	}

	return ReturnWrapper{true, "Query many node Success"}
}

func CreateSwcAttachmentAno(swcUuid string, anoAttachment *dbmodel.SwcAttachmentAnoV1, databaseInfo MongoDbDataBaseInfo) ReturnWrapper {
	attachmentCollection := "Attachment_Ano_" + swcUuid
	collection := databaseInfo.AttachmentDb.Collection(attachmentCollection)
	_ = collection.Drop(context.TODO())
	_, err := collection.InsertOne(context.TODO(), anoAttachment)
	if err != nil {
		return ReturnWrapper{false, err.Error()}
	}

	return ReturnWrapper{true, "Create Ano Attachment successfully!"}
}

func DeleteSwcAttachmentAno(swcUuid string, attachmentUuid string, databaseInfo MongoDbDataBaseInfo) ReturnWrapper {
	attachmentCollection := "Attachment_Ano_" + swcUuid
	collection := databaseInfo.AttachmentDb.Collection(attachmentCollection)

	result := collection.FindOneAndDelete(context.TODO(), bson.D{
		{"uuid", attachmentUuid}})

	_ = collection.Drop(context.Background())

	if result.Err() != nil {
		return ReturnWrapper{false, result.Err().Error()}
	} else {
		return ReturnWrapper{true, "Delete Ano Attachment successfully!"}
	}
}

func UpdateSwcAttachmentAno(swcUuid string, attachmentUuid string, anoAttachment *dbmodel.SwcAttachmentAnoV1, databaseInfo MongoDbDataBaseInfo) ReturnWrapper {
	attachmentCollection := "Attachment_Ano_" + swcUuid
	collection := databaseInfo.AttachmentDb.Collection(attachmentCollection)

	result := collection.FindOneAndUpdate(
		context.TODO(),
		bson.D{{"uuid", attachmentUuid}},
		bson.D{{"$set", bson.D{
			{"APOFILE", anoAttachment.APOFILE},
			{"SWCFILE", anoAttachment.SWCFILE},
		}}},
	)
	if result.Err() != nil {
		if result != nil {
			return ReturnWrapper{false,
				"Update Ano Attachment failed! Error" + result.Err().Error()}
		}

	}
	return ReturnWrapper{true, "Update Ano Attachment Success"}
}

func QuerySwcAttachmentAno(swcUuid string, attachmentUuid string, anoAttachment *dbmodel.SwcAttachmentAnoV1, databaseInfo MongoDbDataBaseInfo) ReturnWrapper {
	attachmentCollection := "Attachment_Ano_" + swcUuid
	var collection = databaseInfo.AttachmentDb.Collection(attachmentCollection)

	result := collection.FindOne(
		context.TODO(),
		bson.D{{"uuid", attachmentUuid}})

	if result.Err() != nil {
		return ReturnWrapper{false, result.Err().Error()}
	} else {
		err := result.Decode(anoAttachment)
		if err != nil {
			return ReturnWrapper{false, err.Error()}
		} else {
			return ReturnWrapper{true, ""}
		}
	}
}

func CreateSwcAttachmentApo(swcUuid string, apoAttachmentCollectionName string, apoAttachment *[]dbmodel.SwcAttachmentApoV1, databaseInfo MongoDbDataBaseInfo) ReturnWrapper {
	collection := databaseInfo.AttachmentDb.Collection(apoAttachmentCollectionName)

	if len(*apoAttachment) != 0 {
		var interfaceSlice []interface{}
		for _, v := range *apoAttachment {
			interfaceSlice = append(interfaceSlice, v)
		}
		_, err := collection.InsertMany(context.TODO(), interfaceSlice)
		if err != nil {
			return ReturnWrapper{false, err.Error()}
		}
	}
	return ReturnWrapper{true, "Create Apo Attachment successfully!"}
}

func DeleteSwcAttachmentApo(swcUuid string, apoAttachmentCollectionName string, databaseInfo MongoDbDataBaseInfo) ReturnWrapper {
	collection := databaseInfo.AttachmentDb.Collection(apoAttachmentCollectionName)
	err := collection.Drop(context.Background())
	if err != nil {
		return ReturnWrapper{false, err.Error()}
	}
	return ReturnWrapper{true, "Delete Apo Attachment successfully!"}
}

func UpdateSwcAttachmentApo(swcUuid string, apoAttachmentCollectionName string, apoAttachment *[]dbmodel.SwcAttachmentApoV1, databaseInfo MongoDbDataBaseInfo) ReturnWrapper {
	collection := databaseInfo.AttachmentDb.Collection(apoAttachmentCollectionName)
	err := collection.Drop(context.Background())
	if err != nil {
		return ReturnWrapper{false, err.Error()}
	}

	if len(*apoAttachment) == 0 {
		return ReturnWrapper{true, "Update Apo Attachment successfully!"}
	}

	var interfaceSlice []interface{}
	for _, v := range *apoAttachment {
		interfaceSlice = append(interfaceSlice, v)
	}
	_, err = collection.InsertMany(context.TODO(), interfaceSlice)
	if err != nil {
		return ReturnWrapper{false, err.Error()}
	}
	return ReturnWrapper{true, "Update Apo Attachment successfully!"}
}

func QuerySwcAttachmentApo(swcUuid string, apoAttachmentCollectionName string, apoAttachment *[]dbmodel.SwcAttachmentApoV1, databaseInfo MongoDbDataBaseInfo) ReturnWrapper {
	collection := databaseInfo.AttachmentDb.Collection(apoAttachmentCollectionName)

	cursor, err := collection.Find(
		context.TODO(),
		bson.D{})

	if err != nil {
		return ReturnWrapper{false, err.Error()}
	}

	if err = cursor.All(context.TODO(), apoAttachment); err != nil {
		logger.GetLogger().Println(err.Error())
		return ReturnWrapper{false, err.Error()}
	}

	return ReturnWrapper{true, "Query Apo Attachment Success"}
}

func RevertSwcNodeData(swcUuid string, swcSnapshotCollectionName string, incrementOperationCollectionName string, endTime time.Time, databaseInfo MongoDbDataBaseInfo) ReturnWrapper {
	collection := databaseInfo.IncrementOperationDb.Collection(incrementOperationCollectionName)
	delFilter := bson.D{{"CreateTime", bson.D{{"$gt", primitive.NewDateTimeFromTime(endTime)}}}}
	delres, err := collection.DeleteMany(context.TODO(), delFilter)
	if err != nil {
		return ReturnWrapper{false, err.Error()}
	}
	println(delres.DeletedCount)

	swcCollection := databaseInfo.SwcDb.Collection(swcUuid)
	err = swcCollection.Drop(context.TODO())
	if err != nil {
		return ReturnWrapper{false, err.Error()}
	}

	srcCollection := databaseInfo.SnapshotDb.Collection(swcSnapshotCollectionName)
	dstCollection := databaseInfo.SwcDb.Collection(swcUuid)

	cursor, err := srcCollection.Find(context.Background(), bson.D{{}})
	if err != nil {
		return ReturnWrapper{
			Status:  false,
			Message: err.Error(),
		}
	}

	var results []interface{}
	batchSize := 100000

	for cursor.Next(context.Background()) {
		var result bson.D
		err := cursor.Decode(&result)
		if err != nil {
			return ReturnWrapper{
				Status:  false,
				Message: err.Error(),
			}
		}
		results = append(results, result)

		if len(results) >= batchSize {
			_, err = dstCollection.InsertMany(context.Background(), results)
			if err != nil {
				return ReturnWrapper{
					Status:  false,
					Message: err.Error(),
				}
			}
			results = results[:0] // 清空切片，准备下一批次
		}
	}

	// 插入剩余的文档
	if len(results) > 0 {
		_, err = dstCollection.InsertMany(context.Background(), results)
		if err != nil {
			return ReturnWrapper{
				Status:  false,
				Message: err.Error(),
			}
		}
	}
	var operations dbmodel.SwcIncrementOperationListV1
	QuerySwcIncrementOperation(incrementOperationCollectionName, &operations, GetDbInstance())
	for _, operation := range operations {
		switch operation.IncrementOperation {
		case IncrementOp_Create:
			CreateSwcData(swcUuid, &operation.SwcData, GetDbInstance())
		case IncrementOp_Delete:
			DeleteSwcData(swcUuid, operation.SwcData, GetDbInstance())
		case IncrementOp_Update:
			ModifySwcData(swcUuid, &operation.SwcData, GetDbInstance())
		case IncrementOp_UpdateNParent:
			UpdateSwcNParent(swcUuid, &operation.NodeNParent, GetDbInstance())
		case IncrementOp_ClearAll:
			ClearAllNode(swcUuid, GetDbInstance())
		case IncrementOp_OverwriteAll:
			CreateSwcData(swcUuid, &operation.SwcData, GetDbInstance())
		}
	}

	return ReturnWrapper{true, "Delete IncrementOperation after given time successfully!"}
}

func GetNewUserIdAndIncrease(databaseInfo MongoDbDataBaseInfo) (ReturnWrapper, int32) {
	collection := databaseInfo.MetaInfoDb.Collection(MetaInfoDbStatusCollectonString)

	var result struct {
		Seq int32
	}

	val := "CurrentNewUserId"
	filter := bson.D{{"AttributeName", val}}
	update := bson.D{
		{"$inc", bson.D{{"seq", 1}}},
		{"$setOnInsert", bson.D{{"AttributeName", val}}},
	}
	opts := options.FindOneAndUpdate().SetUpsert(true).SetReturnDocument(options.After)

	err := collection.FindOneAndUpdate(context.Background(), filter, update, opts).Decode(&result)
	if err != nil {
		return ReturnWrapper{
			Status:  false,
			Message: err.Error(),
		}, -1
	}

	return ReturnWrapper{
		Status:  true,
		Message: "GetNewUserId Successfully!",
	}, result.Seq
}

func CreateAttachmentSwcData(attachmentCollectionName string, swcData *dbmodel.SwcDataV1, databaseInfo MongoDbDataBaseInfo) ReturnWrapper {
	collection := databaseInfo.AttachmentDb.Collection(attachmentCollectionName)

	if len(*swcData) != 0 {
		var interfaceSlice []interface{}
		for _, v := range *swcData {
			interfaceSlice = append(interfaceSlice, v)
		}
		logger.GetLogger().Println("Attachment - Inserting ", len(interfaceSlice), " nodes into ", attachmentCollectionName)
		result, err := collection.InsertMany(context.TODO(), interfaceSlice)
		if err != nil {
			if result != nil {
				return ReturnWrapper{false,
					"Attachment - Insert many node failed! Inserted:" + strconv.Itoa(len(result.InsertedIDs)) +
						" , Error:" + strconv.Itoa(len(interfaceSlice)-len(result.InsertedIDs)) +
						" Total:" + strconv.Itoa(len(interfaceSlice))}
			} else {
				return ReturnWrapper{false, "Insert many node failed!"}
			}
		}

		logger.GetLogger().Println("Attachment - Real Craete nodes in DB: " + strconv.Itoa(len(result.InsertedIDs)))
	}
	return ReturnWrapper{true, "Create Swc Attachment Success"}
}

func DeleteAttachmentSwcData(attachmentCollectionName string, databaseInfo MongoDbDataBaseInfo) ReturnWrapper {
	collection := databaseInfo.AttachmentDb.Collection(attachmentCollectionName)
	err := collection.Drop(context.Background())
	if err != nil {
		return ReturnWrapper{false, err.Error()}
	}
	return ReturnWrapper{true, "Delete Swc Attachment successfully!"}
}

func UpdateAttachmentSwcData(attachmentCollectionName string, swcData *dbmodel.SwcDataV1, databaseInfo MongoDbDataBaseInfo) ReturnWrapper {
	collection := databaseInfo.AttachmentDb.Collection(attachmentCollectionName)
	err := collection.Drop(context.Background())
	if err != nil {
		return ReturnWrapper{false, err.Error()}
	}

	if len(*swcData) != 0 {
		var interfaceSlice []interface{}
		for _, v := range *swcData {
			interfaceSlice = append(interfaceSlice, v)
		}
		logger.GetLogger().Println("Attachment - Inserting ", len(interfaceSlice), " nodes into ", attachmentCollectionName)
		result, err := collection.InsertMany(context.TODO(), interfaceSlice)
		if err != nil {
			if result != nil {
				return ReturnWrapper{false,
					"Attachment - Insert many node failed! Inserted:" + strconv.Itoa(len(result.InsertedIDs)) +
						" , Error:" + strconv.Itoa(len(interfaceSlice)-len(result.InsertedIDs)) +
						" Total:" + strconv.Itoa(len(interfaceSlice))}
			} else {
				return ReturnWrapper{false, "Insert many node failed!"}
			}
		}
	}
	return ReturnWrapper{true, "Update Swc Attachment successfully!"}
}

func QueryAttachmentSwcData(attachmentCollectionName string, swcData *dbmodel.SwcDataV1, databaseInfo MongoDbDataBaseInfo) ReturnWrapper {
	collection := databaseInfo.AttachmentDb.Collection(attachmentCollectionName)

	filterInterface := bson.D{{}}

	cursor, err := collection.Find(context.TODO(), filterInterface)
	if err != nil {
		return ReturnWrapper{false, "Query Swc Attachment failed!"}
	}

	if err = cursor.All(context.TODO(), swcData); err != nil {
		return ReturnWrapper{false, "Query Swc Attachment failed!"}
	}

	logger.GetLogger().Println("Query ", len(*swcData), " node at ", attachmentCollectionName)

	return ReturnWrapper{true, "Query Swc Attachment Success"}
}

// 检查SWC附件集合是否有数据
func CheckAttachmentSwcHasData(attachmentCollectionName string, databaseInfo MongoDbDataBaseInfo) (ReturnWrapper, bool) {
	collection := databaseInfo.AttachmentDb.Collection(attachmentCollectionName)
	
	// 创建一个计数器，只计数一个文档即可判断是否有数据
	count, err := collection.CountDocuments(context.TODO(), bson.D{{}}, options.Count().SetLimit(1))
	if err != nil {
		return ReturnWrapper{false, "Check Attachment Swc Has Data failed!"}, false
	}
	
	// 如果计数大于0，表示有数据
	hasData := count > 0
	return ReturnWrapper{true, "Check Attachment Swc Has Data Success"}, hasData
}

// 获取项目定义的Soma SWC
func GetProjectsDefinedSomaSwc(projectUuids []string, databaseInfo MongoDbDataBaseInfo) (ReturnWrapper, []string) {
	var swcCollection = databaseInfo.MetaInfoDb.Collection(SwcMetaInfoCollectionString)
	var result []string

	// 首先查询所有指定项目下的SWC
	cursor, err := swcCollection.Find(context.TODO(), bson.M{"BelongingProjectUuid": bson.M{"$in": projectUuids}})
	if err != nil {
		return ReturnWrapper{false, "查询项目SWC失败: " + err.Error()}, nil
	}
	defer cursor.Close(context.TODO())

	// 解析所有符合条件的SWC元数据
	var allSwcMetaInfo []dbmodel.SwcMetaInfoV1
	if err = cursor.All(context.TODO(), &allSwcMetaInfo); err != nil {
		return ReturnWrapper{false, "解析SWC元数据失败: " + err.Error()}, nil
	}

	// 遍历所有SWC，检查是否有SwcAttachmentSwcUuid且对应集合有数据
	for _, swcInfo := range allSwcMetaInfo {
		if swcInfo.SwcAttachmentSwcUuid == "" {
			continue
		}

		// 检查SwcAttachmentSwcUuid指向的集合是否有数据
		returnWrapper, hasData := CheckAttachmentSwcHasData(swcInfo.SwcAttachmentSwcUuid, databaseInfo)
		if !returnWrapper.Status {
			logger.GetLogger().Printf("检查SWC %s 的附件数据时出错: %s\n", swcInfo.Base.Uuid, returnWrapper.Message)
			continue
		}

		if hasData {
			result = append(result, swcInfo.Base.Uuid)
		}
	}

	return ReturnWrapper{true, "查询项目Soma SWC成功"}, result
}

func UpdateSwcNParent(swcUuid string, nodeNParent *[]dbmodel.NodeNParentV1, databaseInfo MongoDbDataBaseInfo) (ReturnWrapper, int, int, int, int, []string, []string, []string) {
	// Get the collection for the given swcUuid
	collection := databaseInfo.SwcDb.Collection(swcUuid)

	var updateCount, noUpdateCount, incomingNotExistCount, dbNotExistCount int
	var updateNodes, notExistNodes, dbNotExistNodes []string

	// Get all nodes in the collection
	cursor, err := collection.Find(context.TODO(), bson.D{})
	if err != nil {
		return ReturnWrapper{false, "Failed to retrieve nodes: " + err.Error()}, updateCount, noUpdateCount, incomingNotExistCount, dbNotExistCount, updateNodes, notExistNodes, dbNotExistNodes
	}

	var nodesInDb []dbmodel.SwcNodeDataV1
	if err = cursor.All(context.TODO(), &nodesInDb); err != nil {
		return ReturnWrapper{false, "Failed to retrieve nodes: " + err.Error()}, updateCount, noUpdateCount, incomingNotExistCount, dbNotExistCount, updateNodes, notExistNodes, dbNotExistNodes
	}

	// Create a map for quick lookup of nodes in the database
	nodeMap := make(map[string]dbmodel.SwcNodeDataV1)
	for _, node := range nodesInDb {
		nodeMap[node.Base.Uuid] = node
	}

	// Prepare a slice to hold the write models for the bulk operation
	var writes []mongo.WriteModel

	// Iterate over the nodes in the input data
	for _, node := range *nodeNParent {
		dbNode, exists := nodeMap[node.Uuid]

		// If the node exists in the database
		if exists {
			// If the N or Parent values do not match
			if dbNode.SwcNodeInternalData.N != node.N || dbNode.SwcNodeInternalData.Parent != node.Parent {
				// Prepare the update model
				update := mongo.NewUpdateOneModel()
				update.SetFilter(bson.M{"uuid": node.Uuid})
				update.SetUpdate(bson.D{
					{"$set", bson.D{
						{"SwcData.n", node.N},
						{"SwcData.parent", node.Parent},
					}},
				})
				writes = append(writes, update)
				updateCount++
				updateNodes = append(updateNodes, node.Uuid)
			} else {
				noUpdateCount++
			}
			// Remove the node from the map
			delete(nodeMap, node.Uuid)
		} else {
			incomingNotExistCount++
			notExistNodes = append(notExistNodes, node.Uuid)
		}
	}

	if len(writes) == 0 {
		return ReturnWrapper{true, "Update completed! BulkWrite is empty."}, updateCount, noUpdateCount, incomingNotExistCount, dbNotExistCount, updateNodes, notExistNodes, dbNotExistNodes
	}

	// Execute the bulk operation
	_, err = collection.BulkWrite(context.TODO(), writes)
	if err != nil {
		return ReturnWrapper{false, "Failed to update nodes: " + err.Error()}, updateCount, noUpdateCount, incomingNotExistCount, dbNotExistCount, updateNodes, notExistNodes, dbNotExistNodes
	}

	// Now, nodeMap only contains nodes that exist in the database but not in the input data
	for uuid := range nodeMap {
		dbNotExistCount++
		dbNotExistNodes = append(dbNotExistNodes, uuid)
	}

	return ReturnWrapper{true, fmt.Sprintf("Update completed. Updated: %d, Not Updated: %d, Incoming Not Exist: %d, DB Not Exist: %d.",
		updateCount, noUpdateCount, incomingNotExistCount, dbNotExistCount)}, updateCount, noUpdateCount, incomingNotExistCount, dbNotExistCount, updateNodes, notExistNodes, dbNotExistNodes
}

func ClearAllNode(swcUuid string, databaseInfo MongoDbDataBaseInfo) ReturnWrapper {
	// Get the collection for the given swcUuid
	collection := databaseInfo.SwcDb.Collection(swcUuid)
	err := collection.Drop(context.Background())
	if err != nil {
		return ReturnWrapper{false, err.Error()}
	}
	return ReturnWrapper{true, "Delete all nodes successfully!"}
}
