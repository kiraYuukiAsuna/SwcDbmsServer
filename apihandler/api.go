package apihandler

//func InitializeNewDataBaseIfNotExistHandler(context *gin.Context) {
//	dataBaseNameInfo := dal.DataBaseNameInfo{
//		MetaInfoDataBaseName:              dal.DefaultMetaInfoDataBaseName,
//		SwcDataBaseName:                   dal.DefaultSwcDataBaseName,
//		SwcSnapshotDataBaseName:           dal.DefaultSwcSnapshotDataBaseName,
//		SwcIncrementOperationDataBaseName: dal.DefaultSwcIncrementOperationDataBaseName,
//		SwcAttachmentDataBaseName:         dal.DefaultSwcAttachmentDataBaseName,
//	}
//	dal.InitializeNewDataBaseIfNotExist(dataBaseNameInfo)
//}
//
//func CreateUserHandler(context *gin.Context) {
//
//	var userInfo dbmodel.UserMetaInfoV1
//	userInfo.Base.Id = primitive.NewObjectID()
//	userInfo.Name = "Hanasaka"
//	userInfo.Password = "Hanasaka"
//	userInfo.Description = "Test user"
//
//	_, err := dal.GetDbInstance().MetaInfoDb.Collection(dal.UserMetaInfoCollectionString).InsertOne(context2.TODO(), userInfo)
//	if err != nil {
//		return
//	}
//}
