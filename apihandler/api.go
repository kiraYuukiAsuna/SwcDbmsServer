package apihandler

import (
	"DBMS/dal"
	"DBMS/dbmodel"
	context2 "context"
	"github.com/gin-gonic/gin"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func InitializeNewDataBaseIfNotExistHandler(context *gin.Context) {
	dal.InitializeNewDataBaseIfNotExist(dal.DefaultMetaInfoDataBaseName, dal.DefaultSwcDataBaseName)
}

func CreateUserHandler(context *gin.Context) {

	var userInfo dbmodel.UserMetaInfoV1
	userInfo.Base.Id = primitive.NewObjectID()
	userInfo.Name = "Hanasaka"
	userInfo.Password = "Hanasaka"
	userInfo.Description = "Test user"

	_, err := dal.GetDbInstance().MetaInfoDb.Collection(dbmodel.UserMetaInfoCollectionString).InsertOne(context2.TODO(), userInfo)
	if err != nil {
		return
	}
}
