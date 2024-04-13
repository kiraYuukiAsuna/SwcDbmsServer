package bll

import (
	"DBMS/SwcDbmsCommon/Generated/go/proto/message"
	"DBMS/dal"
	"DBMS/dbmodel"
	"DBMS/errcode"
	"github.com/google/uuid"
	"log"
	"time"
)

func UserLoginTokenGeneration(userMetaInfo dbmodel.UserMetaInfoV1) (string, OnlineUserInfo) {
	userToken := ""
	if _, ok := OnlineUserInfoCache[userMetaInfo.Name]; !ok {
		userToken = uuid.NewString()
		OnlineUserInfoCache[userMetaInfo.Name] = OnlineUserInfo{userMetaInfo, userToken, false, time.Now().Add(30 * time.Second)}
		log.Println("User " + userMetaInfo.Name + " HeartBeat Init")
	} else {
		userToken = OnlineUserInfoCache[userMetaInfo.Name].Token
		onlineUserInfo := OnlineUserInfoCache[userMetaInfo.Name]
		onlineUserInfo.LastHeartBeatTime = time.Now().Add(30 * time.Second)
		OnlineUserInfoCache[userMetaInfo.Name] = onlineUserInfo
		log.Println("User " + userMetaInfo.Name + " HeartBeat Restart")
	}
	return userToken, OnlineUserInfoCache[userMetaInfo.Name]
}

func UserTokenVerify(userVerifyInfo *message.UserVerifyInfoV1) (message.ResponseMetaInfoV1, OnlineUserInfo) {
	var userName = userVerifyInfo.UserName
	var userToken = userVerifyInfo.UserToken
	var userPassword = userVerifyInfo.UserPassword

	bFind := false
	var cachedOnlineUserInfo OnlineUserInfo

	if _, ok := OnlineUserInfoCache[userName]; ok {
		cachedOnlineUserInfo = OnlineUserInfoCache[userName]
		if cachedOnlineUserInfo.Token == userToken {
			bFind = true
		}
	}

	if !bFind {
		if userPassword != "" {
			var userMetaInfo dbmodel.UserMetaInfoV1
			userMetaInfo.Name = userName
			result := dal.QueryUser(&userMetaInfo, dal.GetDbInstance())
			if !result.Status {
				return message.ResponseMetaInfoV1{
					Status:  bFind,
					Id:      errcode.ErrorCannotFindUser,
					Message: "Query User Info Failed! UserName[" + userName + "]",
				}, cachedOnlineUserInfo
			}

			if userMetaInfo.Password == userPassword {
				log.Println("User " + userName + " Login Through implicit user token verify using password")

				_, onlineUserInfoUPAuth := UserLoginTokenGeneration(userMetaInfo)
				return message.ResponseMetaInfoV1{
					Status:  true,
					Id:      "",
					Message: "",
				}, onlineUserInfoUPAuth
			} else {
				return message.ResponseMetaInfoV1{
					Status:  bFind,
					Id:      errcode.ErrorUserPasswordIncorrect,
					Message: "Password is incorrect!",
				}, cachedOnlineUserInfo
			}
		} else {
			return message.ResponseMetaInfoV1{
				Status:  bFind,
				Id:      errcode.ErrorUserTokenVerifyFailed,
				Message: "UserToken verify Failed! Please login again!",
			}, cachedOnlineUserInfo
		}
	} else {
		return message.ResponseMetaInfoV1{
			Status:  bFind,
			Id:      "",
			Message: "",
		}, cachedOnlineUserInfo
	}
}

func RequestApiVersionVerify(requestMetaInfo *message.RequestMetaInfoV1) message.ResponseMetaInfoV1 {
	currentServerRequestMetaInfo := message.RequestMetaInfoV1{}
	currentServerRequestMetaInfo.ApiVersion = "2024.01.19"

	if currentServerRequestMetaInfo.GetApiVersion() != requestMetaInfo.GetApiVersion() {
		return message.ResponseMetaInfoV1{
			Status:  false,
			Id:      errcode.ErrorApiVersionNotConsist,
			Message: "Client ApiVersion is not consist with Server ApiVersion. Please update your client to the newest version!",
		}
	} else {
		return message.ResponseMetaInfoV1{
			Status:  true,
			Id:      "",
			Message: "",
		}
	}
}
