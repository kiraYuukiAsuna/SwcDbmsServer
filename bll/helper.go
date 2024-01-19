package bll

import (
	"DBMS/SwcDbmsCommon/Generated/go/proto/message"
	"DBMS/errcode"
)

func UserTokenVerify(userName string, userToken string) (message.ResponseMetaInfoV1, OnlineUserInfo) {
	bFind := false
	var cachedOnlineUserInfo OnlineUserInfo

	if _, ok := OnlineUserInfoCache[userName]; ok {
		cachedOnlineUserInfo = OnlineUserInfoCache[userName]
		if cachedOnlineUserInfo.Token == userToken {
			bFind = true
		}
	}

	if !bFind {
		return message.ResponseMetaInfoV1{
			Status:  bFind,
			Id:      errcode.Error_UserTokenVerifyFailed,
			Message: "UserToken verify Failed! Please login again!",
		}, cachedOnlineUserInfo
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
	if currentServerRequestMetaInfo.GetApiVersion() != requestMetaInfo.GetApiVersion() {
		return message.ResponseMetaInfoV1{
			Status:  false,
			Id:      errcode.Error_ApiVersionNotConsist,
			Message: "Client ApiVersion is not consist with Server ApiVersion. Please update your client to newest version!",
		}
	} else {
		return message.ResponseMetaInfoV1{
			Status:  true,
			Id:      "",
			Message: "",
		}
	}
}
