package bll

import (
	"DBMS/SwcDbmsCommon/Generated/go/proto/message"
	"DBMS/config"
	"DBMS/dal"
	"DBMS/dbmodel"
	"DBMS/errcode"
	"github.com/google/uuid"
	"log"
	"reflect"
	"time"
)

func UserLoginTokenGeneration(userMetaInfo dbmodel.UserMetaInfoV1) (string, OnlineUserInfo) {
	userToken := ""
	mu.Lock()
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
	var cache = OnlineUserInfoCache[userMetaInfo.Name]
	mu.Unlock()
	return userToken, cache
}

func UserTokenVerify(userVerifyInfo *message.UserVerifyInfoV1) (message.ResponseMetaInfoV1, OnlineUserInfo) {
	var userName = userVerifyInfo.UserName
	var userToken = userVerifyInfo.UserToken
	var userPassword = userVerifyInfo.UserPassword

	bFind := false
	var cachedOnlineUserInfo OnlineUserInfo

	mu.Lock()
	if _, ok := OnlineUserInfoCache[userName]; ok {
		cachedOnlineUserInfo = OnlineUserInfoCache[userName]
		if cachedOnlineUserInfo.Token == userToken {
			bFind = true
		}
	}
	mu.Unlock()

	if !bFind {
		if userPassword != "" {
			var userMetaInfo dbmodel.UserMetaInfoV1
			userMetaInfo.Name = userName
			result := dal.QueryUserByName(&userMetaInfo, dal.GetDbInstance())
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
	currentServerRequestMetaInfo.ApiVersion = config.ApiVersion

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

func AclContainsUser(userUuid string, userPermission []dbmodel.UserPermissionAclV1) (bool, dbmodel.UserPermissionAclV1) {
	for _, userPermissionAcl := range userPermission {
		if userPermissionAcl.UserUuid == userUuid {
			return true, userPermissionAcl
		}
	}

	return false, dbmodel.UserPermissionAclV1{}
}

func AclContainsGroup(userPermissionGroupUuid string, groupPermission []dbmodel.GroupPermissionAclV1) (bool, dbmodel.GroupPermissionAclV1) {
	for _, groupPermissionAcl := range groupPermission {
		if groupPermissionAcl.GroupUuid == userPermissionGroupUuid {
			return true, groupPermissionAcl
		}
	}

	return false, dbmodel.GroupPermissionAclV1{}
}

func PermissionGroupVerify(userMetaInfo *dbmodel.UserMetaInfoV1, requestPermissionName string) bool {
	var authorityStatus = false

	permissionGroupMetaInfo := dbmodel.PermissionGroupMetaInfoV1{}
	permissionGroupMetaInfo.Base.Uuid = userMetaInfo.PermissionGroupUuid
	if result := dal.QueryPermissionGroupByUuid(&permissionGroupMetaInfo, dal.GetDbInstance()); !result.Status {
		return false
	}

	reflectionMemberVariables := reflect.ValueOf(permissionGroupMetaInfo.Ace)
	value := reflectionMemberVariables.FieldByName(requestPermissionName)
	if value.Kind() == reflect.Bool {
		authorityStatus = value.Bool()
	}

	return authorityStatus
}

func PermissionVerify(userMetaInfo *dbmodel.UserMetaInfoV1, permissionMetaInfo *dbmodel.PermissionMetaInfoV1, requestPermissionName string) bool {
	var authorityStatus = false

	if permissionMetaInfo.Owner.UserUuid == userMetaInfo.Base.Uuid {
		authorityStatus = true
	} else if status, userPermissionAcl := AclContainsUser(userMetaInfo.Base.Uuid, permissionMetaInfo.Users); status {
		reflectionMemberVariables := reflect.ValueOf(userPermissionAcl.Ace)
		value := reflectionMemberVariables.FieldByName(requestPermissionName)
		if value.Kind() == reflect.Bool {
			authorityStatus = value.Bool()
		}

	} else if status, groupPermisionAcl := AclContainsGroup(userMetaInfo.PermissionGroupUuid, permissionMetaInfo.Groups); status {
		reflectionMemberVariables := reflect.ValueOf(groupPermisionAcl.Ace)
		value := reflectionMemberVariables.FieldByName(requestPermissionName)
		if value.Kind() == reflect.Bool {
			authorityStatus = value.Bool()
		}
	}

	return authorityStatus
}
