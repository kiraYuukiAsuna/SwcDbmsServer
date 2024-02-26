package bll

import (
	"DBMS/SwcDbmsCommon/Generated/go/proto/message"
	"DBMS/SwcDbmsCommon/Generated/go/proto/request"
	"DBMS/SwcDbmsCommon/Generated/go/proto/response"
	"DBMS/SwcDbmsCommon/Generated/go/proto/service"
	"DBMS/dal"
	"DBMS/dbmodel"
	"context"
	"github.com/google/uuid"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"log"
	"strconv"
	"time"
)

type DBMSServerController struct {
	service.UnimplementedDBMSServer
}

func (D DBMSServerController) CreateUser(ctx context.Context, request *request.CreateUserRequest) (*response.CreateUserResponse, error) {
	apiVersionVerifyResult := RequestApiVersionVerify(request.GetMetaInfo())
	if !apiVersionVerifyResult.Status {
		return &response.CreateUserResponse{
			MetaInfo: &apiVersionVerifyResult,
		}, nil
	}

	userMetaInfo := UserMetaInfoV1ProtobufToDbmodel(request.UserInfo)
	userMetaInfo.Base.Id = primitive.NewObjectID()
	userMetaInfo.Base.Uuid = uuid.NewString()
	userMetaInfo.Base.DataAccessModelVersion = "V1"

	userMetaInfo.Name = request.UserInfo.Name
	userMetaInfo.Password = request.UserInfo.Password
	userMetaInfo.Description = request.UserInfo.Description
	userMetaInfo.UserPermissionGroup = dal.PermissionGroupDefault
	userMetaInfo.CreateTime = time.Now()
	userMetaInfo.HeadPhotoBinData = request.UserInfo.HeadPhotoBinData

	status, newUserId := dal.GetNewUserIdAndIncrease(dal.GetDbInstance())
	if !status.Status {
		return &response.CreateUserResponse{
			MetaInfo: &message.ResponseMetaInfoV1{
				Status:  false,
				Id:      "",
				Message: status.Message,
			},
			UserInfo: UserMetaInfoV1DbmodelToProtobuf(userMetaInfo),
		}, nil
	}
	userMetaInfo.UserId = newUserId

	result := dal.CreateUser(*userMetaInfo, dal.GetDbInstance())
	if result.Status == true {
		log.Println("User " + request.UserInfo.Name + " Created")
		return &response.CreateUserResponse{
			MetaInfo: &message.ResponseMetaInfoV1{
				Status:  true,
				Id:      "",
				Message: result.Message,
			},
			UserInfo: UserMetaInfoV1DbmodelToProtobuf(userMetaInfo),
		}, nil
	} else {
		return &response.CreateUserResponse{
			MetaInfo: &message.ResponseMetaInfoV1{
				Status:  false,
				Id:      "",
				Message: result.Message,
			},
			UserInfo: UserMetaInfoV1DbmodelToProtobuf(userMetaInfo),
		}, nil
	}
}

func (D DBMSServerController) DeleteUser(ctx context.Context, request *request.DeleteUserRequest) (*response.DeleteUserResponse, error) {
	apiVersionVerifyResult := RequestApiVersionVerify(request.GetMetaInfo())
	if !apiVersionVerifyResult.Status {
		return &response.DeleteUserResponse{
			MetaInfo: &apiVersionVerifyResult,
		}, nil
	}

	responseMetaInfo, onlineUserInfoCache := UserTokenVerify(request.GetUserVerifyInfo().GetUserName(), request.GetUserVerifyInfo().GetUserToken())
	if !responseMetaInfo.Status {
		return &response.DeleteUserResponse{
			MetaInfo: &responseMetaInfo,
		}, nil
	}

	if onlineUserInfoCache.UserInfo.UserPermissionGroup != dal.PermissionGroupAdmin {
		return &response.DeleteUserResponse{
			MetaInfo: &message.ResponseMetaInfoV1{
				Status:  false,
				Id:      "",
				Message: "You don't have permission to delete a user!",
			},
		}, nil
	}

	userMetaInfo := dbmodel.UserMetaInfoV1{}
	userMetaInfo.Name = request.UserName

	result := dal.DeleteUser(userMetaInfo, dal.GetDbInstance())
	if result.Status == true {
		log.Println("User " + request.UserName + " Deleted")
		return &response.DeleteUserResponse{
			MetaInfo: &message.ResponseMetaInfoV1{
				Status:  true,
				Id:      "",
				Message: result.Message,
			},
		}, nil
	} else {
		return &response.DeleteUserResponse{
			MetaInfo: &message.ResponseMetaInfoV1{
				Status:  false,
				Id:      "",
				Message: result.Message,
			},
		}, nil
	}
}

func (D DBMSServerController) UpdateUser(ctx context.Context, request *request.UpdateUserRequest) (*response.UpdateUserResponse, error) {
	apiVersionVerifyResult := RequestApiVersionVerify(request.GetMetaInfo())
	if !apiVersionVerifyResult.Status {
		return &response.UpdateUserResponse{
			MetaInfo: &apiVersionVerifyResult,
		}, nil
	}

	responseMetaInfo, _ := UserTokenVerify(request.GetUserVerifyInfo().GetUserName(), request.GetUserVerifyInfo().GetUserToken())
	if !responseMetaInfo.Status {
		return &response.UpdateUserResponse{
			MetaInfo: &responseMetaInfo,
		}, nil
	}

	userMetaInfo := UserMetaInfoV1ProtobufToDbmodel(request.UserInfo)

	result := dal.ModifyUser(*userMetaInfo, dal.GetDbInstance())
	if result.Status == true {
		log.Println("User " + request.UserInfo.Name + " Updated")

		// update online user cache
		if _, ok := OnlineUserInfoCache[userMetaInfo.Name]; !ok {
			currentOnlineUserInfo := OnlineUserInfoCache[userMetaInfo.Name]
			currentOnlineUserInfo.UserInfo = *userMetaInfo
		}

		return &response.UpdateUserResponse{
			MetaInfo: &message.ResponseMetaInfoV1{
				Status:  true,
				Id:      "",
				Message: result.Message,
			},
			UserInfo: UserMetaInfoV1DbmodelToProtobuf(userMetaInfo),
		}, nil
	} else {
		return &response.UpdateUserResponse{
			MetaInfo: &message.ResponseMetaInfoV1{
				Status:  false,
				Id:      "",
				Message: result.Message,
			},
			UserInfo: UserMetaInfoV1DbmodelToProtobuf(userMetaInfo),
		}, nil
	}
}

func (D DBMSServerController) GetUser(ctx context.Context, request *request.GetUserRequest) (*response.GetUserResponse, error) {
	apiVersionVerifyResult := RequestApiVersionVerify(request.GetMetaInfo())
	if !apiVersionVerifyResult.Status {
		return &response.GetUserResponse{
			MetaInfo: &apiVersionVerifyResult,
		}, nil
	}

	//responseMetaInfo, _ := UserTokenVerify(request.GetUserVerifyInfo().GetUserName(), request.GetUserVerifyInfo().GetUserToken())
	//if !responseMetaInfo.Status {
	//	return &response.GetUserResponse{
	//		MetaInfo: &responseMetaInfo,
	//	}, nil
	//}

	userMetaInfo := dbmodel.UserMetaInfoV1{}
	userMetaInfo.Name = request.UserName

	result := dal.QueryUser(&userMetaInfo, dal.GetDbInstance())
	if result.Status == true {
		log.Println("User " + request.UserName + " Get")
		return &response.GetUserResponse{
			MetaInfo: &message.ResponseMetaInfoV1{
				Status:  true,
				Id:      "",
				Message: result.Message,
			},
			UserInfo: UserMetaInfoV1DbmodelToProtobuf(&userMetaInfo),
		}, nil
	} else {
		return &response.GetUserResponse{
			MetaInfo: &message.ResponseMetaInfoV1{
				Status:  false,
				Id:      "",
				Message: result.Message,
			},
			UserInfo: UserMetaInfoV1DbmodelToProtobuf(&userMetaInfo),
		}, nil
	}
}

func (D DBMSServerController) GetAllUser(ctx context.Context, request *request.GetAllUserRequest) (*response.GetAllUserResponse, error) {
	apiVersionVerifyResult := RequestApiVersionVerify(request.GetMetaInfo())
	if !apiVersionVerifyResult.Status {
		return &response.GetAllUserResponse{
			MetaInfo: &apiVersionVerifyResult,
		}, nil
	}

	responseMetaInfo, _ := UserTokenVerify(request.GetUserVerifyInfo().GetUserName(), request.GetUserVerifyInfo().GetUserToken())
	if !responseMetaInfo.Status {
		return &response.GetAllUserResponse{
			MetaInfo: &responseMetaInfo,
		}, nil
	}

	var userMetaInfoList []dbmodel.UserMetaInfoV1
	var protoMessage []*message.UserMetaInfoV1

	result := dal.QueryAllUser(&userMetaInfoList, dal.GetDbInstance())
	if result.Status == true {
		log.Println("User " + request.UserVerifyInfo.GetUserName() + " Try Get AllUser")
		for _, userMetaInfo := range userMetaInfoList {
			userMetaInfo.Password = ""
			protoMessage = append(protoMessage, UserMetaInfoV1DbmodelToProtobuf(&userMetaInfo))
		}
		return &response.GetAllUserResponse{
			MetaInfo: &message.ResponseMetaInfoV1{
				Status:  true,
				Id:      "",
				Message: result.Message,
			},
			UserInfo: protoMessage,
		}, nil
	} else {
		return &response.GetAllUserResponse{
			MetaInfo: &message.ResponseMetaInfoV1{
				Status:  false,
				Id:      "",
				Message: result.Message,
			},
			UserInfo: protoMessage,
		}, nil
	}
}

func (D DBMSServerController) UserLogin(ctx context.Context, request *request.UserLoginRequest) (*response.UserLoginResponse, error) {
	if request == nil {
		return &response.UserLoginResponse{
			MetaInfo: &message.ResponseMetaInfoV1{
				Status:  false,
				Id:      "",
				Message: "Request is nil",
			},
			UserInfo: nil,
			UserVerifyInfo: &message.UserVerifyInfoV1{
				UserName:  request.GetUserName(),
				UserToken: "",
			},
		}, nil
	}

	apiVersionVerifyResult := RequestApiVersionVerify(request.GetMetaInfo())
	if !apiVersionVerifyResult.Status {
		return &response.UserLoginResponse{
			MetaInfo: &apiVersionVerifyResult,
		}, nil
	}

	var userMetaInfo dbmodel.UserMetaInfoV1
	userMetaInfo.Name = request.UserName

	result := dal.QueryUser(&userMetaInfo, dal.GetDbInstance())
	if result.Status == true {
		if userMetaInfo.Password == request.Password {
			log.Println("User " + request.UserName + " Login")
			DailyStatisticsInfo.ActiveUserNumber += 1

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

			return &response.UserLoginResponse{
				MetaInfo: &message.ResponseMetaInfoV1{
					Status:  true,
					Id:      "",
					Message: result.Message,
				},
				UserInfo: UserMetaInfoV1DbmodelToProtobuf(&userMetaInfo),
				UserVerifyInfo: &message.UserVerifyInfoV1{
					UserName:  request.GetUserName(),
					UserToken: userToken,
				},
			}, nil
		} else {
			userMetaInfo.Password = ""
			return &response.UserLoginResponse{
				MetaInfo: &message.ResponseMetaInfoV1{
					Status:  false,
					Id:      "",
					Message: result.Message,
				},
			}, nil
		}
	} else {
		userMetaInfo.Password = ""
		return &response.UserLoginResponse{
			MetaInfo: &message.ResponseMetaInfoV1{
				Status:  false,
				Id:      "",
				Message: result.Message,
			},
		}, nil
	}
}

func (D DBMSServerController) UserLogout(ctx context.Context, request *request.UserLogoutRequest) (*response.UserLogoutResponse, error) {
	if request == nil {
		return &response.UserLogoutResponse{
			MetaInfo: &message.ResponseMetaInfoV1{
				Status:  false,
				Id:      "",
				Message: "Request is nil",
			},
		}, nil
	}

	apiVersionVerifyResult := RequestApiVersionVerify(request.GetMetaInfo())
	if !apiVersionVerifyResult.Status {
		return &response.UserLogoutResponse{
			MetaInfo: &apiVersionVerifyResult,
		}, nil
	}

	responseMetaInfo, _ := UserTokenVerify(request.GetUserVerifyInfo().GetUserName(), request.GetUserVerifyInfo().GetUserToken())
	if !responseMetaInfo.Status {
		return &response.UserLogoutResponse{
			MetaInfo: &responseMetaInfo,
		}, nil
	}

	if _, ok := OnlineUserInfoCache[request.UserVerifyInfo.GetUserName()]; ok {
		onlineUserInfo := OnlineUserInfoCache[request.UserVerifyInfo.GetUserName()]
		onlineUserInfo.expired = true
		OnlineUserInfoCache[request.UserVerifyInfo.GetUserName()] = onlineUserInfo
		delete(OnlineUserInfoCache, request.UserVerifyInfo.GetUserName())

		log.Println("User " + onlineUserInfo.UserInfo.Name + " Logout")

		return &response.UserLogoutResponse{
			MetaInfo: &message.ResponseMetaInfoV1{
				Status:  true,
				Id:      "",
				Message: "Logout Successfully!",
			},
		}, nil
	}
	return &response.UserLogoutResponse{
		MetaInfo: &message.ResponseMetaInfoV1{
			Status:  false,
			Id:      "",
			Message: "Logout Failed!",
		},
	}, nil
}

func (D DBMSServerController) UserOnlineHeartBeatNotifications(ctx context.Context, notification *request.UserOnlineHeartBeatNotification) (*response.UserOnlineHeartBeatResponse, error) {
	if notification == nil {
		return &response.UserOnlineHeartBeatResponse{
			MetaInfo: &message.ResponseMetaInfoV1{
				Status:  false,
				Id:      "",
				Message: "Request is nil",
			},
		}, nil
	}

	apiVersionVerifyResult := RequestApiVersionVerify(notification.GetMetaInfo())
	if !apiVersionVerifyResult.Status {
		return &response.UserOnlineHeartBeatResponse{
			MetaInfo: &apiVersionVerifyResult,
		}, nil
	}

	responseMetaInfo, _ := UserTokenVerify(notification.UserVerifyInfo.UserName, notification.UserVerifyInfo.UserToken)
	if !responseMetaInfo.Status {
		return &response.UserOnlineHeartBeatResponse{
			MetaInfo: &responseMetaInfo,
		}, nil
	}

	var userMetaInfo dbmodel.UserMetaInfoV1
	userMetaInfo.Name = notification.UserVerifyInfo.GetUserName()

	result := dal.QueryUser(&userMetaInfo, dal.GetDbInstance())
	if result.Status == true {
		log.Println("User " + notification.UserVerifyInfo.GetUserName() + " OnlineHeartBeatNotifications")

		userToken := ""
		if _, ok := OnlineUserInfoCache[userMetaInfo.Name]; !ok {
			DailyStatisticsInfo.ActiveUserNumber += 1
			userToken = uuid.NewString()
			OnlineUserInfoCache[userMetaInfo.Name] = OnlineUserInfo{userMetaInfo, userToken, false, time.Now().Add(30 * time.Second)}
			log.Println("User " + userMetaInfo.Name + " HeartBeat Init by HeartBeat Notification")
		} else {
			userToken = OnlineUserInfoCache[userMetaInfo.Name].Token
			onlineUserInfo := OnlineUserInfoCache[userMetaInfo.Name]
			onlineUserInfo.LastHeartBeatTime = time.Now().Add(30 * time.Second)
			OnlineUserInfoCache[userMetaInfo.Name] = onlineUserInfo
			log.Println("User " + onlineUserInfo.UserInfo.Name + " HeartBeat Refresh")
		}

		return &response.UserOnlineHeartBeatResponse{
			MetaInfo: &message.ResponseMetaInfoV1{
				Status:  true,
				Id:      "",
				Message: result.Message,
			},
			UserVerifyInfo: &message.UserVerifyInfoV1{
				UserName:  notification.UserVerifyInfo.GetUserName(),
				UserToken: userToken,
			},
		}, nil
	}
	return &response.UserOnlineHeartBeatResponse{
		MetaInfo: &message.ResponseMetaInfoV1{
			Status:  false,
			Id:      "",
			Message: result.Message,
		},
	}, nil
}

func (D DBMSServerController) GetUserPermissionGroup(ctx context.Context, request *request.GetUserPermissionGroupRequest) (*response.GetUserPermissionGroupResponse, error) {
	apiVersionVerifyResult := RequestApiVersionVerify(request.GetMetaInfo())
	if !apiVersionVerifyResult.Status {
		return &response.GetUserPermissionGroupResponse{
			MetaInfo: &apiVersionVerifyResult,
		}, nil
	}

	responseMetaInfo, _ := UserTokenVerify(request.GetUserVerifyInfo().GetUserName(), request.GetUserVerifyInfo().GetUserToken())
	if !responseMetaInfo.Status {
		return &response.GetUserPermissionGroupResponse{
			MetaInfo: &responseMetaInfo,
		}, nil
	}

	userMetaInfo := dbmodel.UserMetaInfoV1{}
	userMetaInfo.Name = request.UserVerifyInfo.GetUserName()

	var permissionGroupMetaInfo dbmodel.PermissionGroupMetaInfoV1

	result := dal.QueryUser(&userMetaInfo, dal.GetDbInstance())
	if result.Status == true {
		permissionGroupMetaInfo.Name = userMetaInfo.UserPermissionGroup
		result = dal.QueryPermissionGroup(&permissionGroupMetaInfo, dal.GetDbInstance())
		if result.Status == true {
			log.Println("User " + request.UserVerifyInfo.GetUserName() + " GetUserPermissionGroup")
			return &response.GetUserPermissionGroupResponse{
				MetaInfo: &message.ResponseMetaInfoV1{
					Status:  true,
					Id:      "",
					Message: result.Message,
				},
				PermissionGroup: PermissionGroupMetaInfoV1DbmodelToProtobuf(&permissionGroupMetaInfo),
			}, nil
		}

	}
	return &response.GetUserPermissionGroupResponse{
		MetaInfo: &message.ResponseMetaInfoV1{
			Status:  false,
			Id:      "",
			Message: result.Message,
		},
		PermissionGroup: PermissionGroupMetaInfoV1DbmodelToProtobuf(&permissionGroupMetaInfo),
	}, nil
}

func (D DBMSServerController) GetPermissionGroup(ctx context.Context, request *request.GetPermissionGroupRequest) (*response.GetPermissionGroupResponse, error) {
	apiVersionVerifyResult := RequestApiVersionVerify(request.GetMetaInfo())
	if !apiVersionVerifyResult.Status {
		return &response.GetPermissionGroupResponse{
			MetaInfo: &apiVersionVerifyResult,
		}, nil
	}

	responseMetaInfo, _ := UserTokenVerify(request.GetUserVerifyInfo().GetUserName(), request.GetUserVerifyInfo().GetUserToken())
	if !responseMetaInfo.Status {
		return &response.GetPermissionGroupResponse{
			MetaInfo: &responseMetaInfo,
		}, nil
	}

	userMetaInfo := dbmodel.UserMetaInfoV1{}
	userMetaInfo.Name = request.UserVerifyInfo.GetUserName()

	permissionGroupMetaInfo := PermissionGroupMetaInfoV1ProtobufToDbmodel(request.PermissionGroup)

	result := dal.QueryUser(&userMetaInfo, dal.GetDbInstance())
	if result.Status == true {
		result = dal.QueryPermissionGroup(permissionGroupMetaInfo, dal.GetDbInstance())
		if result.Status == true {
			log.Println("User " + request.UserVerifyInfo.GetUserName() + " GetPermissionGroup")
			return &response.GetPermissionGroupResponse{
				MetaInfo: &message.ResponseMetaInfoV1{
					Status:  true,
					Id:      "",
					Message: result.Message,
				},
				PermissionGroup: PermissionGroupMetaInfoV1DbmodelToProtobuf(permissionGroupMetaInfo),
			}, nil
		}

	}
	return &response.GetPermissionGroupResponse{
		MetaInfo: &message.ResponseMetaInfoV1{
			Status:  false,
			Id:      "",
			Message: result.Message,
		},
		PermissionGroup: PermissionGroupMetaInfoV1DbmodelToProtobuf(permissionGroupMetaInfo),
	}, nil
}

func GetAllPermissionGroup(ctx context.Context, request *request.GetAllPermissionGroupRequest) (*response.GetAllPermissionGroupResponse, error) {
	apiVersionVerifyResult := RequestApiVersionVerify(request.GetMetaInfo())
	if !apiVersionVerifyResult.Status {
		return &response.GetAllPermissionGroupResponse{
			MetaInfo: &apiVersionVerifyResult,
		}, nil
	}

	responseMetaInfo, _ := UserTokenVerify(request.GetUserVerifyInfo().GetUserName(), request.GetUserVerifyInfo().GetUserToken())
	if !responseMetaInfo.Status {
		return &response.GetAllPermissionGroupResponse{
			MetaInfo: &responseMetaInfo,
		}, nil
	}

	userMetaInfo := dbmodel.UserMetaInfoV1{}
	userMetaInfo.Name = request.UserVerifyInfo.GetUserName()

	var permissionGroupList []dbmodel.PermissionGroupMetaInfoV1
	var protoMessage []*message.PermissionGroupMetaInfoV1

	result := dal.QueryUser(&userMetaInfo, dal.GetDbInstance())
	if result.Status == true {
		result = dal.QueryAllPermissionGroup(&permissionGroupList, dal.GetDbInstance())
		if result.Status == true {
			log.Println("User " + request.UserVerifyInfo.GetUserName() + " GetAllPermissionGroup")
			for _, permissionGroupMetaInfo := range permissionGroupList {
				protoMessage = append(protoMessage, PermissionGroupMetaInfoV1DbmodelToProtobuf(&permissionGroupMetaInfo))
			}
			return &response.GetAllPermissionGroupResponse{
				MetaInfo: &message.ResponseMetaInfoV1{
					Status:  true,
					Id:      "",
					Message: result.Message,
				},
				PermissionGroupList: protoMessage,
			}, nil
		}

	}
	return &response.GetAllPermissionGroupResponse{
		MetaInfo: &message.ResponseMetaInfoV1{
			Status:  false,
			Id:      "",
			Message: result.Message,
		},
		PermissionGroupList: protoMessage,
	}, nil
}

func (D DBMSServerController) ChangeUserPermissionGroup(ctx context.Context, request *request.ChangeUserPermissionGroupRequest) (*response.ChangeUserPermissionGroupResponse, error) {
	apiVersionVerifyResult := RequestApiVersionVerify(request.GetMetaInfo())
	if !apiVersionVerifyResult.Status {
		return &response.ChangeUserPermissionGroupResponse{
			MetaInfo: &apiVersionVerifyResult,
		}, nil
	}

	responseMetaInfo, _ := UserTokenVerify(request.GetUserVerifyInfo().GetUserName(), request.GetUserVerifyInfo().GetUserToken())
	if !responseMetaInfo.Status {
		return &response.ChangeUserPermissionGroupResponse{
			MetaInfo: &responseMetaInfo,
		}, nil
	}

	userMetaInfo := dbmodel.UserMetaInfoV1{}
	userMetaInfo.Name = request.UserVerifyInfo.GetUserName()

	targetUserMetaInfo := dbmodel.UserMetaInfoV1{}
	userMetaInfo.Name = request.TargetUserName

	result := dal.QueryUser(&userMetaInfo, dal.GetDbInstance())
	if result.Status == true {
		if userMetaInfo.UserPermissionGroup != dal.PermissionGroupAdmin {
			return &response.ChangeUserPermissionGroupResponse{
				MetaInfo: &message.ResponseMetaInfoV1{
					Status:  false,
					Id:      "",
					Message: "You don't have permission to change user group!",
				},
			}, nil
		}

		var permissionGroupMetaInfo dbmodel.PermissionGroupMetaInfoV1

		result = dal.QueryUser(&targetUserMetaInfo, dal.GetDbInstance())
		if result.Status == true {
			permissionGroupMetaInfo.Name = targetUserMetaInfo.UserPermissionGroup
			result = dal.QueryPermissionGroup(&permissionGroupMetaInfo, dal.GetDbInstance())
			if result.Status == true {
				result = dal.ModifyUser(targetUserMetaInfo, dal.GetDbInstance())

				log.Println("User " + request.TargetUserName + " PermissionGroup Changed by " + request.UserVerifyInfo.GetUserName())
				return &response.ChangeUserPermissionGroupResponse{
					MetaInfo: &message.ResponseMetaInfoV1{
						Status:  true,
						Id:      "",
						Message: result.Message,
					},
				}, nil
			} else {
				return &response.ChangeUserPermissionGroupResponse{
					MetaInfo: &message.ResponseMetaInfoV1{
						Status:  false,
						Id:      "",
						Message: result.Message,
					},
				}, nil
			}

		}
	}
	return &response.ChangeUserPermissionGroupResponse{
		MetaInfo: &message.ResponseMetaInfoV1{
			Status:  false,
			Id:      "",
			Message: result.Message,
		},
	}, nil
}

func (D DBMSServerController) CreateProject(ctx context.Context, request *request.CreateProjectRequest) (*response.CreateProjectResponse, error) {
	apiVersionVerifyResult := RequestApiVersionVerify(request.GetMetaInfo())
	if !apiVersionVerifyResult.Status {
		return &response.CreateProjectResponse{
			MetaInfo: &apiVersionVerifyResult,
		}, nil
	}

	responseMetaInfo, _ := UserTokenVerify(request.GetUserVerifyInfo().GetUserName(), request.GetUserVerifyInfo().GetUserToken())
	if !responseMetaInfo.Status {
		return &response.CreateProjectResponse{
			MetaInfo: &responseMetaInfo,
		}, nil
	}

	userMetaInfo := dbmodel.UserMetaInfoV1{}
	userMetaInfo.Name = request.UserVerifyInfo.GetUserName()

	projectMetaInfo := ProjectMetaInfoV1ProtobufToDbmodel(request.ProjectInfo)

	result := dal.QueryUser(&userMetaInfo, dal.GetDbInstance())
	if result.Status == false {
		return &response.CreateProjectResponse{
			MetaInfo: &message.ResponseMetaInfoV1{
				Status:  false,
				Id:      "",
				Message: result.Message,
			},
			ProjectInfo: ProjectMetaInfoV1DbmodelToProtobuf(projectMetaInfo),
		}, nil
	}

	var permissionGroup dbmodel.PermissionGroupMetaInfoV1
	permissionGroup.Name = userMetaInfo.UserPermissionGroup
	result = dal.QueryPermissionGroup(&permissionGroup, dal.GetDbInstance())
	if result.Status == false {
		return &response.CreateProjectResponse{
			MetaInfo: &message.ResponseMetaInfoV1{
				Status:  false,
				Id:      "",
				Message: result.Message,
			},
			ProjectInfo: ProjectMetaInfoV1DbmodelToProtobuf(projectMetaInfo),
		}, nil
	}

	if permissionGroup.Global.WritePermissionCreateProject == false {
		return &response.CreateProjectResponse{
			MetaInfo: &message.ResponseMetaInfoV1{
				Status:  false,
				Id:      "",
				Message: "You don't have permission to create project!",
			},
			ProjectInfo: ProjectMetaInfoV1DbmodelToProtobuf(projectMetaInfo),
		}, nil
	}

	projectMetaInfo.Base.Id = primitive.NewObjectID()
	projectMetaInfo.Base.Uuid = uuid.NewString()
	projectMetaInfo.Base.DataAccessModelVersion = "V1"

	projectMetaInfo.Name = request.ProjectInfo.Name
	projectMetaInfo.Description = request.ProjectInfo.Description
	projectMetaInfo.Creator = request.UserVerifyInfo.GetUserName()

	projectMetaInfo.CreateTime = time.Now()
	projectMetaInfo.LastModifiedTime = time.Now()

	projectMetaInfo.WorkMode = request.ProjectInfo.WorkMode

	result = dal.CreateProject(*projectMetaInfo, dal.GetDbInstance())
	if result.Status == true {
		log.Println("Project " + request.ProjectInfo.Name + " Created")
		DailyStatisticsInfo.CreatedProjectNumber += 1
		return &response.CreateProjectResponse{
			MetaInfo: &message.ResponseMetaInfoV1{
				Status:  true,
				Id:      "",
				Message: result.Message,
			},
			ProjectInfo: ProjectMetaInfoV1DbmodelToProtobuf(projectMetaInfo),
		}, nil
	} else {
		return &response.CreateProjectResponse{
			MetaInfo: &message.ResponseMetaInfoV1{
				Status:  false,
				Id:      "",
				Message: result.Message,
			},
			ProjectInfo: ProjectMetaInfoV1DbmodelToProtobuf(projectMetaInfo),
		}, nil
	}
}

func (D DBMSServerController) DeleteProject(ctx context.Context, request *request.DeleteProjectRequest) (*response.DeleteProjectResponse, error) {
	apiVersionVerifyResult := RequestApiVersionVerify(request.GetMetaInfo())
	if !apiVersionVerifyResult.Status {
		return &response.DeleteProjectResponse{
			MetaInfo: &apiVersionVerifyResult,
		}, nil
	}

	responseMetaInfo, _ := UserTokenVerify(request.GetUserVerifyInfo().GetUserName(), request.GetUserVerifyInfo().GetUserToken())
	if !responseMetaInfo.Status {
		return &response.DeleteProjectResponse{
			MetaInfo: &responseMetaInfo,
		}, nil
	}

	userMetaInfo := dbmodel.UserMetaInfoV1{}
	userMetaInfo.Name = request.UserVerifyInfo.GetUserName()

	projectMetaInfo := dbmodel.ProjectMetaInfoV1{}
	projectMetaInfo.Name = request.ProjectName

	result := dal.QueryUser(&userMetaInfo, dal.GetDbInstance())
	if result.Status == false {
		return &response.DeleteProjectResponse{
			MetaInfo: &message.ResponseMetaInfoV1{
				Status:  false,
				Id:      "",
				Message: result.Message,
			},
			ProjectInfo: ProjectMetaInfoV1DbmodelToProtobuf(&projectMetaInfo),
		}, nil
	}

	var permissionGroup dbmodel.PermissionGroupMetaInfoV1
	permissionGroup.Name = userMetaInfo.UserPermissionGroup
	result = dal.QueryPermissionGroup(&permissionGroup, dal.GetDbInstance())
	if result.Status == false {
		return &response.DeleteProjectResponse{
			MetaInfo: &message.ResponseMetaInfoV1{
				Status:  false,
				Id:      "",
				Message: result.Message,
			},
			ProjectInfo: ProjectMetaInfoV1DbmodelToProtobuf(&projectMetaInfo),
		}, nil
	}

	if permissionGroup.Global.WritePermissionDeleteProject == false {
		return &response.DeleteProjectResponse{
			MetaInfo: &message.ResponseMetaInfoV1{
				Status:  false,
				Id:      "",
				Message: "You don't have permission to delete project!",
			},
			ProjectInfo: ProjectMetaInfoV1DbmodelToProtobuf(&projectMetaInfo),
		}, nil
	}

	result = dal.DeleteProject(projectMetaInfo, dal.GetDbInstance())
	if result.Status == true {
		log.Println("Project " + request.ProjectName + " Deleted")
		DailyStatisticsInfo.DeletedProjectNumber += 1
		return &response.DeleteProjectResponse{
			MetaInfo: &message.ResponseMetaInfoV1{
				Status:  true,
				Id:      "",
				Message: result.Message,
			},
			ProjectInfo: ProjectMetaInfoV1DbmodelToProtobuf(&projectMetaInfo),
		}, nil
	} else {
		return &response.DeleteProjectResponse{
			MetaInfo: &message.ResponseMetaInfoV1{
				Status:  false,
				Id:      "",
				Message: result.Message,
			},
			ProjectInfo: ProjectMetaInfoV1DbmodelToProtobuf(&projectMetaInfo),
		}, nil
	}
}

func (D DBMSServerController) UpdateProject(ctx context.Context, request *request.UpdateProjectRequest) (*response.UpdateProjectResponse, error) {
	apiVersionVerifyResult := RequestApiVersionVerify(request.GetMetaInfo())
	if !apiVersionVerifyResult.Status {
		return &response.UpdateProjectResponse{
			MetaInfo: &apiVersionVerifyResult,
		}, nil
	}

	responseMetaInfo, _ := UserTokenVerify(request.GetUserVerifyInfo().GetUserName(), request.GetUserVerifyInfo().GetUserToken())
	if !responseMetaInfo.Status {
		return &response.UpdateProjectResponse{
			MetaInfo: &responseMetaInfo,
		}, nil
	}

	userMetaInfo := dbmodel.UserMetaInfoV1{}
	userMetaInfo.Name = request.UserVerifyInfo.GetUserName()

	projectMetaInfo := ProjectMetaInfoV1ProtobufToDbmodel(request.ProjectInfo)

	result := dal.QueryUser(&userMetaInfo, dal.GetDbInstance())
	if result.Status == false {
		return &response.UpdateProjectResponse{
			MetaInfo: &message.ResponseMetaInfoV1{
				Status:  false,
				Id:      "",
				Message: result.Message,
			},
			ProjectInfo: ProjectMetaInfoV1DbmodelToProtobuf(projectMetaInfo),
		}, nil
	}

	var permissionGroup dbmodel.PermissionGroupMetaInfoV1
	permissionGroup.Name = userMetaInfo.UserPermissionGroup
	result = dal.QueryPermissionGroup(&permissionGroup, dal.GetDbInstance())
	if result.Status == false {
		return &response.UpdateProjectResponse{
			MetaInfo: &message.ResponseMetaInfoV1{
				Status:  false,
				Id:      "",
				Message: result.Message,
			},
			ProjectInfo: ProjectMetaInfoV1DbmodelToProtobuf(projectMetaInfo),
		}, nil
	}

	if permissionGroup.Global.WritePermissionModifyProject == false {
		return &response.UpdateProjectResponse{
			MetaInfo: &message.ResponseMetaInfoV1{
				Status:  false,
				Id:      "",
				Message: "You don't have permission to update project!",
			},
			ProjectInfo: ProjectMetaInfoV1DbmodelToProtobuf(projectMetaInfo),
		}, nil
	}

	projectMetaInfo.LastModifiedTime = time.Now()

	result = dal.ModifyProject(*projectMetaInfo, dal.GetDbInstance())
	if result.Status == true {
		log.Println("Project " + request.UserVerifyInfo.GetUserName() + " Updated")
		DailyStatisticsInfo.ModifiedProjectNumber += 1
		return &response.UpdateProjectResponse{
			MetaInfo: &message.ResponseMetaInfoV1{
				Status:  true,
				Id:      "",
				Message: result.Message,
			},
			ProjectInfo: ProjectMetaInfoV1DbmodelToProtobuf(projectMetaInfo),
		}, nil
	} else {
		return &response.UpdateProjectResponse{
			MetaInfo: &message.ResponseMetaInfoV1{
				Status:  false,
				Id:      "",
				Message: result.Message,
			},
			ProjectInfo: ProjectMetaInfoV1DbmodelToProtobuf(projectMetaInfo),
		}, nil
	}
}

func (D DBMSServerController) GetProject(ctx context.Context, request *request.GetProjectRequest) (*response.GetProjectResponse, error) {
	apiVersionVerifyResult := RequestApiVersionVerify(request.GetMetaInfo())
	if !apiVersionVerifyResult.Status {
		return &response.GetProjectResponse{
			MetaInfo: &apiVersionVerifyResult,
		}, nil
	}

	responseMetaInfo, _ := UserTokenVerify(request.GetUserVerifyInfo().GetUserName(), request.GetUserVerifyInfo().GetUserToken())
	if !responseMetaInfo.Status {
		return &response.GetProjectResponse{
			MetaInfo: &responseMetaInfo,
		}, nil
	}

	userMetaInfo := dbmodel.UserMetaInfoV1{}
	userMetaInfo.Name = request.UserVerifyInfo.GetUserName()

	projectMetaInfo := dbmodel.ProjectMetaInfoV1{}
	projectMetaInfo.Name = request.ProjectName

	result := dal.QueryUser(&userMetaInfo, dal.GetDbInstance())
	if result.Status == false {
		return &response.GetProjectResponse{
			MetaInfo: &message.ResponseMetaInfoV1{
				Status:  false,
				Id:      "",
				Message: result.Message,
			},
			ProjectInfo: ProjectMetaInfoV1DbmodelToProtobuf(&projectMetaInfo),
		}, nil
	}

	var permissionGroup dbmodel.PermissionGroupMetaInfoV1
	permissionGroup.Name = userMetaInfo.UserPermissionGroup
	result = dal.QueryPermissionGroup(&permissionGroup, dal.GetDbInstance())
	if result.Status == false {
		return &response.GetProjectResponse{
			MetaInfo: &message.ResponseMetaInfoV1{
				Status:  false,
				Id:      "",
				Message: result.Message,
			},
			ProjectInfo: ProjectMetaInfoV1DbmodelToProtobuf(&projectMetaInfo),
		}, nil
	}

	if permissionGroup.Global.ReadPerimissionQuery == false {
		return &response.GetProjectResponse{
			MetaInfo: &message.ResponseMetaInfoV1{
				Status:  false,
				Id:      "",
				Message: "You don't have permission to access this project",
			},
			ProjectInfo: ProjectMetaInfoV1DbmodelToProtobuf(&projectMetaInfo),
		}, nil
	}

	result = dal.QueryProject(&projectMetaInfo, dal.GetDbInstance())
	if result.Status == true {
		log.Println("Project " + request.UserVerifyInfo.GetUserName() + " Get")
		DailyStatisticsInfo.ProjectQueryNumber += 1
		return &response.GetProjectResponse{
			MetaInfo: &message.ResponseMetaInfoV1{
				Status:  true,
				Id:      "",
				Message: result.Message,
			},
			ProjectInfo: ProjectMetaInfoV1DbmodelToProtobuf(&projectMetaInfo),
		}, nil
	} else {
		return &response.GetProjectResponse{
			MetaInfo: &message.ResponseMetaInfoV1{
				Status:  false,
				Id:      "",
				Message: result.Message,
			},
			ProjectInfo: ProjectMetaInfoV1DbmodelToProtobuf(&projectMetaInfo),
		}, nil
	}
}

func (D DBMSServerController) GetAllProject(ctx context.Context, request *request.GetAllProjectRequest) (*response.GetAllProjectResponse, error) {
	apiVersionVerifyResult := RequestApiVersionVerify(request.GetMetaInfo())
	if !apiVersionVerifyResult.Status {
		return &response.GetAllProjectResponse{
			MetaInfo: &apiVersionVerifyResult,
		}, nil
	}

	responseMetaInfo, _ := UserTokenVerify(request.GetUserVerifyInfo().GetUserName(), request.GetUserVerifyInfo().GetUserToken())
	if !responseMetaInfo.Status {
		return &response.GetAllProjectResponse{
			MetaInfo: &responseMetaInfo,
		}, nil
	}

	var projectMetaInfoList []dbmodel.ProjectMetaInfoV1
	var protoMessage []*message.ProjectMetaInfoV1

	result := dal.QueryAllProject(&projectMetaInfoList, dal.GetDbInstance())
	if result.Status == true {
		log.Println("User " + request.UserVerifyInfo.GetUserName() + " Try Get AllProject")
		DailyStatisticsInfo.ProjectQueryNumber += 1
		for _, projectMetaInfo := range projectMetaInfoList {
			protoMessage = append(protoMessage, ProjectMetaInfoV1DbmodelToProtobuf(&projectMetaInfo))
		}
		return &response.GetAllProjectResponse{
			MetaInfo: &message.ResponseMetaInfoV1{
				Status:  true,
				Id:      "",
				Message: result.Message,
			},
			ProjectInfo: protoMessage,
		}, nil
	} else {
		return &response.GetAllProjectResponse{
			MetaInfo: &message.ResponseMetaInfoV1{
				Status:  false,
				Id:      "",
				Message: result.Message,
			},
			ProjectInfo: protoMessage,
		}, nil
	}
}

func (D DBMSServerController) CreateSwc(ctx context.Context, request *request.CreateSwcRequest) (*response.CreateSwcResponse, error) {
	apiVersionVerifyResult := RequestApiVersionVerify(request.GetMetaInfo())
	if !apiVersionVerifyResult.Status {
		return &response.CreateSwcResponse{
			MetaInfo: &apiVersionVerifyResult,
		}, nil
	}

	responseMetaInfo, _ := UserTokenVerify(request.GetUserVerifyInfo().GetUserName(), request.GetUserVerifyInfo().GetUserToken())
	if !responseMetaInfo.Status {
		return &response.CreateSwcResponse{
			MetaInfo: &responseMetaInfo,
		}, nil
	}

	userMetaInfo := dbmodel.UserMetaInfoV1{}
	userMetaInfo.Name = request.UserVerifyInfo.GetUserName()

	swcMetaInfo := SwcMetaInfoV1ProtobufToDbmodel(request.SwcInfo)

	result := dal.QueryUser(&userMetaInfo, dal.GetDbInstance())
	if result.Status == false {
		return &response.CreateSwcResponse{
			MetaInfo: &message.ResponseMetaInfoV1{
				Status:  false,
				Id:      "",
				Message: result.Message,
			},
			SwcInfo: SwcMetaInfoV1DbmodelToProtobuf(swcMetaInfo),
		}, nil
	}

	var permissionGroup dbmodel.PermissionGroupMetaInfoV1
	permissionGroup.Name = userMetaInfo.UserPermissionGroup
	result = dal.QueryPermissionGroup(&permissionGroup, dal.GetDbInstance())
	if result.Status == false {
		return &response.CreateSwcResponse{
			MetaInfo: &message.ResponseMetaInfoV1{
				Status:  false,
				Id:      "",
				Message: result.Message,
			},
			SwcInfo: SwcMetaInfoV1DbmodelToProtobuf(swcMetaInfo),
		}, nil
	}

	if permissionGroup.Project.WritePermissionAddData == false {
		return &response.CreateSwcResponse{
			MetaInfo: &message.ResponseMetaInfoV1{
				Status:  false,
				Id:      "",
				Message: "You don't have permission to create swc node!",
			},
			SwcInfo: SwcMetaInfoV1DbmodelToProtobuf(swcMetaInfo),
		}, nil
	}

	swcMetaInfo.Base.Id = primitive.NewObjectID()
	swcMetaInfo.Base.Uuid = uuid.NewString()
	swcMetaInfo.Base.DataAccessModelVersion = "V1"
	swcMetaInfo.Creator = userMetaInfo.Name
	swcMetaInfo.LastModifiedTime = time.Now()
	swcMetaInfo.CreateTime = time.Now()
	swcMetaInfo.Name = request.SwcInfo.Name
	swcMetaInfo.Description = request.SwcInfo.Description
	swcMetaInfo.SwcType = request.SwcInfo.SwcType

	result = dal.CreateSwc(*swcMetaInfo, dal.GetDbInstance())
	if result.Status {
		log.Println("User " + request.UserVerifyInfo.GetUserName() + "Create Swc " + swcMetaInfo.Name)
		DailyStatisticsInfo.CreatedSwcNumber += 1
		return &response.CreateSwcResponse{
			MetaInfo: &message.ResponseMetaInfoV1{
				Status:  true,
				Id:      "",
				Message: result.Message,
			},
			SwcInfo: SwcMetaInfoV1DbmodelToProtobuf(swcMetaInfo),
		}, nil
	} else {
		return &response.CreateSwcResponse{
			MetaInfo: &message.ResponseMetaInfoV1{
				Status:  false,
				Id:      "",
				Message: result.Message,
			},
			SwcInfo: SwcMetaInfoV1DbmodelToProtobuf(swcMetaInfo),
		}, nil
	}
}

func (D DBMSServerController) DeleteSwc(ctx context.Context, request *request.DeleteSwcRequest) (*response.DeleteSwcResponse, error) {
	apiVersionVerifyResult := RequestApiVersionVerify(request.GetMetaInfo())
	if !apiVersionVerifyResult.Status {
		return &response.DeleteSwcResponse{
			MetaInfo: &apiVersionVerifyResult,
		}, nil
	}

	responseMetaInfo, _ := UserTokenVerify(request.GetUserVerifyInfo().GetUserName(), request.GetUserVerifyInfo().GetUserToken())
	if !responseMetaInfo.Status {
		return &response.DeleteSwcResponse{
			MetaInfo: &responseMetaInfo,
		}, nil
	}

	userMetaInfo := dbmodel.UserMetaInfoV1{}
	userMetaInfo.Name = request.UserVerifyInfo.GetUserName()

	swcMetaInfo := dbmodel.SwcMetaInfoV1{}
	swcMetaInfo.Name = request.SwcName

	result := dal.QueryUser(&userMetaInfo, dal.GetDbInstance())
	if result.Status == false {
		return &response.DeleteSwcResponse{
			MetaInfo: &message.ResponseMetaInfoV1{
				Status:  false,
				Id:      "",
				Message: result.Message,
			},
			SwcInfo: SwcMetaInfoV1DbmodelToProtobuf(&swcMetaInfo),
		}, nil
	}

	var permissionGroup dbmodel.PermissionGroupMetaInfoV1
	permissionGroup.Name = userMetaInfo.UserPermissionGroup
	result = dal.QueryPermissionGroup(&permissionGroup, dal.GetDbInstance())
	if result.Status == false {
		return &response.DeleteSwcResponse{
			MetaInfo: &message.ResponseMetaInfoV1{
				Status:  false,
				Id:      "",
				Message: result.Message,
			},
			SwcInfo: SwcMetaInfoV1DbmodelToProtobuf(&swcMetaInfo),
		}, nil
	}

	if permissionGroup.Project.WritePermissionDeleteData == false {
		return &response.DeleteSwcResponse{
			MetaInfo: &message.ResponseMetaInfoV1{
				Status:  false,
				Id:      "",
				Message: "You don't have permission to create swc!",
			},
			SwcInfo: SwcMetaInfoV1DbmodelToProtobuf(&swcMetaInfo),
		}, nil
	}

	result = dal.DeleteSwc(swcMetaInfo, dal.GetDbInstance())

	var projectMetaInfoList []dbmodel.ProjectMetaInfoV1
	dal.QueryAllProject(&projectMetaInfoList, dal.GetDbInstance())
	for _, projectMetaInfo := range projectMetaInfoList {
		var bFind = false
		for idx, swcValue := range projectMetaInfo.SwcList {
			if swcValue == swcMetaInfo.Name {
				projectMetaInfo.SwcList = append(projectMetaInfo.SwcList[:idx], projectMetaInfo.SwcList[idx:]...)
				bFind = true
				dal.ModifyProject(projectMetaInfo, dal.GetDbInstance())
				break
			}
			if bFind {
				break
			}
		}
	}

	if result.Status {
		result = dal.DeleteSwcDataCollection(swcMetaInfo.Name, dal.GetDbInstance())
		if result.Status {
			log.Println("User " + request.UserVerifyInfo.GetUserName() + "Delete Swc " + swcMetaInfo.Name)
			DailyStatisticsInfo.DeletedSwcNumber += 1
			return &response.DeleteSwcResponse{
				MetaInfo: &message.ResponseMetaInfoV1{
					Status:  true,
					Id:      "",
					Message: result.Message,
				},
				SwcInfo: SwcMetaInfoV1DbmodelToProtobuf(&swcMetaInfo),
			}, nil
		}
	}
	return &response.DeleteSwcResponse{
		MetaInfo: &message.ResponseMetaInfoV1{
			Status:  false,
			Id:      "",
			Message: result.Message,
		},
		SwcInfo: SwcMetaInfoV1DbmodelToProtobuf(&swcMetaInfo),
	}, nil
}

func (D DBMSServerController) UpdateSwc(ctx context.Context, request *request.UpdateSwcRequest) (*response.UpdateSwcResponse, error) {
	apiVersionVerifyResult := RequestApiVersionVerify(request.GetMetaInfo())
	if !apiVersionVerifyResult.Status {
		return &response.UpdateSwcResponse{
			MetaInfo: &apiVersionVerifyResult,
		}, nil
	}

	responseMetaInfo, _ := UserTokenVerify(request.GetUserVerifyInfo().GetUserName(), request.GetUserVerifyInfo().GetUserToken())
	if !responseMetaInfo.Status {
		return &response.UpdateSwcResponse{
			MetaInfo: &responseMetaInfo,
		}, nil
	}

	userMetaInfo := dbmodel.UserMetaInfoV1{}
	userMetaInfo.Name = request.UserVerifyInfo.GetUserName()

	swcMetaInfo := SwcMetaInfoV1ProtobufToDbmodel(request.SwcInfo)

	result := dal.QueryUser(&userMetaInfo, dal.GetDbInstance())
	if result.Status == false {
		return &response.UpdateSwcResponse{
			MetaInfo: &message.ResponseMetaInfoV1{
				Status:  false,
				Id:      "",
				Message: result.Message,
			},
			SwcInfo: SwcMetaInfoV1DbmodelToProtobuf(swcMetaInfo),
		}, nil
	}

	var permissionGroup dbmodel.PermissionGroupMetaInfoV1
	permissionGroup.Name = userMetaInfo.UserPermissionGroup
	result = dal.QueryPermissionGroup(&permissionGroup, dal.GetDbInstance())
	if result.Status == false {
		return &response.UpdateSwcResponse{
			MetaInfo: &message.ResponseMetaInfoV1{
				Status:  false,
				Id:      "",
				Message: result.Message,
			},
			SwcInfo: SwcMetaInfoV1DbmodelToProtobuf(swcMetaInfo),
		}, nil
	}

	if permissionGroup.Project.WritePermissionModifyData == false {
		return &response.UpdateSwcResponse{
			MetaInfo: &message.ResponseMetaInfoV1{
				Status:  false,
				Id:      "",
				Message: "You don't have permission to modify swc!",
			},
			SwcInfo: SwcMetaInfoV1DbmodelToProtobuf(swcMetaInfo),
		}, nil
	}

	swcMetaInfo.LastModifiedTime = time.Now()

	result = dal.ModifySwc(*swcMetaInfo, dal.GetDbInstance())
	if result.Status {
		log.Println("User " + request.UserVerifyInfo.GetUserName() + " Update SwcMetaInfo " + swcMetaInfo.Name)
		DailyStatisticsInfo.ModifiedSwcNumber += 1
		return &response.UpdateSwcResponse{
			MetaInfo: &message.ResponseMetaInfoV1{
				Status:  true,
				Id:      "",
				Message: result.Message,
			},
			SwcInfo: SwcMetaInfoV1DbmodelToProtobuf(swcMetaInfo),
		}, nil
	} else {
		return &response.UpdateSwcResponse{
			MetaInfo: &message.ResponseMetaInfoV1{
				Status:  false,
				Id:      "",
				Message: result.Message,
			},
			SwcInfo: SwcMetaInfoV1DbmodelToProtobuf(swcMetaInfo),
		}, nil
	}
}

func (D DBMSServerController) GetSwcMetaInfo(ctx context.Context, request *request.GetSwcMetaInfoRequest) (*response.GetSwcMetaInfoResponse, error) {
	apiVersionVerifyResult := RequestApiVersionVerify(request.GetMetaInfo())
	if !apiVersionVerifyResult.Status {
		return &response.GetSwcMetaInfoResponse{
			MetaInfo: &apiVersionVerifyResult,
		}, nil
	}

	responseMetaInfo, _ := UserTokenVerify(request.GetUserVerifyInfo().GetUserName(), request.GetUserVerifyInfo().GetUserToken())
	if !responseMetaInfo.Status {
		return &response.GetSwcMetaInfoResponse{
			MetaInfo: &responseMetaInfo,
		}, nil
	}

	userMetaInfo := dbmodel.UserMetaInfoV1{}
	userMetaInfo.Name = request.UserVerifyInfo.GetUserName()

	swcMetaInfo := dbmodel.SwcMetaInfoV1{}
	swcMetaInfo.Name = request.SwcName

	result := dal.QueryUser(&userMetaInfo, dal.GetDbInstance())
	if result.Status == false {
		return &response.GetSwcMetaInfoResponse{
			MetaInfo: &message.ResponseMetaInfoV1{
				Status:  false,
				Id:      "",
				Message: result.Message,
			},
			SwcInfo: SwcMetaInfoV1DbmodelToProtobuf(&swcMetaInfo),
		}, nil
	}

	var permissionGroup dbmodel.PermissionGroupMetaInfoV1
	permissionGroup.Name = userMetaInfo.UserPermissionGroup
	result = dal.QueryPermissionGroup(&permissionGroup, dal.GetDbInstance())
	if result.Status == false {
		return &response.GetSwcMetaInfoResponse{
			MetaInfo: &message.ResponseMetaInfoV1{
				Status:  false,
				Id:      "",
				Message: result.Message,
			},
			SwcInfo: SwcMetaInfoV1DbmodelToProtobuf(&swcMetaInfo),
		}, nil
	}

	if permissionGroup.Global.ReadPerimissionQuery == false {
		return &response.GetSwcMetaInfoResponse{
			MetaInfo: &message.ResponseMetaInfoV1{
				Status:  false,
				Id:      "",
				Message: "You don't have permission to access this swc",
			},
			SwcInfo: SwcMetaInfoV1DbmodelToProtobuf(&swcMetaInfo),
		}, nil
	}

	result = dal.QuerySwc(&swcMetaInfo, dal.GetDbInstance())
	if result.Status {
		log.Println("User " + request.UserVerifyInfo.GetUserName() + " Query SwcMetaInfo " + swcMetaInfo.Name)
		DailyStatisticsInfo.SwcQueryNumber += 1
		return &response.GetSwcMetaInfoResponse{
			MetaInfo: &message.ResponseMetaInfoV1{
				Status:  true,
				Id:      "",
				Message: result.Message,
			},
			SwcInfo: SwcMetaInfoV1DbmodelToProtobuf(&swcMetaInfo),
		}, nil
	} else {
		return &response.GetSwcMetaInfoResponse{
			MetaInfo: &message.ResponseMetaInfoV1{
				Status:  false,
				Id:      "",
				Message: result.Message,
			},
			SwcInfo: SwcMetaInfoV1DbmodelToProtobuf(&swcMetaInfo),
		}, nil
	}
}

func (D DBMSServerController) GetAllSwcMetaInfo(ctx context.Context, request *request.GetAllSwcMetaInfoRequest) (*response.GetAllSwcMetaInfoResponse, error) {
	apiVersionVerifyResult := RequestApiVersionVerify(request.GetMetaInfo())
	if !apiVersionVerifyResult.Status {
		return &response.GetAllSwcMetaInfoResponse{
			MetaInfo: &apiVersionVerifyResult,
		}, nil
	}

	//responseMetaInfo, _ := UserTokenVerify(request.GetUserVerifyInfo().GetUserName(), request.GetUserVerifyInfo().GetUserToken())
	//if !responseMetaInfo.Status {
	//	return &response.GetAllSwcMetaInfoResponse{
	//		MetaInfo: &responseMetaInfo,
	//	}, nil
	//}

	var dbmodelMessage []dbmodel.SwcMetaInfoV1

	var protoMessage []*message.SwcMetaInfoV1
	result := dal.QueryAllSwc(&dbmodelMessage, dal.GetDbInstance())
	if result.Status {
		log.Println("User " + request.UserVerifyInfo.GetUserName() + " Query All SwcMetaInfo ")
		for _, dbMessage := range dbmodelMessage {
			protoMessage = append(protoMessage, SwcMetaInfoV1DbmodelToProtobuf(&dbMessage))
		}
		return &response.GetAllSwcMetaInfoResponse{
			MetaInfo: &message.ResponseMetaInfoV1{
				Status:  true,
				Id:      "",
				Message: result.Message,
			},
			SwcInfo: protoMessage,
		}, nil
	} else {
		return &response.GetAllSwcMetaInfoResponse{
			MetaInfo: &message.ResponseMetaInfoV1{
				Status:  false,
				Id:      "",
				Message: result.Message,
			},
			SwcInfo: protoMessage,
		}, nil
	}
}

func (D DBMSServerController) CreateSwcNodeData(ctx context.Context, request *request.CreateSwcNodeDataRequest) (*response.CreateSwcNodeDataResponse, error) {
	apiVersionVerifyResult := RequestApiVersionVerify(request.GetMetaInfo())
	if !apiVersionVerifyResult.Status {
		return &response.CreateSwcNodeDataResponse{
			MetaInfo: &apiVersionVerifyResult,
		}, nil
	}

	responseMetaInfo, onlineUserInfoCache := UserTokenVerify(request.GetUserVerifyInfo().GetUserName(), request.GetUserVerifyInfo().GetUserToken())
	if !responseMetaInfo.Status {
		return &response.CreateSwcNodeDataResponse{
			MetaInfo: &responseMetaInfo,
		}, nil
	}

	var userMetaInfo dbmodel.UserMetaInfoV1
	userMetaInfo.Name = onlineUserInfoCache.UserInfo.Name

	result := dal.QueryUser(&userMetaInfo, dal.GetDbInstance())
	if result.Status == false {
		return &response.CreateSwcNodeDataResponse{
			MetaInfo: &message.ResponseMetaInfoV1{
				Status:  false,
				Id:      "",
				Message: result.Message,
			},
		}, nil
	}

	var permissionGroup dbmodel.PermissionGroupMetaInfoV1
	permissionGroup.Name = userMetaInfo.UserPermissionGroup
	result = dal.QueryPermissionGroup(&permissionGroup, dal.GetDbInstance())
	if result.Status == false {
		return &response.CreateSwcNodeDataResponse{
			MetaInfo: &message.ResponseMetaInfoV1{
				Status:  false,
				Id:      "",
				Message: result.Message,
			},
		}, nil
	}

	if permissionGroup.Project.WritePermissionAddData == false {
		return &response.CreateSwcNodeDataResponse{
			MetaInfo: &message.ResponseMetaInfoV1{
				Status:  false,
				Id:      "",
				Message: "You don't have permission to create swc node!",
			},
		}, nil
	}

	var swcData dbmodel.SwcDataV1
	for _, swcNodeData := range request.SwcData.SwcData {
		swcData = append(swcData, *SwcNodeDataV1ProtobufToDbmodel(swcNodeData))
	}

	createTime := time.Now()

	var nodesUuid []string

	for idx := range swcData {
		swcData[idx].Creator = userMetaInfo.Name
		swcData[idx].Base.Id = primitive.NewObjectID()
		newUuid := uuid.NewString()
		nodesUuid = append(nodesUuid, newUuid)
		swcData[idx].Base.Uuid = newUuid
		swcData[idx].Base.DataAccessModelVersion = "V1"
		swcData[idx].CreateTime = createTime
		swcData[idx].LastModifiedTime = createTime
		swcData[idx].CheckerUserUuid = ""
	}

	swcMetaInfo := dbmodel.SwcMetaInfoV1{}
	swcMetaInfo.Name = request.SwcName

	result = dal.QuerySwc(&swcMetaInfo, dal.GetDbInstance())
	if !result.Status {
		return &response.CreateSwcNodeDataResponse{
			MetaInfo: &message.ResponseMetaInfoV1{
				Status:  false,
				Id:      "",
				Message: result.Message,
			},
		}, nil
	}

	result = dal.CreateSwcData(swcMetaInfo.Name, &swcData, dal.GetDbInstance())
	if result.Status {
		log.Println("User " + onlineUserInfoCache.UserInfo.Name + " Create Swc node " + swcMetaInfo.Name)
		DailyStatisticsInfo.CreateSwcNodeNumber += 1

		operationRecord := dbmodel.SwcIncrementOperationV1{}
		operationRecord.Base.Id = primitive.NewObjectID()
		operationRecord.Base.Uuid = uuid.NewString()
		operationRecord.Base.DataAccessModelVersion = "V1"
		operationRecord.IncrementOperation = dal.IncrementOp_Create
		operationRecord.SwcData = swcData
		operationRecord.CreateTime = createTime
		dal.CreateIncrementOperation(swcMetaInfo.CurrentIncrementOperationCollectionName, operationRecord, dal.GetDbInstance())

		return &response.CreateSwcNodeDataResponse{
			MetaInfo: &message.ResponseMetaInfoV1{
				Status:  true,
				Id:      "",
				Message: result.Message,
			},
			CreatedNodesUuid: nodesUuid,
		}, nil
	} else {
		return &response.CreateSwcNodeDataResponse{
			MetaInfo: &message.ResponseMetaInfoV1{
				Status:  false,
				Id:      "",
				Message: result.Message,
			},
		}, nil
	}
}

func (D DBMSServerController) DeleteSwcNodeData(ctx context.Context, request *request.DeleteSwcNodeDataRequest) (*response.DeleteSwcNodeDataResponse, error) {
	apiVersionVerifyResult := RequestApiVersionVerify(request.GetMetaInfo())
	if !apiVersionVerifyResult.Status {
		return &response.DeleteSwcNodeDataResponse{
			MetaInfo: &apiVersionVerifyResult,
		}, nil
	}

	responseMetaInfo, onlineUserInfoCache := UserTokenVerify(request.GetUserVerifyInfo().GetUserName(), request.GetUserVerifyInfo().GetUserToken())
	if !responseMetaInfo.Status {
		return &response.DeleteSwcNodeDataResponse{
			MetaInfo: &responseMetaInfo,
		}, nil
	}

	var userMetaInfo dbmodel.UserMetaInfoV1
	userMetaInfo.Name = onlineUserInfoCache.UserInfo.Name

	swcMetaInfo := dbmodel.SwcMetaInfoV1{}
	swcMetaInfo.Name = request.SwcName

	result := dal.QuerySwc(&swcMetaInfo, dal.GetDbInstance())
	if !result.Status {
		return &response.DeleteSwcNodeDataResponse{
			MetaInfo: &message.ResponseMetaInfoV1{
				Status:  false,
				Id:      "",
				Message: result.Message,
			},
		}, nil
	}

	result = dal.QueryUser(&userMetaInfo, dal.GetDbInstance())
	if result.Status == false {
		return &response.DeleteSwcNodeDataResponse{
			MetaInfo: &message.ResponseMetaInfoV1{
				Status:  false,
				Id:      "",
				Message: result.Message,
			},
		}, nil
	}

	var permissionGroup dbmodel.PermissionGroupMetaInfoV1
	permissionGroup.Name = userMetaInfo.UserPermissionGroup
	result = dal.QueryPermissionGroup(&permissionGroup, dal.GetDbInstance())
	if result.Status == false {
		return &response.DeleteSwcNodeDataResponse{
			MetaInfo: &message.ResponseMetaInfoV1{
				Status:  false,
				Id:      "",
				Message: result.Message,
			},
		}, nil
	}

	if permissionGroup.Project.WritePermissionDeleteData == false {
		return &response.DeleteSwcNodeDataResponse{
			MetaInfo: &message.ResponseMetaInfoV1{
				Status:  false,
				Id:      "",
				Message: "You don't have permission to delete swc node!",
			},
		}, nil
	}

	var swcData dbmodel.SwcDataV1
	for _, swcNodeData := range request.SwcData.SwcData {
		swcData = append(swcData, *SwcNodeDataV1ProtobufToDbmodel(swcNodeData))
	}

	createTime := time.Now()

	result = dal.DeleteSwcData(swcMetaInfo.Name, swcData, dal.GetDbInstance())
	if result.Status {
		log.Println("User " + onlineUserInfoCache.UserInfo.Name + " Delete Swc " + swcMetaInfo.Name)
		DailyStatisticsInfo.DeletedSwcNodeNumber += 1

		operationRecord := dbmodel.SwcIncrementOperationV1{}
		operationRecord.Base.Id = primitive.NewObjectID()
		operationRecord.Base.Uuid = uuid.NewString()
		operationRecord.Base.DataAccessModelVersion = "V1"
		operationRecord.IncrementOperation = dal.IncrementOp_Delete
		operationRecord.SwcData = swcData
		operationRecord.CreateTime = createTime
		dal.CreateIncrementOperation(swcMetaInfo.CurrentIncrementOperationCollectionName, operationRecord, dal.GetDbInstance())

		return &response.DeleteSwcNodeDataResponse{
			MetaInfo: &message.ResponseMetaInfoV1{
				Status:  true,
				Id:      "",
				Message: result.Message,
			},
		}, nil
	} else {
		return &response.DeleteSwcNodeDataResponse{
			MetaInfo: &message.ResponseMetaInfoV1{
				Status:  false,
				Id:      "",
				Message: result.Message,
			},
		}, nil
	}
}

func (D DBMSServerController) UpdateSwcNodeData(ctx context.Context, request *request.UpdateSwcNodeDataRequest) (*response.UpdateSwcNodeDataResponse, error) {
	apiVersionVerifyResult := RequestApiVersionVerify(request.GetMetaInfo())
	if !apiVersionVerifyResult.Status {
		return &response.UpdateSwcNodeDataResponse{
			MetaInfo: &apiVersionVerifyResult,
		}, nil
	}

	responseMetaInfo, onlineUserInfoCache := UserTokenVerify(request.GetUserVerifyInfo().GetUserName(), request.GetUserVerifyInfo().GetUserToken())
	if !responseMetaInfo.Status {
		return &response.UpdateSwcNodeDataResponse{
			MetaInfo: &responseMetaInfo,
		}, nil
	}

	var userMetaInfo dbmodel.UserMetaInfoV1
	userMetaInfo.Name = onlineUserInfoCache.UserInfo.Name

	swcMetaInfo := dbmodel.SwcMetaInfoV1{}
	swcMetaInfo.Name = request.SwcName

	result := dal.QuerySwc(&swcMetaInfo, dal.GetDbInstance())
	if !result.Status {
		return &response.UpdateSwcNodeDataResponse{
			MetaInfo: &message.ResponseMetaInfoV1{
				Status:  false,
				Id:      "",
				Message: result.Message,
			},
		}, nil
	}

	result = dal.QueryUser(&userMetaInfo, dal.GetDbInstance())
	if result.Status == false {
		return &response.UpdateSwcNodeDataResponse{
			MetaInfo: &message.ResponseMetaInfoV1{
				Status:  false,
				Id:      "",
				Message: result.Message,
			},
		}, nil
	}

	var permissionGroup dbmodel.PermissionGroupMetaInfoV1
	permissionGroup.Name = userMetaInfo.UserPermissionGroup
	result = dal.QueryPermissionGroup(&permissionGroup, dal.GetDbInstance())
	if result.Status == false {
		return &response.UpdateSwcNodeDataResponse{
			MetaInfo: &message.ResponseMetaInfoV1{
				Status:  false,
				Id:      "",
				Message: result.Message,
			},
		}, nil
	}

	if permissionGroup.Project.WritePermissionModifyData == false {
		return &response.UpdateSwcNodeDataResponse{
			MetaInfo: &message.ResponseMetaInfoV1{
				Status:  false,
				Id:      "",
				Message: "You don't have permission to modify swc node!",
			},
		}, nil
	}

	createTime := time.Now()

	var swcData dbmodel.SwcDataV1

	for _, swcNodeData := range request.SwcData.SwcData {
		var data = *SwcNodeDataV1ProtobufToDbmodel(swcNodeData)
		data.CreateTime = createTime
		data.LastModifiedTime = createTime
		data.Base.Id = primitive.NewObjectID()
		data.Creator = request.GetUserVerifyInfo().GetUserName()
		swcData = append(swcData, data)
	}

	swcMetaInfo.LastModifiedTime = createTime

	result = dal.ModifySwcData(swcMetaInfo.Name, &swcData, dal.GetDbInstance())
	if result.Status {
		log.Println("User " + onlineUserInfoCache.UserInfo.Name + " Update Swc " + swcMetaInfo.Name)
		DailyStatisticsInfo.ModifiedSwcNodeNumber += 1

		operationRecord := dbmodel.SwcIncrementOperationV1{}
		operationRecord.Base.Id = primitive.NewObjectID()
		operationRecord.Base.Uuid = uuid.NewString()
		operationRecord.Base.DataAccessModelVersion = "V1"
		operationRecord.IncrementOperation = dal.IncrementOp_Update
		operationRecord.SwcData = swcData
		operationRecord.CreateTime = createTime
		dal.CreateIncrementOperation(swcMetaInfo.CurrentIncrementOperationCollectionName, operationRecord, dal.GetDbInstance())

		return &response.UpdateSwcNodeDataResponse{
			MetaInfo: &message.ResponseMetaInfoV1{
				Status:  true,
				Id:      "",
				Message: result.Message,
			},
		}, nil
	} else {
		return &response.UpdateSwcNodeDataResponse{
			MetaInfo: &message.ResponseMetaInfoV1{
				Status:  false,
				Id:      "",
				Message: result.Message,
			},
		}, nil
	}
}

func (D DBMSServerController) GetSwcNodeData(ctx context.Context, request *request.GetSwcNodeDataRequest) (*response.GetSwcNodeDataResponse, error) {
	apiVersionVerifyResult := RequestApiVersionVerify(request.GetMetaInfo())
	if !apiVersionVerifyResult.Status {
		return &response.GetSwcNodeDataResponse{
			MetaInfo: &apiVersionVerifyResult,
		}, nil
	}

	responseMetaInfo, _ := UserTokenVerify(request.GetUserVerifyInfo().GetUserName(), request.GetUserVerifyInfo().GetUserToken())
	if !responseMetaInfo.Status {
		return &response.GetSwcNodeDataResponse{
			MetaInfo: &responseMetaInfo,
		}, nil
	}

	userMetaInfo := dbmodel.UserMetaInfoV1{}
	userMetaInfo.Name = request.UserVerifyInfo.GetUserName()

	swcMetaInfo := dbmodel.SwcMetaInfoV1{}
	swcMetaInfo.Name = request.SwcName

	result := dal.QueryUser(&userMetaInfo, dal.GetDbInstance())
	if result.Status == false {
		return &response.GetSwcNodeDataResponse{
			MetaInfo: &message.ResponseMetaInfoV1{
				Status:  false,
				Id:      "",
				Message: result.Message,
			},
			SwcNodeData: request.SwcNodeData,
		}, nil
	}

	var permissionGroup dbmodel.PermissionGroupMetaInfoV1
	permissionGroup.Name = userMetaInfo.UserPermissionGroup
	result = dal.QueryPermissionGroup(&permissionGroup, dal.GetDbInstance())
	if result.Status == false {
		return &response.GetSwcNodeDataResponse{
			MetaInfo: &message.ResponseMetaInfoV1{
				Status:  false,
				Id:      "",
				Message: result.Message,
			},
			SwcNodeData: request.SwcNodeData,
		}, nil
	}

	if permissionGroup.Project.ReadPerimissionQuery == false {
		return &response.GetSwcNodeDataResponse{
			MetaInfo: &message.ResponseMetaInfoV1{
				Status:  false,
				Id:      "",
				Message: "You don't have permission to access this swc node",
			},
			SwcNodeData: request.SwcNodeData,
		}, nil
	}

	var dbmodelMessage dbmodel.SwcDataV1

	var protoMessage message.SwcDataV1

	for _, swcNodeData := range request.SwcNodeData.SwcData {
		dbmodelMessage = append(dbmodelMessage, *SwcNodeDataV1ProtobufToDbmodel(swcNodeData))
	}

	result = dal.QuerySwcData(swcMetaInfo.Name, &dbmodelMessage, dal.GetDbInstance())
	if result.Status {
		log.Println("User " + request.UserVerifyInfo.GetUserName() + " Get SwcData " + swcMetaInfo.Name)

		DailyStatisticsInfo.NodeQueryNumber += 1

		for _, swcNodeData := range dbmodelMessage {
			protoMessage.SwcData = append(protoMessage.SwcData, SwcNodeDataV1DbmodelToProtobuf(&swcNodeData))
		}

		return &response.GetSwcNodeDataResponse{
			MetaInfo: &message.ResponseMetaInfoV1{
				Status:  true,
				Id:      "",
				Message: result.Message,
			},
			SwcNodeData: &protoMessage,
		}, nil
	} else {
		return &response.GetSwcNodeDataResponse{
			MetaInfo: &message.ResponseMetaInfoV1{
				Status:  false,
				Id:      "",
				Message: result.Message,
			},
			SwcNodeData: &protoMessage,
		}, nil
	}
}

func (D DBMSServerController) GetSwcFullNodeData(ctx context.Context, request *request.GetSwcFullNodeDataRequest) (*response.GetSwcFullNodeDataResponse, error) {
	apiVersionVerifyResult := RequestApiVersionVerify(request.GetMetaInfo())
	if !apiVersionVerifyResult.Status {
		return &response.GetSwcFullNodeDataResponse{
			MetaInfo: &apiVersionVerifyResult,
		}, nil
	}

	responseMetaInfo, _ := UserTokenVerify(request.GetUserVerifyInfo().GetUserName(), request.GetUserVerifyInfo().GetUserToken())
	if !responseMetaInfo.Status {
		return &response.GetSwcFullNodeDataResponse{
			MetaInfo: &responseMetaInfo,
		}, nil
	}

	userMetaInfo := dbmodel.UserMetaInfoV1{}
	userMetaInfo.Name = request.UserVerifyInfo.GetUserName()

	swcMetaInfo := dbmodel.SwcMetaInfoV1{}
	swcMetaInfo.Name = request.SwcName

	var dbmodelMessage dbmodel.SwcDataV1
	var protoMessage message.SwcDataV1

	result := dal.QueryUser(&userMetaInfo, dal.GetDbInstance())
	if result.Status == false {
		return &response.GetSwcFullNodeDataResponse{
			MetaInfo: &message.ResponseMetaInfoV1{
				Status:  false,
				Id:      "",
				Message: result.Message,
			},
			SwcNodeData: &protoMessage,
		}, nil
	}

	var permissionGroup dbmodel.PermissionGroupMetaInfoV1
	permissionGroup.Name = userMetaInfo.UserPermissionGroup
	result = dal.QueryPermissionGroup(&permissionGroup, dal.GetDbInstance())
	if result.Status == false {
		return &response.GetSwcFullNodeDataResponse{
			MetaInfo: &message.ResponseMetaInfoV1{
				Status:  false,
				Id:      "",
				Message: result.Message,
			},
			SwcNodeData: &protoMessage,
		}, nil
	}

	if permissionGroup.Project.ReadPerimissionQuery == false {
		return &response.GetSwcFullNodeDataResponse{
			MetaInfo: &message.ResponseMetaInfoV1{
				Status:  false,
				Id:      "",
				Message: "You don't have permission to access this full swc node data!",
			},
			SwcNodeData: &protoMessage,
		}, nil
	}

	result = dal.QueryAllSwcData(swcMetaInfo.Name, &dbmodelMessage, dal.GetDbInstance())
	if result.Status {
		log.Println("User " + request.UserVerifyInfo.GetUserName() + " Get SwcFullNodeData " + swcMetaInfo.Name)
		DailyStatisticsInfo.NodeQueryNumber += 1
		for _, swcNodeData := range dbmodelMessage {
			protoMessage.SwcData = append(protoMessage.SwcData, SwcNodeDataV1DbmodelToProtobuf(&swcNodeData))
		}

		return &response.GetSwcFullNodeDataResponse{
			MetaInfo: &message.ResponseMetaInfoV1{
				Status:  true,
				Id:      "",
				Message: result.Message,
			},
			SwcNodeData: &protoMessage,
		}, nil
	} else {
		return &response.GetSwcFullNodeDataResponse{
			MetaInfo: &message.ResponseMetaInfoV1{
				Status:  false,
				Id:      "",
				Message: result.Message,
			},
			SwcNodeData: &protoMessage,
		}, nil
	}
}

func (D DBMSServerController) GetSwcNodeDataListByTimeAndUser(ctx context.Context, request *request.GetSwcNodeDataListByTimeAndUserRequest) (*response.GetSwcNodeDataListByTimeAndUserResponse, error) {
	apiVersionVerifyResult := RequestApiVersionVerify(request.GetMetaInfo())
	if !apiVersionVerifyResult.Status {
		return &response.GetSwcNodeDataListByTimeAndUserResponse{
			MetaInfo: &apiVersionVerifyResult,
		}, nil
	}

	responseMetaInfo, _ := UserTokenVerify(request.GetUserVerifyInfo().GetUserName(), request.GetUserVerifyInfo().GetUserToken())
	if !responseMetaInfo.Status {
		return &response.GetSwcNodeDataListByTimeAndUserResponse{
			MetaInfo: &responseMetaInfo,
		}, nil
	}

	userMetaInfo := dbmodel.UserMetaInfoV1{}
	userMetaInfo.Name = request.UserVerifyInfo.GetUserName()

	swcMetaInfo := dbmodel.SwcMetaInfoV1{}
	swcMetaInfo.Name = request.SwcName
	var dbmodelMessage dbmodel.SwcDataV1
	var protoMessage message.SwcDataV1

	result := dal.QueryUser(&userMetaInfo, dal.GetDbInstance())
	if result.Status == false {
		return &response.GetSwcNodeDataListByTimeAndUserResponse{
			MetaInfo: &message.ResponseMetaInfoV1{
				Status:  false,
				Id:      "",
				Message: result.Message,
			},
			SwcNodeData: &protoMessage,
		}, nil
	}

	var permissionGroup dbmodel.PermissionGroupMetaInfoV1
	permissionGroup.Name = userMetaInfo.UserPermissionGroup
	result = dal.QueryPermissionGroup(&permissionGroup, dal.GetDbInstance())
	if result.Status == false {
		return &response.GetSwcNodeDataListByTimeAndUserResponse{
			MetaInfo: &message.ResponseMetaInfoV1{
				Status:  false,
				Id:      "",
				Message: result.Message,
			},
			SwcNodeData: &protoMessage,
		}, nil
	}

	if permissionGroup.Project.ReadPerimissionQuery == false {
		return &response.GetSwcNodeDataListByTimeAndUserResponse{
			MetaInfo: &message.ResponseMetaInfoV1{
				Status:  false,
				Id:      "",
				Message: "You don't have permission to access the swc node data!",
			},
			SwcNodeData: &protoMessage,
		}, nil
	}

	var startTime time.Time
	var endTime time.Time

	if request.StartTime != nil {
		startTime = request.StartTime.AsTime()
	} else {
		startTime = time.Date(2023, 9, 1, 0, 0, 0, 0, time.Now().Location())
	}

	if request.EndTime != nil {
		endTime = request.EndTime.AsTime()
	} else {
		startTime = time.Now()
	}

	result = dal.QuerySwcDataByUserAndTime(swcMetaInfo.Name, request.UserName, startTime, endTime, &dbmodelMessage, dal.GetDbInstance())
	if result.Status {
		log.Println("User " + request.UserVerifyInfo.UserName + " Get SwcDataByUserAndTime " + swcMetaInfo.Name)
		DailyStatisticsInfo.NodeQueryNumber += 1

		for _, swcNodeData := range dbmodelMessage {
			protoMessage.SwcData = append(protoMessage.SwcData, SwcNodeDataV1DbmodelToProtobuf(&swcNodeData))
		}

		return &response.GetSwcNodeDataListByTimeAndUserResponse{
			MetaInfo: &message.ResponseMetaInfoV1{
				Status:  true,
				Id:      "",
				Message: result.Message,
			},
			SwcNodeData: &protoMessage,
		}, nil
	} else {
		return &response.GetSwcNodeDataListByTimeAndUserResponse{
			MetaInfo: &message.ResponseMetaInfoV1{
				Status:  false,
				Id:      "",
				Message: result.Message,
			},
			SwcNodeData: &protoMessage,
		}, nil
	}
}

func (D DBMSServerController) BackupFullDatabase(ctx context.Context, request *request.BackupFullDatabaseRequest) (*response.BackupFullDatabaseResponse, error) {
	apiVersionVerifyResult := RequestApiVersionVerify(request.GetMetaInfo())
	if !apiVersionVerifyResult.Status {
		return &response.BackupFullDatabaseResponse{
			MetaInfo: &apiVersionVerifyResult,
		}, nil
	}

	responseMetaInfo, _ := UserTokenVerify(request.GetUserVerifyInfo().GetUserName(), request.GetUserVerifyInfo().GetUserToken())
	if !responseMetaInfo.Status {
		return &response.BackupFullDatabaseResponse{
			MetaInfo: &responseMetaInfo,
		}, nil
	}

	log.Println("Unimplemented")

	return &response.BackupFullDatabaseResponse{
		MetaInfo: &message.ResponseMetaInfoV1{
			Status:  false,
			Id:      "",
			Message: "Unimplemented",
		},
		InstantBackup:   false,
		DelayBackupTime: nil,
	}, nil
}

func (D DBMSServerController) CreateDailyStatistics(ctx context.Context, request *request.CreateDailyStatisticsRequest) (*response.CreateDailyStatisticsResponse, error) {
	apiVersionVerifyResult := RequestApiVersionVerify(request.GetMetaInfo())
	if !apiVersionVerifyResult.Status {
		return &response.CreateDailyStatisticsResponse{
			MetaInfo: &apiVersionVerifyResult,
		}, nil
	}

	responseMetaInfo, _ := UserTokenVerify(request.GetUserVerifyInfo().GetUserName(), request.GetUserVerifyInfo().GetUserToken())
	if !responseMetaInfo.Status {
		return &response.CreateDailyStatisticsResponse{
			MetaInfo: &responseMetaInfo,
		}, nil
	}

	dailyStatisticsInfo := DailyStatisticsMetaInfoV1ProtobufToDbmodel(request.DailyStatisticsInfo)
	dailyStatisticsInfo.Base.Id = primitive.NewObjectID()
	dailyStatisticsInfo.Base.Uuid = uuid.NewString()
	dailyStatisticsInfo.Base.DataAccessModelVersion = "V1"

	dailyStatisticsInfo.Name = request.DailyStatisticsInfo.Name
	dailyStatisticsInfo.Description = request.DailyStatisticsInfo.Description
	dailyStatisticsInfo.Day = request.DailyStatisticsInfo.Day

	dailyStatisticsInfo.CreatedProjectNumber = 0
	dailyStatisticsInfo.CreatedSwcNumber = 0
	dailyStatisticsInfo.CreateSwcNodeNumber = 0

	dailyStatisticsInfo.DeletedProjectNumber = 0
	dailyStatisticsInfo.DeletedSwcNumber = 0
	dailyStatisticsInfo.DeletedSwcNodeNumber = 0

	dailyStatisticsInfo.ModifiedProjectNumber = 0
	dailyStatisticsInfo.ModifiedSwcNumber = 0
	dailyStatisticsInfo.ModifiedSwcNodeNumber = 0

	dailyStatisticsInfo.ProjectQueryNumber = 0
	dailyStatisticsInfo.SwcQueryNumber = 0
	dailyStatisticsInfo.NodeQueryNumber = 0

	dailyStatisticsInfo.ActiveUserNumber = 0

	result := dal.CreateDailyStatistics(*dailyStatisticsInfo, dal.GetDbInstance())
	if result.Status == true {
		log.Println("DailyStatistics " + request.DailyStatisticsInfo.Name + " Created")
		return &response.CreateDailyStatisticsResponse{
			MetaInfo: &message.ResponseMetaInfoV1{
				Status:  true,
				Id:      "",
				Message: result.Message,
			},
			DailyStatisticsInfo: DailyStatisticsMetaInfoV1DbmodelToProtobuf(dailyStatisticsInfo),
		}, nil
	} else {
		return &response.CreateDailyStatisticsResponse{
			MetaInfo: &message.ResponseMetaInfoV1{
				Status:  false,
				Id:      "",
				Message: result.Message,
			},
			DailyStatisticsInfo: DailyStatisticsMetaInfoV1DbmodelToProtobuf(dailyStatisticsInfo),
		}, nil
	}
}

func (D DBMSServerController) DeleteDailyStatistics(ctx context.Context, request *request.DeleteDailyStatisticsRequest) (*response.DeleteDailyStatisticsResponse, error) {
	apiVersionVerifyResult := RequestApiVersionVerify(request.GetMetaInfo())
	if !apiVersionVerifyResult.Status {
		return &response.DeleteDailyStatisticsResponse{
			MetaInfo: &apiVersionVerifyResult,
		}, nil
	}

	responseMetaInfo, _ := UserTokenVerify(request.GetUserVerifyInfo().GetUserName(), request.GetUserVerifyInfo().GetUserToken())
	if !responseMetaInfo.Status {
		return &response.DeleteDailyStatisticsResponse{
			MetaInfo: &responseMetaInfo,
		}, nil
	}

	userMetaInfo := dbmodel.UserMetaInfoV1{}
	userMetaInfo.Name = request.UserVerifyInfo.GetUserName()

	dailyStatisticsInfo := dbmodel.DailyStatisticsMetaInfoV1{}
	dailyStatisticsInfo.Name = request.DailyStatisticsName

	result := dal.QueryUser(&userMetaInfo, dal.GetDbInstance())
	if result.Status == false {
		return &response.DeleteDailyStatisticsResponse{
			MetaInfo: &message.ResponseMetaInfoV1{
				Status:  false,
				Id:      "",
				Message: result.Message,
			},
			DailyStatisticsInfo: DailyStatisticsMetaInfoV1DbmodelToProtobuf(&dailyStatisticsInfo),
		}, nil
	}

	if userMetaInfo.UserPermissionGroup != dal.PermissionGroupAdmin {
		return &response.DeleteDailyStatisticsResponse{
			MetaInfo: &message.ResponseMetaInfoV1{
				Status:  false,
				Id:      "",
				Message: "You don't have permission to delete this DailyStatistics!",
			},
			DailyStatisticsInfo: DailyStatisticsMetaInfoV1DbmodelToProtobuf(&dailyStatisticsInfo),
		}, nil
	}

	result = dal.DeleteDailyStatistics(dailyStatisticsInfo, dal.GetDbInstance())
	if result.Status == true {
		log.Println("DailyStatistics " + request.DailyStatisticsName + " Delete")
		return &response.DeleteDailyStatisticsResponse{
			MetaInfo: &message.ResponseMetaInfoV1{
				Status:  true,
				Id:      "",
				Message: result.Message,
			},
			DailyStatisticsInfo: DailyStatisticsMetaInfoV1DbmodelToProtobuf(&dailyStatisticsInfo),
		}, nil
	} else {
		return &response.DeleteDailyStatisticsResponse{
			MetaInfo: &message.ResponseMetaInfoV1{
				Status:  false,
				Id:      "",
				Message: result.Message,
			},
			DailyStatisticsInfo: DailyStatisticsMetaInfoV1DbmodelToProtobuf(&dailyStatisticsInfo),
		}, nil
	}
}

func (D DBMSServerController) UpdateDailyStatistics(ctx context.Context, request *request.UpdateDailyStatisticsRequest) (*response.UpdateDailyStatisticsResponse, error) {
	apiVersionVerifyResult := RequestApiVersionVerify(request.GetMetaInfo())
	if !apiVersionVerifyResult.Status {
		return &response.UpdateDailyStatisticsResponse{
			MetaInfo: &apiVersionVerifyResult,
		}, nil
	}

	responseMetaInfo, _ := UserTokenVerify(request.GetUserVerifyInfo().GetUserName(), request.GetUserVerifyInfo().GetUserToken())
	if !responseMetaInfo.Status {
		return &response.UpdateDailyStatisticsResponse{
			MetaInfo: &responseMetaInfo,
		}, nil
	}

	dailyStatisticsInfo := DailyStatisticsMetaInfoV1ProtobufToDbmodel(request.DailyStatisticsInfo)

	result := dal.ModifyDailyStatistics(*dailyStatisticsInfo, dal.GetDbInstance())
	if result.Status == true {
		log.Println("DailyStatistics " + request.DailyStatisticsInfo.Name + " Updated")
		return &response.UpdateDailyStatisticsResponse{
			MetaInfo: &message.ResponseMetaInfoV1{
				Status:  true,
				Id:      "",
				Message: result.Message,
			},
			DailyStatisticsInfo: DailyStatisticsMetaInfoV1DbmodelToProtobuf(dailyStatisticsInfo),
		}, nil
	} else {
		return &response.UpdateDailyStatisticsResponse{
			MetaInfo: &message.ResponseMetaInfoV1{
				Status:  false,
				Id:      "",
				Message: result.Message,
			},
			DailyStatisticsInfo: DailyStatisticsMetaInfoV1DbmodelToProtobuf(dailyStatisticsInfo),
		}, nil
	}
}

func (D DBMSServerController) GetDailyStatistics(ctx context.Context, request *request.GetDailyStatisticsRequest) (*response.GetDailyStatisticsResponse, error) {
	apiVersionVerifyResult := RequestApiVersionVerify(request.GetMetaInfo())
	if !apiVersionVerifyResult.Status {
		return &response.GetDailyStatisticsResponse{
			MetaInfo: &apiVersionVerifyResult,
		}, nil
	}

	responseMetaInfo, _ := UserTokenVerify(request.GetUserVerifyInfo().GetUserName(), request.GetUserVerifyInfo().GetUserToken())
	if !responseMetaInfo.Status {
		return &response.GetDailyStatisticsResponse{
			MetaInfo: &responseMetaInfo,
		}, nil
	}

	dailyStatisticsInfo := dbmodel.DailyStatisticsMetaInfoV1{}
	dailyStatisticsInfo.Name = request.GetDailyStatisticsName()

	result := dal.QueryDailyStatistics(&dailyStatisticsInfo, dal.GetDbInstance())
	if result.Status == true {
		log.Println("DailyStatistics " + request.GetDailyStatisticsName() + " Get")
		return &response.GetDailyStatisticsResponse{
			MetaInfo: &message.ResponseMetaInfoV1{
				Status:  true,
				Id:      "",
				Message: result.Message,
			},
			DailyStatisticsInfo: DailyStatisticsMetaInfoV1DbmodelToProtobuf(&dailyStatisticsInfo),
		}, nil
	} else {
		return &response.GetDailyStatisticsResponse{
			MetaInfo: &message.ResponseMetaInfoV1{
				Status:  false,
				Id:      "",
				Message: result.Message,
			},
			DailyStatisticsInfo: DailyStatisticsMetaInfoV1DbmodelToProtobuf(&dailyStatisticsInfo),
		}, nil
	}
}

func (D DBMSServerController) GetAllDailyStatistics(ctx context.Context, request *request.GetAllDailyStatisticsRequest) (*response.GetAllDailyStatisticsResponse, error) {
	apiVersionVerifyResult := RequestApiVersionVerify(request.GetMetaInfo())
	if !apiVersionVerifyResult.Status {
		return &response.GetAllDailyStatisticsResponse{
			MetaInfo: &apiVersionVerifyResult,
		}, nil
	}

	responseMetaInfo, _ := UserTokenVerify(request.GetUserVerifyInfo().GetUserName(), request.GetUserVerifyInfo().GetUserToken())
	if !responseMetaInfo.Status {
		return &response.GetAllDailyStatisticsResponse{
			MetaInfo: &responseMetaInfo,
		}, nil
	}

	var dailyStatisticsInfo []dbmodel.DailyStatisticsMetaInfoV1
	var dailyStatisticsInfoProto []*message.DailyStatisticsMetaInfoV1

	result := dal.QueryAllDailyStatistics(&dailyStatisticsInfo, dal.GetDbInstance())
	if result.Status == true {
		log.Println("User " + request.UserVerifyInfo.GetUserName() + " Get DailyStatistics")

		for _, message := range dailyStatisticsInfo {
			dailyStatisticsInfoProto = append(dailyStatisticsInfoProto, DailyStatisticsMetaInfoV1DbmodelToProtobuf(&message))
		}

		return &response.GetAllDailyStatisticsResponse{
			MetaInfo: &message.ResponseMetaInfoV1{
				Status:  true,
				Id:      "",
				Message: result.Message,
			},
			DailyStatisticsInfo: dailyStatisticsInfoProto,
		}, nil
	} else {
		return &response.GetAllDailyStatisticsResponse{
			MetaInfo: &message.ResponseMetaInfoV1{
				Status:  false,
				Id:      "",
				Message: result.Message,
			},
			DailyStatisticsInfo: dailyStatisticsInfoProto,
		}, nil
	}
}

func (D DBMSServerController) CreateSwcSnapshot(ctx context.Context, request *request.CreateSwcSnapshotRequest) (*response.CreateSwcSnapshotResponse, error) {
	apiVersionVerifyResult := RequestApiVersionVerify(request.GetMetaInfo())
	if !apiVersionVerifyResult.Status {
		return &response.CreateSwcSnapshotResponse{
			MetaInfo: &apiVersionVerifyResult,
		}, nil
	}

	responseMetaInfo, _ := UserTokenVerify(request.GetUserVerifyInfo().GetUserName(), request.GetUserVerifyInfo().GetUserToken())
	if !responseMetaInfo.Status {
		return &response.CreateSwcSnapshotResponse{
			MetaInfo: &responseMetaInfo,
		}, nil
	}

	var swcMetaInfo dbmodel.SwcMetaInfoV1
	swcMetaInfo.Name = request.SwcName
	result := dal.QuerySwc(&swcMetaInfo, dal.GetDbInstance())
	if !result.Status {
		return &response.CreateSwcSnapshotResponse{
			MetaInfo: &message.ResponseMetaInfoV1{
				Status:  false,
				Id:      "",
				Message: result.Message,
			},
		}, nil
	}

	timePoint := time.Now()
	year, mouth, day := timePoint.Date()
	hour, minute, second := timePoint.Clock()
	_ = strconv.Itoa(year) + "-" + mouth.String() + "-" + strconv.Itoa(day-1) + "_" + strconv.Itoa(hour) + ":" + strconv.Itoa(minute) + "-" + strconv.Itoa(second)

	createTime := time.Now()
	var swcSnapshotMetaInfo dbmodel.SwcSnapshotMetaInfoV1
	swcSnapshotMetaInfo.Base.Id = primitive.NewObjectID()
	swcSnapshotMetaInfo.Base.Uuid = uuid.NewString()
	swcSnapshotMetaInfo.Base.DataAccessModelVersion = "V1"
	swcSnapshotMetaInfo.CreateTime = createTime
	swcSnapshotMetaInfo.Creator = request.GetUserVerifyInfo().GetUserName()
	swcSnapshotMetaInfo.SwcSnapshotCollectionName = "Snapshot_" + uuid.NewString()
	swcMetaInfo.SwcSnapshotList = append(swcMetaInfo.SwcSnapshotList, swcSnapshotMetaInfo)

	var swcIncrementOperationMetaInfo dbmodel.SwcIncrementOperationMetaInfoV1
	swcIncrementOperationMetaInfo.Base.Id = primitive.NewObjectID()
	swcIncrementOperationMetaInfo.Base.Uuid = uuid.NewString()
	swcIncrementOperationMetaInfo.Base.DataAccessModelVersion = "V1"
	swcIncrementOperationMetaInfo.CreateTime = createTime
	swcIncrementOperationMetaInfo.StartSnapshot = swcSnapshotMetaInfo.SwcSnapshotCollectionName
	swcIncrementOperationMetaInfo.IncrementOperationCollectionName = "IncrementOperation_" + uuid.NewString()
	swcMetaInfo.SwcIncrementOperationList = append(swcMetaInfo.SwcIncrementOperationList, swcIncrementOperationMetaInfo)

	swcMetaInfo.CurrentIncrementOperationCollectionName = swcIncrementOperationMetaInfo.IncrementOperationCollectionName

	result = dal.CreateSnapshot(swcMetaInfo.Name, swcSnapshotMetaInfo.SwcSnapshotCollectionName, dal.GetDbInstance())
	if !result.Status {
		return &response.CreateSwcSnapshotResponse{
			MetaInfo: &message.ResponseMetaInfoV1{
				Status:  false,
				Id:      "",
				Message: result.Message,
			},
		}, nil
	}

	result = dal.ModifySwc(swcMetaInfo, dal.GetDbInstance())
	if result.Status {
		return &response.CreateSwcSnapshotResponse{
			MetaInfo: &message.ResponseMetaInfoV1{
				Status:  true,
				Id:      "",
				Message: "CreateSwcSnapshot Successfully!",
			},
		}, nil
	}
	return &response.CreateSwcSnapshotResponse{
		MetaInfo: &message.ResponseMetaInfoV1{
			Status:  false,
			Id:      "",
			Message: result.Message,
		},
	}, nil
}

func (D DBMSServerController) GetAllSnapshotMetaInfo(ctx context.Context, request *request.GetAllSnapshotMetaInfoRequest) (*response.GetAllSnapshotMetaInfoResponse, error) {
	apiVersionVerifyResult := RequestApiVersionVerify(request.GetMetaInfo())
	if !apiVersionVerifyResult.Status {
		return &response.GetAllSnapshotMetaInfoResponse{
			MetaInfo: &apiVersionVerifyResult,
		}, nil
	}

	responseMetaInfo, _ := UserTokenVerify(request.GetUserVerifyInfo().GetUserName(), request.GetUserVerifyInfo().GetUserToken())
	if !responseMetaInfo.Status {
		return &response.GetAllSnapshotMetaInfoResponse{
			MetaInfo: &responseMetaInfo,
		}, nil
	}

	var dbmodelMessage dbmodel.SwcMetaInfoV1
	dbmodelMessage.Name = request.SwcName

	var protoMessage []*message.SwcSnapshotMetaInfoV1
	result := dal.QuerySwc(&dbmodelMessage, dal.GetDbInstance())
	if result.Status {
		log.Println("User " + request.UserVerifyInfo.GetUserName() + " Query ")
		for _, dbMessage := range dbmodelMessage.SwcSnapshotList {
			protoMessage = append(protoMessage, SwcSnapshotMetaInfoV1MetaInfoV1DbmodelToProtobuf(&dbMessage))
		}
		return &response.GetAllSnapshotMetaInfoResponse{
			MetaInfo: &message.ResponseMetaInfoV1{
				Status:  true,
				Id:      "",
				Message: result.Message,
			},
			SwcSnapshotList: protoMessage,
		}, nil
	}

	return &response.GetAllSnapshotMetaInfoResponse{
		MetaInfo: &message.ResponseMetaInfoV1{
			Status:  false,
			Id:      "",
			Message: result.Message,
		},
	}, nil
}

func (D DBMSServerController) GetSnapshot(ctx context.Context, request *request.GetSnapshotRequest) (*response.GetSnapshotResponse, error) {
	apiVersionVerifyResult := RequestApiVersionVerify(request.GetMetaInfo())
	if !apiVersionVerifyResult.Status {
		return &response.GetSnapshotResponse{
			MetaInfo: &apiVersionVerifyResult,
		}, nil
	}

	responseMetaInfo, _ := UserTokenVerify(request.GetUserVerifyInfo().GetUserName(), request.GetUserVerifyInfo().GetUserToken())
	if !responseMetaInfo.Status {
		return &response.GetSnapshotResponse{
			MetaInfo: &responseMetaInfo,
		}, nil
	}

	var dbmodelMessage dbmodel.SwcDataV1
	var protoMessage message.SwcDataV1

	result := dal.QuerySwcSnapshot(request.GetSwcSnapshotCollectionName(), &dbmodelMessage, dal.GetDbInstance())
	if result.Status {
		for _, swcNodeData := range dbmodelMessage {
			protoMessage.SwcData = append(protoMessage.SwcData, SwcNodeDataV1DbmodelToProtobuf(&swcNodeData))
		}
		return &response.GetSnapshotResponse{
			MetaInfo: &message.ResponseMetaInfoV1{
				Status:  true,
				Id:      "",
				Message: result.Message,
			},
			SwcNodeData: &protoMessage,
		}, nil
	}

	return &response.GetSnapshotResponse{
		MetaInfo: &message.ResponseMetaInfoV1{
			Status:  false,
			Id:      "",
			Message: result.Message,
		},
	}, nil
}

func (D DBMSServerController) GetAllIncrementOperationMetaInfo(ctx context.Context, request *request.GetAllIncrementOperationMetaInfoRequest) (*response.GetAllIncrementOperationMetaInfoResponse, error) {
	apiVersionVerifyResult := RequestApiVersionVerify(request.GetMetaInfo())
	if !apiVersionVerifyResult.Status {
		return &response.GetAllIncrementOperationMetaInfoResponse{
			MetaInfo: &apiVersionVerifyResult,
		}, nil
	}

	responseMetaInfo, _ := UserTokenVerify(request.GetUserVerifyInfo().GetUserName(), request.GetUserVerifyInfo().GetUserToken())
	if !responseMetaInfo.Status {
		return &response.GetAllIncrementOperationMetaInfoResponse{
			MetaInfo: &responseMetaInfo,
		}, nil
	}

	var dbmodelMessage dbmodel.SwcMetaInfoV1
	dbmodelMessage.Name = request.SwcName

	var protoMessage []*message.SwcIncrementOperationMetaInfoV1
	result := dal.QuerySwc(&dbmodelMessage, dal.GetDbInstance())
	if result.Status {
		log.Println("User " + request.UserVerifyInfo.GetUserName() + " Query ")
		for _, dbMessage := range dbmodelMessage.SwcIncrementOperationList {
			protoMessage = append(protoMessage, SwcIncrementOperationMetaInfoV1MetaInfoV1DbmodelToProtobuf(&dbMessage))
		}
		return &response.GetAllIncrementOperationMetaInfoResponse{
			MetaInfo: &message.ResponseMetaInfoV1{
				Status:  true,
				Id:      "",
				Message: result.Message,
			},
			SwcIncrementOperationMetaInfo: protoMessage,
		}, nil
	}

	return &response.GetAllIncrementOperationMetaInfoResponse{
		MetaInfo: &message.ResponseMetaInfoV1{
			Status:  false,
			Id:      "",
			Message: result.Message,
		},
	}, nil
}

func (D DBMSServerController) GetIncrementOperation(ctx context.Context, request *request.GetIncrementOperationRequest) (*response.GetIncrementOperationResponse, error) {
	apiVersionVerifyResult := RequestApiVersionVerify(request.GetMetaInfo())
	if !apiVersionVerifyResult.Status {
		return &response.GetIncrementOperationResponse{
			MetaInfo: &apiVersionVerifyResult,
		}, nil
	}

	responseMetaInfo, _ := UserTokenVerify(request.GetUserVerifyInfo().GetUserName(), request.GetUserVerifyInfo().GetUserToken())
	if !responseMetaInfo.Status {
		return &response.GetIncrementOperationResponse{
			MetaInfo: &responseMetaInfo,
		}, nil
	}

	var dbmodelMessage dbmodel.SwcIncrementOperationListV1
	var protoMessage message.SwcIncrementOperationListV1

	result := dal.QuerySwcIncrementOperation(request.GetIncrementOperationCollectionName(), &dbmodelMessage, dal.GetDbInstance())
	if result.Status {
		for _, swcNodeData := range dbmodelMessage {
			protoMessage.SwcIncrementOperation = append(protoMessage.SwcIncrementOperation, SwcIncrementOperationListV1DbmodelToProtobuf(&swcNodeData))
		}

		return &response.GetIncrementOperationResponse{
			MetaInfo: &message.ResponseMetaInfoV1{
				Status:  true,
				Id:      "",
				Message: result.Message,
			},
			SwcIncrementOperationList: &protoMessage,
		}, nil
	}

	return &response.GetIncrementOperationResponse{
		MetaInfo: &message.ResponseMetaInfoV1{
			Status:  false,
			Id:      "",
			Message: result.Message,
		},
	}, nil
}

func (D DBMSServerController) CreateSwcAttachmentAno(ctx context.Context, request *request.CreateSwcAttachmentAnoRequest) (*response.CreateSwcAttachmentAnoResponse, error) {
	apiVersionVerifyResult := RequestApiVersionVerify(request.GetMetaInfo())
	if !apiVersionVerifyResult.Status {
		return &response.CreateSwcAttachmentAnoResponse{
			MetaInfo: &apiVersionVerifyResult,
		}, nil
	}

	responseMetaInfo, _ := UserTokenVerify(request.GetUserVerifyInfo().GetUserName(), request.GetUserVerifyInfo().GetUserToken())
	if !responseMetaInfo.Status {
		return &response.CreateSwcAttachmentAnoResponse{
			MetaInfo: &responseMetaInfo,
		}, nil
	}

	anoAttachmentUuid := uuid.NewString()
	attachmentDb := dbmodel.SwcAttachmentAnoV1{
		Base: dbmodel.MetaInfoBase{
			Id:                     primitive.NewObjectID(),
			DataAccessModelVersion: "V1",
			Uuid:                   anoAttachmentUuid,
		},
		APOFILE: request.GetSwcAttachmentAno().APOFILE,
		SWCFILE: request.GetSwcAttachmentAno().SWCFILE,
	}

	result := dal.CreateSwcAttachmentAno(request.GetSwcName(), &attachmentDb, dal.GetDbInstance())
	if result.Status {
		swcMetaInfo := dbmodel.SwcMetaInfoV1{}
		swcMetaInfo.Name = request.GetSwcName()

		res := dal.QuerySwc(&swcMetaInfo, dal.GetDbInstance())
		if res.Status {
			swcMetaInfo.SwcAttachmentAnoMetaInfo.AttachmentUuid = anoAttachmentUuid
			res = dal.ModifySwc(swcMetaInfo, dal.GetDbInstance())
			if res.Status {
				return &response.CreateSwcAttachmentAnoResponse{
					MetaInfo: &message.ResponseMetaInfoV1{
						Status:  true,
						Id:      "",
						Message: result.Message,
					},
					AnoAttachmentUuid: anoAttachmentUuid,
				}, nil
			} else {
				return &response.CreateSwcAttachmentAnoResponse{
					MetaInfo: &message.ResponseMetaInfoV1{
						Status:  false,
						Id:      "",
						Message: result.Message,
					},
				}, nil
			}
		} else {
			return &response.CreateSwcAttachmentAnoResponse{
				MetaInfo: &message.ResponseMetaInfoV1{
					Status:  false,
					Id:      "",
					Message: result.Message,
				},
			}, nil
		}
	}

	return &response.CreateSwcAttachmentAnoResponse{
		MetaInfo: &message.ResponseMetaInfoV1{
			Status:  false,
			Id:      "",
			Message: result.Message,
		},
	}, nil
}

func (D DBMSServerController) DeleteSwcAttachmentAno(ctx context.Context, request *request.DeleteSwcAttachmentAnoRequest) (*response.DeleteSwcAttachmentAnoResponse, error) {
	apiVersionVerifyResult := RequestApiVersionVerify(request.GetMetaInfo())
	if !apiVersionVerifyResult.Status {
		return &response.DeleteSwcAttachmentAnoResponse{
			MetaInfo: &apiVersionVerifyResult,
		}, nil
	}

	responseMetaInfo, _ := UserTokenVerify(request.GetUserVerifyInfo().GetUserName(), request.GetUserVerifyInfo().GetUserToken())
	if !responseMetaInfo.Status {
		return &response.DeleteSwcAttachmentAnoResponse{
			MetaInfo: &responseMetaInfo,
		}, nil
	}

	result := dal.DeleteSwcAttachmentAno(request.GetSwcName(), request.GetAnoAttachmentUuid(), dal.GetDbInstance())
	if result.Status {
		swcMetaInfo := dbmodel.SwcMetaInfoV1{}
		swcMetaInfo.Name = request.GetSwcName()

		res := dal.QuerySwc(&swcMetaInfo, dal.GetDbInstance())
		if res.Status {
			swcMetaInfo.SwcAttachmentAnoMetaInfo.AttachmentUuid = ""
			res = dal.ModifySwc(swcMetaInfo, dal.GetDbInstance())
			if res.Status {
				return &response.DeleteSwcAttachmentAnoResponse{
					MetaInfo: &message.ResponseMetaInfoV1{
						Status:  true,
						Id:      "",
						Message: result.Message,
					},
				}, nil
			} else {
				return &response.DeleteSwcAttachmentAnoResponse{
					MetaInfo: &message.ResponseMetaInfoV1{
						Status:  false,
						Id:      "",
						Message: result.Message,
					},
				}, nil
			}
		} else {
			return &response.DeleteSwcAttachmentAnoResponse{
				MetaInfo: &message.ResponseMetaInfoV1{
					Status:  false,
					Id:      "",
					Message: result.Message,
				},
			}, nil
		}
	}

	return &response.DeleteSwcAttachmentAnoResponse{
		MetaInfo: &message.ResponseMetaInfoV1{
			Status:  false,
			Id:      "",
			Message: result.Message,
		},
	}, nil
}

func (D DBMSServerController) UpdateSwcAttachmentAno(ctx context.Context, request *request.UpdateSwcAttachmentAnoRequest) (*response.UpdateSwcAttachmentAnoResponse, error) {
	apiVersionVerifyResult := RequestApiVersionVerify(request.GetMetaInfo())
	if !apiVersionVerifyResult.Status {
		return &response.UpdateSwcAttachmentAnoResponse{
			MetaInfo: &apiVersionVerifyResult,
		}, nil
	}

	responseMetaInfo, _ := UserTokenVerify(request.GetUserVerifyInfo().GetUserName(), request.GetUserVerifyInfo().GetUserToken())
	if !responseMetaInfo.Status {
		return &response.UpdateSwcAttachmentAnoResponse{
			MetaInfo: &responseMetaInfo,
		}, nil
	}

	attachmentDb := dbmodel.SwcAttachmentAnoV1{
		Base: dbmodel.MetaInfoBase{
			Id:                     primitive.NewObjectID(),
			DataAccessModelVersion: "V1",
			Uuid:                   request.GetAnoAttachmentUuid(),
		},
		APOFILE: request.GetNewSwcAttachmentAno().GetAPOFILE(),
		SWCFILE: request.GetNewSwcAttachmentAno().GetSWCFILE(),
	}

	result := dal.UpdateSwcAttachmentAno(request.GetSwcName(), request.GetAnoAttachmentUuid(), &attachmentDb, dal.GetDbInstance())
	if result.Status {
		swcMetaInfo := dbmodel.SwcMetaInfoV1{}
		swcMetaInfo.Name = request.GetSwcName()

		res := dal.QuerySwc(&swcMetaInfo, dal.GetDbInstance())
		if res.Status {
			swcMetaInfo.SwcAttachmentAnoMetaInfo.AttachmentUuid = request.GetAnoAttachmentUuid()
			res = dal.ModifySwc(swcMetaInfo, dal.GetDbInstance())
			if res.Status {
				return &response.UpdateSwcAttachmentAnoResponse{
					MetaInfo: &message.ResponseMetaInfoV1{
						Status:  true,
						Id:      "",
						Message: result.Message,
					},
				}, nil
			} else {
				return &response.UpdateSwcAttachmentAnoResponse{
					MetaInfo: &message.ResponseMetaInfoV1{
						Status:  false,
						Id:      "",
						Message: result.Message,
					},
				}, nil
			}
		} else {
			return &response.UpdateSwcAttachmentAnoResponse{
				MetaInfo: &message.ResponseMetaInfoV1{
					Status:  false,
					Id:      "",
					Message: result.Message,
				},
			}, nil
		}
	}

	return &response.UpdateSwcAttachmentAnoResponse{
		MetaInfo: &message.ResponseMetaInfoV1{
			Status:  false,
			Id:      "",
			Message: result.Message,
		},
	}, nil
}

func (D DBMSServerController) GetSwcAttachmentAno(ctx context.Context, request *request.GetSwcAttachmentAnoRequest) (*response.GetSwcAttachmentAnoResponse, error) {
	apiVersionVerifyResult := RequestApiVersionVerify(request.GetMetaInfo())
	if !apiVersionVerifyResult.Status {
		return &response.GetSwcAttachmentAnoResponse{
			MetaInfo: &apiVersionVerifyResult,
		}, nil
	}

	responseMetaInfo, _ := UserTokenVerify(request.GetUserVerifyInfo().GetUserName(), request.GetUserVerifyInfo().GetUserToken())
	if !responseMetaInfo.Status {
		return &response.GetSwcAttachmentAnoResponse{
			MetaInfo: &responseMetaInfo,
		}, nil
	}

	attachmentDb := dbmodel.SwcAttachmentAnoV1{}
	attachmentPb := message.SwcAttachmentAnoV1{}

	result := dal.QuerySwcAttachmentAno(request.GetSwcName(), request.GetAnoAttachmentUuid(), &attachmentDb, dal.GetDbInstance())
	if result.Status {
		attachmentPb.Base = &message.MetaInfoBase{}
		attachmentPb.Base.XId = attachmentDb.Base.Id.Hex()
		attachmentPb.Base.DataAccessModelVersion = attachmentDb.Base.DataAccessModelVersion
		attachmentPb.Base.Uuid = attachmentDb.Base.Uuid

		attachmentPb.APOFILE = attachmentDb.APOFILE
		attachmentPb.SWCFILE = attachmentDb.SWCFILE

		return &response.GetSwcAttachmentAnoResponse{
			MetaInfo: &message.ResponseMetaInfoV1{
				Status:  true,
				Id:      "",
				Message: result.Message,
			},
			SwcAttachmentAno: &attachmentPb,
		}, nil
	}

	return &response.GetSwcAttachmentAnoResponse{
		MetaInfo: &message.ResponseMetaInfoV1{
			Status:  false,
			Id:      "",
			Message: result.Message,
		},
	}, nil
}

func (D DBMSServerController) CreateSwcAttachmentApo(ctx context.Context, request *request.CreateSwcAttachmentApoRequest) (*response.CreateSwcAttachmentApoResponse, error) {
	apiVersionVerifyResult := RequestApiVersionVerify(request.GetMetaInfo())
	if !apiVersionVerifyResult.Status {
		return &response.CreateSwcAttachmentApoResponse{
			MetaInfo: &apiVersionVerifyResult,
		}, nil
	}

	responseMetaInfo, _ := UserTokenVerify(request.GetUserVerifyInfo().GetUserName(), request.GetUserVerifyInfo().GetUserToken())
	if !responseMetaInfo.Status {
		return &response.CreateSwcAttachmentApoResponse{
			MetaInfo: &responseMetaInfo,
		}, nil
	}

	var attachmentDb []dbmodel.SwcAttachmentApoV1
	for _, pbData := range request.GetSwcAttachmentApo() {
		dbData := dbmodel.SwcAttachmentApoV1{
			Base: dbmodel.MetaInfoBase{
				Id:                     primitive.NewObjectID(),
				DataAccessModelVersion: "V1",
				Uuid:                   uuid.NewString(),
			},
			N:         pbData.GetN(),
			Orderinfo: pbData.GetOrderinfo(),
			Name:      pbData.GetName(),
			Comment:   pbData.GetComment(),
			Z:         pbData.GetZ(),
			X:         pbData.GetX(),
			Y:         pbData.GetY(),
			Pixmax:    pbData.GetPixmax(),
			Intensity: pbData.GetIntensity(),
			Sdev:      pbData.GetSdev(),
			Volsize:   pbData.GetVolsize(),
			Mass:      pbData.GetMass(),
			ColorR:    pbData.GetColorR(),
			ColorG:    pbData.GetColorG(),
			ColorB:    pbData.GetColorB(),
		}
		attachmentDb = append(attachmentDb, dbData)
	}

	apoAttachmentCollectionName := "Attachment_Apo_" + uuid.NewString()

	result := dal.CreateSwcAttachmentApo(request.GetSwcName(), apoAttachmentCollectionName, &attachmentDb, dal.GetDbInstance())
	if result.Status {
		swcMetaInfo := dbmodel.SwcMetaInfoV1{}
		swcMetaInfo.Name = request.GetSwcName()

		res := dal.QuerySwc(&swcMetaInfo, dal.GetDbInstance())
		if res.Status {
			swcMetaInfo.SwcAttachmentApoMetaInfo.AttachmentUuid = apoAttachmentCollectionName
			res = dal.ModifySwc(swcMetaInfo, dal.GetDbInstance())
			if res.Status {
				return &response.CreateSwcAttachmentApoResponse{
					MetaInfo: &message.ResponseMetaInfoV1{
						Status:  true,
						Id:      "",
						Message: result.Message,
					},
					ApoAttachmentUuid: apoAttachmentCollectionName,
				}, nil
			} else {
				return &response.CreateSwcAttachmentApoResponse{
					MetaInfo: &message.ResponseMetaInfoV1{
						Status:  false,
						Id:      "",
						Message: result.Message,
					},
				}, nil
			}
		} else {
			return &response.CreateSwcAttachmentApoResponse{
				MetaInfo: &message.ResponseMetaInfoV1{
					Status:  false,
					Id:      "",
					Message: result.Message,
				},
			}, nil
		}
	}

	return &response.CreateSwcAttachmentApoResponse{
		MetaInfo: &message.ResponseMetaInfoV1{
			Status:  false,
			Id:      "",
			Message: result.Message,
		},
	}, nil
}

func (D DBMSServerController) DeleteSwcAttachmentApo(ctx context.Context, request *request.DeleteSwcAttachmentApoRequest) (*response.DeleteSwcAttachmentApoResponse, error) {
	apiVersionVerifyResult := RequestApiVersionVerify(request.GetMetaInfo())
	if !apiVersionVerifyResult.Status {
		return &response.DeleteSwcAttachmentApoResponse{
			MetaInfo: &apiVersionVerifyResult,
		}, nil
	}

	responseMetaInfo, _ := UserTokenVerify(request.GetUserVerifyInfo().GetUserName(), request.GetUserVerifyInfo().GetUserToken())
	if !responseMetaInfo.Status {
		return &response.DeleteSwcAttachmentApoResponse{
			MetaInfo: &responseMetaInfo,
		}, nil
	}

	result := dal.DeleteSwcAttachmentApo(request.GetSwcName(), request.GetApoAttachmentUuid(), dal.GetDbInstance())
	if result.Status {
		swcMetaInfo := dbmodel.SwcMetaInfoV1{}
		swcMetaInfo.Name = request.GetSwcName()

		res := dal.QuerySwc(&swcMetaInfo, dal.GetDbInstance())
		if res.Status {
			swcMetaInfo.SwcAttachmentApoMetaInfo.AttachmentUuid = ""
			res = dal.ModifySwc(swcMetaInfo, dal.GetDbInstance())
			if res.Status {
				return &response.DeleteSwcAttachmentApoResponse{
					MetaInfo: &message.ResponseMetaInfoV1{
						Status:  true,
						Id:      "",
						Message: result.Message,
					},
				}, nil
			} else {
				return &response.DeleteSwcAttachmentApoResponse{
					MetaInfo: &message.ResponseMetaInfoV1{
						Status:  false,
						Id:      "",
						Message: result.Message,
					},
				}, nil
			}
		} else {
			return &response.DeleteSwcAttachmentApoResponse{
				MetaInfo: &message.ResponseMetaInfoV1{
					Status:  false,
					Id:      "",
					Message: result.Message,
				},
			}, nil
		}
	}

	return &response.DeleteSwcAttachmentApoResponse{
		MetaInfo: &message.ResponseMetaInfoV1{
			Status:  false,
			Id:      "",
			Message: result.Message,
		},
	}, nil
}

func (D DBMSServerController) UpdateSwcAttachmentApo(ctx context.Context, request *request.UpdateSwcAttachmentApoRequest) (*response.UpdateSwcAttachmentApoResponse, error) {
	apiVersionVerifyResult := RequestApiVersionVerify(request.GetMetaInfo())
	if !apiVersionVerifyResult.Status {
		return &response.UpdateSwcAttachmentApoResponse{
			MetaInfo: &apiVersionVerifyResult,
		}, nil
	}

	responseMetaInfo, _ := UserTokenVerify(request.GetUserVerifyInfo().GetUserName(), request.GetUserVerifyInfo().GetUserToken())
	if !responseMetaInfo.Status {
		return &response.UpdateSwcAttachmentApoResponse{
			MetaInfo: &responseMetaInfo,
		}, nil
	}

	var attachmentDb []dbmodel.SwcAttachmentApoV1
	for _, pbData := range request.GetNewSwcAttachmentApo() {
		dbData := dbmodel.SwcAttachmentApoV1{
			Base: dbmodel.MetaInfoBase{
				Id:                     primitive.NewObjectID(),
				DataAccessModelVersion: "V1",
				Uuid:                   uuid.NewString(),
			},
			N:         pbData.GetN(),
			Orderinfo: pbData.GetOrderinfo(),
			Name:      pbData.GetName(),
			Comment:   pbData.GetComment(),
			Z:         pbData.GetZ(),
			X:         pbData.GetX(),
			Y:         pbData.GetY(),
			Pixmax:    pbData.GetPixmax(),
			Intensity: pbData.GetIntensity(),
			Sdev:      pbData.GetSdev(),
			Volsize:   pbData.GetVolsize(),
			Mass:      pbData.GetMass(),
			ColorR:    pbData.GetColorR(),
			ColorG:    pbData.GetColorG(),
			ColorB:    pbData.GetColorB(),
		}
		attachmentDb = append(attachmentDb, dbData)
	}

	if len(attachmentDb) == 0 {
		return &response.UpdateSwcAttachmentApoResponse{
			MetaInfo: &message.ResponseMetaInfoV1{
				Status:  true,
				Id:      "",
				Message: "No data need to be saved.",
			},
		}, nil
	}

	result := dal.UpdateSwcAttachmentApo(request.GetSwcName(), request.GetApoAttachmentUuid(), &attachmentDb, dal.GetDbInstance())
	if result.Status {
		swcMetaInfo := dbmodel.SwcMetaInfoV1{}
		swcMetaInfo.Name = request.GetSwcName()

		res := dal.QuerySwc(&swcMetaInfo, dal.GetDbInstance())
		if res.Status {
			swcMetaInfo.SwcAttachmentApoMetaInfo.AttachmentUuid = request.GetApoAttachmentUuid()
			res = dal.ModifySwc(swcMetaInfo, dal.GetDbInstance())
			if res.Status {
				return &response.UpdateSwcAttachmentApoResponse{
					MetaInfo: &message.ResponseMetaInfoV1{
						Status:  true,
						Id:      "",
						Message: result.Message,
					},
				}, nil
			} else {
				return &response.UpdateSwcAttachmentApoResponse{
					MetaInfo: &message.ResponseMetaInfoV1{
						Status:  false,
						Id:      "",
						Message: result.Message,
					},
				}, nil
			}
		} else {
			return &response.UpdateSwcAttachmentApoResponse{
				MetaInfo: &message.ResponseMetaInfoV1{
					Status:  false,
					Id:      "",
					Message: result.Message,
				},
			}, nil
		}
	}

	return &response.UpdateSwcAttachmentApoResponse{
		MetaInfo: &message.ResponseMetaInfoV1{
			Status:  false,
			Id:      "",
			Message: result.Message,
		},
	}, nil
}

func (D DBMSServerController) GetSwcAttachmentApo(ctx context.Context, request *request.GetSwcAttachmentApoRequest) (*response.GetSwcAttachmentApoResponse, error) {
	apiVersionVerifyResult := RequestApiVersionVerify(request.GetMetaInfo())
	if !apiVersionVerifyResult.Status {
		return &response.GetSwcAttachmentApoResponse{
			MetaInfo: &apiVersionVerifyResult,
		}, nil
	}

	responseMetaInfo, _ := UserTokenVerify(request.GetUserVerifyInfo().GetUserName(), request.GetUserVerifyInfo().GetUserToken())
	if !responseMetaInfo.Status {
		return &response.GetSwcAttachmentApoResponse{
			MetaInfo: &responseMetaInfo,
		}, nil
	}

	var attachmentDb []dbmodel.SwcAttachmentApoV1
	var attachmentPb []*message.SwcAttachmentApoV1

	result := dal.QuerySwcAttachmentApo(request.GetSwcName(), request.GetApoAttachmentUuid(), &attachmentDb, dal.GetDbInstance())
	if result.Status {
		for _, dbData := range attachmentDb {
			pbData := &message.SwcAttachmentApoV1{
				N:         dbData.N,
				Orderinfo: dbData.Orderinfo,
				Name:      dbData.Name,
				Comment:   dbData.Comment,
				Z:         dbData.Z,
				X:         dbData.X,
				Y:         dbData.Y,
				Pixmax:    dbData.Pixmax,
				Intensity: dbData.Intensity,
				Sdev:      dbData.Sdev,
				Volsize:   dbData.Volsize,
				Mass:      dbData.Mass,
				ColorR:    dbData.ColorR,
				ColorG:    dbData.ColorG,
				ColorB:    dbData.ColorB,
			}
			pbData.Base = &message.MetaInfoBase{
				XId:                    dbData.Base.Id.Hex(),
				DataAccessModelVersion: dbData.Base.DataAccessModelVersion,
				Uuid:                   dbData.Base.Uuid,
			}
			attachmentPb = append(attachmentPb, pbData)
		}

		return &response.GetSwcAttachmentApoResponse{
			MetaInfo: &message.ResponseMetaInfoV1{
				Status:  true,
				Id:      "",
				Message: result.Message,
			},
			SwcAttachmentApo: attachmentPb,
		}, nil
	}

	return &response.GetSwcAttachmentApoResponse{
		MetaInfo: &message.ResponseMetaInfoV1{
			Status:  false,
			Id:      "",
			Message: result.Message,
		},
	}, nil
}

func (D DBMSServerController) RevertSwcVersion(context context.Context, request *request.RevertSwcVersionRequest) (*response.RevertSwcVersionResponse, error) {
	apiVersionVerifyResult := RequestApiVersionVerify(request.GetMetaInfo())
	if !apiVersionVerifyResult.Status {
		return &response.RevertSwcVersionResponse{
			MetaInfo: &apiVersionVerifyResult,
		}, nil
	}

	responseMetaInfo, _ := UserTokenVerify(request.GetUserVerifyInfo().GetUserName(), request.GetUserVerifyInfo().GetUserToken())
	if !responseMetaInfo.Status {
		return &response.RevertSwcVersionResponse{
			MetaInfo: &responseMetaInfo,
		}, nil
	}

	swcMetaInfo := dbmodel.SwcMetaInfoV1{}
	swcMetaInfo.Name = request.GetSwcName()

	endTime := request.GetVersionEndTime().AsTime()

	status := dal.QuerySwc(&swcMetaInfo, dal.GetDbInstance())
	if status.Status {
		// Process SwcSnapshotMetaInfoV1 items
		var latestSnapshot *dbmodel.SwcSnapshotMetaInfoV1
		var newSnapshotList []dbmodel.SwcSnapshotMetaInfoV1
		for _, snapshot := range swcMetaInfo.SwcSnapshotList {
			if snapshot.CreateTime.Before(endTime) || snapshot.CreateTime.Equal(endTime) {
				newSnapshotList = append(newSnapshotList, snapshot)
				if latestSnapshot == nil || snapshot.CreateTime.After(latestSnapshot.CreateTime) {
					latestSnapshot = &snapshot
				}
			}
		}

		// Process SwcIncrementOperationMetaInfoV1 items
		var latestIncrementOperation *dbmodel.SwcIncrementOperationMetaInfoV1
		var newIncrementOperationList []dbmodel.SwcIncrementOperationMetaInfoV1
		for _, incrementOperation := range swcMetaInfo.SwcIncrementOperationList {
			if incrementOperation.CreateTime.Before(endTime) || incrementOperation.CreateTime.Equal(endTime) {
				newIncrementOperationList = append(newIncrementOperationList, incrementOperation)
				if latestIncrementOperation == nil || incrementOperation.CreateTime.After(latestIncrementOperation.CreateTime) {
					latestIncrementOperation = &incrementOperation
				}
			}
		}

		if latestIncrementOperation.StartSnapshot == latestSnapshot.SwcSnapshotCollectionName {

			status = dal.RevertSwcNodeData(request.GetSwcName(), latestSnapshot.SwcSnapshotCollectionName, latestIncrementOperation.IncrementOperationCollectionName, endTime, dal.GetDbInstance())
			if status.Status {

				swcMetaInfo.SwcSnapshotList = newSnapshotList
				swcMetaInfo.SwcIncrementOperationList = newIncrementOperationList
				swcMetaInfo.CurrentIncrementOperationCollectionName = latestIncrementOperation.IncrementOperationCollectionName

				return &response.RevertSwcVersionResponse{
					MetaInfo: &message.ResponseMetaInfoV1{
						Status:  true,
						Id:      "",
						Message: "Revert Successfully!",
					},
				}, nil
			} else {
				return &response.RevertSwcVersionResponse{
					MetaInfo: &message.ResponseMetaInfoV1{
						Status:  false,
						Id:      "",
						Message: status.Message,
					},
				}, nil
			}
		} else {
			return &response.RevertSwcVersionResponse{
				MetaInfo: &message.ResponseMetaInfoV1{
					Status:  false,
					Id:      "",
					Message: "Critical! Dbms cannot decided which increment operation list can be used to revert swc version to " + endTime.String() + "!",
				},
			}, nil
		}
	}
	return &response.RevertSwcVersionResponse{
		MetaInfo: &message.ResponseMetaInfoV1{
			Status:  false,
			Id:      "",
			Message: status.Message,
		},
	}, nil
}
