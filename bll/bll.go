package bll

import (
	"DBMS/Generated/proto/message"
	"DBMS/Generated/proto/request"
	"DBMS/Generated/proto/response"
	"DBMS/Generated/proto/service"
	"DBMS/dal"
	"DBMS/dbmodel"
	"context"
	"github.com/google/uuid"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"log"
	"time"
)

type DBMSServerController struct {
	service.UnimplementedDBMSServer
}

func (D DBMSServerController) CreateUser(ctx context.Context, request *request.CreateUserRequest) (*response.CreateUserResponse, error) {
	userMetaInfo := UserMetaInfoV1ProtobufToDbmodel(request.UserInfo)
	userMetaInfo.Base.Id = primitive.NewObjectID()
	userMetaInfo.Base.Uuid = uuid.NewString()
	userMetaInfo.Base.ApiVersion = "V1"

	userMetaInfo.Name = request.UserInfo.Name
	userMetaInfo.Password = request.UserInfo.Password
	userMetaInfo.Description = request.UserInfo.Description
	userMetaInfo.UserPermissionGroup = dal.PermissionGroupDefault
	userMetaInfo.CreateTime = time.Now()
	userMetaInfo.HeadPhotoBinData = request.UserInfo.HeadPhotoBinData

	result := dal.CreateUser(*userMetaInfo, dal.GetDbInstance())
	if result.Status == true {
		log.Println("User " + request.UserInfo.Name + " Created")
		return &response.CreateUserResponse{
			Status:   true,
			Message:  result.Message,
			UserInfo: UserMetaInfoV1DbmodelToProtobuf(userMetaInfo),
		}, nil
	} else {
		return &response.CreateUserResponse{
			Status:   false,
			Message:  result.Message,
			UserInfo: UserMetaInfoV1DbmodelToProtobuf(userMetaInfo),
		}, nil
	}
}

func (D DBMSServerController) DeleteUser(ctx context.Context, request *request.DeleteUserRequest) (*response.DeleteUserResponse, error) {
	userMetaInfo := UserMetaInfoV1ProtobufToDbmodel(request.UserInfo)

	result := dal.QueryUser(userMetaInfo, dal.GetDbInstance())
	if result.Status == false {
		return &response.DeleteUserResponse{
			Status:   false,
			Message:  result.Message,
			UserInfo: UserMetaInfoV1DbmodelToProtobuf(userMetaInfo),
		}, nil
	}

	if userMetaInfo.UserPermissionGroup != dal.PermissionGroupAdmin {
		return &response.DeleteUserResponse{
			Status:   false,
			Message:  "You don't have permission to delete user!",
			UserInfo: UserMetaInfoV1DbmodelToProtobuf(userMetaInfo),
		}, nil
	}

	result = dal.DeleteUser(*userMetaInfo, dal.GetDbInstance())
	if result.Status == true {
		log.Println("User " + request.UserInfo.Name + " Deleted")
		return &response.DeleteUserResponse{
			Status:   true,
			Message:  result.Message,
			UserInfo: UserMetaInfoV1DbmodelToProtobuf(userMetaInfo),
		}, nil
	} else {
		return &response.DeleteUserResponse{
			Status:   false,
			Message:  result.Message,
			UserInfo: UserMetaInfoV1DbmodelToProtobuf(userMetaInfo),
		}, nil
	}
}

func (D DBMSServerController) UpdateUser(ctx context.Context, request *request.UpdateUserRequest) (*response.UpdateUserResponse, error) {
	userMetaInfo := UserMetaInfoV1ProtobufToDbmodel(request.UserInfo)

	result := dal.ModifyUser(*userMetaInfo, dal.GetDbInstance())
	if result.Status == true {
		log.Println("User " + request.UserInfo.Name + " Updated")
		return &response.UpdateUserResponse{
			Status:   true,
			Message:  result.Message,
			UserInfo: UserMetaInfoV1DbmodelToProtobuf(userMetaInfo),
		}, nil
	} else {
		return &response.UpdateUserResponse{
			Status:   false,
			Message:  result.Message,
			UserInfo: UserMetaInfoV1DbmodelToProtobuf(userMetaInfo),
		}, nil
	}
}

func (D DBMSServerController) GetUser(ctx context.Context, request *request.GetUserRequest) (*response.GetUserResponse, error) {
	userMetaInfo := UserMetaInfoV1ProtobufToDbmodel(request.UserInfo)

	result := dal.QueryUser(userMetaInfo, dal.GetDbInstance())
	if result.Status == true {
		log.Println("User " + request.UserInfo.Name + " Get")
		return &response.GetUserResponse{
			Status:   true,
			Message:  result.Message,
			UserInfo: UserMetaInfoV1DbmodelToProtobuf(userMetaInfo),
		}, nil
	} else {
		return &response.GetUserResponse{
			Status:   false,
			Message:  result.Message,
			UserInfo: UserMetaInfoV1DbmodelToProtobuf(userMetaInfo),
		}, nil
	}
}

func (D DBMSServerController) GetAllUser(ctx context.Context, request *request.GetAllUserRequest) (*response.GetAllUserResponse, error) {
	_ = UserMetaInfoV1ProtobufToDbmodel(request.UserInfo)

	var userMetaInfoList []dbmodel.UserMetaInfoV1
	var protoMessage []*message.UserMetaInfoV1

	result := dal.QueryAllUser(&userMetaInfoList, dal.GetDbInstance())
	if result.Status == true {
		log.Println("User " + request.UserInfo.Name + " Try Get AllUser")
		for _, userMetaInfo := range userMetaInfoList {
			userMetaInfo.Password = ""
			protoMessage = append(protoMessage, UserMetaInfoV1DbmodelToProtobuf(&userMetaInfo))
		}
		return &response.GetAllUserResponse{
			Status:   true,
			Message:  result.Message,
			UserInfo: protoMessage,
		}, nil
	} else {
		return &response.GetAllUserResponse{
			Status:   false,
			Message:  result.Message,
			UserInfo: protoMessage,
		}, nil
	}
}

func (D DBMSServerController) UserLogin(ctx context.Context, request *request.UserLoginRequest) (*response.UserLoginResponse, error) {
	if request == nil {
		return &response.UserLoginResponse{
			Status:   false,
			Message:  "Request is nil",
			UserInfo: nil,
		}, nil
	}
	var userMetaInfo dbmodel.UserMetaInfoV1
	userMetaInfo.Name = request.UserName

	result := dal.QueryUser(&userMetaInfo, dal.GetDbInstance())
	if result.Status == true {
		if userMetaInfo.Password == request.Password {
			log.Println("User " + request.UserName + " Login")

			bFind := false
			for _, onlineUserInfo := range OnlineUserInfoCache {
				if userMetaInfo.Name == onlineUserInfo.UserInfo.Name {
					bFind = true
				}
			}
			if !bFind {
				DailyStatisticsInfo.ActiveUserNumber += 1
				OnlineUserInfoCache = append(OnlineUserInfoCache,
					OnlineUserInfo{userMetaInfo, false, time.Now().Add(30 * time.Second)})
				log.Println("User " + userMetaInfo.Name + " HeartBeat Init")
			}

			return &response.UserLoginResponse{
				Status:   true,
				Message:  result.Message,
				UserInfo: UserMetaInfoV1DbmodelToProtobuf(&userMetaInfo),
			}, nil
		} else {
			userMetaInfo.Password = ""
			return &response.UserLoginResponse{
				Status:   false,
				Message:  result.Message,
				UserInfo: UserMetaInfoV1DbmodelToProtobuf(&userMetaInfo),
			}, nil
		}
	} else {
		userMetaInfo.Password = ""
		return &response.UserLoginResponse{
			Status:   false,
			Message:  result.Message,
			UserInfo: UserMetaInfoV1DbmodelToProtobuf(&userMetaInfo),
		}, nil
	}
}

func (D DBMSServerController) UserLogout(ctx context.Context, request *request.UserLogoutRequest) (*response.UserLogoutResponse, error) {
	if request == nil {
		return &response.UserLogoutResponse{
			Status:  false,
			Message: "Request is nil",
		}, nil
	}

	var userMetaInfo dbmodel.UserMetaInfoV1
	userMetaInfo.Name = request.UserInfo.Name

	result := dal.QueryUser(&userMetaInfo, dal.GetDbInstance())
	if result.Status == true {
		if userMetaInfo.Password == request.UserInfo.Password {
			log.Println("User " + request.UserInfo.Name + " Logout")

			for idx, onlineUserInfo := range OnlineUserInfoCache {
				if userMetaInfo.Name == onlineUserInfo.UserInfo.Name {
					onlineUserInfo.expired = true
					DailyStatisticsInfo.ActiveUserNumber -= 1
					OnlineUserInfoCache = append(OnlineUserInfoCache[:idx], OnlineUserInfoCache[idx+1:]...)
					log.Println("User " + onlineUserInfo.UserInfo.Name + " HeaatBeat Close")
					break
				}
			}

			return &response.UserLogoutResponse{
				Status:  true,
				Message: result.Message,
			}, nil
		}
	}
	userMetaInfo.Password = ""
	return &response.UserLogoutResponse{
		Status:  false,
		Message: result.Message,
	}, nil
}

func (D DBMSServerController) UserOnlineHeartBeatNotifications(ctx context.Context, notification *request.UserOnlineHeartBeatNotification) (*response.UserOnlineHeartBeatResponse, error) {
	if notification == nil {
		return &response.UserOnlineHeartBeatResponse{
			Status:  false,
			Message: "Request is nil",
		}, nil
	}

	var userMetaInfo dbmodel.UserMetaInfoV1
	userMetaInfo.Name = notification.UserInfo.Name

	result := dal.QueryUser(&userMetaInfo, dal.GetDbInstance())
	if result.Status == true {
		if userMetaInfo.Password == notification.UserInfo.Password {
			log.Println("User " + notification.UserInfo.Name + " OnlineHeartBeatNotifications")

			bFind := false
			var idx int
			var onlineUserInfo OnlineUserInfo
			for idx, onlineUserInfo = range OnlineUserInfoCache {
				if userMetaInfo.Name == onlineUserInfo.UserInfo.Name {
					bFind = true
				}
			}
			if bFind {
				OnlineUserInfoCache[idx].LastHeartBeatTime = time.Now().Add(30 * time.Second)
				log.Println("User " + onlineUserInfo.UserInfo.Name + " HeartBeat Refresh")
			} else {
				DailyStatisticsInfo.ActiveUserNumber += 1
				OnlineUserInfoCache = append(OnlineUserInfoCache,
					OnlineUserInfo{userMetaInfo, false, time.Now().Add(30 * time.Second)})
				log.Println("User " + userMetaInfo.Name + " HeartBeat Init by HeartBeat Notification")
			}

			return &response.UserOnlineHeartBeatResponse{
				Status:  true,
				Message: result.Message,
			}, nil
		}
	}
	userMetaInfo.Password = ""
	return &response.UserOnlineHeartBeatResponse{
		Status:  false,
		Message: result.Message,
	}, nil
}

func (D DBMSServerController) GetUserPermissionGroup(ctx context.Context, request *request.GetUserPermissionGroupRequest) (*response.GetUserPermissionGroupResponse, error) {
	userMetaInfo := UserMetaInfoV1ProtobufToDbmodel(request.UserInfo)

	var permissionGroupMetaInfo dbmodel.PermissionGroupMetaInfoV1

	result := dal.QueryUser(userMetaInfo, dal.GetDbInstance())
	if result.Status == true {
		permissionGroupMetaInfo.Name = userMetaInfo.UserPermissionGroup
		result = dal.QueryPermissionGroup(&permissionGroupMetaInfo, dal.GetDbInstance())
		if result.Status == true {
			log.Println("User " + request.UserInfo.Name + " GetUserPermissionGroup")
			return &response.GetUserPermissionGroupResponse{
				Status:          true,
				Message:         result.Message,
				PermissionGroup: PermissionGroupMetaInfoV1DbmodelToProtobuf(&permissionGroupMetaInfo),
			}, nil
		}

	}
	return &response.GetUserPermissionGroupResponse{
		Status:          false,
		Message:         result.Message,
		PermissionGroup: PermissionGroupMetaInfoV1DbmodelToProtobuf(&permissionGroupMetaInfo),
	}, nil
}

func (D DBMSServerController) GetPermissionGroup(ctx context.Context, request *request.GetPermissionGroupRequest) (*response.GetPermissionGroupResponse, error) {
	userMetaInfo := UserMetaInfoV1ProtobufToDbmodel(request.UserInfo)

	permissionGroupMetaInfo := PermissionGroupMetaInfoV1ProtobufToDbmodel(request.PermissionGroup)

	result := dal.QueryUser(userMetaInfo, dal.GetDbInstance())
	if result.Status == true {
		result = dal.QueryPermissionGroup(permissionGroupMetaInfo, dal.GetDbInstance())
		if result.Status == true {
			log.Println("User " + request.UserInfo.Name + " GetPermissionGroup")
			return &response.GetPermissionGroupResponse{
				Status:          true,
				Message:         result.Message,
				PermissionGroup: PermissionGroupMetaInfoV1DbmodelToProtobuf(permissionGroupMetaInfo),
			}, nil
		}

	}
	return &response.GetPermissionGroupResponse{
		Status:          false,
		Message:         result.Message,
		PermissionGroup: PermissionGroupMetaInfoV1DbmodelToProtobuf(permissionGroupMetaInfo),
	}, nil
}

func GetAllPermissionGroup(ctx context.Context, request *request.GetAllPermissionGroupRequest) (*response.GetAllPermissionGroupResponse, error) {
	userMetaInfo := UserMetaInfoV1ProtobufToDbmodel(request.UserInfo)

	var permissionGroupList []dbmodel.PermissionGroupMetaInfoV1
	var protoMessage []*message.PermissionGroupMetaInfoV1

	result := dal.QueryUser(userMetaInfo, dal.GetDbInstance())
	if result.Status == true {
		result = dal.QueryAllPermissionGroup(&permissionGroupList, dal.GetDbInstance())
		if result.Status == true {
			log.Println("User " + request.UserInfo.Name + " GetAllPermissionGroup")
			for _, permissionGroupMetaInfo := range permissionGroupList {
				protoMessage = append(protoMessage, PermissionGroupMetaInfoV1DbmodelToProtobuf(&permissionGroupMetaInfo))
			}
			return &response.GetAllPermissionGroupResponse{
				Status:              true,
				Message:             result.Message,
				PermissionGroupList: protoMessage,
			}, nil
		}

	}
	return &response.GetAllPermissionGroupResponse{
		Status:              false,
		Message:             result.Message,
		PermissionGroupList: protoMessage,
	}, nil
}

func (D DBMSServerController) ChangeUserPermissionGroup(ctx context.Context, request *request.ChangeUserPermissionGroupRequest) (*response.ChangeUserPermissionGroupResponse, error) {
	userMetaInfo := UserMetaInfoV1ProtobufToDbmodel(request.UserInfo)
	targetUserMetaInfo := UserMetaInfoV1ProtobufToDbmodel(request.TargetUserInfo)

	result := dal.QueryUser(userMetaInfo, dal.GetDbInstance())
	if result.Status == true {
		if userMetaInfo.UserPermissionGroup != dal.PermissionGroupAdmin {
			return &response.ChangeUserPermissionGroupResponse{
				Status:  false,
				Message: "You don't have permission to change user group!",
			}, nil
		}

		var permissionGroupMetaInfo dbmodel.PermissionGroupMetaInfoV1

		result = dal.QueryUser(targetUserMetaInfo, dal.GetDbInstance())
		if result.Status == true {
			permissionGroupMetaInfo.Name = targetUserMetaInfo.UserPermissionGroup
			result = dal.QueryPermissionGroup(&permissionGroupMetaInfo, dal.GetDbInstance())
			if result.Status == true {
				result = dal.ModifyUser(*targetUserMetaInfo, dal.GetDbInstance())

				log.Println("User " + request.UserInfo.Name + " Changed PermissionGroup")
				return &response.ChangeUserPermissionGroupResponse{
					Status:  true,
					Message: result.Message,
				}, nil
			} else {
				return &response.ChangeUserPermissionGroupResponse{
					Status:  false,
					Message: result.Message,
				}, nil
			}

		}
	}
	return &response.ChangeUserPermissionGroupResponse{
		Status:  false,
		Message: result.Message,
	}, nil
}

func (D DBMSServerController) CreateProject(ctx context.Context, request *request.CreateProjectRequest) (*response.CreateProjectResponse, error) {
	userMetaInfo := UserMetaInfoV1ProtobufToDbmodel(request.UserInfo)
	projectMetaInfo := ProjectMetaInfoV1ProtobufToDbmodel(request.ProjectInfo)

	result := dal.QueryUser(userMetaInfo, dal.GetDbInstance())
	if result.Status == false {
		return &response.CreateProjectResponse{
			Status:      false,
			Message:     result.Message,
			ProjectInfo: ProjectMetaInfoV1DbmodelToProtobuf(projectMetaInfo),
		}, nil
	}

	var permissionGroup dbmodel.PermissionGroupMetaInfoV1
	permissionGroup.Name = userMetaInfo.UserPermissionGroup
	result = dal.QueryPermissionGroup(&permissionGroup, dal.GetDbInstance())
	if result.Status == false {
		return &response.CreateProjectResponse{
			Status:      false,
			Message:     result.Message,
			ProjectInfo: ProjectMetaInfoV1DbmodelToProtobuf(projectMetaInfo),
		}, nil
	}

	if permissionGroup.Global.WritePermissionCreateProject == false {
		return &response.CreateProjectResponse{
			Status:      false,
			Message:     "You don't have permission to create project!",
			ProjectInfo: ProjectMetaInfoV1DbmodelToProtobuf(projectMetaInfo),
		}, nil
	}

	projectMetaInfo.Base.Id = primitive.NewObjectID()
	projectMetaInfo.Base.Uuid = uuid.NewString()
	projectMetaInfo.Base.ApiVersion = "V1"

	projectMetaInfo.Name = request.ProjectInfo.Name
	projectMetaInfo.Description = request.ProjectInfo.Description
	projectMetaInfo.Creator = request.UserInfo.Name

	projectMetaInfo.CreateTime = time.Now()
	projectMetaInfo.LastModifiedTime = time.Now()

	projectMetaInfo.WorkMode = request.ProjectInfo.WorkMode

	result = dal.CreateProject(*projectMetaInfo, dal.GetDbInstance())
	if result.Status == true {
		log.Println("Project " + request.ProjectInfo.Name + " Created")
		DailyStatisticsInfo.CreatedProjectNumber += 1
		return &response.CreateProjectResponse{
			Status:      true,
			Message:     result.Message,
			ProjectInfo: ProjectMetaInfoV1DbmodelToProtobuf(projectMetaInfo),
		}, nil
	} else {
		return &response.CreateProjectResponse{
			Status:      false,
			Message:     result.Message,
			ProjectInfo: ProjectMetaInfoV1DbmodelToProtobuf(projectMetaInfo),
		}, nil
	}
}

func (D DBMSServerController) DeleteProject(ctx context.Context, request *request.DeleteProjectRequest) (*response.DeleteProjectResponse, error) {
	userMetaInfo := UserMetaInfoV1ProtobufToDbmodel(request.UserInfo)
	projectMetaInfo := ProjectMetaInfoV1ProtobufToDbmodel(request.ProjectInfo)

	result := dal.QueryUser(userMetaInfo, dal.GetDbInstance())
	if result.Status == false {
		return &response.DeleteProjectResponse{
			Status:      false,
			Message:     result.Message,
			ProjectInfo: ProjectMetaInfoV1DbmodelToProtobuf(projectMetaInfo),
		}, nil
	}

	var permissionGroup dbmodel.PermissionGroupMetaInfoV1
	permissionGroup.Name = userMetaInfo.UserPermissionGroup
	result = dal.QueryPermissionGroup(&permissionGroup, dal.GetDbInstance())
	if result.Status == false {
		return &response.DeleteProjectResponse{
			Status:      false,
			Message:     result.Message,
			ProjectInfo: ProjectMetaInfoV1DbmodelToProtobuf(projectMetaInfo),
		}, nil
	}

	if permissionGroup.Global.WritePermissionDeleteProject == false {
		return &response.DeleteProjectResponse{
			Status:      false,
			Message:     "You don't have permission to delete project!",
			ProjectInfo: ProjectMetaInfoV1DbmodelToProtobuf(projectMetaInfo),
		}, nil
	}

	result = dal.DeleteProject(*projectMetaInfo, dal.GetDbInstance())
	if result.Status == true {
		log.Println("Project " + request.ProjectInfo.Name + " Deleted")
		DailyStatisticsInfo.DeletedProjectNumber += 1
		return &response.DeleteProjectResponse{
			Status:      true,
			Message:     result.Message,
			ProjectInfo: ProjectMetaInfoV1DbmodelToProtobuf(projectMetaInfo),
		}, nil
	} else {
		return &response.DeleteProjectResponse{
			Status:      false,
			Message:     result.Message,
			ProjectInfo: ProjectMetaInfoV1DbmodelToProtobuf(projectMetaInfo),
		}, nil
	}
}

func (D DBMSServerController) UpdateProject(ctx context.Context, request *request.UpdateProjectRequest) (*response.UpdateProjectResponse, error) {
	userMetaInfo := UserMetaInfoV1ProtobufToDbmodel(request.UserInfo)
	projectMetaInfo := ProjectMetaInfoV1ProtobufToDbmodel(request.ProjectInfo)

	result := dal.QueryUser(userMetaInfo, dal.GetDbInstance())
	if result.Status == false {
		return &response.UpdateProjectResponse{
			Status:      false,
			Message:     result.Message,
			ProjectInfo: ProjectMetaInfoV1DbmodelToProtobuf(projectMetaInfo),
		}, nil
	}

	var permissionGroup dbmodel.PermissionGroupMetaInfoV1
	permissionGroup.Name = userMetaInfo.UserPermissionGroup
	result = dal.QueryPermissionGroup(&permissionGroup, dal.GetDbInstance())
	if result.Status == false {
		return &response.UpdateProjectResponse{
			Status:      false,
			Message:     result.Message,
			ProjectInfo: ProjectMetaInfoV1DbmodelToProtobuf(projectMetaInfo),
		}, nil
	}

	if permissionGroup.Global.WritePermissionModifyProject == false {
		return &response.UpdateProjectResponse{
			Status:      false,
			Message:     "You don't have permission to update project!",
			ProjectInfo: ProjectMetaInfoV1DbmodelToProtobuf(projectMetaInfo),
		}, nil
	}

	projectMetaInfo.LastModifiedTime = time.Now()

	result = dal.ModifyProject(*projectMetaInfo, dal.GetDbInstance())
	if result.Status == true {
		log.Println("Project " + request.UserInfo.Name + " Updated")
		DailyStatisticsInfo.ModifiedProjectNumber += 1
		return &response.UpdateProjectResponse{
			Status:      true,
			Message:     result.Message,
			ProjectInfo: ProjectMetaInfoV1DbmodelToProtobuf(projectMetaInfo),
		}, nil
	} else {
		return &response.UpdateProjectResponse{
			Status:      false,
			Message:     result.Message,
			ProjectInfo: ProjectMetaInfoV1DbmodelToProtobuf(projectMetaInfo),
		}, nil
	}
}

func (D DBMSServerController) GetProject(ctx context.Context, request *request.GetProjectRequest) (*response.GetProjectResponse, error) {
	userMetaInfo := UserMetaInfoV1ProtobufToDbmodel(request.UserInfo)
	projectMetaInfo := ProjectMetaInfoV1ProtobufToDbmodel(request.ProjectInfo)

	result := dal.QueryUser(userMetaInfo, dal.GetDbInstance())
	if result.Status == false {
		return &response.GetProjectResponse{
			Status:      false,
			Message:     result.Message,
			ProjectInfo: request.ProjectInfo,
		}, nil
	}

	var permissionGroup dbmodel.PermissionGroupMetaInfoV1
	permissionGroup.Name = userMetaInfo.UserPermissionGroup
	result = dal.QueryPermissionGroup(&permissionGroup, dal.GetDbInstance())
	if result.Status == false {
		return &response.GetProjectResponse{
			Status:      false,
			Message:     result.Message,
			ProjectInfo: request.ProjectInfo,
		}, nil
	}

	if permissionGroup.Global.ReadPerimissionQuery == false {
		return &response.GetProjectResponse{
			Status:      false,
			Message:     "You don't have permission to access this project",
			ProjectInfo: request.ProjectInfo,
		}, nil
	}

	result = dal.QueryProject(projectMetaInfo, dal.GetDbInstance())
	if result.Status == true {
		log.Println("Project " + request.UserInfo.Name + " Get")
		DailyStatisticsInfo.ProjectQueryNumber += 1
		return &response.GetProjectResponse{
			Status:      true,
			Message:     result.Message,
			ProjectInfo: ProjectMetaInfoV1DbmodelToProtobuf(projectMetaInfo),
		}, nil
	} else {
		return &response.GetProjectResponse{
			Status:      false,
			Message:     result.Message,
			ProjectInfo: ProjectMetaInfoV1DbmodelToProtobuf(projectMetaInfo),
		}, nil
	}
}

func (D DBMSServerController) GetAllProject(ctx context.Context, request *request.GetAllProjectRequest) (*response.GetAllProjectResponse, error) {
	_ = UserMetaInfoV1ProtobufToDbmodel(request.UserInfo)

	var projectMetaInfoList []dbmodel.ProjectMetaInfoV1
	var protoMessage []*message.ProjectMetaInfoV1

	result := dal.QueryAllProject(&projectMetaInfoList, dal.GetDbInstance())
	if result.Status == true {
		log.Println("User " + request.UserInfo.Name + " Try Get AllProject")
		DailyStatisticsInfo.ProjectQueryNumber += 1
		for _, projectMetaInfo := range projectMetaInfoList {
			protoMessage = append(protoMessage, ProjectMetaInfoV1DbmodelToProtobuf(&projectMetaInfo))
		}
		return &response.GetAllProjectResponse{
			Status:      true,
			Message:     result.Message,
			ProjectInfo: protoMessage,
		}, nil
	} else {
		return &response.GetAllProjectResponse{
			Status:      false,
			Message:     result.Message,
			ProjectInfo: protoMessage,
		}, nil
	}
}

func (D DBMSServerController) CreateSwc(ctx context.Context, request *request.CreateSwcRequest) (*response.CreateSwcResponse, error) {
	userMetaInfo := UserMetaInfoV1ProtobufToDbmodel(request.UserInfo)
	swcMetaInfo := SwcMetaInfoV1ProtobufToDbmodel(request.SwcInfo)

	result := dal.QueryUser(userMetaInfo, dal.GetDbInstance())
	if result.Status == false {
		return &response.CreateSwcResponse{
			Status:  false,
			Message: result.Message,
			SwcInfo: SwcMetaInfoV1DbmodelToProtobuf(swcMetaInfo),
		}, nil
	}

	var permissionGroup dbmodel.PermissionGroupMetaInfoV1
	permissionGroup.Name = userMetaInfo.UserPermissionGroup
	result = dal.QueryPermissionGroup(&permissionGroup, dal.GetDbInstance())
	if result.Status == false {
		return &response.CreateSwcResponse{
			Status:  false,
			Message: result.Message,
			SwcInfo: SwcMetaInfoV1DbmodelToProtobuf(swcMetaInfo),
		}, nil
	}

	if permissionGroup.Project.WritePermissionAddData == false {
		return &response.CreateSwcResponse{
			Status:  false,
			Message: "You don't have permission to create swc node!",
			SwcInfo: SwcMetaInfoV1DbmodelToProtobuf(swcMetaInfo),
		}, nil
	}

	swcMetaInfo.Base.Id = primitive.NewObjectID()
	swcMetaInfo.Base.Uuid = uuid.NewString()
	swcMetaInfo.Base.ApiVersion = "V1"
	swcMetaInfo.Creator = userMetaInfo.Name
	swcMetaInfo.LastModifiedTime = time.Now()
	swcMetaInfo.CreateTime = time.Now()
	swcMetaInfo.Name = request.SwcInfo.Name
	swcMetaInfo.Description = request.SwcInfo.Description
	swcMetaInfo.SwcType = request.SwcInfo.SwcType

	result = dal.CreateSwc(*swcMetaInfo, dal.GetDbInstance())
	if result.Status {
		log.Println("User " + request.UserInfo.Name + "Create Swc " + swcMetaInfo.Name)
		DailyStatisticsInfo.CreatedSwcNumber += 1
		return &response.CreateSwcResponse{
			Status:  true,
			Message: result.Message,
			SwcInfo: SwcMetaInfoV1DbmodelToProtobuf(swcMetaInfo),
		}, nil
	} else {
		return &response.CreateSwcResponse{
			Status:  false,
			Message: result.Message,
			SwcInfo: SwcMetaInfoV1DbmodelToProtobuf(swcMetaInfo),
		}, nil
	}
}

func (D DBMSServerController) DeleteSwc(ctx context.Context, request *request.DeleteSwcRequest) (*response.DeleteSwcResponse, error) {
	userMetaInfo := UserMetaInfoV1ProtobufToDbmodel(request.UserInfo)
	swcMetaInfo := SwcMetaInfoV1ProtobufToDbmodel(request.SwcInfo)

	result := dal.QueryUser(userMetaInfo, dal.GetDbInstance())
	if result.Status == false {
		return &response.DeleteSwcResponse{
			Status:  false,
			Message: result.Message,
			SwcInfo: SwcMetaInfoV1DbmodelToProtobuf(swcMetaInfo),
		}, nil
	}

	var permissionGroup dbmodel.PermissionGroupMetaInfoV1
	permissionGroup.Name = userMetaInfo.UserPermissionGroup
	result = dal.QueryPermissionGroup(&permissionGroup, dal.GetDbInstance())
	if result.Status == false {
		return &response.DeleteSwcResponse{
			Status:  false,
			Message: result.Message,
			SwcInfo: SwcMetaInfoV1DbmodelToProtobuf(swcMetaInfo),
		}, nil
	}

	if permissionGroup.Project.WritePermissionDeleteData == false {
		return &response.DeleteSwcResponse{
			Status:  false,
			Message: "You don't have permission to create swc!",
			SwcInfo: SwcMetaInfoV1DbmodelToProtobuf(swcMetaInfo),
		}, nil
	}

	result = dal.DeleteSwc(*swcMetaInfo, dal.GetDbInstance())

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
		result = dal.DeleteSwcDataCollection(*swcMetaInfo, dal.GetDbInstance())
		if result.Status {
			log.Println("User " + request.UserInfo.Name + "Delete Swc " + swcMetaInfo.Name)
			DailyStatisticsInfo.DeletedSwcNumber += 1
			return &response.DeleteSwcResponse{
				Status:  true,
				Message: result.Message,
				SwcInfo: SwcMetaInfoV1DbmodelToProtobuf(swcMetaInfo),
			}, nil
		}
	}
	return &response.DeleteSwcResponse{
		Status:  false,
		Message: result.Message,
		SwcInfo: SwcMetaInfoV1DbmodelToProtobuf(swcMetaInfo),
	}, nil
}

func (D DBMSServerController) UpdateSwc(ctx context.Context, request *request.UpdateSwcRequest) (*response.UpdateSwcResponse, error) {
	userMetaInfo := UserMetaInfoV1ProtobufToDbmodel(request.UserInfo)
	swcMetaInfo := SwcMetaInfoV1ProtobufToDbmodel(request.SwcInfo)

	result := dal.QueryUser(userMetaInfo, dal.GetDbInstance())
	if result.Status == false {
		return &response.UpdateSwcResponse{
			Status:  false,
			Message: result.Message,
			SwcInfo: SwcMetaInfoV1DbmodelToProtobuf(swcMetaInfo),
		}, nil
	}

	var permissionGroup dbmodel.PermissionGroupMetaInfoV1
	permissionGroup.Name = userMetaInfo.UserPermissionGroup
	result = dal.QueryPermissionGroup(&permissionGroup, dal.GetDbInstance())
	if result.Status == false {
		return &response.UpdateSwcResponse{
			Status:  false,
			Message: result.Message,
			SwcInfo: SwcMetaInfoV1DbmodelToProtobuf(swcMetaInfo),
		}, nil
	}

	if permissionGroup.Project.WritePermissionModifyData == false {
		return &response.UpdateSwcResponse{
			Status:  false,
			Message: "You don't have permission to modify swc!",
			SwcInfo: SwcMetaInfoV1DbmodelToProtobuf(swcMetaInfo),
		}, nil
	}

	swcMetaInfo.LastModifiedTime = time.Now()

	result = dal.ModifySwc(*swcMetaInfo, dal.GetDbInstance())
	if result.Status {
		log.Println("User " + request.UserInfo.Name + " Update SwcMetaInfo " + swcMetaInfo.Name)
		DailyStatisticsInfo.ModifiedSwcNumber += 1
		return &response.UpdateSwcResponse{
			Status:  true,
			Message: result.Message,
			SwcInfo: SwcMetaInfoV1DbmodelToProtobuf(swcMetaInfo),
		}, nil
	} else {
		return &response.UpdateSwcResponse{
			Status:  false,
			Message: result.Message,
			SwcInfo: SwcMetaInfoV1DbmodelToProtobuf(swcMetaInfo),
		}, nil
	}
}

func (D DBMSServerController) GetSwcMetaInfo(ctx context.Context, request *request.GetSwcMetaInfoRequest) (*response.GetSwcMetaInfoResponse, error) {
	userMetaInfo := UserMetaInfoV1ProtobufToDbmodel(request.UserInfo)
	swcMetaInfo := SwcMetaInfoV1ProtobufToDbmodel(request.SwcInfo)

	result := dal.QueryUser(userMetaInfo, dal.GetDbInstance())
	if result.Status == false {
		return &response.GetSwcMetaInfoResponse{
			Status:  false,
			Message: result.Message,
			SwcInfo: request.SwcInfo,
		}, nil
	}

	var permissionGroup dbmodel.PermissionGroupMetaInfoV1
	permissionGroup.Name = userMetaInfo.UserPermissionGroup
	result = dal.QueryPermissionGroup(&permissionGroup, dal.GetDbInstance())
	if result.Status == false {
		return &response.GetSwcMetaInfoResponse{
			Status:  false,
			Message: result.Message,
			SwcInfo: request.SwcInfo,
		}, nil
	}

	if permissionGroup.Global.ReadPerimissionQuery == false {
		return &response.GetSwcMetaInfoResponse{
			Status:  false,
			Message: "You don't have permission to access this swc",
			SwcInfo: request.SwcInfo,
		}, nil
	}

	result = dal.QuerySwc(swcMetaInfo, dal.GetDbInstance())
	if result.Status {
		log.Println("User " + request.UserInfo.Name + " Query SwcMetaInfo " + swcMetaInfo.Name)
		DailyStatisticsInfo.SwcQueryNumber += 1
		return &response.GetSwcMetaInfoResponse{
			Status:  true,
			Message: result.Message,
			SwcInfo: SwcMetaInfoV1DbmodelToProtobuf(swcMetaInfo),
		}, nil
	} else {
		return &response.GetSwcMetaInfoResponse{
			Status:  false,
			Message: result.Message,
			SwcInfo: SwcMetaInfoV1DbmodelToProtobuf(swcMetaInfo),
		}, nil
	}
}

func (D DBMSServerController) GetAllSwcMetaInfo(ctx context.Context, request *request.GetAllSwcMetaInfoRequest) (*response.GetAllSwcMetaInfoResponse, error) {
	_ = UserMetaInfoV1ProtobufToDbmodel(request.UserInfo)

	var dbmodelMessage []dbmodel.SwcMetaInfoV1

	var protoMessage []*message.SwcMetaInfoV1
	result := dal.QueryAllSwc(&dbmodelMessage, dal.GetDbInstance())
	if result.Status {
		log.Println("User " + request.UserInfo.Name + " Query All SwcMetaInfo ")
		for _, dbMessage := range dbmodelMessage {
			protoMessage = append(protoMessage, SwcMetaInfoV1DbmodelToProtobuf(&dbMessage))
		}
		return &response.GetAllSwcMetaInfoResponse{
			Status:  true,
			Message: result.Message,
			SwcInfo: protoMessage,
		}, nil
	} else {
		return &response.GetAllSwcMetaInfoResponse{
			Status:  false,
			Message: result.Message,
			SwcInfo: protoMessage,
		}, nil
	}
}

func (D DBMSServerController) CreateSwcNodeData(ctx context.Context, request *request.CreateSwcNodeDataRequest) (*response.CreateSwcNodeDataResponse, error) {
	userMetaInfo := UserMetaInfoV1ProtobufToDbmodel(request.UserInfo)
	swcMetaInfo := SwcMetaInfoV1ProtobufToDbmodel(request.SwcInfo)

	result := dal.QueryUser(userMetaInfo, dal.GetDbInstance())
	if result.Status == false {
		return &response.CreateSwcNodeDataResponse{
			Status:  false,
			Message: result.Message,
			SwcData: request.SwcData,
		}, nil
	}

	var permissionGroup dbmodel.PermissionGroupMetaInfoV1
	permissionGroup.Name = userMetaInfo.UserPermissionGroup
	result = dal.QueryPermissionGroup(&permissionGroup, dal.GetDbInstance())
	if result.Status == false {
		return &response.CreateSwcNodeDataResponse{
			Status:  false,
			Message: result.Message,
			SwcData: request.SwcData,
		}, nil
	}

	if permissionGroup.Project.WritePermissionAddData == false {
		return &response.CreateSwcNodeDataResponse{
			Status:  false,
			Message: "You don't have permission to create swc node!",
			SwcData: request.SwcData,
		}, nil
	}

	var swcData dbmodel.SwcDataV1
	for _, swcNodeData := range request.SwcData.SwcData {
		swcData = append(swcData, *SwcNodeDataV1ProtobufToDbmodel(swcNodeData))
	}

	for idx := range swcData {
		swcData[idx].Creator = userMetaInfo.Name
		swcData[idx].Base.Id = primitive.NewObjectID()
		swcData[idx].Base.Uuid = uuid.NewString()
		swcData[idx].Base.ApiVersion = "V1"
		swcData[idx].CreateTime = time.Now()
		swcData[idx].LastModifiedTime = time.Now()
		swcData[idx].CheckerUserUuid = ""
	}

	result = dal.CreateSwcData(*swcMetaInfo, swcData, dal.GetDbInstance())
	if result.Status {
		log.Println("User " + request.UserInfo.Name + " Create Swc node " + swcMetaInfo.Name)
		DailyStatisticsInfo.CreateSwcNodeNumber += 1
		return &response.CreateSwcNodeDataResponse{
			Status:  true,
			Message: result.Message,
			SwcData: request.SwcData,
		}, nil
	} else {
		return &response.CreateSwcNodeDataResponse{
			Status:  false,
			Message: result.Message,
			SwcData: request.SwcData,
		}, nil
	}
}

func (D DBMSServerController) DeleteSwcNodeData(ctx context.Context, request *request.DeleteSwcNodeDataRequest) (*response.DeleteSwcNodeDataResponse, error) {
	userMetaInfo := UserMetaInfoV1ProtobufToDbmodel(request.UserInfo)
	swcMetaInfo := SwcMetaInfoV1ProtobufToDbmodel(request.SwcInfo)

	result := dal.QueryUser(userMetaInfo, dal.GetDbInstance())
	if result.Status == false {
		return &response.DeleteSwcNodeDataResponse{
			Status:  false,
			Message: result.Message,
			SwcData: request.SwcData,
		}, nil
	}

	var permissionGroup dbmodel.PermissionGroupMetaInfoV1
	permissionGroup.Name = userMetaInfo.UserPermissionGroup
	result = dal.QueryPermissionGroup(&permissionGroup, dal.GetDbInstance())
	if result.Status == false {
		return &response.DeleteSwcNodeDataResponse{
			Status:  false,
			Message: result.Message,
			SwcData: request.SwcData,
		}, nil
	}

	if permissionGroup.Project.WritePermissionDeleteData == false {
		return &response.DeleteSwcNodeDataResponse{
			Status:  false,
			Message: "You don't have permission to delete swc node!",
			SwcData: request.SwcData,
		}, nil
	}

	var swcData dbmodel.SwcDataV1
	for _, swcNodeData := range request.SwcData.SwcData {
		swcData = append(swcData, *SwcNodeDataV1ProtobufToDbmodel(swcNodeData))
	}

	result = dal.DeleteSwcData(*swcMetaInfo, swcData, dal.GetDbInstance())
	if result.Status {
		log.Println("User " + request.UserInfo.Name + " Delete Swc " + swcMetaInfo.Name)
		DailyStatisticsInfo.DeletedSwcNodeNumber += 1
		return &response.DeleteSwcNodeDataResponse{
			Status:  true,
			Message: result.Message,
			SwcData: request.SwcData,
		}, nil
	} else {
		return &response.DeleteSwcNodeDataResponse{
			Status:  false,
			Message: result.Message,
			SwcData: request.SwcData,
		}, nil
	}
}

func (D DBMSServerController) UpdateSwcNodeData(ctx context.Context, request *request.UpdateSwcNodeDataRequest) (*response.UpdateSwcNodeDataResponse, error) {
	userMetaInfo := UserMetaInfoV1ProtobufToDbmodel(request.UserInfo)
	swcMetaInfo := SwcMetaInfoV1ProtobufToDbmodel(request.SwcInfo)

	result := dal.QueryUser(userMetaInfo, dal.GetDbInstance())
	if result.Status == false {
		return &response.UpdateSwcNodeDataResponse{
			Status:      false,
			Message:     result.Message,
			SwcNodeData: request.SwcNodeData,
		}, nil
	}

	var permissionGroup dbmodel.PermissionGroupMetaInfoV1
	permissionGroup.Name = userMetaInfo.UserPermissionGroup
	result = dal.QueryPermissionGroup(&permissionGroup, dal.GetDbInstance())
	if result.Status == false {
		return &response.UpdateSwcNodeDataResponse{
			Status:      false,
			Message:     result.Message,
			SwcNodeData: request.SwcNodeData,
		}, nil
	}

	if permissionGroup.Project.WritePermissionModifyData == false {
		return &response.UpdateSwcNodeDataResponse{
			Status:      false,
			Message:     "You don't have permission to modify swc node!",
			SwcNodeData: request.SwcNodeData,
		}, nil
	}

	swcNodeData := SwcNodeDataV1ProtobufToDbmodel(request.SwcNodeData)
	swcMetaInfo.LastModifiedTime = time.Now()
	swcNodeData.LastModifiedTime = time.Now()

	result = dal.ModifySwcData(*swcMetaInfo, *swcNodeData, dal.GetDbInstance())
	if result.Status {
		log.Println("User " + request.UserInfo.Name + " Update Swc " + swcMetaInfo.Name)
		DailyStatisticsInfo.ModifiedSwcNodeNumber += 1
		return &response.UpdateSwcNodeDataResponse{
			Status:      true,
			Message:     result.Message,
			SwcNodeData: request.SwcNodeData,
		}, nil
	} else {
		return &response.UpdateSwcNodeDataResponse{
			Status:      false,
			Message:     result.Message,
			SwcNodeData: request.SwcNodeData,
		}, nil
	}
}

func (D DBMSServerController) GetSwcNodeData(ctx context.Context, request *request.GetSwcNodeDataRequest) (*response.GetSwcNodeDataResponse, error) {
	userMetaInfo := UserMetaInfoV1ProtobufToDbmodel(request.UserInfo)
	swcMetaInfo := SwcMetaInfoV1ProtobufToDbmodel(request.SwcInfo)

	result := dal.QueryUser(userMetaInfo, dal.GetDbInstance())
	if result.Status == false {
		return &response.GetSwcNodeDataResponse{
			Status:      false,
			Message:     result.Message,
			SwcNodeData: request.SwcNodeData,
		}, nil
	}

	var permissionGroup dbmodel.PermissionGroupMetaInfoV1
	permissionGroup.Name = userMetaInfo.UserPermissionGroup
	result = dal.QueryPermissionGroup(&permissionGroup, dal.GetDbInstance())
	if result.Status == false {
		return &response.GetSwcNodeDataResponse{
			Status:      false,
			Message:     result.Message,
			SwcNodeData: request.SwcNodeData,
		}, nil
	}

	if permissionGroup.Project.ReadPerimissionQuery == false {
		return &response.GetSwcNodeDataResponse{
			Status:      false,
			Message:     "You don't have permission to access this swc node",
			SwcNodeData: request.SwcNodeData,
		}, nil
	}

	var dbmodelMessage dbmodel.SwcDataV1

	var protoMessage message.SwcDataV1

	for _, swcNodeData := range request.SwcNodeData.SwcData {
		dbmodelMessage = append(dbmodelMessage, *SwcNodeDataV1ProtobufToDbmodel(swcNodeData))
	}

	result = dal.DeleteSwcData(*swcMetaInfo, dbmodelMessage, dal.GetDbInstance())
	if result.Status {
		log.Println("User " + request.UserInfo.Name + " Get SwcData " + swcMetaInfo.Name)

		DailyStatisticsInfo.NodeQueryNumber += 1

		for _, swcNodeData := range dbmodelMessage {
			protoMessage.SwcData = append(protoMessage.SwcData, SwcNodeDataV1DbmodelToProtobuf(&swcNodeData))
		}

		return &response.GetSwcNodeDataResponse{
			Status:      true,
			Message:     result.Message,
			SwcNodeData: &protoMessage,
		}, nil
	} else {
		return &response.GetSwcNodeDataResponse{
			Status:      false,
			Message:     result.Message,
			SwcNodeData: &protoMessage,
		}, nil
	}
}

func (D DBMSServerController) GetSwcFullNodeData(ctx context.Context, request *request.GetSwcFullNodeDataRequest) (*response.GetSwcFullNodeDataResponse, error) {
	userMetaInfo := UserMetaInfoV1ProtobufToDbmodel(request.UserInfo)
	swcMetaInfo := SwcMetaInfoV1ProtobufToDbmodel(request.SwcInfo)

	var dbmodelMessage dbmodel.SwcDataV1
	var protoMessage message.SwcDataV1

	result := dal.QueryUser(userMetaInfo, dal.GetDbInstance())
	if result.Status == false {
		return &response.GetSwcFullNodeDataResponse{
			Status:      false,
			Message:     result.Message,
			SwcNodeData: &protoMessage,
		}, nil
	}

	var permissionGroup dbmodel.PermissionGroupMetaInfoV1
	permissionGroup.Name = userMetaInfo.UserPermissionGroup
	result = dal.QueryPermissionGroup(&permissionGroup, dal.GetDbInstance())
	if result.Status == false {
		return &response.GetSwcFullNodeDataResponse{
			Status:      false,
			Message:     result.Message,
			SwcNodeData: &protoMessage,
		}, nil
	}

	if permissionGroup.Project.ReadPerimissionQuery == false {
		return &response.GetSwcFullNodeDataResponse{
			Status:      false,
			Message:     "You don't have permission to access this full swc node data!",
			SwcNodeData: &protoMessage,
		}, nil
	}

	result = dal.QueryAllSwcData(*swcMetaInfo, &dbmodelMessage, dal.GetDbInstance())
	if result.Status {
		log.Println("User " + request.UserInfo.Name + " Get SwcFullNodeData " + swcMetaInfo.Name)
		DailyStatisticsInfo.NodeQueryNumber += 1
		for _, swcNodeData := range dbmodelMessage {
			protoMessage.SwcData = append(protoMessage.SwcData, SwcNodeDataV1DbmodelToProtobuf(&swcNodeData))
		}

		return &response.GetSwcFullNodeDataResponse{
			Status:      true,
			Message:     result.Message,
			SwcNodeData: &protoMessage,
		}, nil
	} else {
		return &response.GetSwcFullNodeDataResponse{
			Status:      false,
			Message:     result.Message,
			SwcNodeData: &protoMessage,
		}, nil
	}
}

func (D DBMSServerController) GetSwcNodeDataListByTimeAndUser(ctx context.Context, request *request.GetSwcNodeDataListByTimeAndUserRequest) (*response.GetSwcNodeDataListByTimeAndUserResponse, error) {
	userMetaInfo := UserMetaInfoV1ProtobufToDbmodel(request.UserInfo)
	swcMetaInfo := SwcMetaInfoV1ProtobufToDbmodel(request.SwcInfo)

	var dbmodelMessage dbmodel.SwcDataV1
	var protoMessage message.SwcDataV1

	result := dal.QueryUser(userMetaInfo, dal.GetDbInstance())
	if result.Status == false {
		return &response.GetSwcNodeDataListByTimeAndUserResponse{
			Status:      false,
			Message:     result.Message,
			SwcNodeData: &protoMessage,
		}, nil
	}

	var permissionGroup dbmodel.PermissionGroupMetaInfoV1
	permissionGroup.Name = userMetaInfo.UserPermissionGroup
	result = dal.QueryPermissionGroup(&permissionGroup, dal.GetDbInstance())
	if result.Status == false {
		return &response.GetSwcNodeDataListByTimeAndUserResponse{
			Status:      false,
			Message:     result.Message,
			SwcNodeData: &protoMessage,
		}, nil
	}

	if permissionGroup.Project.ReadPerimissionQuery == false {
		return &response.GetSwcNodeDataListByTimeAndUserResponse{
			Status:      false,
			Message:     "You don't have permission to access the swc node data!",
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

	result = dal.QuerySwcDataByUserAndTime(*swcMetaInfo, request.UserName, startTime, endTime, &dbmodelMessage, dal.GetDbInstance())
	if result.Status {
		log.Println("User " + request.UserInfo.Name + " Get SwcDataByUserAndTime " + swcMetaInfo.Name)
		DailyStatisticsInfo.NodeQueryNumber += 1

		for _, swcNodeData := range dbmodelMessage {
			protoMessage.SwcData = append(protoMessage.SwcData, SwcNodeDataV1DbmodelToProtobuf(&swcNodeData))
		}

		return &response.GetSwcNodeDataListByTimeAndUserResponse{
			Status:      true,
			Message:     result.Message,
			SwcNodeData: &protoMessage,
		}, nil
	} else {
		return &response.GetSwcNodeDataListByTimeAndUserResponse{
			Status:      false,
			Message:     result.Message,
			SwcNodeData: &protoMessage,
		}, nil
	}
}

func (D DBMSServerController) BackupFullDatabase(ctx context.Context, request *request.BackupFullDatabaseRequest) (*response.BackupFullDatabaseResponse, error) {
	log.Println("Unimplemented")

	return &response.BackupFullDatabaseResponse{
		Status:          false,
		Message:         "Unimplemented",
		InstantBackup:   false,
		DelayBackupTime: nil,
	}, nil
}

func (D DBMSServerController) CreateDailyStatistics(ctx context.Context, request *request.CreateDailyStatisticsRequest) (*response.CreateDailyStatisticsResponse, error) {
	_ = UserMetaInfoV1ProtobufToDbmodel(request.UserInfo)
	dailyStatisticsInfo := DailyStatisticsMetaInfoV1ProtobufToDbmodel(request.DailyStatisticsInfo)
	dailyStatisticsInfo.Base.Id = primitive.NewObjectID()
	dailyStatisticsInfo.Base.Uuid = uuid.NewString()
	dailyStatisticsInfo.Base.ApiVersion = "V1"

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
			Status:              true,
			Message:             result.Message,
			DailyStatisticsInfo: DailyStatisticsMetaInfoV1DbmodelToProtobuf(dailyStatisticsInfo),
		}, nil
	} else {
		return &response.CreateDailyStatisticsResponse{
			Status:              false,
			Message:             result.Message,
			DailyStatisticsInfo: DailyStatisticsMetaInfoV1DbmodelToProtobuf(dailyStatisticsInfo),
		}, nil
	}
}

func (D DBMSServerController) DeleteDailyStatistics(ctx context.Context, request *request.DeleteDailyStatisticsRequest) (*response.DeleteDailyStatisticsResponse, error) {
	userMetaInfo := UserMetaInfoV1ProtobufToDbmodel(request.UserInfo)
	dailyStatisticsInfo := DailyStatisticsMetaInfoV1ProtobufToDbmodel(request.DailyStatisticsInfo)

	result := dal.QueryUser(userMetaInfo, dal.GetDbInstance())
	if result.Status == false {
		return &response.DeleteDailyStatisticsResponse{
			Status:              false,
			Message:             result.Message,
			DailyStatisticsInfo: request.DailyStatisticsInfo,
		}, nil
	}

	if userMetaInfo.UserPermissionGroup != dal.PermissionGroupAdmin {
		return &response.DeleteDailyStatisticsResponse{
			Status:              false,
			Message:             "You don't have permission to delete this DailyStatistics!",
			DailyStatisticsInfo: request.DailyStatisticsInfo,
		}, nil
	}

	result = dal.DeleteDailyStatistics(*dailyStatisticsInfo, dal.GetDbInstance())
	if result.Status == true {
		log.Println("DailyStatistics " + request.DailyStatisticsInfo.Name + " Delete")
		return &response.DeleteDailyStatisticsResponse{
			Status:              true,
			Message:             result.Message,
			DailyStatisticsInfo: DailyStatisticsMetaInfoV1DbmodelToProtobuf(dailyStatisticsInfo),
		}, nil
	} else {
		return &response.DeleteDailyStatisticsResponse{
			Status:              false,
			Message:             result.Message,
			DailyStatisticsInfo: DailyStatisticsMetaInfoV1DbmodelToProtobuf(dailyStatisticsInfo),
		}, nil
	}
}

func (D DBMSServerController) UpdateDailyStatistics(ctx context.Context, request *request.UpdateDailyStatisticsRequest) (*response.UpdateDailyStatisticsResponse, error) {
	_ = UserMetaInfoV1ProtobufToDbmodel(request.UserInfo)
	dailyStatisticsInfo := DailyStatisticsMetaInfoV1ProtobufToDbmodel(request.DailyStatisticsInfo)

	result := dal.ModifyDailyStatistics(*dailyStatisticsInfo, dal.GetDbInstance())
	if result.Status == true {
		log.Println("DailyStatistics " + request.DailyStatisticsInfo.Name + " Updated")
		return &response.UpdateDailyStatisticsResponse{
			Status:              true,
			Message:             result.Message,
			DailyStatisticsInfo: DailyStatisticsMetaInfoV1DbmodelToProtobuf(dailyStatisticsInfo),
		}, nil
	} else {
		return &response.UpdateDailyStatisticsResponse{
			Status:              false,
			Message:             result.Message,
			DailyStatisticsInfo: DailyStatisticsMetaInfoV1DbmodelToProtobuf(dailyStatisticsInfo),
		}, nil
	}
}

func (D DBMSServerController) GetDailyStatistics(ctx context.Context, request *request.GetDailyStatisticsRequest) (*response.GetDailyStatisticsResponse, error) {
	_ = UserMetaInfoV1ProtobufToDbmodel(request.UserInfo)
	dailyStatisticsInfo := DailyStatisticsMetaInfoV1ProtobufToDbmodel(request.DailyStatisticsInfo)

	result := dal.QueryDailyStatistics(dailyStatisticsInfo, dal.GetDbInstance())
	if result.Status == true {
		log.Println("DailyStatistics " + request.DailyStatisticsInfo.Name + " Get")
		return &response.GetDailyStatisticsResponse{
			Status:              true,
			Message:             result.Message,
			DailyStatisticsInfo: DailyStatisticsMetaInfoV1DbmodelToProtobuf(dailyStatisticsInfo),
		}, nil
	} else {
		return &response.GetDailyStatisticsResponse{
			Status:              false,
			Message:             result.Message,
			DailyStatisticsInfo: DailyStatisticsMetaInfoV1DbmodelToProtobuf(dailyStatisticsInfo),
		}, nil
	}
}

func (D DBMSServerController) GetAllDailyStatistics(ctx context.Context, request *request.GetAllDailyStatisticsRequest) (*response.GetAllDailyStatisticsResponse, error) {
	_ = UserMetaInfoV1ProtobufToDbmodel(request.UserInfo)
	var dailyStatisticsInfo []dbmodel.DailyStatisticsMetaInfoV1
	var dailyStatisticsInfoProto []*message.DailyStatisticsMetaInfoV1

	result := dal.QueryAllDailyStatistics(&dailyStatisticsInfo, dal.GetDbInstance())
	if result.Status == true {
		log.Println("User " + request.UserInfo.Name + " Get DailyStatistics")

		for _, message := range dailyStatisticsInfo {
			dailyStatisticsInfoProto = append(dailyStatisticsInfoProto, DailyStatisticsMetaInfoV1DbmodelToProtobuf(&message))
		}

		return &response.GetAllDailyStatisticsResponse{
			Status:              true,
			Message:             result.Message,
			DailyStatisticsInfo: dailyStatisticsInfoProto,
		}, nil
	} else {
		return &response.GetAllDailyStatisticsResponse{
			Status:              false,
			Message:             result.Message,
			DailyStatisticsInfo: dailyStatisticsInfoProto,
		}, nil
	}
}
