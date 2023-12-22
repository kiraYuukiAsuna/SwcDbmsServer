package bll

import (
	"DBMS/Generated/proto/message"
	"DBMS/dbmodel"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func UserMetaInfoV1ProtobufToDbmodel(protoMessage *message.UserMetaInfoV1) *dbmodel.UserMetaInfoV1 {
	var dbmodelMessage dbmodel.UserMetaInfoV1

	if protoMessage.Base != nil {
		dbmodelMessage.Base.Id, _ = primitive.ObjectIDFromHex(protoMessage.Base.XId)
		dbmodelMessage.Base.Uuid = protoMessage.Base.Uuid
		dbmodelMessage.Base.ApiVersion = protoMessage.Base.ApiVersion
	}

	dbmodelMessage.Name = protoMessage.Name
	dbmodelMessage.Password = protoMessage.Password
	dbmodelMessage.Description = protoMessage.Description
	dbmodelMessage.UserPermissionGroup = protoMessage.UserPermissionGroup

	if protoMessage.CreateTime != nil {
		dbmodelMessage.CreateTime = protoMessage.CreateTime.AsTime()
	}

	if protoMessage.HeadPhotoBinData != nil {
		dbmodelMessage.HeadPhotoBinData = protoMessage.HeadPhotoBinData
	}

	return &dbmodelMessage
}

func UserMetaInfoV1DbmodelToProtobuf(dbmodelMessage *dbmodel.UserMetaInfoV1) *message.UserMetaInfoV1 {
	var protoMessage message.UserMetaInfoV1
	protoMessage.Base = &message.MetaInfoBase{}
	protoMessage.Base.XId = dbmodelMessage.Base.Id.Hex()
	protoMessage.Base.Uuid = dbmodelMessage.Base.Uuid
	protoMessage.Base.ApiVersion = dbmodelMessage.Base.ApiVersion

	protoMessage.Name = dbmodelMessage.Name
	protoMessage.Password = dbmodelMessage.Password
	protoMessage.Description = dbmodelMessage.Description
	protoMessage.UserPermissionGroup = dbmodelMessage.UserPermissionGroup
	protoMessage.CreateTime = timestamppb.New(dbmodelMessage.CreateTime)
	if dbmodelMessage.HeadPhotoBinData != nil {
		protoMessage.HeadPhotoBinData = dbmodelMessage.HeadPhotoBinData
	}

	return &protoMessage
}

func PermissionGroupMetaInfoV1ProtobufToDbmodel(protoMessage *message.PermissionGroupMetaInfoV1) *dbmodel.PermissionGroupMetaInfoV1 {
	var dbmodelMessage dbmodel.PermissionGroupMetaInfoV1
	if protoMessage.Base != nil {
		dbmodelMessage.Base.Id, _ = primitive.ObjectIDFromHex(protoMessage.Base.XId)
		dbmodelMessage.Base.Uuid = protoMessage.Base.Uuid
		dbmodelMessage.Base.ApiVersion = protoMessage.Base.ApiVersion
	}

	dbmodelMessage.Name = protoMessage.Name
	dbmodelMessage.Description = protoMessage.Description
	if protoMessage.GlobalPermission != nil {
		dbmodelMessage.Global.ReadPerimissionQuery = protoMessage.GlobalPermission.ReadPerimissionQuery
		dbmodelMessage.Global.WritePermissionCreateProject = protoMessage.GlobalPermission.WritePermissionCreateProject
		dbmodelMessage.Global.WritePermissionModifyProject = protoMessage.GlobalPermission.WritePermissionModifyProject
		dbmodelMessage.Global.WritePermissionCreateProject = protoMessage.GlobalPermission.WritePermissionCreateProject
	}
	if protoMessage.ProjectPermission != nil {
		dbmodelMessage.Project.ReadPerimissionQuery = protoMessage.ProjectPermission.ReadPerimissionQuery
		dbmodelMessage.Project.WritePermissionAddData = protoMessage.ProjectPermission.WritePermissionAddData
		dbmodelMessage.Project.WritePermissionModifyData = protoMessage.ProjectPermission.WritePermissionModifyData
		dbmodelMessage.Project.WritePermissionDeleteData = protoMessage.ProjectPermission.WritePermissionDeleteData
	}

	return &dbmodelMessage
}

func PermissionGroupMetaInfoV1DbmodelToProtobuf(dbmodelMessage *dbmodel.PermissionGroupMetaInfoV1) *message.PermissionGroupMetaInfoV1 {
	var protoMessage message.PermissionGroupMetaInfoV1

	protoMessage.Base = &message.MetaInfoBase{}
	protoMessage.Base.XId = dbmodelMessage.Base.Id.Hex()
	protoMessage.Base.Uuid = dbmodelMessage.Base.Uuid
	protoMessage.Base.ApiVersion = dbmodelMessage.Base.ApiVersion

	protoMessage.Name = dbmodelMessage.Name
	protoMessage.Description = dbmodelMessage.Description

	protoMessage.GlobalPermission = &message.GlobalPermissionMetaInfoV1{}
	protoMessage.GlobalPermission.ReadPerimissionQuery = dbmodelMessage.Global.ReadPerimissionQuery
	protoMessage.GlobalPermission.WritePermissionCreateProject = dbmodelMessage.Global.WritePermissionCreateProject
	protoMessage.GlobalPermission.WritePermissionModifyProject = dbmodelMessage.Global.WritePermissionModifyProject
	protoMessage.GlobalPermission.WritePermissionDeleteProject = dbmodelMessage.Global.WritePermissionDeleteProject

	protoMessage.ProjectPermission = &message.ProjectPermissionMetaInfoV1{}
	protoMessage.ProjectPermission.ReadPerimissionQuery = dbmodelMessage.Project.ReadPerimissionQuery
	protoMessage.ProjectPermission.WritePermissionAddData = dbmodelMessage.Project.WritePermissionAddData
	protoMessage.ProjectPermission.WritePermissionModifyData = dbmodelMessage.Project.WritePermissionModifyData
	protoMessage.ProjectPermission.WritePermissionDeleteData = dbmodelMessage.Project.WritePermissionDeleteData

	return &protoMessage
}

func ProjectMetaInfoV1ProtobufToDbmodel(protoMessage *message.ProjectMetaInfoV1) *dbmodel.ProjectMetaInfoV1 {
	var dbmodelMessage dbmodel.ProjectMetaInfoV1

	if protoMessage.Base != nil {
		dbmodelMessage.Base.Id, _ = primitive.ObjectIDFromHex(protoMessage.Base.XId)
		dbmodelMessage.Base.Uuid = protoMessage.Base.Uuid
		dbmodelMessage.Base.ApiVersion = protoMessage.Base.ApiVersion
	}

	dbmodelMessage.Name = protoMessage.Name
	dbmodelMessage.Description = protoMessage.Description
	dbmodelMessage.Creator = protoMessage.Creator

	if protoMessage.CreateTime != nil {
		dbmodelMessage.CreateTime = protoMessage.CreateTime.AsTime()
	}

	if protoMessage.LastModifiedTime != nil {
		dbmodelMessage.LastModifiedTime = protoMessage.LastModifiedTime.AsTime()
	}

	if protoMessage.SwcList != nil {
		dbmodelMessage.SwcList = protoMessage.SwcList
	}

	dbmodelMessage.WorkMode = protoMessage.WorkMode

	if protoMessage.UserPermissionOverride != nil {
		for _, protoPermissionOverride := range protoMessage.UserPermissionOverride {
			var projectPermissionOverride dbmodel.UserPermissionOverrideMetaInfoV1
			projectPermissionOverride.Project = dbmodel.ProjectPermissionMetaInfoV1{}
			projectPermissionOverride.Project.ReadPerimissionQuery = protoPermissionOverride.ProjectPermission.ReadPerimissionQuery
			projectPermissionOverride.Project.WritePermissionAddData = protoPermissionOverride.ProjectPermission.WritePermissionAddData
			projectPermissionOverride.Project.WritePermissionModifyData = protoPermissionOverride.ProjectPermission.WritePermissionModifyData
			projectPermissionOverride.Project.WritePermissionDeleteData = protoPermissionOverride.ProjectPermission.WritePermissionDeleteData
			projectPermissionOverride.UserName = protoPermissionOverride.UserName

			dbmodelMessage.UserPermissionOverride = append(dbmodelMessage.UserPermissionOverride, projectPermissionOverride)
		}
	}

	return &dbmodelMessage
}

func ProjectMetaInfoV1DbmodelToProtobuf(dbmodelMessage *dbmodel.ProjectMetaInfoV1) *message.ProjectMetaInfoV1 {
	var protoMessage message.ProjectMetaInfoV1
	protoMessage.Base = &message.MetaInfoBase{}
	protoMessage.Base.XId = dbmodelMessage.Base.Id.Hex()
	protoMessage.Base.Uuid = dbmodelMessage.Base.Uuid
	protoMessage.Base.ApiVersion = dbmodelMessage.Base.ApiVersion

	protoMessage.Name = dbmodelMessage.Name
	protoMessage.Description = dbmodelMessage.Description
	protoMessage.Creator = dbmodelMessage.Creator

	protoMessage.CreateTime = timestamppb.New(dbmodelMessage.CreateTime)
	protoMessage.LastModifiedTime = timestamppb.New(dbmodelMessage.LastModifiedTime)
	protoMessage.SwcList = dbmodelMessage.SwcList
	protoMessage.WorkMode = dbmodelMessage.WorkMode

	for _, dbmodelPermissionOverride := range dbmodelMessage.UserPermissionOverride {
		var protoPermissionOverride message.UserPermissionOverrideMetaInfoV1
		protoPermissionOverride.ProjectPermission = &message.ProjectPermissionMetaInfoV1{}
		protoPermissionOverride.ProjectPermission.ReadPerimissionQuery = dbmodelPermissionOverride.Project.ReadPerimissionQuery
		protoPermissionOverride.ProjectPermission.WritePermissionAddData = dbmodelPermissionOverride.Project.WritePermissionAddData
		protoPermissionOverride.ProjectPermission.WritePermissionModifyData = dbmodelPermissionOverride.Project.WritePermissionModifyData
		protoPermissionOverride.ProjectPermission.WritePermissionDeleteData = dbmodelPermissionOverride.Project.WritePermissionDeleteData
		protoPermissionOverride.UserName = dbmodelPermissionOverride.UserName

		protoMessage.UserPermissionOverride = append(protoMessage.UserPermissionOverride, &protoPermissionOverride)
	}

	return &protoMessage
}

func SwcMetaInfoV1ProtobufToDbmodel(protoMessage *message.SwcMetaInfoV1) *dbmodel.SwcMetaInfoV1 {
	var dbmodelMessage dbmodel.SwcMetaInfoV1
	if protoMessage.Base != nil {
		dbmodelMessage.Base.Id, _ = primitive.ObjectIDFromHex(protoMessage.Base.XId)
		dbmodelMessage.Base.Uuid = protoMessage.Base.Uuid
		dbmodelMessage.Base.ApiVersion = protoMessage.Base.ApiVersion
	}

	dbmodelMessage.Name = protoMessage.Name
	dbmodelMessage.Description = protoMessage.Description
	dbmodelMessage.Creator = protoMessage.Creator
	dbmodelMessage.SwcType = protoMessage.SwcType

	if protoMessage.CreateTime != nil {
		dbmodelMessage.CreateTime = protoMessage.CreateTime.AsTime()
	}
	if protoMessage.LastModifiedTime != nil {
		dbmodelMessage.LastModifiedTime = protoMessage.LastModifiedTime.AsTime()
	}

	return &dbmodelMessage
}

func SwcMetaInfoV1DbmodelToProtobuf(dbmodelMessage *dbmodel.SwcMetaInfoV1) *message.SwcMetaInfoV1 {
	var protoMessage message.SwcMetaInfoV1
	protoMessage.Base = &message.MetaInfoBase{}
	protoMessage.Base.XId = dbmodelMessage.Base.Id.Hex()
	protoMessage.Base.Uuid = dbmodelMessage.Base.Uuid
	protoMessage.Base.ApiVersion = dbmodelMessage.Base.ApiVersion

	protoMessage.Name = dbmodelMessage.Name
	protoMessage.Description = dbmodelMessage.Description
	protoMessage.Creator = dbmodelMessage.Creator
	protoMessage.SwcType = dbmodelMessage.SwcType

	protoMessage.CreateTime = timestamppb.New(dbmodelMessage.CreateTime)
	protoMessage.LastModifiedTime = timestamppb.New(dbmodelMessage.LastModifiedTime)

	return &protoMessage
}

func SwcNodeDataV1ProtobufToDbmodel(protoMessage *message.SwcNodeDataV1) *dbmodel.SwcNodeDataV1 {
	var dbmodelMessage dbmodel.SwcNodeDataV1
	if protoMessage.Base != nil {
		dbmodelMessage.Base.Id, _ = primitive.ObjectIDFromHex(protoMessage.Base.XId)
		dbmodelMessage.Base.Uuid = protoMessage.Base.Uuid
		dbmodelMessage.Base.ApiVersion = protoMessage.Base.ApiVersion
	}
	dbmodelMessage.Creator = protoMessage.Creator
	if protoMessage.CreateTime != nil {
		dbmodelMessage.CreateTime = protoMessage.CreateTime.AsTime()
	}
	if protoMessage.LastModifiedTime != nil {
		dbmodelMessage.LastModifiedTime = protoMessage.LastModifiedTime.AsTime()
	}
	dbmodelMessage.CheckerUserUuid = protoMessage.CheckerUserUuid

	if protoMessage.SwcNodeInternalData != nil {
		dbmodelMessage.SwcNodeInternalData.N = protoMessage.SwcNodeInternalData.N
		dbmodelMessage.SwcNodeInternalData.Type = protoMessage.SwcNodeInternalData.Type
		dbmodelMessage.SwcNodeInternalData.X = protoMessage.SwcNodeInternalData.X
		dbmodelMessage.SwcNodeInternalData.Y = protoMessage.SwcNodeInternalData.Y
		dbmodelMessage.SwcNodeInternalData.Z = protoMessage.SwcNodeInternalData.Z
		dbmodelMessage.SwcNodeInternalData.Radius = protoMessage.SwcNodeInternalData.Radius
		dbmodelMessage.SwcNodeInternalData.Parent = protoMessage.SwcNodeInternalData.Parent
		dbmodelMessage.SwcNodeInternalData.Seg_id = protoMessage.SwcNodeInternalData.SegId
		dbmodelMessage.SwcNodeInternalData.Level = protoMessage.SwcNodeInternalData.Level
		dbmodelMessage.SwcNodeInternalData.Mode = protoMessage.SwcNodeInternalData.Mode
		dbmodelMessage.SwcNodeInternalData.Timestamp = protoMessage.SwcNodeInternalData.Timestamp
		dbmodelMessage.SwcNodeInternalData.Feature_value = protoMessage.SwcNodeInternalData.FeatureValue
	}

	return &dbmodelMessage
}

func SwcNodeDataV1DbmodelToProtobuf(dbmodelMessage *dbmodel.SwcNodeDataV1) *message.SwcNodeDataV1 {
	var protoMessage message.SwcNodeDataV1
	protoMessage.Base = &message.MetaInfoBase{}
	protoMessage.Base.XId = dbmodelMessage.Base.Id.Hex()
	protoMessage.Base.Uuid = dbmodelMessage.Base.Uuid
	protoMessage.Base.ApiVersion = dbmodelMessage.Base.ApiVersion

	protoMessage.Creator = dbmodelMessage.Creator

	protoMessage.CreateTime = timestamppb.New(dbmodelMessage.CreateTime)
	protoMessage.LastModifiedTime = timestamppb.New(dbmodelMessage.LastModifiedTime)

	protoMessage.CheckerUserUuid = dbmodelMessage.CheckerUserUuid

	protoMessage.SwcNodeInternalData = &message.SwcNodeInternalDataV1{}
	protoMessage.SwcNodeInternalData.N = dbmodelMessage.SwcNodeInternalData.N
	protoMessage.SwcNodeInternalData.Type = dbmodelMessage.SwcNodeInternalData.Type
	protoMessage.SwcNodeInternalData.X = dbmodelMessage.SwcNodeInternalData.X
	protoMessage.SwcNodeInternalData.Y = dbmodelMessage.SwcNodeInternalData.Y
	protoMessage.SwcNodeInternalData.Z = dbmodelMessage.SwcNodeInternalData.Z
	protoMessage.SwcNodeInternalData.Radius = dbmodelMessage.SwcNodeInternalData.Radius
	protoMessage.SwcNodeInternalData.Parent = dbmodelMessage.SwcNodeInternalData.Parent
	protoMessage.SwcNodeInternalData.SegId = dbmodelMessage.SwcNodeInternalData.Seg_id
	protoMessage.SwcNodeInternalData.Level = dbmodelMessage.SwcNodeInternalData.Level
	protoMessage.SwcNodeInternalData.Mode = dbmodelMessage.SwcNodeInternalData.Mode
	protoMessage.SwcNodeInternalData.Timestamp = dbmodelMessage.SwcNodeInternalData.Timestamp
	protoMessage.SwcNodeInternalData.FeatureValue = dbmodelMessage.SwcNodeInternalData.Feature_value

	return &protoMessage
}

func DailyStatisticsMetaInfoV1ProtobufToDbmodel(protoMessage *message.DailyStatisticsMetaInfoV1) *dbmodel.DailyStatisticsMetaInfoV1 {
	var dbmodelMessage dbmodel.DailyStatisticsMetaInfoV1
	if protoMessage.Base != nil {
		dbmodelMessage.Base.Id, _ = primitive.ObjectIDFromHex(protoMessage.Base.XId)
		dbmodelMessage.Base.Uuid = protoMessage.Base.Uuid
		dbmodelMessage.Base.ApiVersion = protoMessage.Base.ApiVersion
	}

	dbmodelMessage.Name = protoMessage.Name
	dbmodelMessage.Description = protoMessage.Description
	dbmodelMessage.Day = protoMessage.Day

	dbmodelMessage.CreatedProjectNumber = protoMessage.CreatedProjectNumber
	dbmodelMessage.CreatedSwcNumber = protoMessage.CreatedSwcNumber
	dbmodelMessage.CreateSwcNodeNumber = protoMessage.CreateSwcNodeNumber

	dbmodelMessage.DeletedProjectNumber = protoMessage.DeletedProjectNumber
	dbmodelMessage.DeletedSwcNumber = protoMessage.DeletedSwcNumber
	dbmodelMessage.DeletedSwcNodeNumber = protoMessage.DeletedSwcNodeNumber

	dbmodelMessage.ModifiedProjectNumber = protoMessage.ModifiedProjectNumber
	dbmodelMessage.ModifiedSwcNumber = protoMessage.ModifiedSwcNumber
	dbmodelMessage.ModifiedSwcNodeNumber = protoMessage.ModifiedSwcNodeNumber

	dbmodelMessage.ProjectQueryNumber = protoMessage.ProjectQueryNumber
	dbmodelMessage.SwcQueryNumber = protoMessage.SwcQueryNumber
	dbmodelMessage.NodeQueryNumber = protoMessage.NodeQueryNumber

	dbmodelMessage.ActiveUserNumber = protoMessage.ActiveUserNumber

	return &dbmodelMessage
}

func DailyStatisticsMetaInfoV1DbmodelToProtobuf(dbmodelMessage *dbmodel.DailyStatisticsMetaInfoV1) *message.DailyStatisticsMetaInfoV1 {
	var protoMessage message.DailyStatisticsMetaInfoV1
	protoMessage.Base = &message.MetaInfoBase{}
	protoMessage.Base.XId = dbmodelMessage.Base.Id.Hex()
	protoMessage.Base.Uuid = dbmodelMessage.Base.Uuid
	protoMessage.Base.ApiVersion = dbmodelMessage.Base.ApiVersion

	protoMessage.Name = dbmodelMessage.Name
	protoMessage.Description = dbmodelMessage.Description
	protoMessage.Day = dbmodelMessage.Day

	protoMessage.CreatedProjectNumber = dbmodelMessage.CreatedProjectNumber
	protoMessage.CreatedSwcNumber = dbmodelMessage.CreatedSwcNumber
	protoMessage.CreateSwcNodeNumber = dbmodelMessage.CreateSwcNodeNumber

	protoMessage.DeletedProjectNumber = dbmodelMessage.DeletedProjectNumber
	protoMessage.DeletedSwcNumber = dbmodelMessage.DeletedSwcNumber
	protoMessage.DeletedSwcNodeNumber = dbmodelMessage.DeletedSwcNodeNumber

	protoMessage.ModifiedProjectNumber = dbmodelMessage.ModifiedProjectNumber
	protoMessage.ModifiedSwcNumber = dbmodelMessage.ModifiedSwcNumber
	protoMessage.ModifiedSwcNodeNumber = dbmodelMessage.ModifiedSwcNodeNumber

	protoMessage.ProjectQueryNumber = dbmodelMessage.ProjectQueryNumber
	protoMessage.SwcQueryNumber = dbmodelMessage.SwcQueryNumber
	protoMessage.NodeQueryNumber = dbmodelMessage.NodeQueryNumber

	protoMessage.ActiveUserNumber = dbmodelMessage.ActiveUserNumber

	return &protoMessage
}
