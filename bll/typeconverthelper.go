package bll

import (
	"DBMS/SwcDbmsCommon/Generated/go/proto/message"
	"DBMS/dbmodel"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"google.golang.org/protobuf/types/known/timestamppb"
	"reflect"
)

func UserMetaInfoV1ProtobufToDbmodel(protoMessage *message.UserMetaInfoV1) *dbmodel.UserMetaInfoV1 {
	var dbmodelMessage dbmodel.UserMetaInfoV1

	if protoMessage.Base != nil {
		dbmodelMessage.Base.Id, _ = primitive.ObjectIDFromHex(protoMessage.Base.XId)
		dbmodelMessage.Base.Uuid = protoMessage.Base.Uuid
		dbmodelMessage.Base.DataAccessModelVersion = protoMessage.Base.DataAccessModelVersion
	}

	dbmodelMessage.Name = protoMessage.Name
	dbmodelMessage.Password = protoMessage.Password
	dbmodelMessage.Description = protoMessage.Description
	dbmodelMessage.PermissionGroupUuid = protoMessage.PermissionGroupUuid
	dbmodelMessage.UserId = protoMessage.UserId

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
	protoMessage.Base.DataAccessModelVersion = dbmodelMessage.Base.DataAccessModelVersion

	protoMessage.Name = dbmodelMessage.Name
	protoMessage.Password = dbmodelMessage.Password
	protoMessage.Description = dbmodelMessage.Description
	protoMessage.PermissionGroupUuid = dbmodelMessage.PermissionGroupUuid
	protoMessage.UserId = dbmodelMessage.UserId

	protoMessage.CreateTime = timestamppb.New(dbmodelMessage.CreateTime)
	if dbmodelMessage.HeadPhotoBinData != nil {
		protoMessage.HeadPhotoBinData = dbmodelMessage.HeadPhotoBinData
	}

	return &protoMessage
}

func PermissionGroupAceProtoToDb(protoMessage *message.PermissionGroupAceV1, dbmodelMessage *dbmodel.PermissionGroupAceV1) {
	protoVal := reflect.ValueOf(protoMessage).Elem()
	dbVal := reflect.ValueOf(dbmodelMessage).Elem()

	for i := 0; i < protoVal.NumField(); i++ {
		name := protoVal.Type().Field(i).Name
		dbField := dbVal.FieldByName(name)
		if dbField.IsValid() && dbField.CanSet() {
			dbField.Set(protoVal.Field(i))
		}
	}
}

func PermissionGroupMetaInfoV1ProtobufToDbmodel(protoMessage *message.PermissionGroupMetaInfoV1) *dbmodel.PermissionGroupMetaInfoV1 {
	var dbmodelMessage dbmodel.PermissionGroupMetaInfoV1
	if protoMessage.Base != nil {
		dbmodelMessage.Base.Id, _ = primitive.ObjectIDFromHex(protoMessage.Base.XId)
		dbmodelMessage.Base.Uuid = protoMessage.Base.Uuid
		dbmodelMessage.Base.DataAccessModelVersion = protoMessage.Base.DataAccessModelVersion
	}

	dbmodelMessage.Name = protoMessage.Name
	dbmodelMessage.Description = protoMessage.Description

	if protoMessage.Ace != nil {
		PermissionGroupAceProtoToDb(protoMessage.Ace, &dbmodelMessage.Ace)
	}

	return &dbmodelMessage
}

func PermissionGroupAceDbToProto(dbmodelMessage *dbmodel.PermissionGroupAceV1, protoMessage *message.PermissionGroupAceV1) {
	dbVal := reflect.ValueOf(dbmodelMessage).Elem()
	protoVal := reflect.ValueOf(protoMessage).Elem()

	for i := 0; i < dbVal.NumField(); i++ {
		name := dbVal.Type().Field(i).Name
		protoField := protoVal.FieldByName(name)
		if protoField.IsValid() && protoField.CanSet() {
			protoField.Set(dbVal.Field(i))
		}
	}
}

func PermissionGroupMetaInfoV1DbmodelToProtobuf(dbmodelMessage *dbmodel.PermissionGroupMetaInfoV1) *message.PermissionGroupMetaInfoV1 {
	var protoMessage message.PermissionGroupMetaInfoV1

	protoMessage.Base = &message.MetaInfoBase{}
	protoMessage.Base.XId = dbmodelMessage.Base.Id.Hex()
	protoMessage.Base.Uuid = dbmodelMessage.Base.Uuid
	protoMessage.Base.DataAccessModelVersion = dbmodelMessage.Base.DataAccessModelVersion

	protoMessage.Name = dbmodelMessage.Name
	protoMessage.Description = dbmodelMessage.Description

	protoMessage.Ace = &message.PermissionGroupAceV1{}
	PermissionGroupAceDbToProto(&dbmodelMessage.Ace, protoMessage.Ace)

	return &protoMessage
}

func ProjectMetaInfoV1ProtobufToDbmodel(protoMessage *message.ProjectMetaInfoV1) *dbmodel.ProjectMetaInfoV1 {
	var dbmodelMessage dbmodel.ProjectMetaInfoV1

	if protoMessage.Base != nil {
		dbmodelMessage.Base.Id, _ = primitive.ObjectIDFromHex(protoMessage.Base.XId)
		dbmodelMessage.Base.Uuid = protoMessage.Base.Uuid
		dbmodelMessage.Base.DataAccessModelVersion = protoMessage.Base.DataAccessModelVersion
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

	if protoMessage.Permission != nil {
		if protoMessage.Permission.Owner != nil {
			dbmodelMessage.Permission.Owner.UserUuid = protoMessage.Permission.Owner.UserUuid
			if protoMessage.Permission.Owner.Ace != nil {
				PermissionAceProtoToDb(protoMessage.Permission.Owner.Ace, &dbmodelMessage.Permission.Owner.Ace)
			}
		}
		if protoMessage.Permission.Users != nil {
			for _, protoUserPermission := range protoMessage.Permission.Users {
				var acl dbmodel.UserPermissionAclV1
				acl.UserUuid = protoUserPermission.UserUuid

				if protoUserPermission.Ace != nil {
					PermissionAceProtoToDb(protoUserPermission.Ace, &acl.Ace)
				}

				dbmodelMessage.Permission.Users = append(dbmodelMessage.Permission.Users, acl)
			}
		}
		if protoMessage.Permission.Groups != nil {
			for _, protoGroupPermission := range protoMessage.Permission.Groups {
				var acl dbmodel.GroupPermissionAclV1
				acl.GroupUuid = protoGroupPermission.GroupUuid

				if protoGroupPermission.Ace != nil {
					PermissionAceProtoToDb(protoGroupPermission.Ace, &acl.Ace)
				}

				dbmodelMessage.Permission.Groups = append(dbmodelMessage.Permission.Groups, acl)
			}
		}
	}

	return &dbmodelMessage
}

func ProjectMetaInfoV1DbmodelToProtobuf(dbmodelMessage *dbmodel.ProjectMetaInfoV1) *message.ProjectMetaInfoV1 {
	var protoMessage message.ProjectMetaInfoV1
	protoMessage.Base = &message.MetaInfoBase{}
	protoMessage.Base.XId = dbmodelMessage.Base.Id.Hex()
	protoMessage.Base.Uuid = dbmodelMessage.Base.Uuid
	protoMessage.Base.DataAccessModelVersion = dbmodelMessage.Base.DataAccessModelVersion

	protoMessage.Name = dbmodelMessage.Name
	protoMessage.Description = dbmodelMessage.Description
	protoMessage.Creator = dbmodelMessage.Creator

	protoMessage.CreateTime = timestamppb.New(dbmodelMessage.CreateTime)
	protoMessage.LastModifiedTime = timestamppb.New(dbmodelMessage.LastModifiedTime)
	protoMessage.SwcList = dbmodelMessage.SwcList
	protoMessage.WorkMode = dbmodelMessage.WorkMode

	protoMessage.Permission = &message.PermissionMetaInfoV1{}
	protoMessage.Permission.Owner = &message.UserPermissionAclV1{}
	protoMessage.Permission.Owner.UserUuid = dbmodelMessage.Permission.Owner.UserUuid
	protoMessage.Permission.Owner.Ace = &message.PermissionAceV1{}

	PermissionAceDbToProto(&dbmodelMessage.Permission.Owner.Ace, protoMessage.Permission.Owner.Ace)

	for _, dbUserPermission := range dbmodelMessage.Permission.Users {
		var userPermission message.UserPermissionAclV1
		userPermission.UserUuid = dbUserPermission.UserUuid
		userPermission.Ace = &message.PermissionAceV1{}

		PermissionAceDbToProto(&dbUserPermission.Ace, userPermission.Ace)

		protoMessage.Permission.Users = append(protoMessage.Permission.Users, &userPermission)
	}

	for _, dbGroupPermission := range dbmodelMessage.Permission.Groups {
		var groupPermission message.GroupPermissionAclV1
		groupPermission.GroupUuid = dbGroupPermission.GroupUuid
		groupPermission.Ace = &message.PermissionAceV1{}

		PermissionAceDbToProto(&dbGroupPermission.Ace, groupPermission.Ace)

		protoMessage.Permission.Groups = append(protoMessage.Permission.Groups, &groupPermission)
	}

	return &protoMessage
}

func PermissionAceProtoToDb(protoMessage *message.PermissionAceV1, dbmodelMessage *dbmodel.PermissionAceV1) {
	protoVal := reflect.ValueOf(protoMessage).Elem()
	dbVal := reflect.ValueOf(dbmodelMessage).Elem()

	for i := 0; i < protoVal.NumField(); i++ {
		name := protoVal.Type().Field(i).Name
		dbField := dbVal.FieldByName(name)
		if dbField.IsValid() && dbField.CanSet() {
			dbField.Set(protoVal.Field(i))
		}
	}
}

func SwcMetaInfoV1ProtobufToDbmodel(protoMessage *message.SwcMetaInfoV1) *dbmodel.SwcMetaInfoV1 {
	var dbmodelMessage dbmodel.SwcMetaInfoV1
	if protoMessage.Base != nil {
		dbmodelMessage.Base.Id, _ = primitive.ObjectIDFromHex(protoMessage.Base.XId)
		dbmodelMessage.Base.Uuid = protoMessage.Base.Uuid
		dbmodelMessage.Base.DataAccessModelVersion = protoMessage.Base.DataAccessModelVersion
	}

	dbmodelMessage.Name = protoMessage.Name
	dbmodelMessage.Description = protoMessage.Description
	dbmodelMessage.Creator = protoMessage.Creator
	dbmodelMessage.SwcType = protoMessage.SwcType
	dbmodelMessage.BelongingProjectUuid = protoMessage.BelongingProjectUuid

	if protoMessage.CreateTime != nil {
		dbmodelMessage.CreateTime = protoMessage.CreateTime.AsTime()
	}
	if protoMessage.LastModifiedTime != nil {
		dbmodelMessage.LastModifiedTime = protoMessage.LastModifiedTime.AsTime()
	}

	if protoMessage.SwcSnapshotMetaInfoList != nil {
		for _, snapshotProto := range protoMessage.SwcSnapshotMetaInfoList {
			var snapshotMetaInfo dbmodel.SwcSnapshotMetaInfoV1
			if snapshotProto.Base != nil {
				snapshotMetaInfo.Base.Id, _ = primitive.ObjectIDFromHex(snapshotProto.Base.XId)
				snapshotMetaInfo.Base.Uuid = snapshotProto.Base.Uuid
				snapshotMetaInfo.Base.DataAccessModelVersion = snapshotProto.Base.DataAccessModelVersion
			}

			snapshotMetaInfo.Creator = snapshotProto.Creator
			snapshotMetaInfo.SwcSnapshotCollectionName = snapshotProto.SwcSnapshotCollectionName
			snapshotMetaInfo.CreateTime = snapshotProto.CreateTime.AsTime()

			dbmodelMessage.SwcSnapshotList = append(dbmodelMessage.SwcSnapshotList, snapshotMetaInfo)
		}
	}

	if protoMessage.SwcIncrementOperationMetaInfoList != nil {
		for _, snapshotProto := range protoMessage.SwcIncrementOperationMetaInfoList {
			var snapshotMetaInfo dbmodel.SwcIncrementOperationMetaInfoV1
			if snapshotProto.Base != nil {
				snapshotMetaInfo.Base.Id, _ = primitive.ObjectIDFromHex(snapshotProto.Base.XId)
				snapshotMetaInfo.Base.Uuid = snapshotProto.Base.Uuid
				snapshotMetaInfo.Base.DataAccessModelVersion = snapshotProto.Base.DataAccessModelVersion
			}

			snapshotMetaInfo.StartSnapshot = snapshotProto.StartSnapshot
			snapshotMetaInfo.IncrementOperationCollectionName = snapshotProto.IncrementOperationCollectionName
			snapshotMetaInfo.CreateTime = snapshotProto.CreateTime.AsTime()

			dbmodelMessage.SwcIncrementOperationList = append(dbmodelMessage.SwcIncrementOperationList, snapshotMetaInfo)
		}
	}

	dbmodelMessage.CurrentIncrementOperationCollectionName = protoMessage.CurrentIncrementOperationCollectionName

	if protoMessage.SwcAttachmentAnoMetaInfo != nil {
		dbmodelMessage.SwcAttachmentAnoMetaInfo.AttachmentUuid = protoMessage.SwcAttachmentAnoMetaInfo.GetAttachmentUuid()
	}

	if protoMessage.SwcAttachmentApoMetaInfo != nil {
		dbmodelMessage.SwcAttachmentApoMetaInfo.AttachmentUuid = protoMessage.SwcAttachmentApoMetaInfo.GetAttachmentUuid()
	}

	dbmodelMessage.SwcAttachmentSwcUuid = protoMessage.SwcAttachmentSwcUuid

	if protoMessage.Permission != nil {
		if protoMessage.Permission.Owner != nil {
			dbmodelMessage.Permission.Owner.UserUuid = protoMessage.Permission.Owner.UserUuid
			if protoMessage.Permission.Owner.Ace != nil {
				PermissionAceProtoToDb(protoMessage.Permission.Owner.Ace, &dbmodelMessage.Permission.Owner.Ace)
			}
		}
		if protoMessage.Permission.Users != nil {
			for _, protoUserPermission := range protoMessage.Permission.Users {
				var acl dbmodel.UserPermissionAclV1
				acl.UserUuid = protoUserPermission.UserUuid
				if protoUserPermission.Ace != nil {
					PermissionAceProtoToDb(protoUserPermission.Ace, &acl.Ace)
				}
				dbmodelMessage.Permission.Users = append(dbmodelMessage.Permission.Users, acl)
			}
		}
		if protoMessage.Permission.Groups != nil {
			for _, protoGroupPermission := range protoMessage.Permission.Groups {
				var acl dbmodel.GroupPermissionAclV1
				acl.GroupUuid = protoGroupPermission.GroupUuid
				if protoGroupPermission.Ace != nil {
					PermissionAceProtoToDb(protoGroupPermission.Ace, &acl.Ace)
				}

				dbmodelMessage.Permission.Groups = append(dbmodelMessage.Permission.Groups, acl)
			}
		}
	}

	return &dbmodelMessage
}

func PermissionAceDbToProto(dbmodelMessage *dbmodel.PermissionAceV1, protoMessage *message.PermissionAceV1) {
	dbVal := reflect.ValueOf(dbmodelMessage).Elem()
	protoVal := reflect.ValueOf(protoMessage).Elem()

	for i := 0; i < dbVal.NumField(); i++ {
		name := dbVal.Type().Field(i).Name
		protoField := protoVal.FieldByName(name)
		if protoField.IsValid() && protoField.CanSet() {
			protoField.Set(dbVal.Field(i))
		}
	}
}

func SwcMetaInfoV1DbmodelToProtobuf(dbmodelMessage *dbmodel.SwcMetaInfoV1) *message.SwcMetaInfoV1 {
	var protoMessage message.SwcMetaInfoV1
	protoMessage.Base = &message.MetaInfoBase{}
	protoMessage.Base.XId = dbmodelMessage.Base.Id.Hex()
	protoMessage.Base.Uuid = dbmodelMessage.Base.Uuid
	protoMessage.Base.DataAccessModelVersion = dbmodelMessage.Base.DataAccessModelVersion

	protoMessage.Name = dbmodelMessage.Name
	protoMessage.Description = dbmodelMessage.Description
	protoMessage.Creator = dbmodelMessage.Creator
	protoMessage.SwcType = dbmodelMessage.SwcType
	protoMessage.BelongingProjectUuid = dbmodelMessage.BelongingProjectUuid

	protoMessage.CreateTime = timestamppb.New(dbmodelMessage.CreateTime)
	protoMessage.LastModifiedTime = timestamppb.New(dbmodelMessage.LastModifiedTime)

	for _, snapshotMetaInfo := range dbmodelMessage.SwcSnapshotList {
		var snapshotMetaInfoDbModel message.SwcSnapshotMetaInfoV1
		snapshotMetaInfoDbModel.Base = &message.MetaInfoBase{}
		snapshotMetaInfoDbModel.Base.XId = snapshotMetaInfo.Base.Id.Hex()
		snapshotMetaInfoDbModel.Base.Uuid = snapshotMetaInfo.Base.Uuid
		snapshotMetaInfoDbModel.Base.DataAccessModelVersion = snapshotMetaInfo.Base.DataAccessModelVersion
		snapshotMetaInfoDbModel.CreateTime = timestamppb.New(snapshotMetaInfo.CreateTime)
		snapshotMetaInfoDbModel.SwcSnapshotCollectionName = snapshotMetaInfo.SwcSnapshotCollectionName
		snapshotMetaInfoDbModel.Creator = snapshotMetaInfo.Creator
		protoMessage.SwcSnapshotMetaInfoList = append(protoMessage.SwcSnapshotMetaInfoList, &snapshotMetaInfoDbModel)
	}

	for _, incrementOperationMetaInfo := range dbmodelMessage.SwcIncrementOperationList {
		var incrementOpearationMetaInfoDbModel message.SwcIncrementOperationMetaInfoV1
		incrementOpearationMetaInfoDbModel.Base = &message.MetaInfoBase{}
		incrementOpearationMetaInfoDbModel.Base.XId = incrementOperationMetaInfo.Base.Id.Hex()
		incrementOpearationMetaInfoDbModel.Base.Uuid = incrementOperationMetaInfo.Base.Uuid
		incrementOpearationMetaInfoDbModel.Base.DataAccessModelVersion = incrementOperationMetaInfo.Base.DataAccessModelVersion
		incrementOpearationMetaInfoDbModel.CreateTime = timestamppb.New(incrementOperationMetaInfo.CreateTime)
		incrementOpearationMetaInfoDbModel.StartSnapshot = incrementOperationMetaInfo.StartSnapshot
		incrementOpearationMetaInfoDbModel.IncrementOperationCollectionName = incrementOperationMetaInfo.IncrementOperationCollectionName
		protoMessage.SwcIncrementOperationMetaInfoList = append(protoMessage.SwcIncrementOperationMetaInfoList, &incrementOpearationMetaInfoDbModel)
	}

	protoMessage.CurrentIncrementOperationCollectionName = dbmodelMessage.CurrentIncrementOperationCollectionName

	protoMessage.SwcAttachmentAnoMetaInfo = &message.SwcAttachmentAnoMetaInfoV1{}
	protoMessage.SwcAttachmentAnoMetaInfo.AttachmentUuid = dbmodelMessage.SwcAttachmentAnoMetaInfo.AttachmentUuid

	protoMessage.SwcAttachmentApoMetaInfo = &message.SwcAttachmentApoMetaInfoV1{}
	protoMessage.SwcAttachmentApoMetaInfo.AttachmentUuid = dbmodelMessage.SwcAttachmentApoMetaInfo.AttachmentUuid

	protoMessage.SwcAttachmentSwcUuid = dbmodelMessage.SwcAttachmentSwcUuid

	protoMessage.Permission = &message.PermissionMetaInfoV1{}
	protoMessage.Permission.Owner = &message.UserPermissionAclV1{}
	protoMessage.Permission.Owner.UserUuid = dbmodelMessage.Permission.Owner.UserUuid
	protoMessage.Permission.Owner.Ace = &message.PermissionAceV1{}

	PermissionAceDbToProto(&dbmodelMessage.Permission.Owner.Ace, protoMessage.Permission.Owner.Ace)

	for _, dbUserPermission := range dbmodelMessage.Permission.Users {
		var userPermission message.UserPermissionAclV1
		userPermission.UserUuid = dbUserPermission.UserUuid
		userPermission.Ace = &message.PermissionAceV1{}

		PermissionAceDbToProto(&dbUserPermission.Ace, userPermission.Ace)

		protoMessage.Permission.Users = append(protoMessage.Permission.Users, &userPermission)
	}

	for _, dbGroupPermission := range dbmodelMessage.Permission.Groups {
		var groupPermission message.GroupPermissionAclV1
		groupPermission.GroupUuid = dbGroupPermission.GroupUuid
		groupPermission.Ace = &message.PermissionAceV1{}

		PermissionAceDbToProto(&dbGroupPermission.Ace, groupPermission.Ace)

		protoMessage.Permission.Groups = append(protoMessage.Permission.Groups, &groupPermission)
	}

	return &protoMessage
}

func SwcNodeDataV1ProtobufToDbmodel(protoMessage *message.SwcNodeDataV1) *dbmodel.SwcNodeDataV1 {
	var dbmodelMessage dbmodel.SwcNodeDataV1
	if protoMessage.Base != nil {
		dbmodelMessage.Base.Id, _ = primitive.ObjectIDFromHex(protoMessage.Base.XId)
		dbmodelMessage.Base.Uuid = protoMessage.Base.Uuid
		dbmodelMessage.Base.DataAccessModelVersion = protoMessage.Base.DataAccessModelVersion
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
	protoMessage.Base.DataAccessModelVersion = dbmodelMessage.Base.DataAccessModelVersion

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
		dbmodelMessage.Base.DataAccessModelVersion = protoMessage.Base.DataAccessModelVersion
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
	protoMessage.Base.DataAccessModelVersion = dbmodelMessage.Base.DataAccessModelVersion

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

func SwcSnapshotMetaInfoV1MetaInfoV1ProtobufToDbmodel(protoMessage *message.SwcSnapshotMetaInfoV1) *dbmodel.SwcSnapshotMetaInfoV1 {
	var dbmodelMessage dbmodel.SwcSnapshotMetaInfoV1
	if protoMessage.Base != nil {
		dbmodelMessage.Base.Id, _ = primitive.ObjectIDFromHex(protoMessage.Base.XId)
		dbmodelMessage.Base.Uuid = protoMessage.Base.Uuid
		dbmodelMessage.Base.DataAccessModelVersion = protoMessage.Base.DataAccessModelVersion
	}

	dbmodelMessage.SwcSnapshotCollectionName = protoMessage.SwcSnapshotCollectionName
	dbmodelMessage.Creator = protoMessage.Creator

	if protoMessage.CreateTime != nil {
		dbmodelMessage.CreateTime = protoMessage.CreateTime.AsTime()
	}

	return &dbmodelMessage
}

func SwcSnapshotMetaInfoV1MetaInfoV1DbmodelToProtobuf(dbmodelMessage *dbmodel.SwcSnapshotMetaInfoV1) *message.SwcSnapshotMetaInfoV1 {
	var protoMessage message.SwcSnapshotMetaInfoV1
	protoMessage.Base = &message.MetaInfoBase{}
	protoMessage.Base.XId = dbmodelMessage.Base.Id.Hex()
	protoMessage.Base.Uuid = dbmodelMessage.Base.Uuid
	protoMessage.Base.DataAccessModelVersion = dbmodelMessage.Base.DataAccessModelVersion

	protoMessage.CreateTime = timestamppb.New(dbmodelMessage.CreateTime)
	protoMessage.SwcSnapshotCollectionName = dbmodelMessage.SwcSnapshotCollectionName
	protoMessage.Creator = dbmodelMessage.Creator

	return &protoMessage
}

func SwcIncrementOperationMetaInfoV1MetaInfoV1ProtobufToDbmodel(protoMessage *message.SwcIncrementOperationMetaInfoV1) *dbmodel.SwcIncrementOperationMetaInfoV1 {
	var dbmodelMessage dbmodel.SwcIncrementOperationMetaInfoV1
	if protoMessage.Base != nil {
		dbmodelMessage.Base.Id, _ = primitive.ObjectIDFromHex(protoMessage.Base.XId)
		dbmodelMessage.Base.Uuid = protoMessage.Base.Uuid
		dbmodelMessage.Base.DataAccessModelVersion = protoMessage.Base.DataAccessModelVersion
	}

	dbmodelMessage.StartSnapshot = protoMessage.StartSnapshot
	dbmodelMessage.IncrementOperationCollectionName = protoMessage.IncrementOperationCollectionName

	if protoMessage.CreateTime != nil {
		dbmodelMessage.CreateTime = protoMessage.CreateTime.AsTime()
	}

	return &dbmodelMessage
}

func SwcIncrementOperationMetaInfoV1MetaInfoV1DbmodelToProtobuf(dbmodelMessage *dbmodel.SwcIncrementOperationMetaInfoV1) *message.SwcIncrementOperationMetaInfoV1 {
	var protoMessage message.SwcIncrementOperationMetaInfoV1
	protoMessage.Base = &message.MetaInfoBase{}
	protoMessage.Base.XId = dbmodelMessage.Base.Id.Hex()
	protoMessage.Base.Uuid = dbmodelMessage.Base.Uuid
	protoMessage.Base.DataAccessModelVersion = dbmodelMessage.Base.DataAccessModelVersion

	protoMessage.CreateTime = timestamppb.New(dbmodelMessage.CreateTime)
	protoMessage.StartSnapshot = dbmodelMessage.StartSnapshot
	protoMessage.IncrementOperationCollectionName = dbmodelMessage.IncrementOperationCollectionName

	return &protoMessage
}

func SwcIncrementOperationV1MetaInfoV1ProtobufToDbmodel(protoMessage *message.SwcIncrementOperationV1) *dbmodel.SwcIncrementOperationV1 {
	var dbmodelMessage dbmodel.SwcIncrementOperationV1
	if protoMessage.Base != nil {
		dbmodelMessage.Base.Id, _ = primitive.ObjectIDFromHex(protoMessage.Base.XId)
		dbmodelMessage.Base.Uuid = protoMessage.Base.Uuid
		dbmodelMessage.Base.DataAccessModelVersion = protoMessage.Base.DataAccessModelVersion
	}

	if protoMessage.CreateTime != nil {
		dbmodelMessage.CreateTime = protoMessage.CreateTime.AsTime()
	}

	dbmodelMessage.IncrementOperation = protoMessage.IncrementOperation.String()

	var dbSwcData dbmodel.SwcDataV1
	var dbNParent []dbmodel.NodeNParentV1

	if protoMessage.GetSwcData() != nil {
		for _, swcNodeData := range protoMessage.GetSwcData().GetSwcData() {
			dbSwcData = append(dbSwcData, *SwcNodeDataV1ProtobufToDbmodel(swcNodeData))
		}
	}

	if protoMessage.GetNodeNParent() != nil {
		for _, nodeNParent := range protoMessage.GetNodeNParent() {
			var dbNParentItem dbmodel.NodeNParentV1
			dbNParentItem.Uuid = nodeNParent.NodeUuid
			dbNParentItem.N = nodeNParent.N
			dbNParentItem.Parent = nodeNParent.Parent
			dbNParent = append(dbNParent, dbNParentItem)
		}
	}

	dbmodelMessage.SwcData = dbSwcData
	dbmodelMessage.NodeNParentV1 = dbNParent

	return &dbmodelMessage
}

func SwcIncrementOperationListV1DbmodelToProtobuf(dbmodelMessage *dbmodel.SwcIncrementOperationV1) *message.SwcIncrementOperationV1 {
	var protoMessage message.SwcIncrementOperationV1
	protoMessage.Base = &message.MetaInfoBase{}
	protoMessage.Base.XId = dbmodelMessage.Base.Id.Hex()
	protoMessage.Base.Uuid = dbmodelMessage.Base.Uuid
	protoMessage.Base.DataAccessModelVersion = dbmodelMessage.Base.DataAccessModelVersion
	protoMessage.CreateTime = timestamppb.New(dbmodelMessage.CreateTime)
	protoMessage.IncrementOperation = message.IncrementOperationV1(message.IncrementOperationV1_value[dbmodelMessage.IncrementOperation])

	if dbmodelMessage.SwcData != nil {
		var pbSwcData message.SwcDataV1
		for _, swcNodeData := range dbmodelMessage.SwcData {
			pbSwcData.SwcData = append(pbSwcData.SwcData, SwcNodeDataV1DbmodelToProtobuf(&swcNodeData))
		}
		protoMessage.SwcData = &pbSwcData
	}

	if dbmodelMessage.NodeNParentV1 != nil {
		var pbNodeNParentData []*message.NodeNParentV1
		for _, nodeNParent := range dbmodelMessage.NodeNParentV1 {
			var msgNodeNParent message.NodeNParentV1
			msgNodeNParent.NodeUuid = nodeNParent.Uuid
			msgNodeNParent.N = nodeNParent.N
			msgNodeNParent.Parent = nodeNParent.Parent

			pbNodeNParentData = append(pbNodeNParentData, &msgNodeNParent)
		}
		protoMessage.NodeNParent = pbNodeNParentData
	}

	return &protoMessage
}
