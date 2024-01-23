package dbmodel

import (
	"go.mongodb.org/mongo-driver/bson/primitive"
	"time"
)

const (
	ProjectMetaInfoCollectionString         string = "ProjectMetaInfoCollectionString"
	UserMetaInfoCollectionString            string = "UserMetaInfoCollection"
	PermissionGroupMetaInfoCollectioString  string = "PermissionGroupMetaInfoCollection"
	SwcMetaInfoCollectionString             string = "SwcMetaInfoCollection"
	DailyStatisticsMetaInfoCollectionString string = "DailyStatisticsMetaInfoCollection"
)

const (
	IncrementOp_Unknown string = "Unknown"
	IncrementOp_Create  string = "Create"
	IncrementOp_Delete  string = "Delete"
	IncrementOp_Update  string = "Update"
)

type MetaInfoBase struct {
	Id                     primitive.ObjectID `bson:"_id"`
	DataAccessModelVersion string             `bson:"DataAccessModelVersion"`
	Uuid                   string             `bson:"uuid"`
}

type UserMetaInfoV1 struct {
	Base                MetaInfoBase `bson:"Base,inline"`
	Name                string       `bson:"Name"`
	Password            string       `bson:"Password"`
	Description         string       `bson:"Description"`
	CreateTime          time.Time    `bson:"CreateTime"`
	HeadPhotoBinData    []byte       `bson:"HeadPhotoBinData"`
	UserPermissionGroup string       `bson:"UserPermissionGroup"`
}

type GlobalPermissionMetaInfoV1 struct {
	WritePermissionCreateProject bool `bson:"WritePermissionCreateProject"`
	WritePermissionModifyProject bool `bson:"WritePermissionModifyProject"`
	WritePermissionDeleteProject bool `bson:"WritePermissionDeleteProject"`

	ReadPerimissionQuery bool `bson:"ReadPerimissionQuery"`
}

type ProjectPermissionMetaInfoV1 struct {
	WritePermissionAddData    bool `bson:"WritePermissionAddData"`
	WritePermissionModifyData bool `bson:"WritePermissionModifyData"`
	WritePermissionDeleteData bool `bson:"WritePermissionDeleteData"`

	ReadPerimissionQuery bool `bson:"ReadPerimissionQuery"`
}

type PermissionGroupMetaInfoV1 struct {
	Base MetaInfoBase `bson:"Base,inline"`

	Name        string `bson:"Name"`
	Description string `bson:"Description"`

	Global  GlobalPermissionMetaInfoV1  `bson:"Global"`
	Project ProjectPermissionMetaInfoV1 `bson:"Project"`
}

type UserPermissionOverrideMetaInfoV1 struct {
	Project  ProjectPermissionMetaInfoV1 `bson:"Project"`
	UserName string                      `bson:"UserName"`
}

type ProjectMetaInfoV1 struct {
	Base MetaInfoBase `bson:"Base,inline"`

	Name                   string                             `bson:"Name"`
	Description            string                             `bson:"Description"`
	Creator                string                             `bson:"Creator"`
	CreateTime             time.Time                          `bson:"CreateTime"`
	LastModifiedTime       time.Time                          `bson:"LastModifiedTime"`
	SwcList                []string                           `bson:"SwcList"`
	UserPermissionOverride []UserPermissionOverrideMetaInfoV1 `bson:"UserPermissionOverride"`
	WorkMode               string                             `bson:"WorkMode"`
}

type SwcSnapshotMetaInfoV1 struct {
	Base                      MetaInfoBase `bson:"Base,inline"`
	SwcSnapshotCollectionName string       `bson:"SwcSnapshotCollectionName"`
	CreateTime                time.Time    `bson:"CreateTime"`
	Creator                   string       `bson:"Creator"`
}

type SwcIncrementOperationMetaInfoV1 struct {
	Base                             MetaInfoBase `bson:"Base,inline"`
	StartSnapshot                    string       `bson:"StartSnapshot"`
	CreateTime                       time.Time    `bson:"CreateTime"`
	IncrementOperationCollectionName string       `bson:"IncrementOperationCollectionName"`
}

type SwcIncrementOperationV1 struct {
	Base               MetaInfoBase `bson:"Base,inline"`
	CreateTime         time.Time    `bson:"CreateTime"`
	IncrementOperation string       `bson:"IncrementOperation"`
	SwcData            SwcDataV1    `bson:"SwcNodeData"`
}

type SwcIncrementOperationListV1 = []SwcIncrementOperationV1

type SwcMetaInfoV1 struct {
	Base                                    MetaInfoBase                      `bson:"Base,inline"`
	Name                                    string                            `bson:"Name"`
	Description                             string                            `bson:"Description"`
	Creator                                 string                            `bson:"Creator"`
	SwcType                                 string                            `bson:"SwcType"`
	CreateTime                              time.Time                         `bson:"CreateTime"`
	LastModifiedTime                        time.Time                         `bson:"LastModifiedTime"`
	SwcSnapshotList                         []SwcSnapshotMetaInfoV1           `bson:"SwcSnapshotList"`
	SwcIncrementOperationList               []SwcIncrementOperationMetaInfoV1 `bson:"SwcIncrementOperationList"`
	CurrentIncrementOperationCollectionName string                            `bson:"CurrentIncrementOperationCollectionName"`
	SwcAttachmentAno                        SwcAttachmentAnoMetaInfoV1        `bson:"SwcAttachmentAno"`
	SwcAttachmentApo                        SwcAttachmentApoMetaInfoV1        `bson:"SwcAttachmentApo"`
}

type SwcNodeInternalDataV1 struct {
	N             int32   `bson:"n"`
	Type          int32   `bson:"type"`
	X             float32 `bson:"x"`
	Y             float32 `bson:"y"`
	Z             float32 `bson:"z"`
	Radius        float32 `bson:"radius"`
	Parent        int32   `bson:"parent"`
	Seg_id        int32   `bson:"seg_id"`
	Level         int32   `bson:"level"`
	Mode          int32   `bson:"mode"`
	Timestamp     int32   `bson:"timestamp"`
	Feature_value int32   `bson:"feature_value"`
}

type SwcNodeDataV1 struct {
	Base                MetaInfoBase          `bson:"Base,inline"`
	SwcNodeInternalData SwcNodeInternalDataV1 `bson:"SwcData"`
	Creator             string                `bson:"Creator"`
	CreateTime          time.Time             `bson:"CreateTime"`
	LastModifiedTime    time.Time             `bson:"LastModifiedTime"`
	CheckerUserUuid     string                `bson:"CheckerUserUuid"`
}

type SwcDataV1 = []SwcNodeDataV1

type DailyStatisticsMetaInfoV1 struct {
	Base        MetaInfoBase `bson:"Base,inline"`
	Name        string       `bson:"Name"`
	Description string       `bson:"Description"`
	Day         string       `bson:"Day"`

	CreatedProjectNumber int32 `bson:"CreatedProjectNumber"`
	CreatedSwcNumber     int32 `bson:"CreatedSwcNumber"`
	CreateSwcNodeNumber  int32 `bson:"CreateSwcNodeNumber"`

	DeletedProjectNumber int32 `bson:"DeletedProjectNumber"`
	DeletedSwcNumber     int32 `bson:"DeletedSwcNumber"`
	DeletedSwcNodeNumber int32 `bson:"DeletedSwcNodeNumber"`

	ModifiedProjectNumber int32 `bson:"ModifiedProjectNumber"`
	ModifiedSwcNumber     int32 `bson:"ModifiedSwcNumber"`
	ModifiedSwcNodeNumber int32 `bson:"ModifiedSwcNodeNumber"`

	ProjectQueryNumber int32 `bson:"ProjectQueryNumber"`
	SwcQueryNumber     int32 `bson:"SwcQueryNumber"`
	NodeQueryNumber    int32 `bson:"NodeQueryNumber"`

	ActiveUserNumber int32 `bson:"ActiveUserNumber"`
}

type SwcAttachmentAnoMetaInfoV1 struct {
	AttachmentUuid string `bson:"AttachmentUuid"`
}

type SwcAttachmentApoMetaInfoV1 struct {
	AttachmentUuid string `bson:"AttachmentUuid"`
}

type SwcAttachmentAnoV1 struct {
	Base    MetaInfoBase `bson:"Base,inline"`
	APOFILE string       `bson:"APOFILE"`
	SWCFILE string       `bson:"SWCFILE"`
}

type SwcAttachmentApoV1 struct {
	Base      MetaInfoBase `bson:"Base,inline"`
	N         int32        `bson:"n"`
	Orderinfo string       `bson:"orderinfo"`
	Name      string       `bson:"name"`
	Comment   string       `bson:"comment"`
	Z         float32      `bson:"z"`
	X         float32      `bson:"x"`
	Y         float32      `bson:"y"`
	Pixmax    float32      `bson:"pixmax"`
	Intensity float32      `bson:"intensity"`
	Sdev      float32      `bson:"sdev"`
	Volsize   float32      `bson:"volsize"`
	Mass      float32      `bson:"mass"`
	ColorR    int32        `bson:"colorR"`
	ColorG    int32        `bson:"colorG"`
	ColorB    int32        `bson:"colorB"`
}
