package dal

import "go.mongodb.org/mongo-driver/mongo"

type ReturnWrapper struct {
	Status  bool
	Message string
}

type MongoDbConnectionCreateInfo struct {
	Host     string
	Port     int32
	User     string
	Password string
}

type MongoDbConnectionInfo struct {
	Client *mongo.Client
	Err    error
}

type MongoDbDataBaseInfo struct {
	SwcDb                *mongo.Database
	MetaInfoDb           *mongo.Database
	SnapshotDb           *mongo.Database
	IncrementOperationDb *mongo.Database
	AttachmentDb         *mongo.Database
}

type DataBaseNameInfo struct {
	MetaInfoDataBaseName              string
	SwcDataBaseName                   string
	SwcSnapshotDataBaseName           string
	SwcIncrementOperationDataBaseName string
	SwcAttachmentDataBaseName         string
}

const (
	DefaultMetaInfoDataBaseName string = "MetaInfoDataBase"
	DevMetaInfoDataBaseName     string = "DevMetaInfoDataBase"

	DefaultSwcDataBaseName string = "SwcDataBase"
	DevSwcDataBaseName     string = "DevSwcDataBase"

	DefaultSwcSnapshotDataBaseName string = "DefaultSwcSnapshotDataBase"
	DevSwcSnapshotDataBaseName     string = "DevSwcSnapshotDataBase"

	DefaultSwcIncrementOperationDataBaseName string = "DefaultSwcIncrementOperationDataBase"
	DevSwcIncrementOperationDataBaseName     string = "DevSwcIncrementOperationDataBase"

	DefaultSwcAttachmentDataBaseName string = "DefaultSwcAttachmentDataBaseName"
	DevSwcAttachmentDataBaseName     string = "DevSwcAttachmentDataBaseName"
)

const (
	MetaInfoDbStatusCollectonString         string = "MetaInfoDbStatusCollecton"
	ProjectMetaInfoCollectionString         string = "ProjectMetaInfoCollection"
	UserMetaInfoCollectionString            string = "UserMetaInfoCollection"
	PermissionGroupMetaInfoCollectioString  string = "PermissionGroupMetaInfoCollection"
	SwcMetaInfoCollectionString             string = "SwcMetaInfoCollection"
	DailyStatisticsMetaInfoCollectionString string = "DailyStatisticsMetaInfoCollection"
)

const (
	DefaultAdminSystemUserName     string = "DefaultAdminSystemUserName"
	DefaultAdminSystemUserPassword string = "DefaultAdminSystemUserPassword"
)

const (
	PermissionGroupAdmin       string = "Admin"
	PermissionGroupDefault     string = "Default"
	PermissionGroupGroupLeader string = "GroupLeader"
	PermissionGroupNormalUser  string = "NormalUser"
	PermissionGroupGuest       string = "Guest"
)

const (
	IncrementOp_Unknown       string = "Unknown"
	IncrementOp_Create        string = "Create"
	IncrementOp_Delete        string = "Delete"
	IncrementOp_Update        string = "Update"
	IncrementOp_UpdateNParent string = "UpdateNParent"
	IncrementOp_ClearAll      string = "ClearAll"
)
