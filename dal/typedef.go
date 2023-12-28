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
)

const (
	PermissionGroupAdmin       string = "Admin"
	PermissionGroupDefault     string = "Default"
	PermissionGroupGroupLeader string = "GroupLeader"
	PermissionGroupNormalUser  string = "NormalUser"
	PermissionGroupGuest       string = "Guest"
)

const (
	DefaultAdminSystemUserName     string = "DefaultAdminSystemUserName"
	DefaultAdminSystemUserPassword string = "DefaultAdminSystemUserPassword"
)
