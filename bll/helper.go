package bll

func UserTokenVerify(userName string, userToken string) (bool, OnlineUserInfo) {
	bFind := false
	var cachedOnlineUserInfo OnlineUserInfo
	for _, onlineUserInfo := range OnlineUserInfoCache {
		if onlineUserInfo.Token == userToken && onlineUserInfo.UserInfo.Name == userName {
			cachedOnlineUserInfo = onlineUserInfo
			bFind = true
		}
	}
	return bFind, cachedOnlineUserInfo
}
