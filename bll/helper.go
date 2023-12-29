package bll

func UserTokenVerify(userName string, userToken string) (bool, OnlineUserInfo) {
	bFind := false
	var cachedOnlineUserInfo OnlineUserInfo

	if _, ok := OnlineUserInfoCache[userName]; ok {
		cachedOnlineUserInfo = OnlineUserInfoCache[userName]
		if cachedOnlineUserInfo.Token == userToken {
			bFind = true
		}
	}

	return bFind, cachedOnlineUserInfo
}
