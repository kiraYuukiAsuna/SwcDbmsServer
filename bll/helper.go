package bll

func UserTokenVerify(token string) (bool, OnlineUserInfo) {
	bFind := false
	var cachedOnlineUserInfo OnlineUserInfo
	for _, onlineUserInfo := range OnlineUserInfoCache {
		if onlineUserInfo.Token == token {
			cachedOnlineUserInfo = onlineUserInfo
			bFind = true
		}
	}
	return bFind, cachedOnlineUserInfo
}
