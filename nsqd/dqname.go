// +build !windows

package nsqd

// 根据topic和channel 的名称生成后台队列的名字
func getBackendName(topicName, channelName string) string {
	// backend names, for uniqueness, automatically include the topic... <topic>:<channel>
	backendName := topicName + ":" + channelName
	return backendName
}
