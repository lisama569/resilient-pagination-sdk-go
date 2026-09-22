package sdk

// Version 表示协议版本（主版本.次版本）。
type Version struct {
	Major int
	Minor int
}

// ParseVersion 解析 "major.minor" 形式的版本串。
func ParseVersion(s string) (Version, error) { return Version{}, nil }

func (v Version) String() string { return "" }

// CompatibleWith 判断本端（较旧）能否消费对端（可能较新）的响应：
// 主版本相同即兼容，新增次版本字段由旧端保留而非报错。
func (v Version) CompatibleWith(peer Version) bool { return false }

// Negotiate 在受支持版本列表与对端提供版本之间协商。
func Negotiate(supported []Version, offered Version) (Version, error) {
	return Version{}, nil
}
