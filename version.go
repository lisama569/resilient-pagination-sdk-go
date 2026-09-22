package sdk

import (
	"fmt"
	"strconv"
	"strings"
)

// Version 表示协议版本（主版本.次版本）。
// 兼容规则：主版本相同即兼容；次版本更高的一方可能携带新增字段，
// 旧端必须保留这些字段（见 codec.go 的 Extra），不得报错或丢弃。
type Version struct {
	Major int
	Minor int
}

// ParseVersion 解析 "major.minor" 形式的版本串。
// 格式非法返回 ErrKindMalformed，不透出底层 strconv 异常。
func ParseVersion(s string) (Version, error) {
	parts := strings.Split(s, ".")
	if len(parts) != 2 {
		return Version{}, wrap(ErrKindMalformed, "ParseVersion",
			fmt.Sprintf("版本格式应为 major.minor，实际为 %q", s), nil)
	}
	major, err := strconv.Atoi(parts[0])
	if err != nil || major < 0 {
		return Version{}, wrap(ErrKindMalformed, "ParseVersion",
			fmt.Sprintf("主版本非法: %q", s), err)
	}
	minor, err := strconv.Atoi(parts[1])
	if err != nil || minor < 0 {
		return Version{}, wrap(ErrKindMalformed, "ParseVersion",
			fmt.Sprintf("次版本非法: %q", s), err)
	}
	return Version{Major: major, Minor: minor}, nil
}

func (v Version) String() string {
	return fmt.Sprintf("%d.%d", v.Major, v.Minor)
}

// CompatibleWith 判断本端能否消费对端的响应：主版本相同即兼容。
func (v Version) CompatibleWith(peer Version) bool {
	return v.Major == peer.Major
}

// Negotiate 在受支持版本列表与对端提供版本之间协商。
// 选取主版本一致且次版本不超过对端的最高版本；
// 主版本不受支持返回 ErrKindVersionUnknown，
// 主版本匹配但次版本无法满足返回 ErrKindVersionMismatch。
func Negotiate(supported []Version, offered Version) (Version, error) {
	best := Version{Major: -1, Minor: -1}
	sawMajor := false
	for _, v := range supported {
		if v.Major != offered.Major {
			continue
		}
		sawMajor = true
		if v.Minor <= offered.Minor && v.Minor > best.Minor {
			best = v
		}
	}
	switch {
	case !sawMajor:
		return Version{}, wrap(ErrKindVersionUnknown, "Negotiate",
			fmt.Sprintf("不支持的协议主版本 %d", offered.Major), nil)
	case best.Major < 0:
		return Version{}, wrap(ErrKindVersionMismatch, "Negotiate",
			fmt.Sprintf("主版本 %d 下无兼容的次版本（对端 %s）", offered.Major, offered), nil)
	default:
		return best, nil
	}
}
