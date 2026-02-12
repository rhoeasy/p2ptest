package helper

// SplitInput：拆分控制台输入（仅做这一件事）
func SplitInput(input string) []string {
	var parts []string
	var current string
	for _, c := range input {
		if c == ' ' && current != "" {
			parts = append(parts, current)
			current = ""
		} else if c != ' ' {
			current += string(c)
		}
	}
	if current != "" {
		parts = append(parts, current)
	}
	return parts
}

// JoinParts：拼接消息内容（仅做这一件事）
func JoinParts(parts []string) string {
	res := ""
	for i, p := range parts {
		if i > 0 {
			res += " "
		}
		res += p
	}
	return res
}
