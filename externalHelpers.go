package dbtx

type StringLike interface {
	~string
}

func ToArguments[T StringLike](m map[T]any) map[string]any {
	r := make(map[string]any, len(m))
	for k, v := range m {
		r[string(k)] = v
	}
	return r
}
