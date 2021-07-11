package module

type Type string

// 当前认可的组件类型的常量
const (
	//下载器
	TYPE_DOWNLOADER Type = "downloader"
	//分析器
	TYPE_ANALYZER Type = "analyzer"
	//棗目处理管道
	TYPE_PIPELINE Type = "pipeline"
)

// legalTypeLetterMap 代表合法的组件类型-字母的映射
var legalTypeLetterMap = map[Type]string{
	TYPE_DOWNLOADER: "D",
	TYPE_ANALYZER:   "A",
	TYPE_PIPELINE:   "P",
}

// legalLetterTypeMap 代表合法的字母-组件类型的映射
var legalLetterTypeMap = map[string]Type{
	"D": TYPE_DOWNLOADER,
	"A": TYPE_ANALYZER,
	"P": TYPE_PIPELINE,
}

// LegalType 用于判断给定的组件类型是否合法。
func LegalType(moduleType Type) bool {
	if _, ok := legalTypeLetterMap[moduleType]; ok {
		return true
	}
	return false
}
