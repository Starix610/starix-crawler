package module

type Type string

// 当前认可的组件类型的常量
const (
	//下载器
	TYPE_DOWNLOADER Type = "downloader"
	//分析器
	TYPE_ANALYZER Type = "analyzer"
	//条目处理管道
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

func CheckType(moduleType Type, module Module) bool {
	if moduleType == "" || module == nil {
		return false
	}
	switch module.(type) {
	case Downloader:
		return moduleType == TYPE_DOWNLOADER
	case Analyzer:
		return moduleType == TYPE_ANALYZER
	case Pipeline:
		return moduleType == TYPE_PIPELINE
	}
	return false
}

// GetType 用于获取组件的类型。
// 若给定的组件ID不合法则第一个结果值会是false。
func GetType(mid MID) (bool, Type) {
	parts, err := SplitMID(mid)
	if err != nil {
		return false, ""
	}
	mt, ok := legalLetterTypeMap[parts[0]]
	return ok, mt
}
