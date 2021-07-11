package stub

import "starix-crawler/module"

// myModule 代表组件内部基础接口的实现类型。
type myModule struct {
	// mid 代表组件ID。
	mid module.MID
	// addr 代表组件的网络地址。
	addr string
	// score 代表组件评分。
	score uint64
	// scoreCalculator 代表评分计算器。
	scoreCalculator module.CalculateScore
	// calledCount 代表调用计数。
	calledCount uint64
	// acceptedCount 代表接受计数。
	acceptedCount uint64
	// completedCount 代表成功完成计数。
	completedCount uint64
	// handlingNumber 代表实时处理数。
	handlingNumber uint64
}

// NewModuleInternal 用于创建一个组件内部基础类型的实例
func NewModuleInternal(mid module.MID, scoreCalculator module.CalculateScore) (ModuleInternal, error) {

}
