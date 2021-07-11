package stub

import "starix-crawler/module"

// 组件的内部基础接口的类型，通常情况下不提供给外部使用，主要提供给自行编写组件的人使用
type ModuleInternal interface {
	module.Module
	//把调用计数增 1
	IncrCalledCount()
	//把接受计数增1
	IncrAcceptedCount()
	//把成功完成计数增 1
	IncrCompletedCount()
	//把实时处理数增 1
	IncrHandlingNumber()
	//把实时处理数减 1
	DecrHandlingNumber()
	//用于清空所有计数
	Clear()
}
