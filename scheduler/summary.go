package scheduler

import (
	"sort"
	"starix-crawler/module"
	"starix-crawler/tookit/buffer"
)

// SchedSummary 代表调度器摘要的接口类型
type SchedSummary interface {
	// Struct 用于获得摘要信息的结构化形式
	Struct() SummaryStruct
	// String 用于获得摘要信息的字符串形式
	String() string
}

// mySchedSummary 代表调度器摘要的实现类型。
type mySchedSummary struct {
	// requestArgs 代表请求相关的参数。
	requestArgs RequestArgs
	// dataArgs 代表数据相关参数的容器实例。
	dataArgs DataArgs
	// moduleArgs 代表组件相关参数的容器实例。
	moduleArgs ModuleArgs
	// maxDepth 代表爬取的最大深度。
	maxDepth uint32
	// sched 代表调度器实例。
	sched *myScheduler
}

// newSchedSummary 用于创建一个调度器摘要实例。
func newSchedSummary(
	requestArgs RequestArgs,
	dataArgs DataArgs,
	moduleArgs ModuleArgs,
	sched *myScheduler) SchedSummary {
	if sched == nil {
		return nil
	}
	return &mySchedSummary{
		requestArgs: requestArgs,
		dataArgs:    dataArgs,
		moduleArgs:  moduleArgs,
		sched:       sched,
	}
}

func (ss *mySchedSummary) Struct() SummaryStruct {
	panic("implement me")
}

func (ss *mySchedSummary) String() string {
	panic("implement me")
}

// SummaryStruct 代表调度器摘要的结构。
type SummaryStruct struct {
	RequestArgs     RequestArgs             `json:"request_args"`
	DataArgs        DataArgs                `json:"data_args"`
	ModuleArgs      ModuleArgsSummary       `json:"module_args"`
	Status          string                  `json:"status"`
	Downloaders     []module.SummaryStruct  `json:"downloaders"`
	Analyzers       []module.SummaryStruct  `json:"analyzers"`
	Pipelines       []module.SummaryStruct  `json:"pipelines"`
	ReqBufferPool   BufferPoolSummaryStruct `json:"request_buffer_pool"`
	RespBufferPool  BufferPoolSummaryStruct `json:"response_buffer_pool"`
	ItemBufferPool  BufferPoolSummaryStruct `json:"item_buffer_pool"`
	ErrorBufferPool BufferPoolSummaryStruct `json:"error_buffer_pool"`
	NumURL          uint64                  `json:"url_number"`
}

// BufferPoolSummaryStruct 代表缓冲池的摘要类型。
type BufferPoolSummaryStruct struct {
	BufferCap       uint32 `json:"buffer_cap"`
	MaxBufferNumber uint32 `json:"max_buffer_number"`
	BufferNumber    uint32 `json:"buffer_number"`
	Total           uint64 `json:"total"`
}

// getBufferPoolSummary 用于生成和返回某个数据缓冲池的摘要信息。
func getBufferPoolSummary(bufferPool buffer.Pool) BufferPoolSummaryStruct {
	return BufferPoolSummaryStruct{
		BufferCap:       bufferPool.BufferCap(),
		MaxBufferNumber: bufferPool.MaxBufferNumber(),
		BufferNumber:    bufferPool.BufferNumber(),
		Total:           bufferPool.Total(),
	}
}

// getModuleSummaries 用于获取已注册的某类组件的摘要。
func getModuleSummaries(registrar module.Registrar, mType module.Type) []module.SummaryStruct {
	moduleMap, _ := registrar.GetAllByType(mType)
	summaries := []module.SummaryStruct{}
	if len(moduleMap) > 0 {
		for _, module := range moduleMap {
			summaries = append(summaries, module.Summary())
		}
	}
	if len(summaries) > 1 {
		sort.Slice(summaries,
			func(i, j int) bool {
				return summaries[i].ID < summaries[j].ID
			})
	}
	return summaries
}
