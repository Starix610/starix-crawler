package scheduler

import (
	"context"
	"errors"
	"fmt"
	"helper/log"
	"net/http"
	"starix-crawler/module"
	"starix-crawler/tookit/buffer"
	"starix-crawler/tookit/cmap"
	"sync"
)

// logger 代表日志记录器。
var logger = log.DLogger()

//调度器的接口类型
type Scheduler interface {
	//Init用于初始化调度器。
	//参数requestArgs代表请求相关的参数
	//参数dataArgs代表数据相关的参数
	//参数moduleArgs代表组件相关的参数
	Init(requestArgs RequestArgs, dataArgs DataArgs, moduleArgs ModuleArgs) (err error)
	//Start用于启动调度器并执行爬取流程
	//参数firstHTTPReq代表首次请求，调度器会以此为起始点开始执行爬取流程
	Start(firstHTTPReq *http.Request) (err error)
	//Stop用于停止调度器的运行
	//所有处理模块执行的流程都会被中止
	Stop() (err error)
	//用于获取调度器的状态
	Status() Status
	//ErrorChan用于获得错误通道
	//调度器以及各个处理模块运行过程中出现的所有错误都会被发送到该通道
	//若结果值为nil，则说明错误通道不可用或调度器已停止
	ErrorChan() <-chan error
	//用于判断所有处理模块是否都处于空闲状态
	Idle() bool
	//用于获取摘要实例
	Summary() SchedSummary
}

// myScheduler 代表调度器的实现类型。
type myScheduler struct {
	// maxDepth 代表爬取的最大深度。首次请求的深度为0。
	maxDepth uint32
	// acceptedDomainMap 代表可以接受的URL的主域名的字典。
	acceptedDomainMap cmap.ConcurrentMap
	// registrar 代表组件注册器。
	registrar module.Registrar
	// reqBufferPool 代表请求的缓冲池。
	reqBufferPool buffer.Pool
	// respBufferPool 代表响应的缓冲池。
	respBufferPool buffer.Pool
	// itemBufferPool 代表条目的缓冲池。
	itemBufferPool buffer.Pool
	// errorBufferPool 代表错误的缓冲池。
	errorBufferPool buffer.Pool
	// urlMap 代表已处理的URL的字典。
	urlMap cmap.ConcurrentMap
	// ctx 代表上下文，用于感知调度器的停止。
	ctx context.Context
	// cancelFunc 代表取消函数，用于停止调度器。
	cancelFunc context.CancelFunc
	// status 代表状态。
	status Status
	// statusLock 代表专用于状态的读写锁。
	statusLock sync.RWMutex
	// summary 代表摘要信息。
	summary SchedSummary
}

// NewScheduler 会创建一个调度器实例。
func NewScheduler() Scheduler {
	return &myScheduler{}
}

func (sched *myScheduler) Init(requestArgs RequestArgs, dataArgs DataArgs, moduleArgs ModuleArgs) (err error) {
	sched.ctx.Done()
	//检查状态
	logger.Info("[Scheduler] Check status for initialization...")
	var oldStatus Status
	oldStatus, err = sched.checkAndSetStatus(SCHED_STATUS_INITIALIZING)
	if err != nil {
		return
	}
	defer func() {
		sched.statusLock.Lock()
		if err != nil {
			sched.status = oldStatus
		} else {
			sched.status = SCHED_STATUS_INITIALIZED
		}
		sched.statusLock.Unlock()
	}()
	// 检查参数。
	logger.Info("[Scheduler] Check request arguments...")
	if err = requestArgs.Check(); err != nil {
		return err
	}
	logger.Info("[Scheduler] request arguments are valid.")

	logger.Info("[Scheduler] Check data arguments...")
	if err = dataArgs.Check(); err != nil {
		return err
	}
	logger.Info("[Scheduler] Data arguments are valid.")

	logger.Info("[Scheduler] Check module arguments...")
	if err = moduleArgs.Check(); err != nil {
		return err
	}
	logger.Info("[Scheduler] Module arguments are valid.")

	// 初始化内部字段。
	logger.Info("[Scheduler] Initialize scheduler's fields...")
	if sched.registrar == nil {
		sched.registrar = module.NewRegistrar()
	} else {
		sched.registrar.Clear()
	}
	sched.maxDepth = requestArgs.MaxDepth
	logger.Infof("-- Max depth: %d", sched.maxDepth)
	sched.acceptedDomainMap, _ = cmap.NewConcurrentMap(1, nil)
	for _, domain := range requestArgs.AcceptedDomains {
		sched.acceptedDomainMap.Put(domain, struct{}{})
	}
	logger.Infof("-- Accepted primary domains: %v", requestArgs.AcceptedDomains)

	sched.urlMap, _ = cmap.NewConcurrentMap(16, nil)
	logger.Infof("-- URL map: length: %d, concurrency: %d", sched.urlMap.Len(), sched.urlMap.Concurrency())
	sched.initBufferPool(dataArgs)
	sched.resetContext()
	sched.summary = newSchedSummary(requestArgs, dataArgs, moduleArgs, sched)
	// 注册组件。
	logger.Info("[Scheduler] Register modules...")
	if err = sched.registerModules(moduleArgs); err != nil {
		return err
	}
	logger.Info("[Scheduler] Scheduler has been initialized.")
	return nil
}

// 用于状态检查，并在条件满足时设置状态
func (sched *myScheduler) checkAndSetStatus(wantedStatus Status) (oldStatus Status, err error) {
	sched.statusLock.Lock()
	defer sched.statusLock.Unlock()
	oldStatus = sched.status
	err = checkStatus(oldStatus, wantedStatus, nil)
	if err == nil {
		sched.status = wantedStatus
	}
	return
}

// checkStatus 用于状态检查。
// 参数 currentStatus 代表当前状态。
// 参数 wantedStatus 代表想要的状态。
// 检查规则：
//    1.处于正在初始化、正在启动或正在停止状态时，不能从外部改变状态。
//    2.想要的状态只能是正在初始化、正在启动或正在停止状态中的一个。
//    3.处于未初始化状态时，不能变为正在启动或正在停止状态。
//    4.处于已启动状态时，不能变为正在初始化或正在启动状态。
//    5.处于未启动状态时，不能变为正在停止状态
func checkStatus(currentStatus Status, wantedStatus Status, lock sync.Locker) (err error) {
	//省略部分代码
	return
}

func (sched *myScheduler) Start(firstHTTPReq *http.Request) (err error) {
	defer func() {
		if p := recover(); p != nil {
			errMsg := fmt.Sprintf("Fatal scheduler error: %sched", p)
			logger.Fatal(errMsg)
			err = genError(errMsg)
		}
	}()
	logger.Info("[Scheduler] Start scheduler...")
	//检查状态
	logger.Info("[Scheduler] Check status for start...")
	var oldStatus Status
	oldStatus, err = sched.checkAndSetStatus(SCHED_STATUS_STARTING)
	if err != nil {
		return
	}
	defer func() {
		sched.statusLock.Lock()
		if err != nil {
			sched.status = oldStatus
		} else {
			sched.status = SCHED_STATUS_STARTED
		}
		sched.statusLock.Unlock()
	}()
	logger.Info("[Scheduler] Check first HTTP request...")
	if firstHTTPReq == nil {
		err = genParameterError("nil first HTTP request")
		return
	}
	// 获得首次请求的主域名，并将其添加到可接受的主域名的字典。
	logger.Info("[Scheduler] Get the primary domain...")
	logger.Infof("-- Host: %s", firstHTTPReq.Host)
	var primaryDomain string
	primaryDomain, err = getPrimaryDomain(firstHTTPReq.Host)
	if err != nil {
		return
	}
	logger.Infof("-- Primary domain: %s", primaryDomain)
	sched.acceptedDomainMap.Put(primaryDomain, struct{}{})
	// 开始调度数据和组件。
	if err = sched.checkBufferPoolForStart(); err != nil {
		return
	}
	sched.download()
	sched.analyze()
	sched.pick()
	logger.Info("[Scheduler] Scheduler has been started.")
	// 放入第一个请求。
	firstReq := module.NewRequest(firstHTTPReq, 0)
	sched.sendReq(firstReq)
	return nil
}

func (sched *myScheduler) Stop() (err error) {
	panic("implement me")
}

func (sched *myScheduler) Status() Status {
	panic("implement me")
}

func (sched *myScheduler) ErrorChan() <-chan error {
	panic("implement me")
}

func (sched *myScheduler) Idle() bool {
	panic("implement me")
}

func (sched *myScheduler) Summary() SchedSummary {
	panic("implement me")
}

// download 会从请求缓冲池取出请求并下载，然后把得到的响应放入响应缓冲池。
func (sched *myScheduler) download() {
	go func() {
		for {
			if sched.canceled() {
				break
			}
			datum, err := sched.reqBufferPool.Get()
			if err != nil {
				logger.Warnln("The request buffer pool was closed. Break request reception.")
				break
			}
			req, ok := datum.(*module.Request)
			if !ok {
				errMsg := fmt.Sprintf("incorrect request type: %T", datum)
				sendError(errors.New(errMsg), "", sched.errorBufferPool)
			}
			sched.downloadOne(req)
		}
	}()
}

// downloadOne 会根据给定的请求执行下载并把响应放入响应缓冲池。
func (sched *myScheduler) downloadOne(req *module.Request) {
	if req == nil {
		return
	}
	if sched.canceled() {
		return
	}
	m, err := sched.registrar.Get(module.TYPE_DOWNLOADER)
	if err != nil || m == nil {
		errMsg := fmt.Sprintf("couldn't get a downloader: %s", err)
		sendError(errors.New(errMsg), "", sched.errorBufferPool)
		// 失败则将请求放回请求缓冲此
		sched.sendReq(req)
		return
	}
	downloader, ok := m.(module.Downloader)
	if !ok {
		errMsg := fmt.Sprintf("incorrect downloader type: %T (MID: %s)", m, m.ID())
		sendError(errors.New(errMsg), m.ID(), sched.errorBufferPool)
		sched.sendReq(req)
		return
	}
	resp, err := downloader.Download(req)
	if resp != nil {
		sendResp(resp, sched.respBufferPool)
	}
	if err != nil {
		sendError(err, m.ID(), sched.errorBufferPool)
	}
}

// sendResp 会向响应缓冲池发送响应。
func sendResp(resp *module.Response, respBufferPool buffer.Pool) bool {
	if resp == nil || respBufferPool == nil || respBufferPool.Closed() {
		return false
	}
	go func(resp *module.Response) {
		if err := respBufferPool.Put(resp); err != nil {
			logger.Warnln("The response buffer pool was closed. Ignore response sending.")
		}
	}(resp)
	return true
}

// analyze 会从响应缓冲池取出响应并解析，
// 然后把得到的条目或请求放入相应的缓冲池。
func (sched *myScheduler) analyze() {
	go func() {
		for {
			if sched.canceled() {
				break
			}
			datum, err := sched.respBufferPool.Get()
			if err != nil {
				logger.Warnln("The response buffer pool was closed. Break response reception.")
				break
			}
			resp, ok := datum.(*module.Response)
			if !ok {
				errMsg := fmt.Sprintf("incorrect response type: %T", datum)
				sendError(errors.New(errMsg), "", sched.errorBufferPool)
			}
			sched.analyzeOne(resp)
		}
	}()
}

// analyzeOne 会根据给定的响应执行解析并把结果放入相应的缓冲池。
func (sched *myScheduler) analyzeOne(resp *module.Response) {
	if resp == nil {
		return
	}
	if sched.canceled() {
		return
	}
	m, err := sched.registrar.Get(module.TYPE_ANALYZER)
	if err != nil || m == nil {
		errMsg := fmt.Sprintf("couldn't get an analyzer: %s", err)
		sendError(errors.New(errMsg), "", sched.errorBufferPool)
		sendResp(resp, sched.respBufferPool)
		return
	}
	analyzer, ok := m.(module.Analyzer)
	if !ok {
		errMsg := fmt.Sprintf("incorrect analyzer type: %T (MID: %s)",
			m, m.ID())
		sendError(errors.New(errMsg), m.ID(), sched.errorBufferPool)
		sendResp(resp, sched.respBufferPool)
		return
	}
	dataList, errs := analyzer.Analyze(resp)
	if dataList != nil {
		for _, data := range dataList {
			if data == nil {
				continue
			}
			switch d := data.(type) {
			case *module.Request:
				sched.sendReq(d)
			case module.Item:
				sendItem(d, sched.itemBufferPool)
			default:
				errMsg := fmt.Sprintf("Unsupported data type %T! (data: %#v)", d, d)
				sendError(errors.New(errMsg), m.ID(), sched.errorBufferPool)
			}
		}
	}
	if errs != nil {
		for _, err := range errs {
			sendError(err, m.ID(), sched.errorBufferPool)
		}
	}
}

// canceled 用于判断调度器的上下文是否已被取消。
func (sched *myScheduler) canceled() bool {
	select {
	case <-sched.ctx.Done():
		return true
	default:
		return false
	}
}
