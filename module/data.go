package module

import "net/http"

// Request 代表数据请求的类型
type Request struct {
	// httpReq: 原生http请求实例
	httpReq *http.Request
	// depth: 请求深度
	depth uint32
}

func NewRequest(httpReq *http.Request, depth uint32) *Request {
	return &Request{httpReq: httpReq, depth: depth}
}

func (req *Request) HttpReq() *http.Request {
	return req.httpReq
}

func (req *Request) Depth() uint32 {
	return req.depth
}

// Response 代表数据响应的类型
type Response struct {
	httpResp *http.Response
	depth    uint32
}

func NewResponse(httpResp *http.Response, depth uint32) *Response {
	return &Response{httpResp: httpResp, depth: depth}
}

func (resp *Response) HttpResp() *http.Response {
	return resp.httpResp
}

func (resp *Response) Depth() uint32 {
	return resp.depth
}

// Data 数据的接口类型（请求、响应、数据项都归为数据标签）
type Data interface {
	Valid() bool
}

// Item 通用数据项类型
type Item map[string]interface{}

// 实现Data接口，判断请求是否有效
func (req *Request) Valid() bool {
	return req.httpReq != nil && req.httpReq.URL != nil
}

// 实现Data接口，判断响应是否有效
func (resp *Response) Valid() bool {
	return resp.httpResp != nil && resp.httpResp.Body != nil
}

// 实现Data接口，判断数据项是否有效
func (item Item) Valid() bool {
	return item != nil
}
