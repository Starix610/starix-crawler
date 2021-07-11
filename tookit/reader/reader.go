package reader

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
)

// MultipleReader 代表多重读取器的接口。
type MultipleReader interface {
	// Reader 用于获取一个可关闭读取器ReadCloser
	// 后者会持有本多重读取器中的数据。
	Reader() io.ReadCloser
}

// myMultipleReader 代表多重读取器的实现类型。
type myMultipleReader struct {
	data []byte
}

func NewMultipleReader(reader io.Reader) (MultipleReader, error) {
	var data []byte
	var err error
	if reader != nil {
		data, err = ioutil.ReadAll(reader)
		if err != nil {
			return nil, fmt.Errorf("multiple reader: couldn't create a new one: %s", err)
		}
	} else {
		data = []byte{}
	}
	return &myMultipleReader{
		data: data,
	}, nil
}

func (r *myMultipleReader) Reader() io.ReadCloser {
	// NopCloser返回一个可关闭的ReadCloser（Close()方法是空操作，即实际无需真正关闭）
	return ioutil.NopCloser(bytes.NewReader(r.data))
}
