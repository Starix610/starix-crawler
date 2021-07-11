package module

import (
	"fmt"
	"net"
	"starix-crawler/errors"
)

type MID string

// midTemplate 代表组件ID的模板，由组件类型+序列号+组件地址组成
var midTemplate = "%s%d|%s"

// GenMID 会根据给定参数生成组件ID
func GenMID(mtype Type, sn uint64, maddr net.Addr) (MID, error) {
	if !LegalType(mtype) {
		errMsg := fmt.Sprintf("illegal module type: %s", mtype)
		return "", errors.NewIllegalParameterError(errMsg)
	}
	letter := legalTypeLetterMap[mtype]
	var midStr string
	if maddr == nil {
		midStr = fmt.Sprintf(midTemplate, letter, sn, "")
		midStr = midStr[:len(midStr)-1]
	} else {
		midStr = fmt.Sprintf(midTemplate, letter, sn, maddr.String())
	}
	return MID(midStr), nil
}
