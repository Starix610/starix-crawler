package module

import (
	"fmt"
	"net"
	"starix-crawler/errors"
	"strconv"
	"strings"
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

// SplitMID 用于分解组件ID。
// 若分解成功，则第一个结果值长度为3
// 并依次包含组件类型字母、序列号和组件网络地址（如果有的话）。
func SplitMID(mid MID) ([]string, error) {
	var (
		letter string
		snStr  string
		addr   string
	)
	midStr := string(mid)
	if len(midStr) <= 1 {
		return nil, errors.NewIllegalParameterError("insufficient MID")
	}
	letter = midStr[:1]
	if _, ok := legalLetterTypeMap[letter]; !ok {
		return nil, errors.NewIllegalParameterError(fmt.Sprintf("illegal module type letter: %s", letter))
	}
	snAndAddr := midStr[1:]
	index := strings.LastIndex(snAndAddr, "|")
	if index < 0 {
		snStr = snAndAddr
		if !legalSN(snStr) {
			return nil, errors.NewIllegalParameterError(fmt.Sprintf("illegal module SN: %s", snStr))
		}
	} else {
		snStr = snAndAddr[:index]
		if !legalSN(snStr) {
			return nil, errors.NewIllegalParameterError(fmt.Sprintf("illegal module SN: %s", snStr))
		}
		addr = snAndAddr[index+1:]
		index := strings.LastIndex(addr, ":")
		if index < 0 {
			return nil, errors.NewIllegalParameterError(fmt.Sprintf("illegal module address: %s", addr))
		}
		ipStr := addr[:index]
		if ip := net.ParseIP(ipStr); ip == nil {
			return nil, errors.NewIllegalParameterError(fmt.Sprintf("illegal module IP: %s", ipStr))
		}
		portStr := addr[index+1:]
		if _, err := strconv.ParseUint(portStr, 10, 64); err != nil {
			return nil, errors.NewIllegalParameterError(fmt.Sprintf("illegal module port: %s", portStr))
		}
	}
	return []string{letter, snStr, addr}, nil
}

func legalSN(snStr string) bool {
	if _, err := strconv.ParseUint(snStr, 10, 64); err != nil {
		return false
	}
	return true
}
