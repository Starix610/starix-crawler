package downloader

import "starix-crawler/errors"

// genError 用于生成爬虫错误值。
func genError(errMsg string) error {
	return errors.NewCrawlerError(errors.ERROR_TYPE_DOWNLOADER, errMsg)
}

// genErrorBy 用于生成爬虫错误值。
func genErrorBy(err error) error {
	return errors.NewCrawlerErrorBy(errors.ERROR_TYPE_DOWNLOADER, err)
}

// genParameterError 用于生成爬虫参数错误值。
func genParameterError(errMsg string) error {
	return errors.NewCrawlerErrorBy(errors.ERROR_TYPE_DOWNLOADER, errors.NewIllegalParameterError(errMsg))
}
