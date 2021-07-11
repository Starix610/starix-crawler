package scheduler

import "starix-crawler/errors"

func genError(errMsg string) error {
	return errors.NewCrawlerError(errors.ERROR_TYPE_SCHEDULER, errMsg)
}
