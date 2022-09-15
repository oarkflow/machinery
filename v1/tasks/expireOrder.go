package tasks

import (
	"time"
)

type ExpireOrderTask struct {
	*Signature
	*CommonSignature  `bson:",inline"`
	*OrderExpiredTask `bson:",inline"`
}

type OrderExpiredTask struct {
	InsCode      string     `bson:"insCode,omitempty"`
	IntStoreCode string     `bson:"intStoreCode,omitempty"`
	TraceID      string     `bson:"traceID,omitempty"`
	ValidTime    int        `bson:"validTime,omitempty"`
	ExpiredTime  *time.Time `bson:"expiredTime,omitempty"`
}

func (e *ExpireOrderTask) GetSig() *Signature {
	return e.Signature
}

func (e *ExpireOrderTask) GetCommon() *CommonSignature {
	return e.CommonSignature
}

func (e *ExpireOrderTask) RetryTimeout() int {
	return 0
}
