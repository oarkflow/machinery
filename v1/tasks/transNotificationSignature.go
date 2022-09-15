package tasks

import (
	"time"
)

const (
	QUE_TRANS_NOTIFICATION       = "task_trans_notification"
	DELAY_QUE_TRANS_NOTIFICATION = "delay_task_trans_notification"
)

type SignatureInterface interface {
	GetSig() *Signature
	GetCommon() *CommonSignature
	RetryTimeout() int
}

type CommonSignature struct {
	CreateTime  time.Time `bson:"createTime,omitempty"`
	UpdateTime  time.Time `bson:"updateTime,omitempty"`
	State       string    `bson:"state,omitempty"`
	ProductCode string    `bson:"productCode,omitempty"` // 产品名称、模块名称
	TaskType    MsgType   `bson:"taskType,omitempty"`    // 任务类型
	EventType   string    `bson:"eventType,omitempty"`   // 事件类型
	TaskID      string    `bson:"taskID,omitempty"`      // 数据实体唯一ID
}

// uniqueID:productCode+taskType+eventType+taskID
type TransNotification struct {
	InsCode          string `bson:"insCode,omitempty"`
	IntStoreCode     string `bson:"intStoreCode,omitempty"`
	EvoTransID       string `bson:"evoTransID,omitempty"`
	FromChanMsg      string `bson:"fromChanMsg,omitempty"` // 渠道异步消息报文（内部字段）
	ToMerMsg         string `bson:"toMerMsg,omitempty"`    // 对下异步通知内容（内部字段）
	SendTimes        int    `bson:"sendTimes,omitempty"`   // 发送次数
	Destination      string `bson:"destination,omitempty"` // 消息投递目的地
	TraceID          string `bson:"traceID,omitempty"`
	NotificationType string `bson:"notificationType,omitempty"`
}

// trans notification signature
type NotificationSignature struct {
	*Signature
	CommonSignature    `bson:",inline"`
	*TransNotification `bson:",inline"`
}

func NewNotificationSignature(name string, args []Arg, tn *TransNotification) *NotificationSignature {
	sig, _ := NewSignature(name)
	sig.RetryCount = 9
	sig.RoutingKey = QUE_TRANS_NOTIFICATION
	create := time.Now()
	ns := NotificationSignature{
		Signature: sig,
		CommonSignature: CommonSignature{
			CreateTime: create,
			UpdateTime: create,
			State:      StateCreated,
		},
		TransNotification: tn,
	}
	return &ns
}

func (ns *NotificationSignature) GetSig() *Signature {
	return ns.Signature
}

func (ns *NotificationSignature) GetCommon() *CommonSignature {
	return &ns.CommonSignature
}

func (ns *NotificationSignature) RetryTimeout() int {
	return 0
}
