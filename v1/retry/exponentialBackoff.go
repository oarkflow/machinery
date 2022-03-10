package retry

import (
	"sync"

	"github.com/RichardKnop/machinery/v1/config"
)

type TransExponentialBackoff []int

var (
	defaultTransExponentialBackoff = TransExponentialBackoff([]int{15, 15, 30, 180, 1800, 3600, 5400, 7200, 14400})
)

var NotificationInternals TransExponentialBackoff

var once sync.Once

func LoadInternals(cnf *config.Config) TransExponentialBackoff {
	once.Do(func() {
		if cnf.SignatureConfig != nil && cnf.SignatureConfig.TransNotification != nil {
			NotificationInternals = cnf.SignatureConfig.TransNotification.Intervals
		} else {
			NotificationInternals = defaultTransExponentialBackoff
		}
	})
	return NotificationInternals
}

// [15,15,30,180,1800,3600,5400,7200,14400]
func TransNotificationBackoff(retries int) int {
	if retries > len(NotificationInternals) {
		return NotificationInternals[len(NotificationInternals)-1]
	}
	retries--
	return NotificationInternals[retries]
}
