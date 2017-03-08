package leader

import (
	"testing"
	"time"
)

func TestBackoff(t *testing.T) {
	for i := 0; i < 10; i++ {
		currWait := 2 * time.Second
		maxWait := 10 * time.Second
		for i := 0; i < 10; i++ {
			wait := backoff(currWait, maxWait)
			if wait < currWait || wait >= maxWait {
				t.Errorf("unexpected wait=%s from currWait=%s, maxWait=%s", wait, currWait, maxWait)
			}
			currWait = wait
		}
	}
}
