package stream

import (
	svcsdktypes "github.com/aws/aws-sdk-go-v2/service/kinesis/types"
)

func isStreamActive(status *string) bool {
	if status == nil {
		return false
	}

	return *status == string(svcsdktypes.StreamStatusActive)
}
