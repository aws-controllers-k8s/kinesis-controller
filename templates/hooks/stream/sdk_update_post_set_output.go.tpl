	if resp.TargetShardCount != nil {
		ko.Status.OpenShardCount = aws.Int64(int64(*resp.TargetShardCount))
	}
