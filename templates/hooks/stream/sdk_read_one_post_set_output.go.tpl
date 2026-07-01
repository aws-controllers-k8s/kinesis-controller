
	if !isStreamActive(ko.Status.StreamStatus) {
		return &resource{ko}, ackrequeue.Needed(fmt.Errorf("resource is not active"))
	}

	// We need to get the tags that are in the AWS resource
	ko.Spec.Tags, err = rm.getTags(ctx, ko.Spec.Name)
	if err != nil {
		return nil, err
	}

	// Read the resource-based policy attached to the stream (if any) so that
	// the delta comparison reflects the actual policy state.
	if ko.Status.ACKResourceMetadata != nil && ko.Status.ACKResourceMetadata.ARN != nil {
		policy, err := rm.getResourcePolicyWithContext(ctx, (*string)(ko.Status.ACKResourceMetadata.ARN))
		if err != nil {
			return nil, err
		}
		ko.Spec.ResourcePolicy = policy
	}

	// Surface the currently-enabled shard-level metrics into the spec field so
	// that the delta comparison reflects the stream's actual enhanced
	// monitoring state. ShardLevelMetrics is not returned at its spec path by
	// DescribeStreamSummary; it lives under the read-only EnhancedMonitoring
	// status field.
	ko.Spec.ShardLevelMetrics = nil
	for _, em := range ko.Status.EnhancedMonitoring {
		if em == nil {
			continue
		}
		ko.Spec.ShardLevelMetrics = append(ko.Spec.ShardLevelMetrics, em.ShardLevelMetrics...)
	}

	// WarmThroughputMiBps is returned by DescribeStreamSummary inside a nested
	// WarmThroughput object (as TargetMiBps) rather than at its flat spec path,
	// so surface it here to keep the delta comparison stable after an update.
	if resp.StreamDescriptionSummary.WarmThroughput != nil &&
		resp.StreamDescriptionSummary.WarmThroughput.TargetMiBps != nil {
		warmThroughputMiBpsCopy := int64(*resp.StreamDescriptionSummary.WarmThroughput.TargetMiBps)
		ko.Spec.WarmThroughputMiBps = &warmThroughputMiBpsCopy
	} else {
		ko.Spec.WarmThroughputMiBps = nil
	}

	// ShardCount is not returned at its spec path by DescribeStreamSummary; the
	// stream reports its current shard count via the read-only OpenShardCount
	// field. Mirror it into Spec.ShardCount so the delta comparison is stable for
	// a PROVISIONED stream whose desired shard count already matches the actual
	// one (otherwise a nil latest would drive a perpetual, failing
	// UpdateShardCount loop). compareShardCount ignores ShardCount for ON_DEMAND
	// streams, where capacity is managed automatically.
	if ko.Status.OpenShardCount != nil {
		shardCountCopy := *ko.Status.OpenShardCount
		ko.Spec.ShardCount = &shardCountCopy
	} else {
		ko.Spec.ShardCount = nil
	}
