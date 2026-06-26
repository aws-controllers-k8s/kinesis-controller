
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

	if !isStreamActive(r.ko.Status.StreamStatus) {
		return &resource{ko}, ackrequeue.Needed(fmt.Errorf("resource is not active"))
	}
