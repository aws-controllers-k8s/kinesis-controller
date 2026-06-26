	if delta.DifferentAt("Spec.Tags") {
		err := rm.syncTags(
			ctx,
			latest,
			desired,
		)
		if err != nil {
			return nil, err
		}
	}
	// ResourcePolicy is managed out-of-band via the Put/DeleteResourcePolicy
	// APIs rather than the UpdateShardCount call used for the standard update
	// path.
	if delta.DifferentAt("Spec.ResourcePolicy") {
		if err := rm.syncResourcePolicy(ctx, desired, latest); err != nil {
			return nil, err
		}
	}
	// MaxRecordSizeInKiB is mutated via the dedicated UpdateMaxRecordSize API,
	// not the UpdateShardCount call used for the standard update path.
	if delta.DifferentAt("Spec.MaxRecordSizeInKiB") {
		if err := rm.syncMaxRecordSize(ctx, desired, latest); err != nil {
			return nil, err
		}
	}
	// ShardLevelMetrics is managed out-of-band via the
	// Enable/DisableEnhancedMonitoring APIs rather than the UpdateShardCount
	// call used for the standard update path.
	if delta.DifferentAt("Spec.ShardLevelMetrics") {
		if err := rm.syncShardLevelMetrics(ctx, desired, latest); err != nil {
			return nil, err
		}
	}
	// WarmThroughputMiBps is mutated via the dedicated UpdateStreamWarmThroughput
	// API, not the UpdateShardCount call used for the standard update path.
	if delta.DifferentAt("Spec.WarmThroughputMiBps") {
		if err := rm.syncWarmThroughput(ctx, desired, latest); err != nil {
			return nil, err
		}
	}
	if !delta.DifferentExcept("Spec.Tags", "Spec.ResourcePolicy", "Spec.MaxRecordSizeInKiB", "Spec.ShardLevelMetrics", "Spec.WarmThroughputMiBps") {
		return desired, nil
	}