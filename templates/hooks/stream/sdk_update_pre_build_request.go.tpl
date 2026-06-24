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
	if delta.DifferentAt("Spec.EncryptionType") {
		if err := rm.syncEncryption(ctx, desired, latest); err != nil {
			return nil, err
		}
	}
	if delta.DifferentAt("Spec.RetentionPeriodHours") {
		if err := rm.syncRetentionPeriod(ctx, desired, latest); err != nil {
			return nil, err
		}
	}
	if delta.DifferentAt("Spec.ShardLevelMetrics") {
		if err := rm.syncEnhancedMonitoring(ctx, desired, latest); err != nil {
			return nil, err
		}
	}
	if delta.DifferentAt("Spec.StreamModeDetails") || delta.DifferentAt("Spec.StreamModeDetails.StreamMode") {
		if err := rm.syncStreamMode(ctx, desired, latest); err != nil {
			return nil, err
		}
	}
	if delta.DifferentAt("Spec.MaxRecordSizeInKiB") {
		if err := rm.syncMaxRecordSize(ctx, desired, latest); err != nil {
			return nil, err
		}
	}
	if delta.DifferentAt("Spec.WarmThroughputMiBps") {
		if err := rm.syncWarmThroughput(ctx, desired, latest); err != nil {
			return nil, err
		}
	}
	// UpdateShardCount is the only ACK-generated update operation; every other
	// mutable property is handled by the dedicated sync calls above. Skip it
	// unless the shard count itself changed.
	if !delta.DifferentAt("Spec.ShardCount") {
		return desired, nil
	}
