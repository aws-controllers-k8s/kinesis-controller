	if delta.DifferentAt("Spec.Tags") {
		if err := rm.syncTags(ctx, latest, desired); err != nil {
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

	// Needs requeue because the update changes the Stream state to UPDATING
	if delta.DifferentAt("Spec.ShardLevelMetrics") {
		if err := rm.syncShardLevelMetrics(ctx, desired, latest); err != nil {
			return nil, err
		}
		return desired, ackrequeue.NeededAfter(
			fmt.Errorf("stream update in progress, requeuing"),
			ackrequeue.DefaultRequeueAfterDuration,
		)
	}

	// Needs requeue because the update changes the Stream state to UPDATING
	if delta.DifferentAt("Spec.MaxRecordSizeInKiB") {
		if err := rm.syncMaxRecordSize(ctx, desired, latest); err != nil {
			return nil, err
		}
		return desired, ackrequeue.NeededAfter(
			fmt.Errorf("stream update in progress, requeuing"),
			ackrequeue.DefaultRequeueAfterDuration,
		)
	}

	// Needs requeue because the update changes the Stream state to UPDATING
	if delta.DifferentAt("Spec.WarmThroughputMiBps") {
		if err := rm.syncWarmThroughput(ctx, desired, latest); err != nil {
			return nil, err
		}
		return desired, ackrequeue.NeededAfter(
			fmt.Errorf("stream update in progress, requeuing"),
			ackrequeue.DefaultRequeueAfterDuration,
		)
	}
	
	// Needs requeue because the update changes the Stream state to UPDATING
	if delta.DifferentAt("Spec.StreamModeDetails") {
		if err := rm.syncStreamMode(ctx, desired, latest); err != nil {
			return nil, err
		}
		return desired, ackrequeue.NeededAfter(
			fmt.Errorf("stream update in progress, requeuing"),
			ackrequeue.DefaultRequeueAfterDuration,
		)
	}

	if !delta.DifferentExcept("Spec.Tags", "Spec.ResourcePolicy", "Spec.MaxRecordSizeInKiB", "Spec.ShardLevelMetrics", "Spec.WarmThroughputMiBps", "Spec.StreamModeDetails") {
		return desired, nil
	}
