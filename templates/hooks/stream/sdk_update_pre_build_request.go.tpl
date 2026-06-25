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
	// The resource-based policy is managed out-of-band of UpdateShardCount.
	if delta.DifferentAt("Spec.ResourcePolicy") {
		if err := rm.syncResourcePolicy(ctx, desired, latest); err != nil {
			return nil, err
		}
	}
	if !delta.DifferentExcept("Spec.Tags", "Spec.ResourcePolicy") {
		return desired, nil
	}