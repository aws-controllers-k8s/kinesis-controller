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
	if !delta.DifferentExcept("Spec.Tags", "Spec.ResourcePolicy") {
		return desired, nil
	}