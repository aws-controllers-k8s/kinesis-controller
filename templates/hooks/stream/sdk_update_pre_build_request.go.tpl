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
	// Retention period and encryption are applied via dedicated API calls whose
	// responses carry no updated state. Track whether any of them ran so we can
	// requeue afterwards: the next ReadOne re-reads the observed Status, which is
	// what the delta hook compares the desired Spec inputs against. Encryption
	// additionally moves the stream into the UPDATING state, so requeuing also
	// prevents a follow-on UpdateShardCount call from failing.
	syncedSideEffect := false
	if delta.DifferentAt("Spec.DesiredRetentionPeriodHours") {
		if err := rm.syncRetentionPeriod(ctx, desired, latest); err != nil {
			return nil, err
		}
		syncedSideEffect = true
	}
	if delta.DifferentAt("Spec.DesiredEncryptionType") ||
		delta.DifferentAt("Spec.EncryptionKeyARN") {
		if err := rm.syncEncryption(ctx, desired, latest); err != nil {
			return nil, err
		}
		syncedSideEffect = true
	}
	if syncedSideEffect {
		return desired, ackrequeue.NeededAfter(
			fmt.Errorf("stream retention/encryption update in progress, requeuing"),
			ackrequeue.DefaultRequeueAfterDuration,
		)
	}
	if !delta.DifferentExcept(
		"Spec.Tags",
		"Spec.DesiredRetentionPeriodHours",
		"Spec.DesiredEncryptionType",
		"Spec.EncryptionKeyARN",
	) {
		return desired, nil
	}
