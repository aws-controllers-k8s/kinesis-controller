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
        err := rm.updateStreamEncryption(ctx, latest, desired)
        if err != nil {
            return nil, err
        }
    }

	if delta.DifferentAt("Spec.RetentionPeriodHours") {
    		err:=rm.updateRetentionPeriodHours(ctx, latest, desired)
    		if err != nil {return nil, err}
    }


    if !delta.DifferentExcept("Spec.Tags","Spec.RetentionPeriodHours","Spec.EncryptionType") {
        return desired, nil
    }