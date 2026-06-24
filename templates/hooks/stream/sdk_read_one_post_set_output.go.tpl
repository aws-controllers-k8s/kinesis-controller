	// We need to get the tags that are in the AWS resource
	ko.Spec.Tags, err = rm.getTags(ctx, ko.Spec.Name)
	if err != nil {
		return nil, err
	}

	// Populate the Spec fields that mirror observed AWS state. sdkFind always
	// overwrites these so that the "latest" resource passed to delta detection
	// reflects actual AWS state rather than whatever was last written to the CR.
	if ko.Status.RetentionPeriodHours != nil {
		retentionCopy := *ko.Status.RetentionPeriodHours
		ko.Spec.DesiredRetentionPeriodHours = &retentionCopy
	}
	if ko.Status.EncryptionType != nil {
		ko.Spec.DesiredEncryptionType = ko.Status.EncryptionType
	}
	if ko.Status.KeyID != nil {
		ko.Spec.EncryptionKeyARN = ko.Status.KeyID
	}

	if !isStreamActive(r.ko.Status.StreamStatus) {
		return &resource{ko}, ackrequeue.Needed(fmt.Errorf("resource is not active"))
	}
