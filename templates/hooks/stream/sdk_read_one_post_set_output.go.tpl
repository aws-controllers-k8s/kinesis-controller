
	// We need to get the tags that are in the AWS resource
	ko.Spec.Tags, err = rm.getTags(ctx, ko.Spec.Name)
	if err != nil {
		return nil, err
	}

	// Normalize the configurable fields that are managed through dedicated
	// Kinesis operations so that (1) an unset field is never reconciled against
	// the server-side default (opt-in management), and (2) semantically-equal
	// values - the "ALL" metric expansion and the asymmetric warm-throughput
	// read shape - do not produce a perpetual diff.
	ko.Spec.ShardLevelMetrics = reconcileShardLevelMetrics(
		r.ko.Spec.ShardLevelMetrics,
		flattenEnhancedMonitoring(ko.Status.EnhancedMonitoring),
	)
	if r.ko.Spec.WarmThroughputMiBps == nil {
		ko.Spec.WarmThroughputMiBps = nil
	} else {
		ko.Spec.WarmThroughputMiBps = warmThroughputTarget(resp.StreamDescriptionSummary.WarmThroughput)
	}
	if r.ko.Spec.RetentionPeriodHours == nil {
		ko.Spec.RetentionPeriodHours = nil
	}
	if r.ko.Spec.EncryptionType == nil {
		ko.Spec.EncryptionType = nil
		ko.Spec.KeyID = nil
	}
	if r.ko.Spec.MaxRecordSizeInKiB == nil {
		ko.Spec.MaxRecordSizeInKiB = nil
	}
	if r.ko.Spec.StreamModeDetails == nil {
		ko.Spec.StreamModeDetails = nil
	}

	if !isStreamActive(r.ko.Status.StreamStatus) {
		return &resource{ko}, ackrequeue.Needed(fmt.Errorf("resource is not active"))
	}
