	// Tags, retention period and encryption are not part of CreateStream and are
	// applied via separate API calls in the update path. Mark the resource as
	// not-yet-synced so it is requeued into sdkUpdate to converge them.
	if ko.Spec.Tags != nil ||
		ko.Spec.DesiredRetentionPeriodHours != nil ||
		ko.Spec.DesiredEncryptionType != nil {
		msg := "post-create configuration (tags, retention period, encryption) pending"
		reason := "PostCreateConfigurationPending"
		ackcondition.SetSynced(&resource{ko}, corev1.ConditionFalse, &msg, &reason)
	}
