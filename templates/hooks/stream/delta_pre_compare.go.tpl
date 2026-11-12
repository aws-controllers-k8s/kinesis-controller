	// DescribeStreamSummary returns an empty EncryptionType for unencrypted
	// streams, which the generated sdk.go maps to nil. Treat nil as NONE so
	// that a desired value of NONE does not produce a spurious delta against
	// an unencrypted stream and trigger an infinite reconcile loop.
	if a.ko.Spec.DesiredEncryptionType != nil && b.ko.Spec.DesiredEncryptionType == nil {
		none := "NONE"
		b.ko.Spec.DesiredEncryptionType = &none
	}

	// Before late-init runs, desired Spec fields are nil while latest (from
	// sdkFind mirroring) has the observed value. Suppress nil-vs-observed
	// deltas so that update hooks are not called with nil desired values.
	if a.ko.Spec.DesiredRetentionPeriodHours == nil && b.ko.Spec.DesiredRetentionPeriodHours != nil {
		a.ko.Spec.DesiredRetentionPeriodHours = b.ko.Spec.DesiredRetentionPeriodHours
	}
	if a.ko.Spec.DesiredEncryptionType == nil && b.ko.Spec.DesiredEncryptionType != nil {
		a.ko.Spec.DesiredEncryptionType = b.ko.Spec.DesiredEncryptionType
	}
	if a.ko.Spec.EncryptionKeyARN == nil && b.ko.Spec.EncryptionKeyARN != nil {
		a.ko.Spec.EncryptionKeyARN = b.ko.Spec.EncryptionKeyARN
	}
