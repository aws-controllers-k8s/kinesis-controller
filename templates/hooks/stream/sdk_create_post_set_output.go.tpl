	// Tags and the resource policy are applied on a follow-up reconcile, so mark
	// the resource as not-yet-synced to trigger one.
	if ko.Spec.Tags != nil || ko.Spec.ResourcePolicy != nil {
		ackcondition.SetSynced(&resource{ko}, corev1.ConditionFalse, nil, nil)
	}