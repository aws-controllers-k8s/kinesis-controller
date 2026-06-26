
    // We need to get the tags that are in the AWS resource
    ko.Spec.Tags, err = rm.getTags(ctx, ko.Spec.Name)
    if err != nil {
        return nil, err
    }

	// Read the resource-based policy attached to the stream (if any) so that
	// the delta comparison reflects the actual policy state.
	if ko.Status.ACKResourceMetadata != nil && ko.Status.ACKResourceMetadata.ARN != nil {
		policy, err := rm.getResourcePolicyWithContext(ctx, (*string)(ko.Status.ACKResourceMetadata.ARN))
		if err != nil {
			return nil, err
		}
		ko.Spec.ResourcePolicy = policy
	}

	if !isStreamActive(r.ko.Status.StreamStatus) {
		return &resource{ko}, ackrequeue.Needed(fmt.Errorf("resource is not active"))
	}
