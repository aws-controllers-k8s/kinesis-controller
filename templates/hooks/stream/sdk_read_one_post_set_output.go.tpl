
    // We need to get the tags that are in the AWS resource
    ko.Spec.Tags, err = rm.getTags(ctx, ko.Spec.Name)
    if err != nil {
        return nil, err
    }

    // Read the stream's resource-based policy (if any) into the spec so drift
    // against the desired policy can be detected.
    if ko.Status.ACKResourceMetadata != nil && ko.Status.ACKResourceMetadata.ARN != nil {
        ko.Spec.ResourcePolicy, err = rm.getResourcePolicy(ctx, (*string)(ko.Status.ACKResourceMetadata.ARN))
        if err != nil {
            return nil, err
        }
    }

	if !isStreamActive(r.ko.Status.StreamStatus) {
		return &resource{ko}, ackrequeue.Needed(fmt.Errorf("resource is not active"))
	}
