
    // We need to get the tags that are in the AWS resource
    ko.Spec.Tags, err = rm.getTags(ctx, ko.Spec.Name)
    if err != nil {
        return nil, err
    }

	if !isStreamActive(ko.Status.StreamStatus) {
		return &resource{ko}, ackrequeue.NeededAfter(
			fmt.Errorf("resource is not active"),
			ackrequeue.DefaultRequeueAfterDuration,
		)
	}
