
    // We need to get the tags that are in the AWS resource
    ko.Spec.Tags, err = rm.getTags(ctx, string(*ko.Spec.Name))

	if !isStreamActive(r.ko.Status.StreamStatus) {
		return &resource{ko}, ackrequeue.Needed(fmt.Errorf("resource is not active"))
	}
