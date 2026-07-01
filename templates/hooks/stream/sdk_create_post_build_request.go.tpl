	// ShardCount must not be supplied when creating a stream in ON_DEMAND
	// capacity mode: Kinesis manages capacity automatically and rejects the
	// request with InvalidArgumentException ("ShardCount cannot be set while
	// creating stream in On-Demand StreamMode"). Drop it from the request so an
	// ON_DEMAND stream can still carry a (ignored) ShardCount in its spec.
	if isOnDemand(desired) {
		input.ShardCount = nil
	}
