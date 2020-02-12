ErrMsg = {
	userIdErr: "Must be logged in to operate on partitioned collection",
	groupErr: "Must have group assigned to operate on partitioned collection"
};

Helpers = {
	isDirectSelector(selector) {
		return selector && (
			typeof(selector)==='string'
			|| typeof(selector._id)==='string'
		);
	},
	// Because of https://github.com/HarvardEconCS/turkserver-meteor/issues/44
	// _id: { $in: [ ... ] } queries should be short-circuited as well for users
	isDirectUserSelector(selector) {
		return selector && (
			typeof(selector)==='string'
			|| typeof(selector._id)==='string'
			|| typeof(selector.username)==='string'
			|| (typeof(selector._id)==='object' && selector._id.$in!=null)
		);
	}
};
