// Publish admin and group for users that have it
Meteor.publish(null, function () {
	return this.userId && Meteor.users.direct.find(this.userId, {fields: {admin: 1, group: 1}});
});

// Special hook for Meteor.users to scope for each group
function userFindHook(userId, selector /*, options*/) {
	if (Partitioner._directOps.get() === true
		|| Helpers.isDirectUserSelector(selector)
		|| Partitioner._searchAllUsers.get() === true
	) return true;

	let groupId = Partitioner._currentGroup.get();

	// This hook doesn't run if we're not in a method invocation or publish
	// function, and Partitioner._currentGroup is not set
	if (!userId && !groupId) return true;

	if (!groupId) {
		const user = Meteor.users.direct.findOne(userId, {fields: {group: 1}});

		// user will be undefined inside reactive publish when user is deleted while subscribed
		if (!user) return false;

		groupId = user.group;

		// If user is admin and not in a group, proceed as normal (select all users)
		// do user2 findOne separately so that the findOne above can hit the cache
		if (!groupId && Meteor.users.direct.findOne(userId, {fields: {admin: 1}}).admin) return true;

		// Normal users need to be in a group
		if (!groupId) throw new Meteor.Error(403, ErrMsg.groupErr);

		Partitioner._currentGroup.set(groupId);
	}

	filter = {
		"group": groupId,
		"admin": {$exists: false}
	};
	if (selector == null) {
		this.args[0] = filter;
	} else {
		Object.assign(selector, filter);
	}

	return true;
}

// No allow/deny for find so we make our own checks
function findHook(userId, selector, options) {
	if (
		// Don't scope for direct operations
		Partitioner._directOps.get() === true

		// for find(id) we should not touch this
		// TODO this may allow arbitrary finds across groups with the right _id
		// We could amend this in the future to {_id: someId, _groupId: groupId}
		// https://github.com/mizzao/meteor-partitioner/issues/9
		// https://github.com/mizzao/meteor-partitioner/issues/10
		|| Helpers.isDirectSelector(selector)

	) return true;

	// Check for global hook
	let groupId = Partitioner._currentGroup.get();

	if (!groupId) {
		if (!userId) throw new Meteor.Error(403, ErrMsg.userIdErr);

		groupId = Partitioner.getUserGroup(userId);
		if (!groupId) throw new Meteor.Error(403, ErrMsg.groupErr);

		Partitioner._currentGroup.set(groupId);
	}

	// force the selector to scope for the _groupId
	if (selector == null) {
		this.args[0] = {_groupId: groupId};
	} else {
		selector._groupId = groupId;
	}

	// Adjust options to not return _groupId
	if (options == null) {
		this.args[1] = {fields: {_groupId: 0}};
	} else {
		// If options already exist, add {_groupId: 0} unless fields has {foo: 1} somewhere
		if (options.fields == null) options.fields = {};
		if (!Object.values(options.fields).some(v => v)) options.fields._groupId = 0;
	}

	return true;
};

function insertHook(userId, doc) {
	// Don't add group for direct inserts
	if (Partitioner._directOps.get() === true) return true;

	let groupId = Partitioner._currentGroup.get();

	if (!groupId) {
		if (!userId) throw new Meteor.Error(403, ErrMsg.userIdErr);

		groupId = Partitioner.getUserGroup(userId);
		if (!groupId) throw new Meteor.Error(403, ErrMsg.groupErr);
	}

	doc._groupId = groupId;
	return true;
};

function userInsertHook(userId, doc) {
	// Don't add group for direct inserts
	if (Partitioner._directOps.get() === true) return true;

	const groupId = Partitioner._currentGroup.get() || (userId && Partitioner.getUserGroup(userId))

	if (groupId) doc.group = groupId;

	return true;
};

// Attach the find/insert hooks to Meteor.users
Meteor.users.before.find(userFindHook);
Meteor.users.before.findOne(userFindHook);
Meteor.users.before.insert(userInsertHook);

function getPartitionedIndex(index) {
	const defaultIndex = {_groupId: 1};

	if (!index) {
		return defaultIndex;
	}

	return {...defaultIndex, ...index};
}

Partitioner = {
	// Meteor environment variables for scoping group operations
	_currentGroup: new Meteor.EnvironmentVariable(),
	_directOps: new Meteor.EnvironmentVariable(),
	_searchAllUsers: new Meteor.EnvironmentVariable(),

	setUserGroup(userId, groupId) {
		check(userId, String);
		check(groupId, String);

		if (Meteor.users.direct.findOne(userId, {fields: {group: 1}}).group) {
			throw new Meteor.Error(403, "User is already in a group");
		}

		return Meteor.users.update(userId, {$set: {group: groupId}});
	},

	getUserGroup(userId) {
		check(userId, String);
		return (Meteor.users.direct.findOne(userId, {fields: {group: 1}}) || {}).group;
	},

	clearUserGroup(userId) {
		check(userId, String);
		return Meteor.users.direct.update(userId, {$unset: {group: 1}});
	},

	group() {
		const groupId = this._currentGroup.get();
		if (groupId) return groupId;

		let userId;
		try {
			userId = Meteor.userId();
		} catch (error) {}

		return userId && this.getUserGroup(userId);
	},

	bindGroup(groupId, func) {
		return this._currentGroup.withValue(groupId, func);
	},

	bindUserGroup(userId, func) {
		const groupId = Partitioner.getUserGroup(userId);

		if (!groupId) {
			Meteor._debug(`Dropping operation because ${userId} is not in a group`);
			return;
		}

		return Partitioner.bindGroup(groupId, func);
	},

	directOperation(func) {
		return this._directOps.withValue(true, func);
	},

	_isAdmin(_id) {
		return !!Meteor.users.direct.findOne({_id, admin: true}, {fields: {_id: 1}});
	},

	partitionCollection(collection, options={}) {
		// Because of the deny below, need to create an allow validator
		// on an insecure collection if there isn't one already
		if (collection._isInsecure()) {
			collection.allow({
				insert: () => true,
				update: () => true,
				remove: () => true,
			});
		}

		// Idiot-proof the collection against admin users
		collection.deny({
			insert: this._isAdmin,
			update: this._isAdmin,
			remove: this._isAdmin
		});
		collection.before.find(findHook);
		collection.before.findOne(findHook);
		// These will hook the _validated methods as well

		collection.before.insert(insertHook);
		// No update/remove hook necessary, findHook will be used automatically

		// Index the collections by groupId on the server for faster lookups across groups
		return collection._ensureIndex(getPartitionedIndex(options.index), options.indexOptions);
	},
};

// Accounts.createUser, etc, checks for case-insensitive matches of the email address
// however, it uses Meteor.users.find which only operates on the partitioned collection
// so will not find a matching user in a different group.
// Hence make them use Meteor.users.direct.find instead.
// Don't wrap createUser with Partitioner.directOperation because want inserted user doc to be
// automatically assigned to the group

['createUser', 'findUserByEmail', 'findUserByUsername'].forEach(fn => {
	const orig = Accounts[fn];
	Accounts[fn] = function() {
		return Partitioner._searchAllUsers.withValue(true, () => orig.apply(this, arguments));
	};
});

TestFuncs = {
	getPartitionedIndex: getPartitionedIndex,
	userFindHook: userFindHook,
	findHook: findHook,
	insertHook: insertHook
};
