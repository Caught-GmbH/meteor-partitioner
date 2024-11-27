const multipleGroupCollections = {};
let allowDirectIdSelectors = false;

// Publish admin and group for users that have it
Meteor.publish(null, function () {
	return this.userId && Meteor.users._partitionerDirect.find(this.userId, {fields: {admin: 1, group: 1}});
});

// Special hook for Meteor.users to scope for each group
async function userFindAsyncHook(userId, selector /*, options*/) {
	const isDirectSelector = Helpers.isDirectUserSelector(selector);
	if (
		((allowDirectIdSelectors || Partitioner._searchAllUsers.get()) && isDirectSelector)
		|| Partitioner._directOps.get() === true
	) return true;

	let groupId = Partitioner._currentGroup.get();

	// This hook doesn't run if we're not in a method invocation or publish
	// function, and Partitioner._currentGroup is not set
	if (!userId && !groupId) return true;

	if (!groupId) {
		const user = await Meteor.users._partitionerDirect.findOneAsync(userId, {fields: {group: 1}});

		// user will be undefined inside reactive publish when user is deleted while subscribed
		if (!user) return false;

		groupId = user.group;

		// If user is admin and not in a group, proceed as normal (select all users)
		// do user2 findOne separately so that the findOne above can hit the cache
		if (!groupId && await Meteor.users._partitionerDirect.findOneAsync(userId, {fields: {admin: 1}}).admin) return true;

		// Normal users need to be in a group
		if (!groupId) throw new Meteor.Error(403, ErrMsg.groupErr);

		Partitioner._currentGroup._set(groupId);
	}

	filter = {
		"group": groupId,
	};
	if (!isDirectSelector) {
		filter.admin = {$exists: false}
	}
	if (selector == null) {
		this.args[0] = filter;
	} else if (typeof selector == 'string') {
		filter._id = selector;
		this.args[0] = filter;
	} else {
		Object.assign(selector, filter);
	}

	return true;
}

function userFindHook(userId, selector /*, options*/) {
	return Meteor.wrapAsync(userFindAsyncHook)(userId, selector);
}

async function hookGetSetGroupAsync(userId) {
	let groupId = Partitioner._currentGroup.get();

	if (!groupId) {
		if (!userId) throw new Meteor.Error(403, ErrMsg.userIdErr);

		groupId = await Partitioner.getUserGroupAsync(userId);
		if (!groupId) throw new Meteor.Error(403, ErrMsg.groupErr);

		Partitioner._currentGroup._set(groupId);
	}

	return groupId;
}

function hookGetSetGroup(userId) {
	let groupId = Partitioner._currentGroup.get();

	if (!groupId) {
		if (!userId) throw new Meteor.Error(403, ErrMsg.userIdErr);

		groupId = Partitioner.getUserGroup(userId);
		if (!groupId) throw new Meteor.Error(403, ErrMsg.groupErr);

		Partitioner._currentGroup._set(groupId);
	}

	return groupId;
}

// No allow/deny for find so we make our own checks
function findHook(userId, selector, options) {
	if (
		// Don't scope for direct operations
		Partitioner._directOps.get() === true
		// Fixed in 3.0.0 - now we even add the groupId to direct selectors
		// https://github.com/mizzao/meteor-partitioner/issues/9
		// https://github.com/mizzao/meteor-partitioner/issues/10
		|| (allowDirectIdSelectors && Helpers.isDirectSelector(selector))

	) return true;

	const groupId = hookGetSetGroup(userId);

	// force the selector to scope for the _groupId
	if (selector == null) {
		this.args[0] = {
			_groupId: groupId,
		};
	} else if (typeof selector == 'string') {
		this.args[0] = {
			_id: selector,
			_groupId: groupId,
		};
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

async function insertAsyncHook(multipleGroups, userId, doc) {
	// Don't add group for direct inserts
	if (Partitioner._directOps.get() === true) return true;

	const groupId = hookGetSetGroup(userId);

	doc._groupId = multipleGroups ? [groupId] : groupId;
	return true;
};

function insertHook(multipleGroups, userId, doc) {
	return Meteor.wrapAsync(insertAsyncHook)(multipleGroups, userId, doc);
}

function userInsertAsyncHook(userId, doc) {
	// Don't add group for direct inserts
	if (Partitioner._directOps.get() === true) return true;

	const groupId = hookGetSetGroup(userId);

	doc.group = groupId;

	return true;
};

function upsertAsyncHook(multipleGroups, userId, selector, doc) {
	// Don't add group for direct inserts
	if (Partitioner._directOps.get() === true) return true;

	const groupId = hookGetSetGroup(userId);

	doc._groupId = multipleGroups ? [groupId] : groupId;
	return true;
};

function upsertHook(multipleGroups, userId, selector, doc) {
	return Meteor.wrapAsync(upsertAsyncHook)(multipleGroups, userId, selector, doc);
}

function userUpsertAsyncHook(userId, selector, doc) {
	// Don't add group for direct inserts
	if (Partitioner._directOps.get() === true) return true;

	const groupId = hookGetSetGroup(userId);

	doc.group = groupId;
	return true;
};

function userUpsertHook(userId, selector, doc) {
	return Meteor.wrapAsync(userUpsertAsyncHook)(userId, selector, doc);
}

// Attach the find/insert hooks to Meteor.users
Meteor.users._partitionerBefore.find(userFindHook);
Meteor.users._partitionerBefore.findOneAsync(userFindAsyncHook);
Meteor.users._partitionerBefore.insertAsync(userInsertAsyncHook);
Meteor.users._partitionerBefore.upsertAsync(userUpsertAsyncHook);

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

	async setUserGroupAsync(userId, groupId) {
		check(userId, String);
		check(groupId, String);

		if (await Meteor.users._partitionerDirect.findOneAsync(userId, {fields: {group: 1}}).group) {
			throw new Meteor.Error(403, "User is already in a group");
		}

		return Meteor.users._partitionerDirect.update(userId, {$set: {group: groupId}});
	},

	// Static method to backwards-compatibly
	setUserGroup(userId, groupId) {
		return Meteor.wrapAsync(this.setUserGroupAsync)(userId, groupId);
	},

	async getUserGroupAsync(userId) {
		check(userId, String);
		return (await Meteor.users._partitionerDirect.findOneAsync(userId, {fields: {group: 1}}) || {}).group;
	},

	// Static method to backwards-compatibly
	getUserGroup(userId) {
		return Meteor.wrapAsync(this.getUserGroupAsync)(userId);
	},

	async clearUserGroupAsync(userId) {
		check(userId, String);
		return await Meteor.users._partitionerDirect.updateAsync(userId, {$unset: {group: 1}});
	},

	// Static method to backwards-compatibly
	clearUserGroup(userId) {
		return Meteor.wrapAsync(this.clearUserGroupAsync)(userId);
	},

	async groupAsync() {
		const groupId = this._currentGroup.get();
		if (groupId) return groupId;

		let userId;
		try {
			userId = Meteor.userId();
		} catch (error) {}

		return userId && await this.getUserGroupAsync(userId);
	},

	// Static method to backwards-compatibly
	group() {
		return Meteor.wrapAsync(this.groupAsync)();
	},

	bindGroup(groupId, func) {
		return this._currentGroup.withValue(groupId, func);
	},

	async bindUserGroupAsync(userId, func) {
		const groupId = Partitioner.getUserGroupAsync(userId);

		if (!groupId) {
			Meteor._debug(`Dropping operation because ${userId} is not in a group`);
			return;
		}

		return await Partitioner.bindGroup(groupId, func);
	},

	// Static method to backwards-compatibly
	bindUserGroup(userId, func) {
		return Meteor.wrapAsync(this.bindUserGroupAsync)(userId, func);
	},

	directOperation(func) {
		return this._directOps.withValue(true, func);
	},

	async _isAdminAsync(_id) {
		return !!(await Meteor.users._partitionerDirect.findOneAsync({_id, admin: true}, {fields: {_id: 1}}));
	},

	// Static method to backwards-compatibly
	_isAdmin(_id) {
		return Meteor.wrapAsync(this._isAdminAsync)(_id);
	},

	async addToGroupAsync(collection, entityId, groupId) {
		if (!multipleGroupCollections[collection._name]) {
			throw new Meteor.Error(403, ErrMsg.multiGroupErr);
		}

		let currentGroupIds = (await collection._partitionerDirect.findOneAsync(entityId, {fields: {_groupId: 1}}))?._groupId;
		if (!currentGroupIds) {
			currentGroupIds = [groupId];
		} else if (typeof currentGroupIds == 'string') {
			currentGroupIds = [currentGroupIds];
		}

		if (currentGroupIds.indexOf(groupId) == -1) {
			currentGroupIds.push(groupId);
			await collection._partitionerDirect.updateAsync(entityId, {$set: {_groupId: currentGroupIds}});
		}
		return currentGroupIds;
	},

	// Static method to backwards-compatibly
	addToGroup(collection, entityId, groupId) {
		return Meteor.wrapAsync(this.addToGroupAsync)(collection, entityId, groupId);
	},

	async removeFromGroupAsync(collection, entityId, groupId) {
		if (!multipleGroupCollections[collection._name]) {
			throw new Meteor.Error(403, ErrMsg.multiGroupErr);
		}

		let currentGroupIds = await collection._partitionerDirect.findOneAsync(entityId, {fields: {_groupId: 1}})?._groupId;
		if (!currentGroupIds) {
			return [];
		}

		if (typeof currentGroupIds == 'string') {
			currentGroupIds = [currentGroupIds];
		}
		const index = currentGroupIds.indexOf(groupId);
		if (index != -1) {
			currentGroupIds.splice(index, 1);
			await collection._partitionerDirect.updateAsync(entityId, {$set: {_groupId: currentGroupIds}});
		}

		return currentGroupIds;
	},

	// Static method to backwards-compatibly
	removeFromGroup(collection, entityId, groupId) {
		return Meteor.wrapAsync(this.removeFromGroupAsync)(collection, entityId, groupId);
	},

	async partitionCollection(collection, options = {}) {
		return Meteor.wrapAsync(async () => {
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
				insert: await this._isAdminAsync,
				update: await this._isAdminAsync,
				remove: await this._isAdminAsync
			});
			collection._partitionerBefore.find(findHook);
			collection._partitionerBefore.findOneAsync(findHook);
			collection._partitionerBefore.upsertAsync((...args) => upsertHook(options.multipleGroups, ...args));
			// These will hook the _validated methods as well

			collection._partitionerBefore.insertAsync((...args) => insertHook(options.multipleGroups, ...args));
			// No update/remove hook necessary, findHook will be used automatically

			// store a hash of which collections allow multiple groups
			if (options.multipleGroups) {
				multipleGroupCollections[collection._name] = true;
			}

			// Index the collections by groupId on the server for faster lookups across groups
			return collection.createIndex ? collection.createIndex(getPartitionedIndex(options.index), options.indexOptions)
				: collection._ensureIndex(getPartitionedIndex(options.index), options.indexOptions);
		})();
	},

	get allowDirectIdSelectors() {
		return allowDirectIdSelectors;
	},
	set allowDirectIdSelectors(val) {
		if (typeof val != 'boolean') {
			throw new Error('Partitioner.allowDirectIdSelectors can only be boolean');
		}
		allowDirectIdSelectors = val;
		if (val) {
			console.warn('WARNING: setting Partitioner.allowDirectIdSelectors = true may allow unsafe operations!');
		}
	},
};

// Accounts.createUser, etc, checks for case-insensitive matches of the email address
// however, it uses Meteor.users.find which only operates on the partitioned collection
// so will not find a matching user in a different group.
// Hence make them use Meteor.users._partitionerDirect.find instead.
// Don't wrap createUser with Partitioner.directOperation because want inserted user doc to be
// automatically assigned to the group

['createUser', 'findUserByEmail', 'findUserByUsername', '_attemptLogin'].forEach(fn => {
	const orig = Accounts[fn];
	if (orig) {
		Accounts[fn] = function() {
			return Partitioner._searchAllUsers.withValue(true, () => orig.apply(this, arguments));
		};
	}
});
