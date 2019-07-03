Partitioner = {}

###
  Client selector modifiers
###

Partitioner.group = ->
  userId = Meteor.userId()
  return unless userId
  return Meteor.users.findOne(userId, fields: {group: 1})?.group

# This can be replaced - currently not documented
# Don't retrieve full user object - fix bug #32
Partitioner._isAdmin = (userId) -> Meteor.users.find({_id: userId, admin: true}, {fields: {_id: 1}}).count() > 0

userFindHook = (userId, selector, options) ->
  # Do the usual find for no user or single selector
  return true if !userId or Helpers.isDirectUserSelector(selector)

  # No hooking needed for regular users, taken care of on server
  return true unless Partitioner._isAdmin(Meteor.userId())

  # Don't have admin see itself for global finds
  unless @args[0]
    @args[0] =
      admin: {$exists: false}
  else
    selector.admin = {$exists: false}
  return true

Meteor.users.before.find userFindHook
Meteor.users.before.findOne userFindHook

insertHook = (userId, doc) ->
  throw new Meteor.Error(403, ErrMsg.userIdErr) unless userId
  groupId = Partitioner.group()
  throw new Meteor.Error(403, ErrMsg.groupErr) unless groupId
  doc._groupId = groupId
  return true

# Add in groupId for client so as not to cause unexpected sync changes
Partitioner.partitionCollection = (collection) ->
  # No find hooks needed if server side filtering works properly

  collection.before.insert insertHook

TestFuncs =
  userFindHook: userFindHook
  insertHook: insertHook