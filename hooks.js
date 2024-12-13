let publishUserId;

if (Meteor.isServer) {
  publishUserId = new Meteor.EnvironmentVariable();
  const _publish = Meteor.publish;
  Meteor.publish = function (name, handler, options) {
    return _publish.call(
      this,
      name,
      function (...args) {
        // This function is called repeatedly in publications
        return publishUserId.withValue(this && this.userId, () => handler.apply(this, args));
      },
      options,
    );
  };
}

function getUserId() {
  let userId;

  try {
    // Will throw an error unless within method call.
    // Attempt to recover gracefully by catching:
    userId = Meteor.userId && Meteor.userId();
  } catch (e) {}

  if (userId == null && Meteor.isServer) {
    // Get the userId if we are in a publish function.
    userId = publishUserId.get();
  }

  return userId;
}

const proto = Mongo.Collection.prototype;
const directEnv = new Meteor.EnvironmentVariable(false);
const selectionMethods = ["find", "findOneAsync", "insertAsync", "updateAsync", "removeAsync", "upsertAsync"];
const fetchMethods = [
  "fetchAsync",
  "observeAsync",
  "observeChangesAsync",
  "countAsync",
  "forEachAsync",
  "mapAsync",
  Symbol.asyncIterator,
];

// create the collection._partitionerBefore.* methods
// have to create it initially using a getter so we can store self=this and create a new group of functions which have access to self
Object.defineProperty(proto, "_partitionerBefore", {
  get() {
    // console.log('creating before functions', this._name);
    const self = this;
    const fns = {};
    selectionMethods.forEach(
      (method) =>
        (fns[method] = function (hookFn) {
          self[`_groupingBefore_${method}`] = hookFn;
        }),
    );
    // replace the .direct prototype with the created object, so we don't have to recreate it every time.
    Object.defineProperty(this, "_partitionerBefore", { value: fns });
    return fns;
  },
});

// create the collection._partitionerDirect.* methods
// have to create it initially using a getter so we can store self=this and create a new group of functions which have access to self
Object.defineProperty(proto, "_partitionerDirect", {
  get() {
    // console.log('creating direct functions', this._name);
    const self = this;
    const fns = {};
    selectionMethods.forEach(
      (method) =>
        (fns[method] = async function (...args) {
          return directEnv.withValue(true, async () => {
            return proto[method].apply(self, args);
          });
        }),
    );
    // replace the .direct prototype with the created object, so we don't have to recreate it every time.
    Object.defineProperty(this, "_partitionerDirect", { value: fns });
    return fns;
  },
});

global.hookLogging = false;
// if (Meteor.isServer) global.hookLogging = true;

global.uuu = false;

selectionMethods.forEach(method => {
  const _orig = proto[method];
  // if the method is find, we do not replace the original method
  // because it is sync and we replace the childfetch methods instead
  if (method == 'find') {
    proto[method] = function(...args) {
      const self = this;
      const cursor = _orig.apply(this, args); // we need to get cursor to get fetch methods
      const userId = getUserId();
      // Method _observeChanges is used by Meteor publications
      // Store original method to prevent infinite loop
      if(!cursor._mongo._observeChangesOrig) {
        cursor._mongo._observeChangesOrig = cursor._mongo._observeChanges;
      }
      // Modify cursor and then call original method
      cursor._mongo._observeChanges = async function(...args) {
        if(self._groupingBefore_find) {
          const selector = cursor._cursorDescription.selector;
          await self._groupingBefore_find.call({args}, userId, selector, {});
          cursor._cursorDescription.selector = selector;
        }
        return cursor._mongo._observeChangesOrig.apply(this, args);
      };

      // Now modify all cursor methods
      // ...methods after find (fetchAsync, countAsync...)
      fetchMethods.forEach(fetchMethod => {
        const _orig = cursor[fetchMethod];
        cursor[fetchMethod] = async function(...args) {
          // modify the selector in the cursor before calling the fetch method
          if(self._groupingBefore_find) {
            const selector = cursor._cursorDescription.selector;
            const userId = getUserId();
            await self._groupingBefore_find.call({args}, userId, selector, {});
            cursor._cursorDescription.selector = selector;
          }
          // run the original fetch method
          return _orig.apply(this, args);
        };
      });
      return cursor;
    };
    return;
  }

  // Now modify data manipulation async methods (insertAsync, updateAsync...)
  // this replaces all async original methods
  // except find, which is sync and needs to be handled differently
  proto[method] = async function(...args) {
    if (directEnv.get()) return _orig.apply(this, args);
    // give the hooks a private context so that they can modify this.args without putting this.args onto the prototype
    const context = {args};
    const userId = getUserId();
    global.hookLogging && typeof args[0]!='string' && console.log('hook', '\n\n');

    // if the method is update or remove, automatically apply the find hooks to limit the update/remove to the user's group
    if ((method=='updateAsync' || method=='removeAsync') && this._groupingBefore_find) {
      global.hookLogging && typeof args[0]!='string' && console.log('hook', 'b4i', this._name+"."+method, JSON.stringify(args[0]), JSON.stringify(args[1]));
      // don't send args[1] for update or remove.
      // need to send empty object instead to prevent args[1] being modified
      await this._groupingBefore_find.call(context, userId, args[0], {});
      global.hookLogging && typeof args[0]!='string' && console.log('hook', 'afi', this._name+"."+method, JSON.stringify(args[0]), JSON.stringify(args[1]));
    }

    // run the hook
    if (this['_groupingBefore_'+method]) {
      global.hookLogging && typeof args[0]!='string' && console.log('hook', 'b4', this._name+"."+method, JSON.stringify(args[0]), JSON.stringify(args[1]));
      await this['_groupingBefore_'+method].call(context, userId, args[0], args[1], args[2]);
      global.hookLogging && typeof args[0]!='string' && console.log('hook', 'af', this._name+"."+method, JSON.stringify(args[0]), JSON.stringify(args[1]));
    }

    // run the original method
    return _orig.apply(this, args);
  }
});