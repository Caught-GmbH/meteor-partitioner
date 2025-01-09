Package.describe({
  name: "wildhart:partitioner",
  summary: "Transparently divide a meteor app into different instances shared between groups of users.",
  version: "3.1.0",
  git: "https://github.com/wildhart/meteor-partitioner"
});

Package.onUse(function (api) {
  api.versionsFrom(["3.0"]);
  // Client & Server deps
  api.use([
    'accounts-base',
    'check',
    'ecmascript',
    'ddp', // Meteor.publish available
    'mongo' // Mongo.Collection available
  ]);

  api.addFiles('common.js');
  api.addFiles('hooks.js', 'server');
  api.addFiles('hooks_client.js', 'client');
  api.addFiles('grouping.js', 'server');
  api.addFiles('grouping_client.js', 'client');

  api.export(['Partitioner', 'Grouping']);
});
