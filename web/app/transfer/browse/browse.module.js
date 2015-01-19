'use strict';

/** Module for browsing transfer endpoints. */
angular.module('stork.transfer.browse', [
  'mgcrea.ngStrap.typeahead'
])

.config(function ($typeaheadProvider) {
  angular.extend($typeaheadProvider.defaults, {
    animation: false,
    minLength: 1,
    limit: 10,
    position: 'bottom'
  });
})

.service('history', function (stork) {
  var history = [];
  var listeners = [];

  this.history = history;

  this.fetch = function (uri) {
    stork.history(uri).then(function (h) {
      history = h;
      for (var i = 0; i < listeners.length; i++)
        listeners[i](history);
    });
  };

  this.fetch();

  /** Add a callback for when history changes. */
  this.listen = function (f) {
    if (!angular.isFunction(f))
      return;
    listeners.push(f);
    f(history);
  };
})

.controller('History', function (history) {
  var me = this;
  this.list = [];
  history.listen(function (history) {
    me.list = history;
  });
})

.controller('Browse', function ($scope, stork, $q, $modal, $window, user, history) {
  // Reset (or initialize) the browse pane.
  $scope.reset = function () {
    $scope.uri = {};
    $scope.state = {disconnected: true};
    delete $scope.error;
    delete $scope.root;
  };

  $scope.reset();

  // Set the endpoint URI.
  $scope.go = function (uri) {
    if (!uri)
      return $scope.reset();
    if (typeof uri === 'string')
      uri = new URI(uri).normalize();
    else
      uri = uri.clone().normalize();
    $scope.uri.parsed = uri;
    $scope.uri.text = uri.readable();
    if ($scope.uri.state)
      $scope.uri.state.changed = false;
    else
      $scope.uri.state = {};
    $scope.end.uri = uri.toString();

    delete $scope.root;
    delete $scope.state.disconnected;
    $scope.state.loading = true;

    history.fetch(uri.toString());

    $scope.fetch(uri).then(function (list) {
      delete $scope.state.loading;
      $scope.root = list;
      $scope.root.name = uri.readable();
      $scope.open = true;
      $scope.unselectAll();
    }, function (error) {
      delete $scope.state.loading;
      $scope.error = error;
    });
  };

  /* Reload the existing listing. */
  $scope.refresh = function () {
    $scope.go($scope.uri.parsed);
  };

  /* Fetch and cache the listing for the given URI. */
  $scope.fetch = function (uri) {
    var scope = this;
    scope.loading = true;
    delete scope.error;

    var ep = angular.copy($scope.end);
    ep.uri = uri.href();

    return stork.ls(ep, 1).then(
      function (d) {
        if (scope.root)
          d.name = scope.root.name || d.name;
        return scope.root = d;
      }, function (e) {
        return $q.reject(scope.error = e);
      }
    ).finally(function () {
      scope.loading = false;
    });
  };

  // Get the path URI of this item.
  $scope.path = function () {
    if ($scope.root === this.root)
      return $scope.uri.parsed.clone();
    return this.parent().path().segmentCoded(this.root.name);
  };

  // Open the mkdir dialog.
  $scope.mkdir = function () {
    $modal({
      title: 'Create Directory',
      contentTemplate: 'new-folder.html'
    }).$scope = $scope;

    result.then(function (pn) {
      var u = new URI(pn[0]).segment(pn[1]);
      return stork.mkdir(u.href()).then(
        function (m) {
          $scope.refresh();
        }, function (e) {
          alert('Could not create folder.');
        }
      );
    });
  };

  /* Open cred modal. */
  $scope.credModal = function () {
    $modal({
      title: 'Select Credential',
      contentTemplate: 'select-credential.html',
      scope: $scope
    });
  };

  // Delete the selected files.
  $scope.rm = function (uris) {
    _.each(uris, function (u) {
      if (confirm("Delete "+u+"?")) {
        return stork.delete(u).then(
          function () {
            $scope.refresh();
          }, function (e) {
            alert('Could not delete file: '+e);
          }
        );
      }
    });
  };

  // Download the selected file.
  $scope.download = function (uris) {
    if (uris == undefined || uris.length == 0)
      return alert('You must select a file.')
    else if (uris.length > 1)
      return alert('You can only download one file at a time.');

    var end = {
      uri: uris[0],
      credential: $scope.end.credential
    };
    stork.get(end);
  };

  // Return the scope corresponding to the parent directory.
  $scope.parent = function () {
    if (this !== $scope) {
      if (this.$parent.root !== this.root)
        return this.$parent;
      return this.$parent.parent();
    }
  };

  $scope.up_dir = function () {
    var u = $scope.uri.parsed;
    if (!u) return;
    u = u.clone().filename('../').normalize();
    $scope.go(u);
  };

  $scope.toggle = function () {
    var scope = this;
    var root = scope.root;
    if (root && root.dir && (scope.open = !scope.open) && !root.files) {
      scope.fetch(scope.path());
    }
  };

  $scope.select = function (e) {
    var scope = this;
    var u = this.path();

    // Unselect text.
    if (document.selection && document.selection.empty)
      document.selection.empty();
    else if (window.getSelection)
      window.getSelection().removeAllRanges();

    if (!this.selected) {
      if (!e.ctrlKey)
        $scope.unselectAll();
      $scope.end.$selected[u] = this.root;
      this.root.selected = true;
    } else {
      if (!e.ctrlKey)
        $scope.unselectAll();
      delete $scope.end.$selected[u];
      delete this.root.selected;
    }

    console.log($scope.end.$selected);
    console.log(this);
  };

  $scope.unselectAll = function () {
    var s = $scope.end.$selected;
    if (s) _.each(s, function (f) {
      f.selected = false;
    })
    $scope.end.$selected = { };
  };

  $scope.selectedUris = function () {
    return _.keys($scope.end.$selected);
  };

  $scope.openOAuth = function (url) {
    $window.oAuthCallback = function (uuid) {
      $scope.end.credential = {uuid: uuid};
      $scope.go($scope.uri.text);
      $scope.$apply();
    };
    var child = $window.open(url, 'oAuthWindow');
    return false;
  };
});
