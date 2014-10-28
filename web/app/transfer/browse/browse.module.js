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

.controller('Browse', function ($scope, stork, $q, $modal, user) {
  // Reset (or initialize) the browse pane.
  $scope.reset = function () {
    $scope.uri = {};
    $scope.state = {};
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
    $scope.uri.state.changed = false;
    $scope.end.uri = uri.toString();

    delete $scope.root;

    $scope.fetch(uri).then(function (list) {
      $scope.root = list;
      $scope.root.name = uri.readable();
      $scope.open = true;
      $scope.unselectAll();
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
    $modal.open({
      templateUrl: 'new-folder.html',
      scope: $scope
    }).result.then(function (pn) {
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
    console.log(uris);
    if (uris == undefined || uris.length == 0)
      alert('You must select a file.')
    else if (uris.length > 1)
      alert('You can only download one file at a time.');
    else
      stork.get(uris[0]);
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
      $scope.end.$selected[u] = new function () {
        this.unselect = function () {
          delete scope.selected;
        };
      };
      this.selected = true;
    } else {
      if (!e.ctrlKey)
        $scope.unselectAll();
      delete $scope.end.$selected[u];
      delete this.selected;
    }
  };

  $scope.unselectAll = function () {
    var s = $scope.end.$selected;
    if (s) _.each(s, function (f) {
      f.unselect();
    })
    $scope.end.$selected = { };
  };

  $scope.selectedUris = function () {
    return _.keys($scope.end.$selected);
  };
});
