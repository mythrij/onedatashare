'use strict';

/** The main Stork AngularJS module. Welcome! */
angular.module('stork', [
  'ngRoute', 'ngResource', 'ui', 'cgBusy', 'pasvaz.bindonce',
  'mgcrea.ngStrap', 'mgcrea.ngStrap.collapse', 'mgcrea.ngStrap.tooltip',
  'stork.util', 'stork.user', 'stork.transfer'
  ], function ($provide, $routeProvider) {
    /* This is where you can add routes and change page titles. */
    $routeProvider.when('/', {
      title: 'Home',
      templateUrl: 'app/home/home.html'
    }).when('/transfer', {
      title: 'Transfer',
      templateUrl: 'app/transfer/transfer.html',
      controller: 'Transfer',
      requireLogin: true
    }).when('/user', {
      title: 'User Settings',
      templateUrl: 'app/user/user.html',
      requireLogin: true
    }).when('/terms', {
      title: 'Terms of Service',
      templateUrl: 'app/legal/terms.html'
    }).when('/privacy', {
      title: 'Privacy Policy',
      templateUrl: 'app/legal/privacy.html'
    }).otherwise({
      redirectTo: '/'
    });
  }
)

/** Provides easy access to Stork API resources. */
.factory('stork', function ($window, $http, $q) {
  var gr = function (r) { return r.data };
  var ge = function (r) { return $q.reject(r.data.error) };
  return {
    $uri: function (path, query) {
      var uri = new URI('/api/stork/'+path);
      if (typeof query === 'object')
        query = URI.buildQuery(query);
      if (typeof query === 'string')
        uri.query(query);
      return uri.readable();
    },
    $post: function (name, data) {
      return $http.post(this.$uri(name), data).then(gr, ge);
    },
    $get: function (name, data) {
      return $http.get(this.$uri(name, data)).then(gr, ge);
    },
    $download: function (uri) {
      uri = this.$uri('get', {uri: uri});
      console.log(uri);
      $window.open(uri);
    },
    login: function (info) {
      return this.$post('user', angular.extend({
        action: 'login'
      }, info));
    },
    register: function (info) {
      return this.$post('user', angular.extend({
        action: 'register'
      }, info));
    },
    history: function (uri) {
      if (uri) return this.$post('user', {
        action: 'history',
        uri: uri
      });
      return this.$get('user', {action: 'history'});
    },
    ls: function (ep, d) {
      if (typeof ep === 'string')
        ep = { uri: ep };
      return this.$post('ls', angular.extend(angular.copy(ep), {
        depth: d||0
      }));
    },
    cancel: function (id) {
      return this.$post('cancel', {
        range: id
      });
    },
    q: function (filter, range) {
      return this.$post('q', {
        'status': filter || 'all',
        'range': range
      });
    },
    mkdir: function (ep) {
      if (typeof ep === 'string')
        ep = { uri: ep };
      return this.$post('mkdir', ep);
    },
    delete: function (ep) {
      if (typeof ep === 'string')
        ep = { uri: ep };
      return this.$post('delete', ep);
    },
    submit: function (job) {
      return this.$post('submit', job);
    },
    get: function (uri) {
      this.$download(uri);
    }
  };
})

.config(function ($tooltipProvider) {
  /* Configure AngularStrap tooltips. */
  angular.extend($tooltipProvider.defaults, {
    animation: false,
    placement: 'top',
    container: 'body',
    trigger: 'focus hover'
  });
})

.value('cgBusyDefaults',{
    message:'Loading',
    backdrop: false,
    delay: 200
})

.run(function ($location, $document, $rootScope, user) {
  $rootScope.$on('$routeChangeSuccess',
    function (event, current, previous) {
      if (!current.$$route)
        return;
      if (current.$$route.requireLogin)
        user.checkAccess();
      $document[0].title = 'StorkCloud - '+current.$$route.title;
    }
  );
})

.controller('RegisterCtrl', function ($scope, stork, $location, user, $modal) {
  $scope.register = function (u) {
    return stork.register(u).then(function (d) {
      user.saveLogin(d);
      $location.path('/');
      delete $scope.user;
    }, function (e) {
      alert(e);
    })
  }
})

.controller('CredCtrl', function ($scope, $modal) {
  $scope.creds = [ ];
  /* Open a modal, return a future credential. */
  $scope.newCredential = function (type) {
    return $modal.open({
      templateUrl: 'add-cred-modal.html',
      scope: $scope
    }).result;
  };
});
