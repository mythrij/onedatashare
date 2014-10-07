'use strict';

/** Everything user-related. */
angular.module('stork.user', [
  'ngCookies', 'stork'
])

.service('user', function (stork, $location, $rootScope, $cookies) {
  this.user = function () {
    return $rootScope.$user;
  };
  this.login = function (info) {
    if (info) {
      var f = stork.login(info);
      f.then(this.saveLogin, this.forgetLogin);
      return f;
    }
  };
  this.saveLogin = function (u) {
    $rootScope.$user = u;
    $cookies.email = u.email;
    $cookies.hash = u.hash;
  };
  this.forgetLogin = function () {
    delete $rootScope.$user;
    delete $cookies.email;
  };
  this.checkAccess = function (redirectTo) {
    if (!$rootScope.$user)
      $location.path(redirectTo||'/');
  };
  this.history = function (uri) {
    return (uri || !$rootScope.$user.history) ?
      stork.history(uri).then(function (h) {
        return $rootScope.$user.history = h;
      }) : $rootScope.$user.history;
  };

  // If there's a cookie, attempt to log in.
  var u = {
    email: $cookies.email,
    hash:  $cookies.hash
  };

  if (u.email && u.hash)
    this.login(u);
  else
    this.forgetLogin();
})

.controller('UserCtrl', function ($scope, stork, user) {
  $scope.login = function (info) {
    return user.login(info);
  };
});
