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
  this.setPassword = function (op, np) {
    return stork.postUser({
      action: "password",
      oldPassword: op,
      newPassword: np
    });
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

.controller('User', function ($scope, $modal, $location, user) {
  /* If info is given, log the user in. Otherwise show modal. */
  $scope.login = function (info, then) {
    if (!info)
      return $modal({
        title: 'Log in',
        container: 'body',
        contentTemplate: '/app/user/login.html'
      });
    return user.login(info).then(function (v) {
      if (then)
        then(v);
      $modal({
        title: "Welcome!",
        content: "You have successfully logged in.",
        show: true
      });
    }, function (error) {
      $scope.error = error;
    });
  };

  /* Log the user out. */
  $scope.logout = function () {
    user.forgetLogin();
    $location.path('/');
    $modal({
      content: "You have successfully logged out.",
      show: true
    });
  };

  $scope.changePassword = function (op, np1, np2) {
    if (np1 != np2) {
      $modal({
        title: "Mismatched password",
        content: "The passwords do not match.",
        show: true
      });
      return false;
    }

    return user.setPassword(op, np1).then(function (d) {
      $modal({
        title: "Success!",
        content: "Password successfully changed! Please log in again.",
        show: true
      });
      user.forgetLogin();
    }, function (e) {
      $modal({
        title: "Error",
        content: e.error,
        show: true
      });
    });
  };
})

.controller('Register', function ($scope, stork, user, $modal) {
  $scope.register = function (u) {
    return stork.register(u).then(function (d) {
      $modal({
        title: "Welcome!",
        content: "Thank for you registering with StorkCloud! "+
                 "Please check your email for further instructions.",
        show: true
      });
      delete $scope.user;
    }, function (e) {
      $modal({
        title: "Registration Problem",
        content: e.error,
        show: true
      });
    })
  }
});
