'use strict';

/** Module for doing stuff with credentials. */
angular.module('stork.credentials', [])

.controller('Credentials', function ($scope, $modal, stork) {
  $scope.creds = {};
  stork.cred().then(function (creds) {
    $scope.creds = creds;
  });

  /* Open a modal, return a future credential. */
  $scope.newCredential = function (type) {
    return $modal.open({
      templateUrl: 'add-cred-modal.html',
      scope: $scope
    }).result;
  };
})

/** Controller for selecting credentials. */
.controller('SelectCredential', function ($scope) {
  $scope.cred = angular.copy($scope.end.credential);

  if ($scope.cred)
    $scope.selected = $scope.cred.uuid || $scope.cred.type;

  $scope.saveCredential = function (cred) {
    $scope.end.credential = cred;
    $scope.$hide();
  };

  $scope.changeSelection = function (s) {
    if (!s) {
      $scope.cred = undefined;
    } else if (s == 'gss') {
      $scope.cred = {type: s};
    } else {
      $scope.cred = {uuid: s};
    }
  };
})

.controller('OAuth', function ($routeParams, $window) {
  var uuid = $routeParams.uuid;
  $window.opener.oAuthCallback(uuid);
  $window.close();
});
