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
      controller: 'TransferCtrl',
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
    rm: function (id) {
      return this.$post('rm', {
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

.controller('TransferCtrl', function ($scope, user, stork, $modal) {
  // Hardcoded options.
  $scope.optSet = [{
      'title': 'Use transfer optimization',
      'param': 'optimizer',
      'description':
        'Automatically adjust transfer options using the given optimization algorithm.',
      'choices': [ ['None', null], ['2nd Order', '2nd_order'], ['PCP', 'pcp'] ]
    },{
      'title': 'Overwrite existing files',
      'param': 'overwrite',
      'description':
        'By default, destination files with conflicting file names will be overwritten. '+
        'Saying no here will cause the transfer to fail if there are any conflicting files.',
      'choices': [ ['Yes', true], ['No', false] ]
    },{
      'title': 'Verify file integrity',
      'param': 'verify',
      'description':
        'Enable this if you want checksum verification of transferred files.',
      'choices': [ ['Yes', true], ['No', false] ]
    },{
      'title': 'Encrypt data channel',
      'param': 'encrypt',
      'description':
        'Enables data transfer encryption, if supported. This provides additional data security '+
        'at the cost of transfer speed.',
      'choices': [ ['Yes', true], ['No', false] ]
    },{
      'title': 'Compress data channel',
      'param': 'compress',
      'description':
        'Compresses data over the wire. This may improve transfer '+
        'speed if the data is text-based or structured.',
      'choices': [ ['Yes', true], ['No', false] ]
    }
  ];

  $scope.job = {
    src:  $scope.left  = { },
    dest: $scope.right = { },
    options: {
      'optimizer': null,
      'overwrite': true,
      'verify'   : false,
      'encrypt'  : false,
      'compress' : false
    }
  };

  $scope.canTransfer = function (src, dest, contents) {
    if (!src || !dest || !src.uri || !dest.uri)
      return false;
    if (_.size(src.$selected) < 1 || _.size(dest.$selected) != 1)
      return false;
    return true;
  };

  $scope.transfer = function (src, dest, contents) {
    var job = angular.copy($scope.job);
    job.src.uri  = _.keys(src.$selected);
    job.dest.uri = _.keys(dest.$selected);

    $modal.open({
      templateUrl: 'xfer-modal.html',
      controller: function ($scope) {
        $scope.job = job
      }
    }).result.then(function (job) {
      return stork.submit(job).then(
        function (d) {
          alert('Job submitted successfully!');
          return d;
        }, function (e) {
          alert(e);
        }
      )
    });
  };
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
}).controller('QueueCtrl', function ($scope, $rootScope, stork, $timeout) {
  $scope.filters = {
    all: function (j) {
      return true
    },
    pending: function (j) {
      return {
        scheduled: true, processing: true, paused: true
      }[j.status]
    },
    done: function (j) {
      return {
        removed: true, failed: true, complete: true
      }[j.status]
    },
    scheduled:  function (j) { return j.status == 'scheduled' },
    processing: function (j) { return j.status == 'processing' },
    paused:     function (j) { return j.status == 'paused' },
    removed:    function (j) { return j.status == 'removed' },
    failed:     function (j) { return j.status == 'failed' },
    complete:   function (j) { return j.status == 'complete' },
  };
  $scope.filterList = [
    'scheduled', 'processing', 'paused',
    'removed', 'failed', 'complete', null,
    'pending', 'done', 'all'
  ];
  $scope.filter = 'all';

  $scope.jobs = { };
  $scope.auto = true;

  // Pagination
  $scope.perPage = 5;
  $scope.page = 1;

  $scope.pager = function (len) {
    var s = $scope.page-2;
    if (s <= 0)
      s = 1;
    var e = s+5;
    if (e > len/$scope.perPage)
      e = len/$scope.perPage+1;
    return _.range(s, e+1);
  };
  $scope.setPage = function (p) {
    $scope.page = p;
  };

  $scope.$on('$destroy', function (event) {
    // Clean up the auto-refresh timer.
    if ($rootScope.autoTimer) {
      $timeout.cancel($rootScope.autoTimer);
      delete $rootScope.autoTimer;
    }
  });

  $scope.toggleAuto = function () {
    if ($scope.auto = !$scope.auto)
      $scope.autoRefresh();
  };

  $scope.autoRefresh = function () {
    if ($rootScope.autoTimer) {
      $timeout.cancel($rootScope.autoTimer);
      delete $rootScope.autoTimer;
    } if ($scope.auto) {
      $scope.refresh().then(function () {
        $rootScope.autoTimer = $timeout($scope.autoRefresh, 1000)
      }, function () {
        $scope.auto = false
      });
    }
  };

  $scope.cancel = function (j) {
    if (j.job_id &&
        confirm("Are you sure you want to remove job "+j.job_id+"?"))
      return stork.rm(j.job_id).then(
        function (m) {
          j.status = 'removed';
        }, function (e) {
          alert("failure!");
        }
      );
  };

  $scope.set_filter = function (f) {
    $scope.filter = f;
  };

  $scope.job_filter = function (j) {
    return j && $scope.filter_set[$scope.filter][j.status];
  }

  $scope.refresh = function () {
    return stork.q().then(
      function (jobs) {
        for (var i in jobs) {
          var j = jobs[i];
          var i = j.job_id+'';
          if (!i)
            continue;
          if (!$scope.jobs)
            $scope.jobs = { };
          if ($scope.jobs[i])
            angular.extend($scope.jobs[i], j);
          else
            $scope.jobs[i] = j;
        }
      }
    );
  };
  $scope.color = {
    processing: 'progress-bar-success progress-striped active',
    scheduled:  'progress-bar-warning',
    complete:   '',
    removed:    'progress-bar-danger',
    failed:     'progress-bar-danger'
  };

  $scope.autoRefresh();
});
