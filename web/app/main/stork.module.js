'use strict';

angular.module('stork', ['ngRoute', 'ui.bootstrap', 'ui', 'stork.util'],
  function ($provide, $routeProvider) {
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
).factory('Stork', function ($http, $q) {
  var api = function (r) {
    return '/api/stork/'+r
  };
  var gr = function (r) { return r.data };
  var ge = function (r) { return $q.reject(r.data.error) };
  return {
    $post: function (name, data) {
      return $http.post(api(name), data).then(gr, ge);
    },
    $get: function (name, data) {
      return $http.get(api(name), data).then(gr, ge);
    },
    $download: function (uri) {
      uri = api('get')+'?uri='+uri;
      var id = 'hiddenDownloadFrame';
      var iframe = document.getElementById(id);
      if (!iframe) {
        iframe = document.createElement('iframe');
        iframe.id = id;
        iframe.style.display = 'none';
        document.body.appendChild(iframe);
      }
      iframe.src = uri;
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
      return this.$post('user', {
        action: 'history',
        uri: uri
      });
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
}).factory('User', function (Stork, $location, $rootScope) {
  return {
    user: function () {
      return $rootScope.$user;
    },
    saveLogin: function (u) {
      $rootScope.$user = u;
      var exp = { expires: 31536000 };
      Cookies.set('email', u.email, exp);
      Cookies.set('hash', u.hash, exp);
    },
    forgetLogin: function (error) {
      delete $rootScope.$user;
      Cookies.expire('email');
    },
    login: function (info) {
      if (info) {
        var f = Stork.login(info);
        f.then(this.saveLogin, this.forgetLogin);
        return f;
      }
    },
    checkAccess: function (redirectTo) {
      if (!$rootScope.$user)
        $location.path(redirectTo||'/');
    },
    history: function (uri) {
      return uri ? Stork.history(uri).then(function (h) {
        return $rootScope.$user.history = h;
      }) : $rootScope.$user.history();
    },
    $init: function () {
      var u = {
        email: Cookies.get('email'),
        hash:  Cookies.get('hash')
      };

      if (u.email && u.hash)
        this.login(u);
      else
        this.forgetLogin(u);

      delete this.$init;
      return this;
    }
  }.$init()
}).config(function ($tooltipProvider) {
  $tooltipProvider.options({
    appendToBody: true,
    animation: false
  });
}).run(function ($location, $document, $rootScope, User) {
  $rootScope.$on('$routeChangeSuccess',
    function (event, current, previous) {
      if (!current.$$route)
        return;
      if (current.$$route.requireLogin)
        User.checkAccess();
      $document[0].title = 'StorkCloud - '+current.$$route.title;
    }
  );
}).controller('LoginCtrl', function ($scope, Stork, $location, User, $modal) {
  $scope.logout = function () {
    User.forgetLogin();
    $location.path('/login');
  };
  $scope.login = function (u) {
    return User.login(u).then(function (o) {
      $scope.$close();
    }, function (e) {
      alert(e);
    });
  };
  $scope.loginModal = function () {
    $modal.open({
      templateUrl: '/app/user/login.html',
      controller: 'LoginCtrl',
      size: 'sm'
    });
  };
}).controller('RegisterCtrl', function ($scope, Stork, $location, User, $modal) {
  $scope.register = function (u) {
    return Stork.register(u).then(function (d) {
      User.saveLogin(d);
      $location.path('/');
      delete $scope.user;
    }, function (e) {
      alert(e);
    })
  }
}).controller('TransferCtrl', function ($scope, User, Stork, $modal) {
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
      return Stork.submit(job).then(
        function (d) {
          alert('Job submitted successfully!');
          return d;
        }, function (e) {
          alert(e);
        }
      )
    });
  };
}).controller('BrowseCtrl', function ($scope, Stork, $q, $modal, User) {
  $scope.uri_state = { };
  $scope.showHidden = false;

  // Fetch and cache the listing for the given URI.
  $scope.fetch = function (u) {
    var scope = this;
    scope.loading = true;

    var ep = angular.copy($scope.end);
    ep.uri = u.href();

    return Stork.ls(ep, 1).then(
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

  // Open the mkdir dialog.
  $scope.mkdir = function () {
    $modal.open({
      templateUrl: 'new-folder.html',
      scope: $scope
    }).result.then(function (pn) {
      var u = new URI(pn[0]).segment(pn[1]);
      return Stork.mkdir(u.href()).then(
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
        return Stork.delete(u).then(
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
      alert('You must select a file.')
    else if (uris.length > 1)
      alert('You can only download one file at a time.');
    else
      Stork.get(uris[0]);
  };

  // Return the scope corresponding to the parent directory.
  $scope.parentNode = function () {
    if (this !== $scope) {
      if (this.$parent.root !== this.root)
        return this.$parent;
      return this.$parent.parentNode();
    }
  };

  // Get or set the endpoint URI.
  $scope.uri = function (u) {
    if (u === undefined) {
      if (this.root !== $scope.root) {
        u = this.parentNode().uri();
        return u.segment(URI.encode(this.root.name));
      } else {
        return new URI($scope.end.uri);
      }
    } else if (!u) {
      if (this.root === $scope.root)
        delete $scope.end.uri;
    } else {
      if (typeof u === 'string') {
        var u = new URI(u).normalize();
      } else {
        u = u.clone().normalize();
      }

      $scope.temp_uri = u.readable();
      $scope.uri_state.changed = false;
      return new URI($scope.end.uri = u.href());
    }
  };

  $scope.refresh = function (u) {
    if (u === undefined)
      u = $scope.temp_uri;
    u = $scope.uri(u);

    // Clean up from last refresh.
    $scope.unselectAll();
    delete $scope.error;
    delete $scope.root;

    // Add the URL to the local history.
    User.history(u.toString());

    if (!u) {
      delete $scope.root;
      $scope.uri_state.changed = false;
    } else {
      $scope.open = true;
      $scope.fetch(u).then(
        function (f) {
          if (f.dir)
            $scope.uri(u = u.filename(u.filename()+'/'));
          f.name = u.toString();
          return f
        }, function (e) {
          $scope.error = e;
        }
      )
    }
  };

  $scope.up_dir = function () {
    var u = $scope.uri();
    if (!u) return;
    u = u.clone().filename('../').normalize();
    $scope.refresh(u);
  };

  $scope.toggle = function () {
    var scope = this
    if (scope.root && scope.root.dir)
    if (scope.open = !scope.open)
    if (!scope.root.files) {
      // We're opening, fetch subdirs if we haven't.
      scope.fetch(scope.uri());
    }
  };

  $scope.select = function (e) {
    var scope = this;
    var u = this.uri().toString();

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
}).controller('CredCtrl', function ($scope, $modal) {
  $scope.creds = [ ];
  /* Open a modal, return a future credential. */
  $scope.newCredential = function (type) {
    return $modal.open({
      templateUrl: 'add-cred-modal.html',
      scope: $scope
    }).result;
  };
}).controller('QueueCtrl', function ($scope, $rootScope, Stork, $timeout) {
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
      return Stork.rm(j.job_id).then(
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
    return Stork.q().then(
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

// Collapsible panels
$(document).on('click', '.panel-collapse-header', function (e) {
  var h = $(this).closest('.panel');
  var b = h.find('.collapse');
  b.slideToggle(function () {
    h.toggleClass('panel-collapsed');
  });
});

// Tooltips
$(document).on('mouseover', '[title]', function () {
  $('.tooltip').remove();
  $(this).tooltip({
    animation: 'none',
    container: 'body',
    placement: 'auto top'
  });
  $(this).tooltip('show');
});
