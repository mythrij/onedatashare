angular.module('stork', ['ui.bootstrap', 'ui'],
  function ($provide, $routeProvider) {
    $routeProvider.when('/', {
      title: 'Home',
      templateUrl: 'home.html'
    }).when('/transfer', {
      title: 'Transfer',
      templateUrl: 'transfer.html',
      controller: TransferCtrl,
      requireLogin: true
    }).when('/login', {
      title: 'Login',
      templateUrl: 'login.html',
      controller: LoginCtrl
    }).when('/user', {
      title: 'User Settings',
      templateUrl: 'user.html',
      controller: LoginCtrl
    }).when('/terms', {
      title: 'Terms of Service',
      templateUrl: 'terms.html'
    }).when('/privacy', {
      title: 'Privacy Policy',
      templateUrl: 'privacy.html'
    }).otherwise({
      redirectTo: '/'
    })
  }
).filter('size', function () {
  return function (bytes, precision) {
    var b = bytes     || 0
    var p = precision || 0
    var s = 'kMGTPEZY'
    for (var i = 0; i < s.length && b >= 1000; i++) {
      b /= 1000
      var c = s.charAt(i)
    }
    return c ? b.toFixed(p)+c : b.toFixed(0)
  }
}).filter('percent', function () {
  return function (p, precision, symbol) {
    if (!p || p.done < 0 || p.total <= 0) return
    var d = p.done
    var t = p.total
    var p = precision || 0
    var n = 100*d/t
    n = (n < 0) ? 0 : (n > 100) ? 100 : n
    return n.toFixed(p)+(symbol||'%')
  }
}).filter('progress', function (sizeFilter) {
  return function (p, precision, symbol) {
    if (!p) return
    var t = sizeFilter(p.total)
    var d = sizeFilter(p.done)
    return d+'/'+t
  }
}).filter('values', function () {
  return _.values
}).filter('keys', function () {
  return _.keys
}).filter('pairs', function () {
  return _.pairs
}).filter('moment', function () {
  return function (input, format) {
    return moment(input, format).fromNow()
  }
}).directive('bsRoute', function ($location) {
  return {
    link: function (scope, elm, attrs) {
      var check = function () {
        if ($location.path() == attrs.bsRoute)
          elm.addClass('active')
        else
          elm.removeClass('active')
      }
      scope.$on('$routeChangeSuccess',
        function (event, current, previous) {
          check()
        }
      )
    }
  }
}).config(function ($provide) {
  $provide.factory('$stork', function ($http, $q) {
    var api = function (r) {
      return '/api/stork/'+r
    }
    var gr = function (r) { return r.data }
    var ge = function (r) { return $q.reject(r.data) }
    return {
      $post: function (name, data) {
        return $http.post(api(name), data).then(gr, ge)
      },
      $get: function (name, data) {
        return $http.get(api(name), { params: data }).then(gr, ge)
      },
      login: function (info) {
        return this.$post('user', angular.extend({
          action: 'login'
        }, info))
      },
      register: function (info) {
        return this.$post('user', angular.extend({
          action: 'register'
        }, info))
      },
      ls: function (u, d) {
        return this.$get('ls', {
          'uri': u,
          'depth': d||0
        })
      },
      q: function (filter, range) {
        return this.$get('q', {
          'status': filter || 'all',
          'range': range
        })
      },
      submit: function (job) {
        return this.$post('submit', job)
      }
    }
  })
}).config(function ($provide) {
  $provide.factory('$user', function ($stork, $location, $rootScope) {
    return {
      user: function () {
        return $rootScope.$user
      },
      saveLogin: function (u) {
        $rootScope.$user = {
          user_id:   u.user_id,
          pass_hash: u.pass_hash
        }
        var exp = { expires: 31536000 }
        Cookies.set('user_id',   u.user_id,   exp)
        Cookies.set('pass_hash', u.pass_hash, exp)
      },
      forgetLogin: function () {
        delete $rootScope.$user
        Cookies.expire('user_id')
        Cookies.expire('pass_hash')
      },
      logIn: function (info) {
        if (info)
          return $stork.login(info).then(this.saveLogin, this.forgetLogin)
      },
      checkAccess: function (redirectTo) {
        if (!$rootScope.$user)
          $location.path(redirectTo||'/')
      },
      $init: function () {
        var u = {
          user_id:   Cookies.get('user_id'),
          pass_hash: Cookies.get('pass_hash')
        }

        if (u.user_id && u.pass_hash)
          this.logIn(u)
        else
          this.forgetLogin(u)

        delete this.$init
        return this
      }
    }.$init()
  })
}).config(function ($tooltipProvider) {
  $tooltipProvider.options({
    appendToBody: true,
    animation: false
  })
}).run(function ($location, $rootScope, $user) {
  $rootScope.$on('$routeChangeSuccess',
    function (event, current, previous) {
      if (!current.$$route)
        return
      if (current.$$route.requireLogin)
        $user.checkAccess()
      $rootScope.$template = current.templateUrl
      $rootScope.$title = 'StorkCloud - '+current.$$route.title
    }
  )
})

function LoginCtrl($scope, $stork, $location, $user) {
  $scope.logOut = function () {
    $user.forgetLogin()
    $location.path('/login')
  }
  $scope.logIn = function (u) {
    $user.logIn(u).then(function () {
      $location.path('/')
    }, function (e) {
      alert(error)
    })
  }
}

function RegisterCtrl($scope, $stork, $location, $user, $modal) {
  $scope.register = function (u) {
    return $stork.register(u).then(function (d) {
      $user.saveLogin(d)
      $location.path('/')
      delete $scope.user
    }, function (e) {
      alert(e)
    })
  }
}

function TransferCtrl($scope, $user, $stork, $modal) {
  var makeEnd = function (o) {
    var u = o.uri
    for (u in o.selected) break
    return {
      uri: u,
      cred: o.cred,
      module: o.module
    }
  }

  $scope.left = {
    selected: { }
  }
  $scope.right = {
    selected: { }
  }

  $scope.job = function (s, d) {
    return {
      src:  makeEnd(s),
      dest: makeEnd(d)
    }
  }

  $scope.canTransfer = function (src, dest, contents) {
    if (!src || !dest || !src.uri || !dest.uri)
      return false
    if (_.size(src.selected) < 1 || _.size(dest.selected) != 1)
      return false
    return true
  }

  $scope.transfer = function (src, dest, contents) {
    if ($scope.src = src)
    if ($scope.dest = dest) $modal.open({
      templateUrl: 'xfer_modal.html',
      scope: $scope
    })
  }

  $scope.submit = function (job) {
    return $stork.submit(job).then(
      function (d) {
        return d
      }, function (e) {
        alert(e)
        return e
      }
    )
  }
}

function BrowseCtrl($scope, $stork, $q) {
  $scope.uri_state = { }
  $scope.show_dots = false

  var is_hidden = function (f) {
    return f.name && f.name.charAt(0) == '.'
  }

  var all_hidden = function (f) {
    if (!f.dir || !f.files || f.files.length == 0)
      return false
    for (k in f.files)
      if (!is_hidden(f.files[k])) return false
    return true
  }

  // Fetch and cache the listing for the given URI.
  var fetch = function (u) {
    var scope = this
    return $stork.ls(u.toString(), 1).then(
      function (d) {
        for (k in d.files) d.files[k].$uri = function () {
          return u.clone().segment(this.name)
        }
        return d
      }
    )
  }

  // Get or set the endpoint URI.
  $scope.uri = function (u) {
    if (u === undefined) {
      return $scope.end.uri
    } if (!u) {
      delete $scope.end.uri
    } else {
      if (typeof u === 'string') {
        var u = new URI(u).normalize()
      } else {
        u = u.clone().normalize()
      }

      $scope.temp_uri = u.toString()
      $scope.uri_state.changed = false
      return $scope.end.uri = u
    }
  }

  $scope.refresh = function (u) {
    if (u === undefined)
      u = $scope.temp_uri
    u = $scope.uri(u)

    if (!u) {
      delete $scope.root
      $scope.uri_state.changed = false
    } else {
      $scope.root = fetch(u).then(
        function (f) {
          if (f.dir)
            $scope.uri(u = u.filename(u.filename()+'/'))
          f.$open = true
          f.name = u.toString()
          f.$uri = function () { return u.clone() }
          return f
        }, function (e) {
          return { $error: e }
        }, function () {
          return this.f
        }
      )
    } $scope.unselect()
  }

  $scope.up_dir = function () {
    var u = $scope.uri()
    if (!u) return
    u = u.clone().filename('../').normalize()
    $scope.refresh(u)
  }

  $scope.tree_classes = function (f) {
    return {
      'dir': f.dir,
      'file': !f.dir,
      'loading': f.$loading,
      'alert-warning': !!f.$error,
      'open': f.$open,
      'dot': is_hidden(f) && !f.$selected,
      'alert-info': f.$selected,
      'empty': f.files && f.files.length == 0
    }
  }

  $scope.toggle = function (f) {
    if (f && f.dir)
    if (f.$open = !f.$open)
    if (!f.files) {
      // We're opening, fetch subdirs if we haven't.
      f.files = fetch(f.$uri(), f).then(
        function (f) {
          return f.files
        }, function (e) {
          f.$error = e
        }
      )
    }
  }

  $scope.all_hidden = all_hidden

  $scope.select = function (f) {
    var u = f.$uri().toString()
    if (!this.end.selected[u]) {
      this.end.selected[u] = f
      f.$selected = true
    } else {
      delete this.end.selected[u]
      delete f.$selected
    }
  }

  $scope.unselect = function () {
    this.end.selected = { }
  }

  $scope.selected_files = function () {
    return Object.keys(this.end.selected)
  }
}

function CredCtrl($scope, $modal) {
  $scope.creds = [ ]
  $scope.credChanged = function () {
    if ($scope.end.cred == 'new-myproxy') $modal.open({
      templateUrl: 'add_cred_modal.html',
      scope: $scope
    }).result.then(function (c) {
      $scope.end.cred = c
    }, function () {
      delete $scope.end.cred
    })
  }
}

function QueueCtrl($scope, $rootScope, $stork, $timeout) {
  $scope.filter_set = {
    all: {
      scheduled:  true, processing: true, paused:   true,
      removed:    true, failed:     true, complete: true
    },
    pending: {
      scheduled:  true, processing: true, paused:   true
    },
    done: {
      removed:    true, failed:     true, complete: true
    },
    scheduled:  { scheduled:  true },
    processing: { processing: true },
    paused:     { paused:     true },
    removed:    { removed:    true },
    failed:     { failed:     true },
    complete:   { complete:   true }
  }
  $scope.filters = [
    'scheduled', 'processing', 'paused',
    'removed', 'failed', 'complete', null,
    'pending', 'done', 'all'
  ]
  $scope.filter = 'pending'

  $scope.jobs = { }
  $scope.auto = true

  $scope.$on('$destroy', function (event) {
    // Clean up the auto-refresh timer.
    if ($rootScope.autoTimer) {
      $timeout.cancel($rootScope.autoTimer)
      delete $rootScope.autoTimer
    }
  })

  $scope.toggleAuto = function () {
    if ($scope.auto = !$scope.auto)
      $scope.autoRefresh()
  }

  $scope.autoRefresh = function () {
    if ($rootScope.autoTimer) {
      $timeout.cancel($rootScope.autoTimer)
      delete $rootScope.autoTimer
    } if ($scope.auto) {
      $scope.refresh().then(function () {
        $rootScope.autoTimer = $timeout($scope.autoRefresh, 1000)
      })
    }
  }

  $scope.set_filter = function (f) {
    $scope.filter = f
  }

  $scope.job_filter = function (j) {
    return j && $scope.filter_set[$scope.filter][j.status]
  }

  $scope.refresh = function () {
    return $stork.q().then(
      function (jobs) {
        for (var i in jobs) {
          var j = jobs[i]
          var i = j.job_id+''
          if (!i)
            continue
          if (!$scope.jobs)
            $scope.jobs = { }
          if ($scope.jobs[i])
            angular.extend($scope.jobs[i], j)
          else
            $scope.jobs[i] = j
        }
      }
    )
  }
  $scope.color = function (j) {
    return {
      processing: 'progress-bar-success',
      scheduled:  'progress-bar-warning',
      complete:   '',
      removed:    'progress-bar-danger',
      failed:     'progress-bar-danger'
    }[j.status]
  }

  $scope.autoRefresh()
}

// Collapsible panels
$(document).on('click', '.panel-collapse-header', function (e) {
  var h = $(this).closest('.panel')
  var b = h.find('.collapse')
  b.slideToggle(function () {
    h.toggleClass('panel-collapsed')
  })
})

// Tooltips
$(document).on('mouseover', '[tips]', function () {
  $(this).tooltip({
    'animation': false,
    'container': 'body'
  })
  $(this).tooltip('show')
})
