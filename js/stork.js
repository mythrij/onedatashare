// Some fixes until Angular UI merges Bootstrap 3 changes.
angular.module('template/pagination/pagination.html', []).run(
  function($templateCache) {
    $templateCache.put("template/pagination/pagination.html",
      "<ul class=\"pagination\">\n" +
      "  <li ng-repeat=\"page in pages\">" +
      "    <a href ng-class=\"{'btn-primary active': page.active, disabled: page.disabled}\" ng-click=\"selectPage(page.number)\">{{page.text}}</a>" +
      "</li></ul>\n")
  }
);

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
    }).when('/user', {
      title: 'User Settings',
      templateUrl: 'user.html',
      requireLogin: true
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
}).filter('paginate', function () {
  return function (input, page, per) {
    page = (!page || page < 1) ? 1  : page
    per  = (!per  || per  < 1) ? 10 : per
    return input.slice(per*(page-1), per*page)
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
}).directive('focusMe', function ($timeout) {    
  return {    
    link: function (scope, element, attrs, model) {                
      $timeout(function () {
        $(element[0]).focus()
      }, 20)
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
      mkdir: function (uri) {
        return this.$post('mkdir', {
          'uri': name
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
          email:     u.email,
          hash: u.hash
        }
        var exp = { expires: 31536000 }
        Cookies.set('user.email', u.email, exp)
        Cookies.set('user.hash', u.hash, exp)
      },
      forgetLogin: function () {
        delete $rootScope.$user
        Cookies.expire('user.email')
        Cookies.expire('user.hash')
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
          email: Cookies.get('user.email'),
          hash:  Cookies.get('user.hash')
        }

        if (u.email && u.hash)
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

function LoginCtrl($scope, $stork, $location, $user, $modal) {
  $scope.logOut = function () {
    $user.forgetLogin()
    $location.path('/login')
  }
  $scope.logIn = function (u) {
    console.log($scope.user)
    return $user.logIn(u).then(function () {
      $scope.$close()
    }, function (e) {
      alert(error)
    })
  }
  $scope.loginModal = function () {
    $modal.open({
      templateUrl: '/parts/login.html',
      controller: LoginCtrl
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

function BrowseCtrl($scope, $stork, $q, $modal) {
  $scope.uri_state = { }
  $scope.showHidden = false

  // Fetch and cache the listing for the given URI.
  $scope.fetch = function (u) {
    var scope = this
    scope.loading = true
    return $stork.ls(u.href(), 1).then(
      function (d) {
        for (k in d.files) d.files[k].$uri = function () {
          return u.clone().segment(URI.encode(this.name))
        }
        return scope.root = d
      }, function (e) {
        return $q.reject(scope.error = e)
      }
    ).always(function () {
      scope.loading = false
    })
  }

  // Open the mkdir dialog.
  $scope.mkdir = function () {
    $modal.open({
      templateUrl: 'new-folder.html'
    }).result.then(function (name) {
      alert(name)
    })
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

      $scope.temp_uri = u.readable()
      $scope.uri_state.changed = false
      return $scope.end.uri = u
    }
  }

  $scope.refresh = function (u) {
    if (u === undefined)
      u = $scope.temp_uri
    u = $scope.uri(u)

    // Clean up from last refresh.
    $scope.unselect()
    delete $scope.error
    delete $scope.root

    if (!u) {
      delete $scope.root
      $scope.uri_state.changed = false
    } else {
      $scope.open = true
      $scope.fetch(u).then(
        function (f) {
          if (f.dir)
            $scope.uri(u = u.filename(u.filename()+'/'))
          f.name = u.toString()
          f.$uri = function () { return u.clone() }
          return f
        }, function (e) {
          $scope.error = e
        }
      )
    }
  }

  $scope.up_dir = function () {
    var u = $scope.uri()
    if (!u) return
    u = u.clone().filename('../').normalize()
    $scope.refresh(u)
  }

  $scope.toggle = function () {
    var scope = this
    if (scope.root && scope.root.dir)
    if (scope.open = !scope.open)
    if (!scope.root.files) {
      // We're opening, fetch subdirs if we haven't.
      scope.fetch(scope.root.$uri(), scope.root)
    }
  }

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
    return _.keys(this.end.selected)
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
  }
  $scope.filterList = [
    'scheduled', 'processing', 'paused',
    'removed', 'failed', 'complete', null,
    'pending', 'done', 'all'
  ]
  $scope.filter = 'pending'

  $scope.jobs = { }
  $scope.auto = true

  // Pagination
  $scope.perPage = 5
  $scope.page = 1

  $scope.pager = function (len) {
    var s = $scope.page-2
    if (s <= 0)
      s = 1
    var e = s+5
    if (e > len/$scope.perPage)
      e = len/$scope.perPage+1
    return _.range(s, e+1)
  }
  $scope.setPage = function (p) {
    $scope.page = p
  }
  $scope.numPages = function (len) {
    
  }

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
      }, function () {
        $scope.auto = false
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
$(document).on('mouseover', '[title]', function () {
  $(this).tooltip({
    'animation': false,
    'container': 'body'
  })
  $(this).tooltip('show')
})
