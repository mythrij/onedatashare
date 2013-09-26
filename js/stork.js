angular.module('stork', ['$strap.directives'],
  function ($provide, $routeProvider, $locationProvider) {
    $routeProvider.
      when('/',         {templateUrl: 'home.html'}).
      when('/transfer', {templateUrl: 'transfer.html'}).
      when('/login',    {templateUrl: 'login.html'}).
      when('/terms',    {templateUrl: 'terms.html'}).
      when('/privacy',  {templateUrl: 'privacy.html'}).
      otherwise({redirectTo: '/'})

    $provide.factory('$stork', $stork)
  }
)

// A service for maintaining user session state.
$stork = function ($http) {
  var user = {
    user_id:   Cookies.get('user_id'),
    pass_hash: Cookies.get('pass_hash')
  }
  var stork = {
    api: function (r) {
      return '/api/stork/'+r
    },
    user: function () {
      return user
    },
    logged_in: function () {
      return user.user_id && user.pass_hash
    },
    logged_out: function () {
      return !this.logged_in()
    },
    logout: function () {
      user.user_id   = null
      user.pass_hash = null
      Cookies.expire('user_id')
      Cookies.expire('pass_hash')
    },
    execute: function (info, action) {
      var url = this.api('user')
      if (action) url += '?action='+action
      return $http.post(url, info).success(
        function (data) {
          user.user_id   = data.user_id
          user.pass_hash = data.pass_hash
          var exp = { expires: 3153600 }
          Cookies.set('user_id',   data.user_id,   exp)
          Cookies.set('pass_hash', data.pass_hash, exp)
        }
      ).error(this.logout)
    },
    login: function (info) {
      if (!info) info = user
      return this.execute(info, 'login')
    },
    register: function (info) {
      return this.execute(info, 'register')
    },
    ls: function (u, d) {
      return $http.post(this.api('ls'), {
        'uri': u,
        'depth': d||0
      })
    },
    q: function (s, range) {
      return $http.post(this.api('q'), {
        'status': s || 'all',
        'range': range
      }).then(function (r) {
        return r.data
      })
    }
  }

  // If login cookies are present, check them.
  if (stork.logged_in())
    stork.login(user)
  else
    stork.logout()

  return stork
}

function UserCtrl($scope, $location, $stork) {
  var info = { }
  $scope.info = info
  $scope.user = $stork.user()
  $scope.logout = function () {
    $stork.logout()
    $location.path('/login')
    return false
  }
  $scope.login_state = function () {
    return $stork.logged_in() ? 'logged-in' : 'logged-out'
  }
  $scope.login = function () {
    $stork.login($scope.info).success(function () {
      $location.path('/')
    }).error(function (data) {
      msg.show(data, 'danger')
    })
  }
  $scope.register= function () {
    $stork.register($scope.info).success(function () {
      $location.path('/')
    }).error(function (data) {
      msg.show(data, 'danger')
    })
  }
  $scope.logged_in = function () {
    return $stork.logged_in()
  }
  $scope.logged_out = function () {
    return $stork.logged_out()
  }
}

function TransferCtrl($scope) {
  $scope.ends = { }
  $scope.show_ends = function () {
    return this.ends['left'].toString()+' -> '+
           this.ends['right'].toString() 
  }
}

function BrowseCtrl($scope, $stork, $q) {
  $scope.temp_uri = ''
  $scope.uri_state = { }
  $scope.selected = null
  $scope.f = null  // The root of the tree.
  $scope.show_dots = false
  $scope.selected = null
  $scope.error = ''

  var munge_data = function (s, d) {
    for (k in d.files) d.files[k].uri = function () {
      return s.uri().clone().segment(this.name)
    }
  }

  // Determine which file browser this is.
  var w = $scope.w = function () {
    return $scope.right ? 'right' :
           $scope.left  ? 'left'  : 'unknown'
  }
  // Get or set the endpoint URI.
  $scope.uri = function (u) {
    if (u === '') {
      this.ends[w()] = undefined
    } else if (!u) {
      // Do nothing.
    } else if (typeof u === 'string') {
      var u = new URI(u).normalize()
      this.ends[w()] = u
      this.temp_uri = u.toString()
      this.uri_state.changed = false
    } else if (u) {
      u = u.clone().normalize()
      this.ends[w()] = u
      this.temp_uri = u.toString()
      this.uri_state.changed = false
    } return this.ends[w()]
  }
  $scope.refresh = function (u) {
    if (u === undefined)
      u = this.temp_uri
    u = this.uri(u)

    if (!u)
      return this.f = null

    var eq = $q.defer()
    this.error = eq.promise
    this.f = $stork.ls(u.toString(), 1).then(
      function (o) {
        if (o.data.dir) {
          u.filename(u.filename()+'/').normalize()
          $scope.uri(u)
        }
        o.data.uri = function () { return u }
        o.data.name = u.toString()
        munge_data(o.data, o.data)
        o.data.open = o.data.dir
        o.data.shown = o.data.files
        eq.resolve(null)

        return o.data
      }, function (e) {
        eq.resolve(e.data)
      }
    )
  }
  $scope.size = function (f) {
    return prettySize(f.size, 1)
  }
  $scope.up_dir = function () {
    var u = this.uri()
    if (!u) return
    u = u.clone().filename('..').normalize()
    this.refresh(u)
  }
  $scope.tree_classes = function (d) {
    return {
      dir: d.dir,
      file: !d.dir,
      loading: d.loading,
      inaccessible: !!d.error,
      open: d.open,
      dot: d.name[0] == '.',
      selected: this.selected == d
    }
  }
  $scope.toggle = function (d) {
    if (!d.dir) {
      return
    } if (d.open = !d.open) {
      // We're opening, fetch subdirs if we haven't.
      if (!d.files) {
        d.loading = true
        d.files = $stork.ls(d.uri().toString(), 1).then(
          function (o) {
            munge_data(d, o.data)
            d.loading = false
            d.shown = d.files
            return o.data.files
          }
        )
      } else {
        d.shown = d.files
      }
    } else {
      // We're closing, go ahead and destroy the DOM.
      //d.shown = []
      d.shown = d.files
    }
  }
  $scope.is_empty = function (d) {
    return $.isEmptyObject(d)
  }
  $scope.select = function (s) {
    this.selected = s
  }
}

function CredCtrl($scope) {
  $scope.creds = [ ]
}

function QueueCtrl($scope, $stork) {
  var job = function (id) {
    return {
      "user_id": "bwross",
      "src": {
        "uri": "ftp://storkcloud.org/doc1"
      },
      "job_id": id,
      "dest": {
        "uri": "ftp://storkcloud.org/doc2"
      },
      "queue_timer": {
        "end": 1378292520101,
        "start": 1378292519977
      },
      "attempts": 0,
      "max_attempts": 10,
      "run_timer": {
        "end": 1378292520101,
        "start": 1378292519979
      },
      "progress": {
        "bytes": {
          "inst": -1,
          "avg": 87949,
          "total": 500,
          "done": 200
        },
        "files": {
          "total": 1,
          "done": 1
        }
      },
      "status": "processing"
    }
  }

  $scope.jobs = [ job(1), job(2), job(3) ]
  $scope.auto = true
  $scope.all = false

  var filter = function () {
    return $scope.all ? 'all' : 'pending'
  }

  $scope.refresh = function () {
    //this.jobs = $stork.q(filter())
    this.jobs[0].progress.bytes.done += 30
  }
  $scope.pretty_info = function (j) {
    return angular.toJson(j, true)
  }
  $scope.pretty = function (s) {
    return prettySize(s)
  }
  $scope.percent = function (p) {
    var t = p.total || 0
    var d = p.done  || 0
    var n = (d > 0 && t > 0) ? (100*d/t) : 100
    n = (n < 0) ? 0 : (n > 100) ? 100 : n
    return n+'%'
  }
  $scope.pw = function (j) {
    return { width: this.percent(j.progress.bytes) }
  }
  $scope.progress = function (p, s) {
    s = s || ''
    var t = prettySize(p.total)+s
    var d = prettySize(p.done)+s
    return d+'/'+t
  }
  $scope.status = function (j) {
    var s = j.status
    return s.charAt(0).toUpperCase() + s.slice(1)
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
}

// Prettify a size in bytes.
function prettySize(b, p) {
  p = (p === undefined) ? 2 : p
  b = (b === undefined) ? 0 : b
  var ss = ['', 'k', 'M', 'G', 'T']
  var s = ''
  for (var i = 1; i < ss.length && b >= 1000; i++) {
    b /= 1000
    s = ss[i]
  }
  return (s == '') ? b.toFixed(0) : b.toFixed(p)+s
}

var dls_uri = '/api/stork/ls' 
var queueTimer = null
var tableLoaded = false
// Event handlers for various forms.
$(document).on('ajax-done', '.login-form', function (e) {
  //saveLoginCookies(d.data)
  checkLoginCookies('home')
  msg.show('Login successful. Welcome!', 'success')
})

$(document).on('ajax-done', '.signup-form', function (e) {
  checkLoginCookies()
  msg.show('Registration successful. Welcome!', 'success')
})

$(document).on('ajax-done', '#myproxy-form', function (e) {
  $('.modal').modal('hide')
  msg.show('Credential successfully added!', 'success')
  updateCredList()
})

// Make tooltips work.
$(document).on('mouseover', '.tips', function () {
  $(this).tooltip({
    'animation': false,
    'container': 'body'
  })
  $(this).tooltip('show')
})

// Pop up an alert at the top of the page.
msg = function() {}
msg.show = function(m, t) {
  $('#ajax-page-content').prepend(msg.box(m, t))
}
msg.box = function(m, t) {
  var d = $('<div class="alert alert-'+t+' alert-dismissable">')
    .append($('<button type="button" class="close" data-dismiss="alert">')
      .html('<i class="icon-remove-sign">'))
    .append($('<div>').html(m))
  return d.animate({'opacity':'toggle'})
}

// Collapsible panels
$(document).on('click', '.panel-collapse-header', function (e) {
  var h = $(this).closest('.panel')
  var b = h.find('.collapse')
  b.slideToggle(function () {
    h.toggleClass('panel-collapsed')
  })
})

$(document).on('click', '.ph', function () {
  alert('Sorry! This is a placeholder.')
})

// Directory browsing
// ------------------
function onAjaxReload() {
  // Bind URI stuff
  $('.uri input').bind('keyup change', function (e) {
    if (e.keyCode == 13) {
      $(this).closest('.file-list').trigger('reload')
    } else if ($(this).attr('uri') != $(this).val()) {
      setButtonMode($(this).next(), 'reload')
    } else {
      setButtonMode($(this).next(), 'transfer')
    }
  })

  // Trigger event on file-list depending on button mode.
  $('.uri a').click(function () {
    var mode = $(this).attr('rel')
    if ($(this).hasClass('disabled') || !mode)
      return false
    $(this).closest('.file-list').trigger(mode)
  })

  // Function to get an endpoint descriptor from a file list.
  var getEndpoint = function (w, selector) {
    var ep = { }
    var path

    if (!selector)
      selector = $('li.root', w)

    ep.uri = $('.uri input', w).attr('uri')

    ep.cred = $('select.credential :selected', w).attr('value')
    if (!ep.cred)
      delete ep.cred

    path = $(selector, w).attr('rel')

    if (path)
      ep.uri += path

    return ep
  }

  // Set file-list events.
  $('.file-list').bind('reload', function () {
    var uri = $(this).find('.uri input').val()
    var w = $(this).attr('id')

    // Save the newly loaded URI to state.
    $(this).find('.uri input').attr('uri', uri)
    $.cookie(w+'-uri', uri)

    if (!uri)
      return $(this).find('.tree div').html('')

    $stork.list($('.tree > div', this), {
      'root': getEndpoint(this)
    })

    setButtonMode($(this).find('.uri a'), 'transfer')
  }).bind('transfer', function () {
    if ($('.uri a', this).hasClass('disabled'))
      return

    // Find the other file list according to rel.
    var other = $(this).attr('rel')
    var job = {'src': {}, 'dest': {}}

    if (other)
      other = $('#'+other)

    submitJob(getEndpoint(this,  'li.selected'),
              getEndpoint(other, 'li.selected'))
  })

  updateCredList()

  checkLogin()

  // Check cookies for past URIs.
  $('#left .uri input').val($.cookie('left-uri'))
  $('#right .uri input').val($.cookie('right-uri'))
  $('.file-list').trigger('reload')

  $stork.queue($('#stork-queue table'))
}

// Update the credentials list.
function updateCredList() {
  $.get('/api/stork/info?type=cred').done(function (d) {
    var c = $('.saved-credentials').empty()
    if ($.isEmptyObject(d)) {
      c.append($('<option disabled>').text('(none)'))
    } else for (var k in d) {
      c.append($('<option>').val(k).text(d[k].type+' - '+k))
    }
  })
}

// Helper for changing button modes.
function setButtonMode(e, mode) {
  if (mode == 'transfer') {
    e.removeClass('reload')
  } else if (mode == 'reload') {
    e.addClass('reload')
  } return e.attr('rel', mode)
}

// Debugging
function promptDLS() {
  var dls = prompt("Enter new DLS URL", dls_uri)
  if (dls)
    dls_uri = dls
  $('.file-list').trigger('reload')
}

function submitJob(src, dest) {
  if (src.length <= 0 && dest.length <= 0)
    return
  var ad = { 'src': src, 'dest': dest }
  var text = $('<div class="container text-center">')
    .append($('<big>').text(src.uri))
    .append($('<big style="padding: 0 0.5em">')
      .append($('<i class="icon-long-arrow-right">')))
    .append($('<big>').text(dest.uri))

  bootbox.setDefaults({title: "Confirm Transfer"})
  bootbox.confirm(text, function (yes) {
    if (yes)
      submitAd(ad)
  })
}

function submitAd(ad) {
  if (!ad)
    return
  if (typeof ad == 'string')
    ad = JSON.parse(ad)
  $.ajax({
    url:"/api/stork/submit",
    type:"POST",
    data: $.param(ad),
    contentType: "application/x-www-form-urlencoded",
    dataType: "json"
  }).done(function (data) {
    var s = "Response: "+data['response']
    if (data['job_id'] != null)
      s += "\nJob ID: "+data['job_id']
    $('#stork-queue table').trigger('reload')
  }).fail(function (data) {
    msg.show(data['responseText'], 'danger')
  })
}

function removeJob(id) {
  bootbox.setDefaults({title: 'Cancel Job '+id})
  bootbox.confirm('Are you sure you want to cancel this job?', function (yes) {
    if (yes) {
      var ad = { 'range': id }
      $.ajax({
        url:"/api/stork/rm",
        type:"POST",
        data: $.param(ad),
        contentType: "application/x-www-form-urlencoded",
        dataType: "json"
      }).done(function (data) {
        var s = "Response: "+data['response']
        if (data['job_id'] != null)
          s += "\nJob ID: "+data['job_id']
        $('#stork-queue table').trigger('reload')
      }).fail(function (data) {
        msg.show(data['responseText'], 'danger')
      })
    }
  })
}

function selectItem(w, e) {
  $('#'+w+' .selected').removeClass('selected')
  e.addClass('selected')
}
