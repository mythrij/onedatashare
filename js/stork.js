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
      var url = api('user')
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

}

function BrowseCtrl($scope, $stork) {
  $scope.uri = ''
  $scope.selected = null
  $scope.root = {
    name: '/',
    dir: true,
    files: [
      { name: 'sub1' }
    ]
  }

  $scope.refresh = function () {
    $stork.ls(uri, 1).success(function () {
      
    })
  }
}

function BrowseListCtrl($scope) {
}

function CredCtrl($scope) {
  $scope.creds = [ ]
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
