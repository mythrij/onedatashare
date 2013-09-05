angular.module('stork', []).config(['$routeProvider',
  function($routeProvider) {
    $routeProvider.
      when('/', {templateUrl: 'home.html'}).
      when('/transfer', {templateUrl: 'transfer.html'}).
      when('/terms', {templateUrl: 'terms.html'}).
      when('/privacy', {templateUrl: 'privacy.html'}).
      otherwise({redirectTo: '/'});
  }
]);

var dls_uri = '/api/stork_ls' 
var queueTimer = null
var tableLoaded = false

// The last data object received by the AJAX form handler.
var really_ugh_ajax_form_hack = null

// Event handler for all AJAX forms.
$(document).on('submit', 'form.ajax-form', function (e) {
  var form = $(e.target)

  form.ajaxSubmit({
    success: function (m) {
      form.clearForm()
      $('input', form).blur()
      really_ugh_ajax_form_hack = m
      form.trigger('ajax-done', m)
    }, error: function (m) {
      $('.alert', form).alert('close')
      form.prepend(msg.box($('<span>').text(m.responseText), 'danger'))
      really_ugh_ajax_form_hack = m.responseText
      form.trigger('ajax-fail', m)
    }, always: function (m) {
      form.trigger('ajax-always', m)
    }
  })
  e.preventDefault()
})

function saveLoginCookies(d) {
  $.cookie('user_id', d.user_id, { expires: 30 })
  $.cookie('pass_hash', d.pass_hash, { expires: 30 })
}

// Event handlers for various forms.
$(document).on('ajax-done', '.login-form', function (e) {
  //saveLoginCookies(d.data)
  saveLoginCookies(really_ugh_ajax_form_hack)
  checkLoginCookies('home')
  msg.show('Login successful. Welcome!', 'success')
})

$(document).on('ajax-done', '.signup-form', function (e) {
  saveLoginCookies(really_ugh_ajax_form_hack)
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

// Navigation
// ----------
// AJAX load a page into the content frame and change the subnavbar and title.
function navLoadPage(page) {
  $('.navbar .active').removeClass('active')

  if (!page)
    page = window.location.hash.replace("#", "")
  if (!page)
    page = "home"
  page = page.replace("#", "").replace("/", "")

  $('.navbar li[rel="'+page+'"]').addClass('active')

  if (g_loading_ajax_page)
    g_loading_ajax_page.abort()

  // Check if we've already fetched and cached the page.
  if (g_ajax_pages[page]) {
    setAjaxPageContent(g_ajax_pages[page])
  } else {
    g_loading_ajax_page = $.get("/ajax/"+page).success(function (d) {
      setAjaxPageContent(g_ajax_pages[page] = d)
    }).error(function (r) {
      $('#ajax-page-content').html(r['resonseText'])
    }).always(function () {
      g_loading_ajax_page = null
    })
  }
}

// Login stuff
// -----------
// Check for login cookies and update UI accordingly.
function checkLoginCookies() {
  var user_id = $.cookie('user_id')
  if (user_id) {
    $('#user-button span').text(user_id)
    $('body').removeClass('logged-out').addClass('logged-in')
  } else {
    $('body').removeClass('logged-in').addClass('logged-out')
  }
}

// Check the stored username and password. Update user info box.
function checkLogin(req) {
  if (!$.cookie('user_id'))
    return

  $.post("/api/stork_user").done(function (data) {
    // Make sure response is right.
    if (!data['user_id'] || !data['pass_hash'])
      return

    saveLoginCookies(data)
    checkLoginCookies()
  })
}

// Delete the login cookies.
function logout() {
  $.removeCookie('user_id')
  $.removeCookie('pass_hash')
  $.removeCookie('password')
  checkLoginCookies()
}

// AJAX pages stuff
// ----------------
// Set the page content frame to some HTML. Take out subnavbar, too.
function setAjaxPageContent(html) {
  document.title = 'StorkCloud - '+title

  var div = $('<div>').html(html)
  var title = $('title', div).remove().html()

  if (title)
    document.title = 'StorkCloud - '+title
  else
    document.title = 'StorkCloud'

  $('#ajax-page-content').html(div.html())
  onAjaxReload()
  checkLoginCookies()
}

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
  $.get('/api/stork_info?type=cred').done(function (d) {
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
    url:"/api/stork_submit",
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
        url:"/api/stork_rm",
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
