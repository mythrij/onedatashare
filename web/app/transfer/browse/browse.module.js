// stork.list.js
// -------------
var $stork = $stork || {};

// Cast on anything to replace its contents with a file tree.
$stork.list = function (e, o) {
  // Sanitize options.
  if (!o)       o = {}
  if (!o.root)  o.root  = { }
  if (!o.url)   o.url   = '/api/stork/ls'
  if (!o.cache) o.cache = { }

  function ce(t, c) {
    return $(document.createElement(t)).addClass(c)
  }
  
  $(this).each(function() {
    // Get the listing for a tree entry and cache the results.
    // Return the XHR object.
    function fetchListing(path, d) {
      var ad = $.extend({}, o.root)
      ad.uri = ad.uri+path

      if (d) ad.depth = d

      return $.ajax(o.url, {
        data: ad
      }).done(function (data) {
        convertAjaxResults(data)
        cachedList(path, data)
      }).fail(function (data) {
        console.log('failed: '+JSON.stringify(data))
      })
    }

    // Call this when a user expands a list entry.
    function expandList(e) {
      // Get the path associated with this element.
      var path = $(e).attr('rel')
      if (!path)
        return $(e).addClass('inaccessible')

      // Check if we can render the listing from cached data.
      var cl = cachedList(e.attr('rel'))
      if (cl)
        renderList(e, cl)

      // If we're already fetching, don't bother fetching again.
      if ($(e).hasClass('wait'))
        return
      $(e).addClass('open')

      if (!cl)
        $(e).addClass('wait')

      // Fetch the listing for this entry and update if needed.
      return fetchListing(path).done(function (data) {
        // Re-render the listing. TODO: Detect updates.
        renderList(e, data)
        // If there are subdirs, prefetch. Assume dirs come first.
        if (data['files'])
          fetchListing(path, 1)
        $(e).removeClass('inaccessible')
      }).fail(function () {
        $(e).addClass('inaccessible')
        $(e).removeClass('open')
      }).always(function () {
        $(e).removeClass('wait')
      })
    }

    // Check if a file/directory name should be ignored.
    function ignored(n) {
      return !n || n == '.' || n == '..'
    }

    // Convert results into a proper tree.
    function convertAjaxResults(data) {
      var f = data['files']
      var fn = f ? { } : undefined

      delete data['name']
      delete data['files']

      if (f) for (var i = 0; i < f.length; i++) {
        var n = f[i]['name']
        if (ignored(n)) continue
        fn[n] = convertAjaxResults(f[i])
      }

      if (fn)
        data['files'] = fn

      return data
    }

    // Get/set the cached listing.
    function cachedList(path, data) {
      path = path.replace(/^\s*\/*|\/*\s*$/g,'').split(/\/+/)
      var t = o.cache

      if (!path[0])
        return data ? (o.cache = data) : o.cache
      while (path.length > 1)
        if (!(t = t['files'][path.shift()])) return
      if (data)
        return t['files'][path.shift()] = data
      else
        return t['files'][path.shift()]
    }

    // Check if a cached listing needs to be re-rendered. Super gross.
    function hasChanged(e, n) {
      var o = $(e).data('list')

      if (!e || !n || !o)
        return true

      var od = o['dirs']
      var of = o['files']
      var nd = n['dirs']
      var nf = n['files']
      var sd = {}
      var sf = {}

      if (!od != !nd || !of != !nf)
        return true
      if (od && od.length != nd.length)
        return true
      if (of && of.length != nf.length)
        return true
      if (od) for (var i = 0; i < od.length; i++)
        sd[od[i]['name']] = true
      if (of) for (var i = 0; i < of.length; i++)
        sf[of[i]['name']] = true
      if (nd) for (var i = 0; i < nd.length; i++)
        if (!sd.hasOwnProperty(nd[i]['name']))
          return true
        else delete sd[nd[i]['name']]
      if (nf) for (var i = 0; i < nf.length; i++)
        if (!sf.hasOwnProperty(nf[i]['name']))
          return true
        else delete sf[nf[i]['name']]
      for (var k in sd)
        if (sd.hasOwnProperty(k)) return true
      for (var k in sf)
        if (sf.hasOwnProperty(k)) return true
      return false
    }

    // Generate the DOM for a listing. Pass a selector for the entry
    // element as e. Adds a ul inside of it.
    function renderList(e, data) {
      var f = data['files']
      var u = ce('ul', 'stork-list')
      var t = $(e).attr('rel')

      // Get selected item first, if there is one.
      var sr = $('.selected', e).attr('rel')

      if (f) for (var k in f) {
        if (f[k]['dir'])
          u.append(createItem('dir', k, t+k+'/', f[k]))
        else
          u.append(createItem('file', k, t+k, f[k]))
      }

      $('ul', e).remove()
      $(e).append(u)

      // Reselect element if there was one.
      if (sr) $('li[rel="'+sr+'"]', e).addClass('selected')
    }

    // Create the DOM element for a list item.
    function createItem(c, n, t, o) {
      var e = ce('li', c).attr('rel', t).append(ce('i'))
      e.append(ce('div').text(n))
      if (o && o['error']) e.addClass('inaccessible')
      if (n.charAt(0) == '.') e.addClass('dot')
      return e
    }

    // Toggle a directory, or force it open or closed.
    var toggle = function(oc) {
      var e = $(this).parent()
      if (oc != 'close' && !e.hasClass('open')) {
        expandList(e)
      } else if (oc != 'open' && e.hasClass('open')) {
        e.removeClass('open')
        e.removeClass('wait')
        $('ul', e).remove()
      }
    }

    // Sanitize root.
    if (typeof o.root.uri === 'string')
      o.root.uri = o.root.uri.replace(/\/+$/g, '')

    // Add delegated bindings.
    $(e).off('click dblclick mousedown')
    $(e).on('click', 'li.dir > i', toggle)
        .on('dblclick', 'li.dir > div', toggle)
        .on('mousedown', 'li > div', function () {
          $(this).closest('.root').parent()
            .find('.selected').removeClass('selected')
          $(this).parent().addClass('selected')
        })

    // Get root of tree with the root URI. Return fetch promise.
    $(e).empty()
    var d = createItem('dir root', o.root.uri).attr('rel', '/')
    $(e).append(ce('ul', 'stork-list').append(d))

    return expandList(d)
  })
}
