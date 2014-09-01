if (!$stork)
  var $stork = {}

// Cast on a table to create a queue table out of it. The table should
// have a number of <th>'s predefined which set the title and other column
// attributes 
$stork.queue = function (e, o) {
  // Sanitize options.
  if (!o)        o = {}
  if (!o.url)    o.url    = '/api/stork_q?status=all'
  if (!o.method) o.method = 'GET'
  if (!o.key)    o.key    = 'job_id'


  // Where all of the row data will be stored.
  var rows = {}

  // Determine ordering for columns from rel attributes.
  var columns = []
  $('thead th', e).each(function (i, th) {
    columns.push($(th).attr('rel'))
  })

  // Prettify a size in bytes.
  function prettySize(b) {
    b = b || 0
    var ps = ['', 'k', 'M', 'G', 'T']
    var p = ''
    for (var i = 0; i < ps.length && b >= 1000; i++) {
      b /= 1000
      p = ps[i]
    }
    return (p == '') ? b.toFixed(0) : b.toFixed(2)+p
  }

  function toggleInfoRow() {
    var t = $(this).closest('tr')
    var next = t.next()
    if (next.hasClass('info-row')) {
      next.remove()
    } else {
      t.after(infoRow(rows[t.attr('rel')]))
    }
  }

  // Renderers for known fields. A renderer takes the Stork job and
  // produces HTML or a DOM element to display it. It can optionally
  // also accept its parent element as the second argument.
  var renderers = {
    'info': function (d) {
      var i = $('<i class="btn btn-default icon-info" title="Details">')
        .click(toggleInfoRow)
      var x = $('<i class="btn btn-default icon-remove">')
        .click(function () { removeJob(d.job_id|0) })
      if (d.status == 'complete' || d.status == 'failed' || d.status == 'removed')
        x.attr('disabled', 'disabled').addClass('btn-disabled')
      return $('<div class="btn-group">').append(i).append(x)
    },

    // Render a progress bar that fills the cell.
    'progress': function (d) {
      var div = $('<div class="progress-bar" role="progressbar" title="Cancel job">')
      var span = $('<span>')
      var c = 'progress'
      div.attr('aria-valuemin', 0)
      div.attr('aria-valuemax', 100)

      var b = d.progress.bytes
      if (b && d.status == 'processing') {
        var w = (b.total <= 0 || b.done < 0) ? 0 : b.done / b.total
        w = Math.round(w*100)
        w = (w < 0) ? 0 : (w > 100) ? 100 : w
        div.attr('aria-valuenow', w).css('width', w+'%')
        span.text(w+'%')
      } else {
        span.text(d.status || 'unknown')
        div.attr('aria-valuenow', 100).css('width', '100%')
      }

      if (d.status == 'processing')
        c += ' progress-striped active'

      div.addClass({
        processing: 'progress-bar-success',
        scheduled:  'progress-bar-warning',
        complete:   '',
        removed:    'progress-bar-danger',
        failed:     'progress-bar-danger'
      }[d.status])

      return $('<div>').addClass(c).append(div).append(span)
    },

    // Render the transfer speed in a pretty fashion.
    'speed': function (d) {
      var b = d.progress.bytes
      var h = $('<div>')
      if (!b)
        return
      if (b.avg > 0)
        h.text(prettySize(b.avg)+'B/s')
      if (b.inst > 0)
        h.append($('<div>').text('('+prettySize(d.inst)+'B/s)'))
      return h
    },

    'endpoints': function (d) {
      return $('<div>').append($('<span>').text(d.src.uri  || d.src))
                       .append($('<span>').text(d.dest.uri || d.dest))
    }
  }

  // Create a cell using a key and the given data.
  function createCell(k, d) {
    var td = $('<td>').attr('rel', k)
    if (k) {
      if (renderers[k])
        td.html(renderers[k](d, td))
      else
        td.text(d[k])
    } return td
  }

  // Update or create a table row from data.
  // TODO: Comparing HTML like this is bad...
  function updateRowCells(d, r) {
    var id = $(r).attr('rel')
    $('td', r).each(function (_, c) {
      var rel = $(c).attr('rel')
      var n = createCell(rel, d)
      if (n.html() != $(c).html())
        $(c).replaceWith(createCell(rel, d))
    })
  } function createRow(d) {
    var r = $('<tr>').attr('rel', d[o.key])
    for (var i = 0; i < columns.length; i++)
      r.append(createCell(columns[i], d))
    return r
  }

  // Create a row that spans the whole table with the given text.
  function createFullRow(m) {
    return $('<tr class="text-center">').append(
      $('<td>').attr('colspan', columns.length).text(m))
  }

  // Create or update a row containing all of the job information.
  function infoRow(d, r) {
    var s = JSON.stringify(d, undefined, 2)
    if (!r)
      r = $('<tr class="info-row">').attr('rel', d[o.key]).append(
            $('<td>').attr('colspan', 0).append(
              $('<pre>')))
    if (s !=  $('pre', r).text())
      $('pre', r).text(JSON.stringify(d, undefined, 2))
    return $(r)
  }

  // Regenerate a row with the given data.
  function updateRow(r, d) {
    var id = $(r).attr('rel')
    if (d) {
      if ($(r).hasClass('info-row'))
        return infoRow(d, $(r))
      return updateRowCells(d, r)
    }
  }

  // Clear all the data rows in the table.
  function clearTable() {
    $('tbody > tr', e).remove()
  }

  // Redraw the current view by fetching from the server.
  var refresh_timeout = null
  var current_fetch = null
  function refreshView() {
    // Cancel current request.
    if (current_fetch)
      current_fetch.abort()

    // Fetch the latest queue view.
    return current_fetch = $.ajax(o.url, {
      type: o.method,
      data: { reverse: true }
    }).done(function (d) {
      var body = $('tbody', e)

      if ($.isEmptyObject(d))
        return body.html(createFullRow("No jobs found..."))
      $('td[colspan=5]', body).remove()

      // Cache row data and update visible columns.
      for (var k in d) if (d[k][o.key]) {
        var rs = body.find('tr[rel="'+d[k][o.key]+'"]')
        var count = 0

        // Update all the cells in rows whose values have changed.
        rs.each(function (_, r) {
          updateRow(r, d[k])
          count++  // Hackish.
        })

        if (!count)
          body.append(createRow(d[k]))

        // Cache row data.
        rows[d[k][o.key]] = d[k]
      }
    }).fail(function (d) {
      $('tbody', e).html(createFullRow(d['responseText']))
      $('#auto-refresh-checkbox').prop('checked', false)
    }).always(function () {
      if (refresh_timeout)
        clearTimeout(refresh_timeout)
      refresh_timeout = null
      current_fetch = null
      if ($('#auto-refresh-checkbox:checked').length)
        refresh_timeout = setTimeout(refreshView, 1000)
    })
  }

  // Attach event bindings.
  $(e).bind('reload', refreshView)

  // Load the table.
  clearTable() 
  refreshView()
}
