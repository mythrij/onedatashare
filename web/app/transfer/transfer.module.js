'use strict';

/** Module for controlling and monitoring transfers. */
angular.module('stork.transfer', [
  'stork.transfer.browse', 'stork.transfer.queue'
])

.controller('Transfer', function ($scope, user, stork, $modal) {
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
    var job = $scope.job;
    job.src.uri  = _.keys(src.$selected)[0];
    job.dest.uri = _.keys(dest.$selected)[0];
    console.log(job);

    var modal = $modal({
      title: 'Transfer',
      contentTemplate: 'transfer-modal.html'
    });

    modal.$scope.job = job;
    modal.$scope.submit = $scope.submit;
  };

  $scope.submit = function (job, then) {
    return stork.submit(job).then(
      function (d) {
        if (then)
          then(d);
        $modal({
          title: 'Success!',
          content: 'Job accepted with ID '+d.job_id
        });
        return d;
      }, function (e) {
        if (then)
          then(e);
        $modal({
          title: 'Failed to submit job',
          content: e
        });
        throw e;
      }
    );
  };
})
