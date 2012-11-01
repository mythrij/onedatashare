#!/bin/bash

# Check for -i switch
[ "$1" == "-i" ] && {
  echo '[ protocols = "file,ftp,gsiftp"; name = "globus_url_copy module" ]'
  exit 0
}

# Transfer the file
exec /usr/local/globus-5.2.2/bin/globus-url-copy $@

exit $?
