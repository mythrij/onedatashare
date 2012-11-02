#!/bin/bash

# Check for -i switch
[ "$1" == "-i" ] && {
  echo '[ protocols = "file,ftp,gsiftp"; name = "globus_url_copy module" ]'
  exit 0
}

message () { echo "[ message = \"$@\"; ]"; }
  
message "beginning transfer..."
exec globus-url-copy $@

exit $?
