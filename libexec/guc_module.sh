#!/bin/sh

# Check for -i switch
[ "$1" == "-i" ] && {
  echo '[ protocols = "file,ftp,gsiftp"; name = "globus_url_copy module" ]'
  exit 0
}

# Get size of file

# Transfer the file
/usr/local/globus-5.2.2/bin/globus-url-copy "$1" "$2"

exit $?
