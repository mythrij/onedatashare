#!/bin/bash

read -d '' AD <<END
[
  name = "globus_url_copy module"
  description = "An example of a shell script transfer module."
  protocols = {file,ftp,gsiftp}
]
END

# Check for -i switch
[ "$1" == "-i" ] && {
  echo "$AD"
  exit 0
}

message () { echo "[ message = \"$@\"; ]"; }
  
message "beginning transfer..."
exec globus-url-copy $@

exit $?
