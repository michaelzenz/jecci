docker ps|grep jecci|awk '{print $1}'|xargs docker rm -f
