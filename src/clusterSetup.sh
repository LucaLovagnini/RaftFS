#before you can run this script you need to install shyaml,sshpass and pssh:
#sudo pip install shyaml
#sudo apt-get install sshpass
#sudo apt-get install pssh
cat RaftFS/servers.yaml | shyaml keys-0 |
  while read -r -d $'\0' value; do
      if [ ! $value == "RaftArgs" ]; then
       address=$(cat RaftFS/servers.yaml | shyaml get-value $value.machineIP | xargs -0 -n 1 echo)
       username=$(cat RaftFS/servers.yaml | shyaml get-value $value.username | xargs -0 -n 1 echo)
       password=$(cat RaftFS/servers.yaml | shyaml get-value $value.password | xargs -0 -n 1 echo)
       echo "$username@$address">>hosts
       #uploading my fingerprint (in order to use pssh)
       sshpass -p $password ssh-copy-id -oStrictHostKeyChecking=no $username@$address
       ssh $username@$address mkdir RaftFS
       scp -r . $username@$address:RaftFS
       ssh $username@$addreess mvn package -f RaftFS/pom.xml
      fi
       echo $address $username $password
  done
