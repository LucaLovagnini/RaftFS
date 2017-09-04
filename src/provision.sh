sudo apt-get update
sudo apt-get install -y openjdk-7-jdk
sudo apt-get install icedtea-7-plugin
sudo update-java-alternatives -s java-1.7.0-openjdk-i386
echo "export JAVA_HOME=/usr/lib/jvm/java-1.7.0-openjdk-i386" >> /home/vagrant/.profile
echo "export PATH=$JAVA_HOME/bin:$PATH" >> /home/vagrant/.profile
sudo apt-get install -y maven
mvn clean -f /vagrant/RaftFS/pom.xml
mvn package -f /vagrant/RaftFS/pom.xml
if [ $1 == "TRUE" ] ; then
	echo "I'M A SERVER!"
	sudo mv /vagrant/RaftFS/target/RaftFS-1.0-SNAPSHOT-jar-with-dependencies.jar /vagrant/
	sudo cp /vagrant/RaftFS/servers.yaml /vagrant/
	sudo cp /vagrant/RaftFS/log4j.properties /vagrant/
else
	echo "I'M A CLIENT!"
fi
