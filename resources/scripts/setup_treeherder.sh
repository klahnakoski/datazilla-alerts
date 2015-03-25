# USE THIS TO INSTALL INTO STANDARD EC2 INSTANCE
sudo yum -y install python27
mkdir  /home/ec2-user/temp
cd  /home/ec2-user/temp
wget https://bootstrap.pypa.io/get-pip.py
sudo python27 get-pip.py

cd  /home/ec2-user
sudo yum -y install git
git clone https://github.com/klahnakoski/datazilla-alerts.git
cd /home/ec2-user/datazilla-alerts/
git checkout treeherder
sudo pip install -r requirements.txt

#INSTALL CRON JOB
cd  /home/ec2-user
chmod +x /home/ec2-user/datazilla-alerts/resources/scripts/treeherder_import.sh
echo "*/10 * * * * /home/ec2-user/datazilla-alerts/resources/scripts/treeherder_import.sh" > cron.txt
crontab cron.txt
rm -rf cron.txt
