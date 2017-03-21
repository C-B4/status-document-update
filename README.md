# Installation instructions 
#### Clone repo and run the following

* sudo yum install python34-setuptools
* sudo easy_install-3.4 pip
* sudo pip3.4 install -r requirements.txt
* chmod +x statusesMigrationScript.py

#### Run Example 
./statusesMigrationScript.py --contact-points 10.132.0.5 --application-ids-path ./applicationIds.txt

#### Additional flags
* -v verbos 
* -h help 
* --prev-snapshot-file-path hiermark file path.
