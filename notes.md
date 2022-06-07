##SETUP

setup a new venv :
`sudo python3 -m venv venv/`
possibly need this before :
`sudo apt-get install python3-venv`

to install all requirements with pip :
`pip install -r requirements.txt`

Add private files with scp :
`scp -P XXXX FILE USER@IP:/home/arlo/r2`

if locale is not available :
`sudo locale-gen fr_FR.UTF-8`

crontab command : 
`*/10 7-23 * * * cd ~/r2 && sh run.sh`