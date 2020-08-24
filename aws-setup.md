## Setting up new EC2 instance with Flask server
### 1. EC2 instance
https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/AccessingInstancesLinux.html

- Create instance in AWS EC2 Dashboard, generate .pem file and add inbound firewall rule for SSH.
- Connect with the instance using `ssh -i /path/my-key-pair.pem my-instance-user-name@my-instance-public-dns-name` (default instance user name in Ubuntu is 'ubuntu')

### 2. Setting up Python virtual environment on Ubuntu instance
- Update list of available packages with `sudo apt update`
- To install pip and python3-venv run `sudo apt install python3-pip && sudo apt-get install python3-venv`
- If you need docker run `sudo apt  install docker.io && sudo apt install docker-compose`
- Create new folder and set up virtual environment and install Flask:
    - `python3 -m venv venv && source venv/bin/activate`
    - `pip install flask`