aws ec2 run-instances --image-id ami-02354e95b39ca8dec \
    --count 1 --instance-type t2.small --region <some-region> --key-name anarayanan  \
    --security-group-ids '<some-sg>' \
   --tag-specifications 'ResourceType=instance,Tags=[{Key=Name,Value=<some-name>}]' 

sleep 50

export instance_id="$(aws ec2 describe-instances --filters "Name=tag:Name,Values=<some-name>*" \
"Name=instance-state-name,Values=running" \
--output text --query 'Reservations[*].Instances[*].InstanceId')"

export public_dns="$(aws ec2 describe-instances --instance-ids "${instance_id}" \
--query "Reservations[*].Instances[*].PublicDnsName" --output=text)"

ssh -o StrictHostKeyChecking=no -i /path/to/pem ec2-user@"${public_dns}"

sudo yum -y update
sudo yum -y upgrade
sudo yum install -y gcc zlib zlib-devel openssl openssl-devel gcc-c++ bzip2-devel libffi-devel xz-devel


wget https://www.python.org/ftp/python/3.7.0/Python-3.7.0.tar.xz
tar xJf Python-3.7.0.tar.xz
cd Python-3.7.0

./configure
make
sudo make install

cd ..
rm Python-3.7.0.tar.xz
sudo rm -rf Python-3.7.0

curl -L http://download.osgeo.org/libspatialindex/spatialindex-src-1.8.5.tar.gz | tar xz
cd spatialindex-src-1.8.5

./configure --prefix=/home/ec2-user/lambda/local
make
sudo make install
sudo ldconfig
cd


sudo /usr/local/bin/pip3 install virtualenv
/usr/local/bin/virtualenv ~/lambda_env
source ~/lambda_env/bin/activate

pip install geopandas
# Somtimes rtree won't be installed!! Not sure why but may need to run line again
pip install rtree
pip install requests

export LD_LIBRARY_PATH="/home/ec2-user/lambda/local/lib/:LD_LIBRARY_PATH"

cd ./lambda_env/lib/python3.7/site-packages
zip -r9 ~/equity_tool_deployment_package.zip *
cd

cd ~/lambda
zip -r9 ~/equity_tool_deployment_package.zip local/lib
cd

scp -i [path/to/pem] [local/path/to/handler.py] ec2-user@[instance-ip]:~/

zip -r9u equity_tool_deployment_package.zip equity_calculations.py

aws s3 cp equity_tool_deployment_package.zip s3://<equity_file_bucket>/lambda/equity_tool_deployment_package.zip


