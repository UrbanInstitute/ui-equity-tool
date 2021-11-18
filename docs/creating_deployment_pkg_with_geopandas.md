## Using Geopandas (and other modules with compiled packages) in AWS Lambda

#### Updated 10/14/19 to work with the latest Amazon Linux default AMI 

Some python libraries-like Geopandas - require native C or Fortran
packages that aren't available on Lambda. To get these non standard
python packages into Lambda and use them, we need to spin up the same ec2 
instance type that Lambda uses, install the required binaries, create a 
virtual environment, install the packages (and their dependencies), 
and zip up both the environment and the binaries. 

If you install compiled binaries you will also need to explicitly modify the 
`LD_LIBRARY_PATH` environment variable so Lambda knows where to look for your
compiled packages. In this example, we installed all our required binaries 
into a subfolder called `local/lib`, so we need to set the `LD_LIBRARY_PATH`
variable equal to `local/lib:$LD_LIBRARY_PATH`. 

The rest of this document will guide you through creating a deployment package that
installs geopandas and its required C++ libraries for use in lambda functions

## Installing geopandas on a deployment package

1. Create an Amazon EC2 instance using the default Amazon Linux AMI used in 
   Lambda. Note you will need to already have a `.pem` file for use with the
   instance. You will also need to run `aws configure` and set up a user with
   access and secret access keys that have the appropriate permissions to spin 
   up an ec2 instance. If you don't have the appropriate permissions to do that,
   reach out to an AWS admin in tech and data. 

```bash
# Spin up new EC2 instance using default Linux AMI
# Note this image-id will need to be updated manually as Amazon updates their linux ami
# This is a sg with full ssh access from anywhere. May want to change for security purposes \
# MAKE SURE TO CHANGE BUDGET CODE AS NEEDED \
aws ec2 run-instances --image-id ami-02354e95b39ca8dec \
    --count 1 --instance-type t2.small --region <some-regionsom> --key-name <pem>  \
    --security-group-ids '<some-sg>' \
   --tag-specifications 'ResourceType=instance,Tags=[{Key=Name,Value=geopandas-lambda-setup}]' 

sleep 50

export instance_id="$(aws ec2 describe-instances --filters "Name=tag:Name,Values=geopandas*" \
"Name=instance-state-name,Values=running" \
--output text --query 'Reservations[*].Instances[*].InstanceId')"

export public_dns="$(aws ec2 describe-instances --instance-ids "${instance_id}" \
--query "Reservations[*].Instances[*].PublicDnsName" --output=text)"

```

2. SSH into the instance
```bash
ssh -o StrictHostKeyChecking=no -i /path/to/pem ec2-user@"${public_dns}"
```

3.  Update, and install Python 3.9.6, Also install some linux packages
``` bash
sudo yum -y update
sudo yum -y upgrade
sudo yum install -y gcc zlib zlib-devel openssl openssl-devel gcc-c++ bzip2-devel libffi-devel xz-devel


wget https://www.python.org/ftp/python/3.9.6/Python-3.9.6.tar.xz
tar xJf Python-3.9.6.tar.xz
cd Python-3.9.6

./configure
make
sudo make install

cd ..
rm Python-3.9.6.tar.xz
sudo rm -rf Python-3.9.6

```

4. Install libspatialindex (A required C library for Geopandas).

```bash
curl -L http://download.osgeo.org/libspatialindex/spatialindex-src-1.8.5.tar.gz | tar xz
cd spatialindex-src-1.8.5

./configure --prefix=/home/ec2-user/lambda/local
make
sudo make install
sudo ldconfig
cd


```
Note that we explicitly installed libspatialindex in a shared folder called lambda/local so we
can easily zip it up later. If your packages have other required C or Fortran
libraries install them at the same location with the same prefix.

5. Set up a virtual environment and source it. Here we call it lambda_env

```bash
sudo /usr/local/bin/pip3.9 install virtualenv
/usr/local/bin/virtualenv ~/lambda_env
source ~/lambda_env/bin/activate
```

6. Install required python libraries

```bash
pip install geopandas
pip install rtree
pip install requests
#In previous lambda versions, you needed to install boto3 to get most recent 
# version bc the boto3 version on aws lambda is older and NOT compatible with 
# equity_calculations.py. However AWS updated the versions of boto3 available 
# on lambda and as of 9/1/2019, you don't need to install boto3
#pip install boto3


export LD_LIBRARY_PATH="/home/ec2-user/lambda/local/lib/:LD_LIBRARY_PATH"
#export LD_LIBRARY_PATH="/home/ec2-user/lambda/local/lib/:LD_LIBRARY_PATH"
deactivate
```

Note: We also change the LD_LIBRARY_PATH environment path variable to let
Geopandas know where libspatial index is installed so it can access it.
This is necessary for local testing of your python scripts to make sure you
have all the required libraries you need. You should perform local testing to
see if you can successfully import all the python modules you installed and if 
the linux libraries you installed are working. In the lambda function, you
will need to set the `LD_LIBRARY_PATH` global environment equal to 
`local/lib:$LD_LIBRARY_PATH` . 


7. Copy your python script(s) from your local file to the EC2 instance. In this
   case we have a handler and a worker script. Note you can only use the  `scp`
   command when copying files from your local computer to an ec2 instance.

```Bash
scp -i [path/to/pem] [local/path/to/handler.py] ec2-user@[instance-ip]:~/

```

As mentioned above you should test your python scripts in the virtual
environment to make sure you have all the required libraries for your code to
run. You can do that by running the following


```Bash
source lambda_env/bin/activate
python handler.py
deactivate
```

And if your code runs without errors, then you can go on to the next step. 
If you don't want to test your scripts, you should at the very least activate 
your virtual environment, and confirm that your python packages
can be successfully imported. 


1. Zip up the python packages and the libspatial C library

```bash1
#zip python packages
cd ./lambda_env/lib/python3.9/site-packages
zip -r9 ~/equity_tool_deployment_package.zip *
cd

#zip libspatial library
cd ~/lambda
zip -r9 ~/equity_tool_deployment_package.zip local/lib
cd

```

9. Move your python script(s) into the zipped folder. 

```Bash
cd
zip -r9u equity_tool_deployment_package.zip equity_calculations.py
```

10. Copy the zip file from the instance back to your local computer (or to s3 or
    somewhere you have access)

```bash
# AN: The below line isn't working for me anymore, so i just copied the file to S3 and then downloaded
scp -i "/path/to/pem" ec2-user@<instance-ip>:~/equity_tool_deployment_package.zip /local/path/equity_tool_deployment_package.zip

```


11. If you didn't copy your script into the instance, add it the zip file. 
Then upload the zip file to S3, where it can be used in lambda functions. 
Note that S3 deployment packages are limited to 50MB zipped but the actual 
limit is 262 MB unzipped. This zipped package with geopandas is ~ 69MB and works fine.


12. *Remember to terminate your instance! Any instances that are still running or
    even stopped still costs money!*


### Important Notes about this process:

- Make sure all shared libraries are installed on the same path (in this case it
was lambda/local) and you zip up that shared folder
- Make sure to test your python script in the virtual environment to make sure
you have all the required libraries and dependencies
- Make sure to zip the *content* of the project directory, and **not** the
directory itself.
- Actual S3 file size limit: unzipped file cannot be greater than 262 MB.
Technically the zipped file upload limit from S3 is 50 MB but Lambda doesn't
strictly care as long as the unzipped file is still less than 262 MB.
- If you have compiled packages (like libspatialindex), you need to modify the
  LD_LIBRARY_PATH environment variable for the lambda function to run.
  Specifically you need to set the following env variables in lambda:
  LD_LIBRARY_PATH: local/lib:$LD_LIBRARY_PATH