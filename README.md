# distributed-lock-python
Distributed Lock using AWS DynamoDB in Python

This is based on the post written [here](https://hello.bitsnbytes.world/2023/11/27/distributed-locks-with-dynamodb-for-python/)

To get started have python 3 installed then run
`pip3 install -r requirements.txt`

Then run terraform
```
terraform init
terraform plan
terraform apply
```

Then with two terminals opened up run in each
`python3 main.py`

