Helper to connect to AWS MSK using IAM EC2 Role credentials

Example IAM Policy to access cluster and create, write and read topics
```
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "kafka-cluster:Connect",
                "kafka-cluster:AlterCluster",
                "kafka-cluster:DescribeCluster"
            ],
            "Resource": "arn:aws:kafka:REGION:ACCOUNT_NUMBER:cluster/CLUSTER_NAME/CLUSTER_ID"
        },
        {
            "Effect": "Allow",
            "Action": [
                "kafka-cluster:DescribeTopic",
                "kafka-cluster:CreateTopic",
                "kafka-cluster:WriteData",
                "kafka-cluster:ReadData"
            ],
            "Resource": "arn:aws:kafka:REGION:ACCOUNT_NUMBER:topic/CLUSTER_NAME/CLUSTER_ID/*"
        },
        {
            "Effect": "Allow",
            "Action": [
                "kafka-cluster:AlterGroup",
                "kafka-cluster:DescribeGroup"
            ],
            "Resource": "arn:aws:kafka:REGION:ACCOUNT_NUMBER:group/CLUSTER_NAME/CLUSTER_ID/*"
        }
    ]
}
```