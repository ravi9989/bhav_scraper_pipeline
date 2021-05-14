To deploy the lambda with ci/cd for first time.

1. Change the values in Params w.r.t your repostory:
 - GitHubRepo
 - GitHubBranch
 - DeployStackName (e.g: stackname-deploy)
2. Give a proper stackname and execute the below command
 - aws cloudformation create-stack --stack-name <stack-name> --template-body file://./deploy-lambda.json --parameters file://./params.json