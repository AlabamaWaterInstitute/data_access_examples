# Parse command line arguments
while [[ $# -gt 0 ]]; do
  case "$1" in
    -a|--aws-acct-id)
      AWS_ACCT_ID="$2"
      shift 2
      ;;
    -i|--image-name)
      IMAGE_NAME="$2"
      shift 2
      ;;
    -r|--repo-name)
      REPO_NAME="$2"
      shift 2
      ;;
    --region)
      REGION="$2"
      shift 2
      ;;
    --test)
      TEST="$2"
      shift 2
      ;;      
    *)
      echo "Unknown argument: $1"
      exit 1
      ;;
  esac
done

# Use the variables as needed
echo "AWS_ACCT_ID: $AWS_ACCT_ID"
echo "IMAGE_NAME: $IMAGE_NAME"
echo "REPO_NAME: $REPO_NAME"
echo "REGION: $REGION"
echo "TEST: $TEST"

# build docker image
echo "Building docker image ${REPO_NAME}"
docker build --no-cache -t ${IMAGE_NAME} .

# Validate aws credentials
echo "Validating credentials"
aws ecr get-login-password --region ${REGION} | docker login --username AWS --password-stdin "${AWS_ACCT_ID}.dkr.ecr.us-west-2.amazonaws.com"

# Create repo
echo "Checking image repo status"
repository_info=$(aws ecr describe-repositories --region ${REGION} --repository-names ${REPO_NAME} 2>/dev/null)
if [[ -z "${repository_info}" ]]; then
    echo "${REPO_NAME} doesn't exist"
    echo "Creating ${REPO_NAME}"
    aws ecr create-repository \
        --region ${REGION} \
        --repository-name ${REPO_NAME} \
        --image-scanning-configuration scanOnPush=true
    sleep 10

else
    echo "${REPO_NAME} exists"
fi

# Tag the image
echo "Tagging docker image as ${IMAGE_NAME}"
docker tag ${IMAGE_NAME} "${AWS_ACCT_ID}.dkr.ecr.${REGION}.amazonaws.com/${REPO_NAME}:${IMAGE_NAME}"

# Push the image
echo "Pushing ${IMAGE_NAME} to ${AWS_ACCT_ID}.dkr.ecr.${REGION}.amazonaws.com/${REPO_NAME}"
docker push "${AWS_ACCT_ID}.dkr.ecr.${REGION}.amazonaws.com/${REPO_NAME}:${IMAGE_NAME}"

# Update the function code
echo "Updating function code"
aws lambda update-function-code \
    --function-name  forcingprocessor \
    --image-uri ${AWS_ACCT_ID}.dkr.ecr.${REGION}.amazonaws.com/${REPO_NAME}:${IMAGE_NAME} \
    --region ${REGION}

# Test
if [[ "$TEST" == "TRUE" ]]; then
    echo "Testing deployment"
    aws s3api put-object \
      --bucket nwm.test \
      --key 02.conus.nc.txt \
      --body ~/code/data_access_examples/data/cache/02.conus.nc.txt
else
    echo "Lambda function was not tested"
fi
