FROM public.ecr.aws/lambda/python:3.12 AS base

# Install dependencies
# pyarrow wheels are available via pip for manylinux — fully compatible with Lambda
RUN pip install --no-cache-dir pyarrow boto3

# Copy function code
COPY main.py ${LAMBDA_TASK_ROOT}

# Set the CMD to your handler
CMD ["main.handler"]
