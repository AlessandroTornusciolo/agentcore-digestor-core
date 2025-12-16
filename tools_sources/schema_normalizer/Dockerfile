FROM public.ecr.aws/lambda/python:3.12

RUN pip install --no-cache-dir \
    pandas \
    numpy

COPY main.py ${LAMBDA_TASK_ROOT}

CMD ["main.handler"]
