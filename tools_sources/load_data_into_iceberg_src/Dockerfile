FROM public.ecr.aws/lambda/python:3.12

RUN pip install --no-cache-dir \
    awswrangler \
    pyarrow \
    pandas

COPY main.py ${LAMBDA_TASK_ROOT}

CMD ["main.handler"]
