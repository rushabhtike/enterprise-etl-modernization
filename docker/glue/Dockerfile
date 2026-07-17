FROM public.ecr.aws/glue/aws-glue-libs:5

USER hadoop

RUN python3 -m pip install \
    --user \
    --no-cache-dir \
    jupyterlab \
    ipykernel

WORKDIR /home/hadoop/workspace

EXPOSE 8888
EXPOSE 4040