FROM python:3.7
RUN pip install --upgrade pip
RUN pip install wtforms==2.3.3 && \
    pip install 'apache-airflow[postgres]==1.10.14' && \
    pip install dbt==0.20 && \
    pip install SQLAlchemy==1.3.23 && \
    pip install markupsafe==2.0.1

RUN mkdir /project
COPY scripts_airflow/ /project/scripts/

RUN chmod +x /project/scripts/init.sh
ENTRYPOINT [ "/project/scripts/init.sh" ]