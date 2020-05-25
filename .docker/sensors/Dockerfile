FROM python

RUN apt-get update && apt-get install supervisor -y
RUN pip install requests
RUN mkdir /app
RUN ls -la /app/
COPY spawn_sensors.py /app/spawn_sensors.py
RUN touch /var/log/sensors.log

COPY sensors.conf /etc/supervisor/conf.d/sensors.conf

CMD python /app/spawn_sensors.py
