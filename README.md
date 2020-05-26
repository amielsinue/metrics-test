In order to test this on local you have to install docker

then run 
`docker-compose up sensors`

Unit Tests
==
Run 
```bash
docker-compose up rununittests
```


Then to see an example of the data you can open index.html file

AIOHttp
=========
This help us to process ton of requests

PostgresSQL
========== 
This help us to process all those requests pretty fast and we can easily scale

Improvements:
==============
- Create a table where we can register devices and create api so we know how many devices we have
- look for a better strategy to display summary, maybe views or any cache to prevent overload database
- implement read replicas too if we are gonna have toon of request to get info.

Troubleshoting
===
Sometimes when we have same port being used (8080) for other applications the docker container is not running
so in order to fix that issue you have to change the port on dockerfile to be like this

```
  metricsapi:
    build: .
    ports:
      - "8081:8080"
```

After this you will need to modify the index.html and update the port number that is being use to consume the api
```
$.getJSON("http://localhost:8081/readings/summary?start="+xValue+"&end="+yValue, addData);
```

then you can try again

```bash
docker-compose up sensors
``` 

