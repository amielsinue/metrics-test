In order to test this on local you have to install docker

then run 
`docker-compose up sensors`

Then to see an example of the data you can open index.html file

AIOHttp
=========
This help us to process to handle too of requests

PostgresSQL
========== 
This help us to process all those requests pretty fast and we can easily scale

Improvements:
==============
- Create a table where we can register devices and create api so we know how many devices we have
- look for a better strategy to display summary, maybe views or any cache to prevent overload database
- implement read replicas too if we are gonna have toon of request to get info.