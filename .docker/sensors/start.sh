#!/usr/bin/env bash
service supervisor force-reload

tail -f /var/log/sensors.log