#!/usr/bin/env bash
envsubst < /opt/zz-cep/config/config_env.json > /opt/zz-cep/config/config.json
envsubst < /opt/zz-cep/config/log4j2_env.xml > /opt/zz-cep/config/log4j2.xml
/opt/zz-cep/bin/cep-start.sh /opt/zz-cep/config/config.json