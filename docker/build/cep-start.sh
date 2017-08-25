#!/usr/bin/env bash
envsubst < /opt/zz-cep/config/config_env.json > /opt/zz-cep/config/config.json
/opt/zz-cep/bin/cep-start.sh /opt/zz-cep/config/config.json