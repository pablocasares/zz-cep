#!/usr/bin/env bash
envsubst < /opt/cep/config/config_env.json > /opt/cep/config/config.json
/opt/cep/bin/cep-start.sh /opt/cep/config/config.json