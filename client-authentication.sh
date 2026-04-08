#!/bin/bash

KEYCLOAK_URL="http://localhost:8080/realms/TEST/protocol/openid-connect/token"
CLIENT_ID="test-client"
CLIENT_SECRET="PzXBNFJsBUMr97uojTFt5eAh2fkGeAH6"

for i in $(seq 1 1000000); do
    RESPONSE=$(curl -s -o /dev/null -w "%{http_code} %{time_total}" -X POST "$KEYCLOAK_URL" \
        -H "Content-Type: application/x-www-form-urlencoded" \
        -d "grant_type=client_credentials" \
        -d "client_id=$CLIENT_ID" \
        -d "client_secret=$CLIENT_SECRET")

    STATUS=$(echo $RESPONSE | awk '{print $1}')
    TIME=$(echo $RESPONSE | awk '{print $2}')

    echo "Request #$i - Status: $STATUS - Time: ${TIME}s"
#    sleep 1
done
