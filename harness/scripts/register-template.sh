#!/usr/bin/env bash
# Register a template spec through the gateway's admin API and activate the
# new version. Run it where the admin API is reachable (the Compose network,
# e.g. from a container on llm-synapse-gw_default):
#
#   GATEWAY_ADMIN_TOKEN=... harness/scripts/register-template.sh m4-faq-coalesce \
#     harness/profiles/templates/m4-faq-coalesce.json
#
# ADMIN_URL defaults to http://gateway-1:8081. Prints the activated version.
set -euo pipefail

id=${1:?usage: register-template.sh <id> <spec.json>}
spec=${2:?usage: register-template.sh <id> <spec.json>}
url=${ADMIN_URL:-http://gateway-1:8081}
token=${GATEWAY_ADMIN_TOKEN:?set GATEWAY_ADMIN_TOKEN (GATEWAY_ADMIN_TOKEN_PROMPT_OWNERS in deploy/compose/gateway.env)}

resp=$(curl -sS -w '\n%{http_code}' -X POST -H "Authorization: Bearer $token" \
  -H 'Content-Type: application/json' --data-binary "@$spec" "$url/admin/v1/templates/$id/versions")
code=${resp##*$'\n'}
body=${resp%$'\n'*}
if [[ $code != 201 && $code != 200 ]]; then
  echo "register $id: HTTP $code: $body" >&2
  exit 1
fi
version=$(sed -n 's/.*"version":\([0-9]*\).*/\1/p' <<<"$body" | head -1)
curl -sS -f -X POST -H "Authorization: Bearer $token" -H 'Content-Type: application/json' \
  --data '{"status":"active"}' "$url/admin/v1/templates/$id/versions/$version/status" >/dev/null
echo "$id version $version active"
