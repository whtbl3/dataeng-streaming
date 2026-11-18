#!/usr/bin/env bash
# Test the guard hooks without opening Claude. Run: bash scripts/test_hooks.sh
set -u
HOOK=.claude/hooks/protect_secrets.py
pass=0; fail=0
expect() { # expect <exit_code> <path>
  echo "{\"tool_input\":{\"file_path\":\"$2\"}}" | python3 "$HOOK" 2>/dev/null
  got=$?
  if [ "$got" -eq "$1" ]; then pass=$((pass+1)); echo "ok   $2 -> $got"
  else fail=$((fail+1)); echo "FAIL $2 -> $got (expected $1)"; fi
}
expect 2 ".env"
expect 2 "config/.env.production"
expect 2 "keys/server.pem"
expect 2 "secrets/api.txt"
expect 2 "infra/aws/terraform.tfvars"
expect 2 "infra/aws/terraform.tfstate"
expect 2 "home/.aws/credentials"
expect 0 ".env.example"
expect 0 "infra/aws/terraform.tfvars.example"
expect 0 "dbt/profiles.yml"
expect 0 "src/app.py"
expect 0 "README.md"
echo "passed=$pass failed=$fail"
[ "$fail" -eq 0 ]
