#!/bin/sh
# Fill the gateway secrets in a copied gateway.env that the committed example
# leaves empty, so nothing well-known ships with the repo:
#
#   scripts/compose-secrets.sh [deploy/compose]
#
#   GATEWAY_ADMIN_TOKEN_PROMPT_OWNERS  a random 192-bit token
#   GATEWAY_TEMPLATES_POSTGRES_DSN     built from POSTGRES_USER, POSTGRES_PASSWORD
#                                      and POSTGRES_DB in the copied postgres.env,
#                                      so the two always agree
#
# `make compose-env` runs it after copying (locally and in CI). A value that
# is already set is never touched, so re-running is safe. A Postgres password
# with characters a URL cannot hold unescaped is reported and the DSN left
# empty (the registry and admin API stay off until the operator sets it).
# POSIX sh, so it also runs where only sh exists (e.g. Alpine images).
set -eu

dir=${1:-deploy/compose}
genv=$dir/gateway.env
penv=$dir/postgres.env

[ -f "$genv" ] || exit 0

# empty FILE KEY: true when FILE has KEY= with no value.
empty() { grep -q "^$2=[[:space:]]*$" "$1"; }

# value FILE KEY: the value of KEY.
value() { sed -n "s/^$2=//p" "$1" | head -n 1; }

# set FILE KEY VALUE: replace the empty KEY= line. VALUE must not hold '|' or '&'.
set_value() {
	tmp=$(mktemp "$1.XXXXXX")
	sed "s|^$2=[[:space:]]*$|$2=$3|" "$1" > "$tmp"
	cat "$tmp" > "$1" # keep the file's mode and owner
	rm -f "$tmp"
	echo "generated $2 in $1"
}

key=GATEWAY_ADMIN_TOKEN_PROMPT_OWNERS
if empty "$genv" "$key"; then
	set_value "$genv" "$key" "$(head -c 24 /dev/urandom | od -An -tx1 | tr -d ' \n')"
fi

key=GATEWAY_TEMPLATES_POSTGRES_DSN
if empty "$genv" "$key" && [ -f "$penv" ]; then
	user=$(value "$penv" POSTGRES_USER)
	pass=$(value "$penv" POSTGRES_PASSWORD)
	db=$(value "$penv" POSTGRES_DB)
	case "$user$pass$db" in
	'' ) ;;
	*[!A-Za-z0-9._~-]* ) echo "$penv: user, password and database must be URL-safe ([A-Za-z0-9._~-]) to build $key; set it by hand" >&2 ;;
	* ) [ -n "$user" ] && [ -n "$pass" ] && [ -n "$db" ] &&
		set_value "$genv" "$key" "postgres://$user:$pass@postgres:5432/$db?sslmode=disable" ;;
	esac
fi
