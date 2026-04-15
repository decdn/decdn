#!/bin/bash
set -euo pipefail  # Exit on error, undefined vars, and pipeline failures
IFS=$'\n\t'       # Word splitting on newlines and tabs only (spaces preserved)

# If the script exits for any reason before completing successfully,
# set default-deny policies so the container is locked down rather than
# left wide open with ACCEPT policies from the setup phase.
lockdown_on_failure() {
    echo "FATAL: Firewall setup failed. Setting default-deny policies for safety."
    iptables -P INPUT DROP 2>/dev/null || true
    iptables -P OUTPUT DROP 2>/dev/null || true
    iptables -P FORWARD DROP 2>/dev/null || true
}
trap lockdown_on_failure ERR EXIT

# Critical domains must resolve or the script aborts (with retry).
# Optional domains are best-effort — CDN/analytics that may use CNAME-only records.
CRITICAL_DOMAINS=(
    "github.com"
    "api.github.com"
    "api.anthropic.com"
    "sentry.io"
    "registry.npmjs.org"
    "crates.io"
    "static.crates.io"
    "index.crates.io"
    # pre-commit bootstraps Python hook envs via `pip install .`, which
    # fetches the PEP 517 build toolchain (setuptools) and declared runtime
    # deps from these two hosts. Without them, the `pre-commit-hooks`
    # environment fails to install and every commit hits the same error.
    "pypi.org"
    "files.pythonhosted.org"
)

OPTIONAL_DOMAINS=(
    "statsig.anthropic.com"
    "statsig.com"
    "marketplace.visualstudio.com"
    "vscode.blob.core.windows.net"
    "update.code.visualstudio.com"
    "static.rust-lang.org"
    "sepolia-rollup.arbitrum.io"
    "arb-sepolia.g.alchemy.com"
)

# Resolve a domain to A record IPs with retries.
# Returns IPs on stdout, exits 1 if all retries fail.
resolve_domain() {
    local domain="$1"
    local retries=3
    local ips=""
    for ((i=1; i<=retries; i++)); do
        ips=$(dig +noall +answer A "$domain" | awk '$4 == "A" {print $5}')
        if [ -n "$ips" ]; then
            echo "$ips"
            return 0
        fi
        if [ "$i" -lt "$retries" ]; then
            echo "  Retry $i/$retries for $domain..." >&2
            sleep 2
        fi
    done
    return 1
}

# Add an IP to the allowed-domains ipset, failing on real errors.
add_to_ipset() {
    local ip="$1"
    local domain="$2"
    local output
    if output=$(ipset add allowed-domains "$ip" 2>&1); then
        echo "Adding $ip for $domain"
    elif [[ "$output" == *"already added"* ]]; then
        echo "  $ip for $domain (already in set)"
    else
        echo "ERROR: Failed to add $ip for $domain to ipset: $output"
        exit 1
    fi
}

# 1. Extract Docker DNS info BEFORE any flushing
DOCKER_DNS_RULES=$(iptables-save -t nat | grep "127\.0\.0\.11" || true)

# Flush existing rules and delete existing ipsets
iptables -F
iptables -X
iptables -t nat -F
iptables -t nat -X
iptables -t mangle -F
iptables -t mangle -X

if ipset list allowed-domains &>/dev/null; then
    ipset destroy allowed-domains
fi

# Ensure policies are permissive during setup (needed if script re-runs
# after a previous run already set DROP policies)
iptables -P INPUT ACCEPT
iptables -P OUTPUT ACCEPT
iptables -P FORWARD ACCEPT

# 2. Selectively restore ONLY internal Docker DNS resolution
if [ -n "$DOCKER_DNS_RULES" ]; then
    echo "Restoring Docker DNS rules..."
    iptables -t nat -N DOCKER_OUTPUT 2>/dev/null || true
    iptables -t nat -N DOCKER_POSTROUTING 2>/dev/null || true
    while IFS= read -r rule; do
        [[ "$rule" =~ ^-A ]] || continue
        iptables -t nat $rule || echo "WARNING: Failed to restore NAT rule: $rule"
    done <<< "$DOCKER_DNS_RULES"
else
    echo "No Docker DNS rules to restore"
fi

# Detect host gateway early — needed for DNS rules below
HOST_IP=$(ip route | grep default | cut -d" " -f3)
if [ -z "$HOST_IP" ]; then
    echo "ERROR: Failed to detect host IP"
    exit 1
fi
echo "Host gateway detected as: $HOST_IP"

# Allow DNS to any destination — the actual DNS server varies by Docker network
# mode (127.0.0.11 on user-defined networks, host gateway or external DNS on
# default bridge). HTTP/HTTPS is restricted via ipset; DNS itself is not a
# meaningful exfiltration vector for this threat model.
iptables -A OUTPUT -p udp --dport 53 -j ACCEPT
iptables -A OUTPUT -p tcp --dport 53 -j ACCEPT
iptables -A INPUT -p udp --sport 53 -m state --state ESTABLISHED,RELATED -j ACCEPT
iptables -A INPUT -p tcp --sport 53 -m state --state ESTABLISHED,RELATED -j ACCEPT
# Allow localhost
iptables -A INPUT -i lo -j ACCEPT
iptables -A OUTPUT -o lo -j ACCEPT

# Create ipset (supports both CIDR ranges and individual IPs)
ipset create allowed-domains hash:net

# Fetch GitHub meta information and aggregate + add their IPv4 ranges
echo "Fetching GitHub IP ranges..."
gh_ranges=$(curl -sS --fail --connect-timeout 10 https://api.github.com/meta) || {
    echo "ERROR: Failed to fetch GitHub IP ranges (curl exit code: $?)"
    exit 1
}

if ! echo "$gh_ranges" | jq -e '.web and .api and .git' >/dev/null; then
    echo "ERROR: GitHub API response missing required fields"
    exit 1
fi

echo "Processing GitHub IPs..."
github_cidrs=$(echo "$gh_ranges" | jq -r '(.web + .api + .git)[]' | grep '\.' | aggregate -q)
if [ -z "$github_cidrs" ]; then
    echo "ERROR: No GitHub CIDR ranges produced after aggregation"
    exit 1
fi

cidr_count=0
while read -r cidr; do
    if [[ ! "$cidr" =~ ^[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}/[0-9]{1,2}$ ]]; then
        echo "ERROR: Invalid CIDR range from GitHub meta: $cidr"
        exit 1
    fi
    echo "Adding GitHub range $cidr"
    ipset add allowed-domains "$cidr"
    cidr_count=$((cidr_count + 1))
done <<< "$github_cidrs"
echo "Added $cidr_count GitHub CIDR ranges"

# Resolve and add critical domains (fail-hard with retry)
for domain in "${CRITICAL_DOMAINS[@]}"; do
    echo "Resolving $domain (critical)..."
    if ! ips=$(resolve_domain "$domain"); then
        echo "ERROR: Failed to resolve critical domain $domain after retries"
        exit 1
    fi
    while read -r ip; do
        add_to_ipset "$ip" "$domain"
    done <<< "$ips"
done

# Resolve and add optional domains (warn-and-skip)
for domain in "${OPTIONAL_DOMAINS[@]}"; do
    echo "Resolving $domain (optional)..."
    if ! ips=$(resolve_domain "$domain"); then
        echo "WARNING: Could not resolve optional domain $domain — skipping"
        continue
    fi
    while read -r ip; do
        add_to_ipset "$ip" "$domain"
    done <<< "$ips"
done

# Allow traffic to/from host gateway (non-DNS traffic, e.g. Docker API)
iptables -A INPUT -s "$HOST_IP" -j ACCEPT
iptables -A OUTPUT -d "$HOST_IP" -j ACCEPT

# Allow outbound SSH only to allowed domains (e.g., GitHub git IP ranges)
iptables -A OUTPUT -p tcp --dport 22 -m set --match-set allowed-domains dst -j ACCEPT

# Allow established connections for already approved traffic
iptables -A INPUT -m state --state ESTABLISHED,RELATED -j ACCEPT
iptables -A OUTPUT -m state --state ESTABLISHED,RELATED -j ACCEPT

# Allow only specific outbound traffic to allowed domains
iptables -A OUTPUT -m set --match-set allowed-domains dst -j ACCEPT

# Explicitly REJECT all other outbound traffic for immediate feedback
iptables -A OUTPUT -j REJECT --reject-with icmp-admin-prohibited

# Set default policies to DROP *last* — all ACCEPT rules are already in place,
# so a failure here cannot brick networking
iptables -P INPUT DROP
iptables -P FORWARD DROP
iptables -P OUTPUT DROP

echo "Firewall configuration complete"

# Verify ipset is populated
IPSET_COUNT=$(ipset list allowed-domains | grep -c "^[0-9]" || echo "0")
if [ "$IPSET_COUNT" -lt 5 ]; then
    echo "ERROR: ipset contains only $IPSET_COUNT entries — expected many more"
    exit 1
fi
echo "ipset contains $IPSET_COUNT entries"

# Verify blocked traffic
echo "Verifying firewall rules..."
if curl --connect-timeout 5 https://example.com >/dev/null 2>&1; then
    echo "ERROR: Firewall verification failed - was able to reach https://example.com"
    exit 1
else
    echo "Firewall verification passed - unable to reach https://example.com as expected"
fi

# Verify critical services are reachable
for verify_url in \
    "https://api.github.com/zen" \
    "https://api.anthropic.com"; do
    if ! curl --connect-timeout 5 -o /dev/null -sS "$verify_url"; then
        echo "ERROR: Firewall verification failed - unable to reach $verify_url"
        exit 1
    fi
    echo "Firewall verification passed - $verify_url reachable"
done

# Remove the failure trap — setup completed successfully
trap - ERR EXIT
