#!/usr/bin/env python3
"""Change the isolated Redis endpoint address without restarting messaging APs."""

import argparse
from collections import Counter
import ipaddress
import json
import subprocess
import time
import uuid

from main_flow import AP_HEALTH_ENDPOINTS, ROOT, application_health, compose, demo
from webhook_flow import key, read_items


def docker(*args):
    return subprocess.run(["docker", *args], capture_output=True, text=True,
                          check=True, timeout=30).stdout.strip()


def inspect(container):
    return json.loads(docker("inspect", container))[0]


def redis_connections():
    clients = compose("exec", "-T", "redis", "redis-cli", "--raw", "CLIENT", "LIST")
    addresses = []
    for line in clients.splitlines():
        fields = dict(part.split("=", 1) for part in line.split() if "=" in part)
        if fields.get("lib-name", "").lower().startswith("lettuce"):
            addresses.append(fields["addr"].rsplit(":", 1)[0])
    return dict(Counter(addresses))


def wait_health(timeout=90, expected_connections=None):
    started = time.monotonic()
    health = {}
    while time.monotonic() - started < timeout:
        health = application_health()
        if len(health) == len(AP_HEALTH_ENDPOINTS) and all(value == "UP" for value in health.values()):
            connections = redis_connections()
            if all(connections.get(address, 0) >= count for address, count in (expected_connections or {}).items()):
                return {"seconds": round(time.monotonic() - started, 2), "health": health,
                        "redisConnections": connections}
        time.sleep(1)
    raise TimeoutError(f"Redis address recovery failed: {health}")


def run():
    report = {"runId": uuid.uuid4().hex[:8], "pass": False}
    destination = ROOT / ".monitoring/redis-dns-latest.json"
    destination.parent.mkdir(exist_ok=True)

    def checkpoint(stage):
        report["stage"] = stage
        destination.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
        print(stage, flush=True)

    container = compose("ps", "--quiet", "redis")
    before = inspect(container)
    assert before["Config"]["Labels"]["com.docker.compose.project"] == "platform-messaging-monitoring"
    networks = before["NetworkSettings"]["Networks"]
    assert len(networks) == 1, "Expected one isolated monitoring network"
    network, endpoint = next(iter(networks.items()))
    original_ip = endpoint["IPAddress"]
    topology = json.loads(docker("network", "inspect", network))[0]
    used = {entry["IPv4Address"].split("/")[0] for entry in topology["Containers"].values()}
    used.update(entry.get("Gateway", "") for entry in topology["IPAM"]["Config"])
    subnet = next(ipaddress.ip_network(entry["Subnet"]) for entry in topology["IPAM"]["Config"]
                  if ":" not in entry["Subnet"])
    # Only allocate within this project's network, preserving the original address for restoration.
    new_ip = next(str(ip) for ip in reversed(list(subnet.hosts())) if str(ip) not in used)
    report.update({"network": network, "originalIp": original_ip, "changedIp": new_ip})
    apps = {service: compose("ps", "--quiet", service) for service in AP_HEALTH_ENDPOINTS}
    starts = {service: inspect(identifier)["State"]["StartedAt"] for service, identifier in apps.items()}

    def move(address):
        if network in inspect(container)["NetworkSettings"]["Networks"]:
            docker("network", "disconnect", network, container)
        aliases = [arg for alias in endpoint.get("Aliases", []) for arg in ("--alias", alias)]
        docker("network", "connect", "--ip", address, *aliases, network, container)
        # Simulate replacement closing existing sockets, while preserving the same Redis data.
        compose("exec", "-T", "redis", "redis-cli", "CLIENT", "KILL", "TYPE", "normal", "SKIPME", "yes")
        assert inspect(container)["NetworkSettings"]["Networks"][network]["IPAddress"] == address

    checkpoint("checking-application-health")
    wait_health()
    flow_args = argparse.Namespace(rate=1, seconds=1, errors=True, secondary=True,
                                   startup_timeout=60, timeout=120)
    # Health uses reactive connections; exercise synchronous business connections before moving Redis too.
    demo(flow_args)
    report["warmupFlow"] = json.loads((ROOT / ".monitoring/demo-latest.json").read_text())
    expected_connections = redis_connections()
    assert len(expected_connections) == 6, f"Expected the six Redis APs: {expected_connections}"
    report["expectedRedisConnections"] = expected_connections
    checkpoint("changing-redis-address")
    try:
        move(new_ip)
        report["changedAddressRecovery"] = wait_health(expected_connections=expected_connections)
        checkpoint("new-address-recovered")
        demo(flow_args)
        report["mainFlow"] = json.loads((ROOT / ".monitoring/demo-latest.json").read_text())
        assert all(inspect(identifier)["State"]["StartedAt"] == starts[service]
                   for service, identifier in apps.items()), "An AP restarted during address recovery"
        report["applicationsRestarted"] = False
    except Exception as failure:
        report["failure"] = f"{type(failure).__name__}: {failure}"
        checkpoint("address-change-failed")
        raise
    finally:
        checkpoint("restoring-redis-address")
        move(original_ip)
        report["restoredAddressRecovery"] = wait_health(expected_connections=expected_connections)
        report["restoredIp"] = inspect(container)["NetworkSettings"]["Networks"][network]["IPAddress"]
        checkpoint("original-address-restored")
    assert all(inspect(identifier)["State"]["StartedAt"] == starts[service]
               for service, identifier in apps.items()), "An AP restarted during address restoration"
    identifiers = set(report["warmupFlow"]["cases"]) | set(report["mainFlow"]["cases"])
    assert not read_items("ORIGIN", [key(identifier, "META") for identifier in identifiers]), "ORIGIN remains"
    report["originsDeleted"] = len(identifiers)
    report["pass"] = True
    checkpoint("complete")
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    run()
