#!/usr/bin/env python

# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation; either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
#
# See LICENSE for more details.
#
# Copyright (c) 2026 ScyllaDB

import re
import time
from enum import Enum

from longevity_test import LongevityTest
from sdcm.cluster import DB_LOG_PATTERN_RESHARDING_FINISH, DB_LOG_PATTERN_RESHARDING_START
from sdcm.sct_events.system import InfoEvent
from sdcm.utils.tablets.common import wait_no_tablets_migration_running
from sdcm.wait import wait_for, wait_for_log_lines


class NodeMigrationStatus(str, Enum):
    USES_VNODES = "uses vnodes"
    MIGRATING = "migrating to tablets"
    USES_TABLETS = "uses tablets"


class MigrationStatusMap(dict):
    """Maps host_id -> NodeMigrationStatus, indexable by node object or host_id string."""

    def __getitem__(self, key):
        host_id = getattr(key, "host_id", key)
        return super().__getitem__(host_id)


def get_nodetool_migrate_to_tablets_status(node, keyspace: str) -> MigrationStatusMap:
    """
    Run 'nodetool migrate-to-tablets status <keyspace>' on the given node and return
    migration status for all nodes in the cluster.

    Example output:
        Nodes:
        Host ID                              Status
        dc1c5a03-8878-49da-a6f3-bd27d6a2d85d uses vnodes
        b1428349-2154-41c9-a5c1-dd33c71bd571 migrating to tablets

    Returns: MigrationStatusMap of host_id -> NodeMigrationStatus, indexable by node object too.
    """
    res = node.run_nodetool(f"migrate-to-tablets status {keyspace}")
    pattern = re.compile(r"(?P<host_id>[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})\s+(?P<status>.+)")
    statuses = MigrationStatusMap()
    for line in res.stdout.splitlines():
        match = pattern.match(line.strip())
        if match:
            statuses[match.group("host_id")] = NodeMigrationStatus(match.group("status").strip())
    return statuses


class VnodeToTabletMigrationTest(LongevityTest):
    """Test vnode to tablet migration scenarios."""

    def restart_node_after_migration(self, node) -> None:
        """Restart a node after migrate-to-tablets upgrade, waiting for resharding to complete.

        After the upgrade the node reshards all data on startup. For large datasets
        (hundreds of GB) this can take 15+ minutes. This method monitors the scylla
        log for resharding start/finish and logs the elapsed duration so slow reshards
        are visible in test output.

        Args:
            node: The DB node to restart.
        """
        reshard_timeout = 3600
        self.log.info(
            "Restarting node %s after migrate-to-tablets upgrade (reshard_timeout=%ds)", node.name, reshard_timeout
        )
        node.run_nodetool("drain")
        node.stop_scylla(verify_down=True)

        reshard_start = time.time()
        with wait_for_log_lines(
            node=node,
            start_line_patterns=[DB_LOG_PATTERN_RESHARDING_START],
            end_line_patterns=[DB_LOG_PATTERN_RESHARDING_FINISH],
            start_timeout=600,
            end_timeout=reshard_timeout,
            error_msg_ctx=f"Resharding on {node.name} after tablet migration",
        ):
            node.start_scylla(verify_up=False, timeout=reshard_timeout * 2)
        reshard_duration = time.time() - reshard_start
        self.log.info(
            "Resharding on %s completed in %.1f seconds (%.1f minutes)",
            node.name,
            reshard_duration,
            reshard_duration / 60,
        )

        node.wait_db_up(timeout=reshard_timeout)
        self.db_cluster.wait_for_nodes_up_and_normal(nodes=[node], timeout=reshard_timeout)
        node.wait_node_fully_start(timeout=reshard_timeout)

    def test_vnode_to_tablet_migration(self):
        """
        Test vnode to tablet migration procedure:

        1. Fill the database with data via prepare_write_cmd.
        2. Launch main stress load (stress_cmd) in the background — runs concurrently with migration.
        3. Start migration for all user keyspaces:
               nodetool migrate-to-tablets start <ks>  (once per keyspace)
        4. Rolling restart - for each node in order:
               nodetool migrate-to-tablets upgrade      (cluster-wide, run on each node)
               drain + stop + start node
        5. Finalize migration for all user keyspaces:
               nodetool migrate-to-tablets finalize <ks>  (once per keyspace)
        6. Wait for all tablet topology operations to quiesce.
        7. Verify migrated keyspaces use tablets via system.tablets metrics.
        8. Wait for main stress load to finish.
        """
        self.db_cluster.add_nemesis(nemesis=self.get_nemesis_class(), tester_obj=self)

        prepare_write_cmd = self.params.get("prepare_write_cmd")

        InfoEvent(message="Step 1 - Filling database with data before migration").publish()
        self.run_pre_create_keyspace()
        self.run_pre_create_schema()
        self.run_prepare_write_cmd()

        if not prepare_write_cmd or not self.params.get("nemesis_during_prepare"):
            self.db_cluster.start_nemesis()

        keyspace_num = self.params.get("keyspace_num")

        InfoEvent(message="Step 2 - Starting main stress load concurrently with migration").publish()
        stress_queue = []
        stress_cmd = self.params.get("stress_cmd")
        self.assemble_and_run_all_stress_cmd(stress_queue, stress_cmd, keyspace_num)

        coordinator_node = self.db_cluster.data_nodes[0]
        keyspaces = self.db_cluster.get_test_keyspaces()
        if not keyspaces:
            raise AssertionError(
                "No user keyspaces found to migrate. Ensure prepare_write_cmd populates at least one keyspace."
            )
        self.log.info("User keyspaces to migrate to tablets: %s", keyspaces)

        InfoEvent(message=f"Step 3 - Starting tablet migration for keyspaces: {keyspaces}").publish()
        for ks in keyspaces:
            coordinator_node.run_nodetool(f"migrate-to-tablets start {ks}")
            nodes_status = get_nodetool_migrate_to_tablets_status(coordinator_node, ks)
            for node in self.db_cluster.data_nodes:
                assert nodes_status[node] == NodeMigrationStatus.USES_VNODES, (
                    f"[ks={ks}] Expected {NodeMigrationStatus.USES_VNODES!r} for {node.host_id}, got {nodes_status[node]!r}"
                )

        InfoEvent(message="Step 4 - Rolling restart with migrate-to-tablets upgrade").publish()
        for node in self.db_cluster.data_nodes:
            wait_for(
                func=lambda n=node: not n.running_nemesis,
                step=30,
                timeout=600,
                text=f"Waiting for nemesis on {node.name} to finish before upgrade",
            )
            with self.nemesis_allocator.nodes_running_nemesis(node, "vnode_to_tablet_migration"):
                self.log.info("Preparing node %s (ip=%s) for tablet migration", node.name, node.ip_address)
                node.run_nodetool("migrate-to-tablets upgrade")
                self.log.info("Verify that the node status changed from vnodes to migrating to tablets")
                for ks in keyspaces:
                    nodes_status = get_nodetool_migrate_to_tablets_status(node, ks)
                    assert nodes_status[node] == NodeMigrationStatus.MIGRATING, (
                        f"[ks={ks}] Expected {NodeMigrationStatus.MIGRATING!r} for {node.host_id}, got {nodes_status[node]!r}"
                    )
                self.restart_node_after_migration(node)
                self.log.info("Node %s is back up and normal after restart", node.name)

            self.log.info("Waiting for node %s status to change to 'uses tablets'", node.name)
            for ks in keyspaces:
                wait_for(
                    func=lambda n=node, k=ks: get_nodetool_migrate_to_tablets_status(n, k)[n]
                    == NodeMigrationStatus.USES_TABLETS,
                    step=60,
                    timeout=3600,
                    text=f"Waiting for node {node.name} to reach {NodeMigrationStatus.USES_TABLETS!r} for keyspace {ks}",
                    throw_exc=True,
                )

        InfoEvent(message="Step 5 - Finalizing tablet migration").publish()
        for ks in keyspaces:
            coordinator_node.run_nodetool(f"migrate-to-tablets finalize {ks}")

        InfoEvent(message="Step 6 - Waiting for tablet migration to fully complete").publish()
        for migration_node in self.db_cluster.data_nodes:
            wait_no_tablets_migration_running(migration_node, timeout=7200)

        InfoEvent(message="Step 7 - Verifying migrated keyspaces have tablets in system.tablets").publish()
        with self.db_cluster.cql_connection_patient(coordinator_node, connect_timeout=600) as session:
            result = session.execute("SELECT keyspace_name FROM system.tablets")
            tablet_keyspaces = {row.keyspace_name for row in result}
        self.log.info("Keyspaces with tablets: %s", tablet_keyspaces)
        for ks in keyspaces:
            assert ks in tablet_keyspaces, f"Keyspace {ks} not found in system.tablets after migration"

        InfoEvent(message="Step 8 - Waiting for main stress load to finish").publish()
        for stress in stress_queue:
            self.verify_stress_thread(stress)
