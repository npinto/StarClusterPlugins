"""Torque StarCluster Plugin for Gentoo-based HVM-EBS AMIs

TODO:
* insure consistency on reboot
* on add node method
"""

from starcluster.clustersetup import ClusterSetup
#from starcluster.logger import log

node_configure_mom = """
cat > /var/spool/torque/mom_priv/config << EOF
\$pbsserver master 
\$logevent 255 
\$usecp *:/home /home 
\$rcpcmd /usr/bin/scp
EOF
"""

node_start_services = """
/etc/init.d/pbs_mom start
"""

master_configure_server = """
echo 'y' > pbs_server -t create

qmgr << EOF
set server scheduling = True
set server acl_host_enable = True
set server acl_hosts = master
set server acl_users = root
set server managers = root@localhost
set server operators = root@localhost
set server log_events = 511
set server query_other_jobs = True
set server scheduler_iteration = 60
set server node_ping_rate = 30
set server node_check_rate = 150
set server tcp_timeout = 6
set server mom_job_sync = True
set server keep_completed = 300
set server record_job_info = True
set server default_queue = route.q
EOF

echo 'root@localhost' > /var/spool/torque/server_priv/acl_svr/operators
echo 'root@localhost' > /var/spool/torque/server_priv/acl_svr/managers

qmgr << EOF
create queue route.q
set queue route.q queue_type = Route
set queue route.q resources_default.nodes = 1
set queue route.q resources_default.walltime = 01:00:00
set queue route.q route_destinations = batch.q
set queue route.q started = True
set queue route.q enabled = True
EOF

qmgr << EOF
create queue batch.q
set queue batch.q queue_type = Execution
set queue batch.q resources_default.nodes = 1
set queue batch.q resources_default.walltime = 01:00:00
set queue batch.q enabled = True
set queue batch.q started = True
set queue batch.q resources_default.neednodes = batch.q
EOF


touch /var/spool/torque/sched_priv/usage

mv -f /var/spool/torque/sched_priv/sched_config{,.orig}
cat > /var/spool/torque/sched_priv/sched_config << EOF
round_robin: False  all
by_queue: True      prime
by_queue: True      non_prime
strict_fifo: false  ALL
fair_share: true   ALL
help_starving_jobs  false   ALL
sort_queues true    ALL
load_balancing: true   ALL
sort_by: shortest_job_first ALL
log_filter: 256
dedicated_prefix: ded
max_starve: 24:00:00
half_life: 24:00:00
unknown_shares: 10
sync_time: 1:00:00
EOF
"""

master_start_services = """
/etc/init.d/pbs_server start
/etc/init.d/pbs_sched start
"""


class TorqueSetup(ClusterSetup):

    def run(self, nodes, master, user, user_shell, volumes):

        # -- configure torque's server and scheduler on the master node
        master.ssh.execute(master_configure_server)

        # -- configure torque's clients on each node and complete the
        # configuration on the master node
        for node in nodes:
            if node is master:
                continue
            node.ssh.execute(node_configure_mom)
            node_spec = node.alias + ' '
            np = int(node.ssh.execute(
                "grep '^processor.*: [0-9]*$' /proc/cpuinfo | wc -l")[0])
            node_spec += "np=%d " % np
            gpus = int(node.ssh.execute("ls /dev/nvidia[0-9]* | wc -w")[0])
            if gpus > 0:
                node_spec += "gpus=%d " % gpus
            node_spec += "batch.q"  # default queue
            print node_spec
            master.ssh.execute(
                "echo %s >> /var/spool/torque/server_priv/nodes" % node_spec,
                )

        # -- start services
        for node in nodes:
            if node is master:
                master.ssh.execute(master_start_services)
            node.ssh.execute(node_start_services)
