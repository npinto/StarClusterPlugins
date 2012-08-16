"""Torque StarCluster Plugin for Gentoo-based HVM-EBS AMIs

TODO:
* insure consistency on reboot
* on add node method
"""

from starcluster import clustersetup
from starcluster.logger import log

node_configure_mom = """
cat > /var/spool/torque/mom_priv/config << EOF
\$pbsserver master 
\$logevent 255 
\$usecp *:/home /home 
\$rcpcmd /usr/bin/scp
EOF
"""

master_configure_server = """
mkdir -p /var/spool/torque
echo 'master' > /var/spool/torque/server_name

pbs_server -t create

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



class TorqueSetup(clustersetup.DefaultClusterSetup):

    def run(self, nodes, master, user, user_shell, volumes):

        #master.ssh.execute(
            #"killall -9 pbs_server; killall -9 pbs_sched; CLEAN_DELAY=0 emerge -C torque; rm -rvf /var/spool/torque; FEATURES=buildpkg emerge -g -j torque",
            #silent=False)
        #import IPython; ipshell = IPython.embed; ipshell(banner1='ipshell')

        # -- configure torque's server and scheduler on the master node
        log.info("Configuring torque server...")
        master.ssh.execute(master_configure_server)

        # -- configure torque's clients on each node and complete the
        # configuration on the master node
        for node in nodes[1:]:
            log.info("Configuring torque node '%s'..." % node.alias)
            node.ssh.execute(node_configure_mom)
            self._add_torque_node_to_master(node, master)

        # -- (re)start services
        log.info("Starting torque services...")
        self._force_deamon_restart(master, 'pbs_server')
        for node in nodes[1:]:
            self._start_torque_node_daemon(node)
        self._force_deamon_restart(master, 'pbs_sched')

        # -- print infos / debug
        log.debug("Torque server information:")
        master.ssh.execute("qmgr -c 'l s'")
        master.ssh.execute("qmgr -c 'p s'")

        log.debug("Torque nodes information:")
        for node in nodes[1:]:
            master.ssh.execute('momctl -h %s -d 2' % node.alias)
        master.ssh.execute("qnodes")

    def on_add_node(self, node, nodes, master, user, user_shell, volumes):
        print 'on_add_node'
        #import IPython; ipshell = IPython.embed; ipshell(banner1='ipshell')

    def on_remove_node(self, node, nodes, master, user, user_shell, volumes):
        pass

    def _add_torque_node_to_master(self, node, master):

        node_spec = node.alias + ' '

        np = int(node.ssh.execute(
            "grep '^processor.*: [0-9]*$' /proc/cpuinfo | wc -l")[0])
        node_spec += "np=%d " % np

        gpus = int(node.ssh.execute("ls /dev/nvidia[0-9]* | wc -w")[0])
        if gpus > 0:
            node_spec += "gpus=%d " % gpus

        node_spec += "batch.q"  # our default queue

        log.debug("Using node_spec = '%s'", node_spec)
        master.ssh.execute(
            "echo %s >> /var/spool/torque/server_priv/nodes" % node_spec,
            )

    def _start_torque_node_daemon(self, node):
        self._force_deamon_restart(node, 'pbs_mom')

    def _remove_torque_node_from_master(self, node, master):
        cmd = ("sed -i -e 's/^%s .*batch.q$//g' "
               "/var/spool/torque/server_priv/nodes" % node.alias)
        master.ssh.execute(cmd)

    def _force_deamon_restart(self, machine, service):
        # -- dirty work around the poorly written init.d script
        # XXX: fix init.d stuff in torque's ebuild
        machine.ssh.execute("/etc/init.d/%s stop &> /dev/null | true" % service)
        machine.ssh.execute("killall -9 %s &> /dev/null | true" % service)
        machine.ssh.execute("sleep 2")
        machine.ssh.execute("/etc/init.d/%s start" % service)
