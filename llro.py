#!/usr/bin/env python3
import yaml
import logging
from icmplib import async_multiping
import asyncio
import argparse
# import pyroute2
import os

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)-8s %(message)s',
)


class LowestLatencyRoutesOptimizer:
    def __init__(self, config: dict):
        self.config = config
        self.current_routes = {}

    def run(self):
        """
        Runs the main loop

        Parameters:
            None

        Returns:
            None
        """
        if self.config.get('delete_preadded_routes'):
            self.clear_routes()
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self.run_async())

    def clear_routes(self):
        """
        Clears the routes that are not needed.

        Iterates over the source IP addresses specified in the configuration file and checks if they are present in the routes. If a route has a gateway IP that is not in the list of source IP addresses, it is removed. Additionally, if the destination IP of the route is in the list of IP addresses to monitor, it is also removed.

        Parameters:
            None

        Returns:
            None
        """
        source_ips = set()
        for interface, src_ips in self.config['interfaces'].items():
            source_ips.update(src_ips)

        dst_ips = set(self.config['monitor'])
        for _dst_ips in self.config.get('also_route', {}).values():
            dst_ips.update(_dst_ips)

        # as pyroute2 is unstabile as hell i simply replace it with local "ip route" calls.
        for host in source_ips:
            try:
                os.system(f"ip route del {host}/32")
            except Exception as e:
                logging.exception(e)

        """
        with pyroute2.NDB() as ndb:
            for host in dst_ips:
                try:
                    with ndb.routes[f"{host}/32"] as route:
                        gw = route['gateway']
                        if gw not in source_ips:
                            continue

                        logging.info("Remove %s => %s", host, gw)
                        route.remove()
                except KeyError:
                    continue
        """

    def apply_route_config(self, host, gateway):
        """
        Applies the route configuration.

        Adds the route to the routing table for the given host and gateway. Additionally, if the also_route configuration is specified, it will also add the route for the given host to the routing table of the specified hosts.

        Parameters:
            host (str): The host to add the route for.
            gateway (str): The gateway to use for the route.

        Returns:
            None
        """
        hosts_to_add = [host] + self.config.get('also_route', {}).get(host, [])

        try:
            for host in hosts_to_add:
                # as pyroute2 is unstabile as hell i simply replace it with local "ip route" calls.
                os.system(f"ip route add {host}/32 via {gateway}")
                os.system(f"ip route replace {host}/32 via {gateway}")
                """
                with pyroute2.NDB() as ndb:
                    try:
                        with ndb.routes[{'dst': f"{host}/32"}] as route:
                            logging.info("Update %s => %s (%s)", host, gateway, route['gateway'])
                            route.set('gateway', gateway)
                    except pyroute2.netlink.exceptions.NetlinkError:
                        pass
                    except KeyError:
                        logging.info("Add %s => %s", host, gateway)
                        ndb.routes.create(dst=f"{host}/32", gateway=gateway).commit()
                """
            self.current_routes[host] = gateway
        except Exception as e:
            logging.exception(e)


    async def run_async(self):
        """
        Runs the main loop of the optimizer.

        The loop is responsible for sending ICMP requests to the hosts and setting the routing based on the results.

        Parameters:
            None

        Returns:
            None
        """
        checks = 0
        sums = {}
        loop = asyncio.get_event_loop()
        while True:
            tasks = []
            sources = []
            for etherif, src_ips in self.config['interfaces'].items():
                for src_ip in src_ips:
                    tasks.append(loop.create_task(async_multiping(self.config['monitor'], count=self.config.get(
                        'test_count', 3), source=src_ip, interval=float(self.config.get('test_interval', 0.5)))))
                    sources.append(src_ip)

            result = await asyncio.gather(*tasks)

            host_data = {}
            sources_up = set()
            for x, hosts in enumerate(result):
                source = sources[x]
                for host in hosts:
                    if not host.is_alive:
                        continue

                    sources_up.add(source)

                    if not host.address in sums:
                        sums[host.address] = {}

                    if not source in sums[host.address]:
                        sums[host.address][source] = {
                            'rtt': 0,
                            'loss': 0
                        }

                    if not host.address in host_data:
                        host_data[host.address] = []

                    sums[host.address][source]['rtt'] += host.avg_rtt
                    sums[host.address][source]['loss'] += host.packet_loss

                    host_data[host.address].append((source, host.avg_rtt, host.packet_loss))

            force_reset = False
            for host, results in host_data.items():
                if host in self.current_routes and self.current_routes[host] not in sources_up:
                    force_reset = True
                host_data[host] = sorted(results, key=lambda y: (y[2], y[1]))
                logging.debug("%s: %s", host, host_data[host])

            checks += 1
            if checks >= self.config.get('test_count', 10) or force_reset or not self.current_routes:
                for host, results in sums.items():
                    host_data = []
                    for source, metrics in results.items():
                        metrics['rtt'] = metrics['rtt'] / self.config.get('test_count', 10)
                        metrics['loss'] = metrics['loss'] / self.config.get('test_count', 10)
                        host_data.append((source, metrics['rtt'], metrics['loss']))
                        logging.info("%s: %s: %s %s", host, source, metrics['rtt'], metrics['loss'])
                    host_data = sorted(host_data, key=lambda y: (y[2], y[1]))

                    if host not in self.current_routes:
                        # no routing set
                        self.apply_route_config(host, host_data[0][0])
                        continue

                    if self.current_routes[host] == host_data[0][0]:
                        logging.debug("Current route is fastest route")
                        continue

                    if results[self.current_routes[host][0]]['loss'] > self.config.get('paketloss_threshold', 5):
                        logging.warning("Current route has paketloss, need to switch")
                    else:
                        rtt_diff = results[self.current_routes[host]]['rtt'] - results[host_data[0][0]]['rtt']

                        if rtt_diff < self.config.get('rtt_threshold', 20):
                            logging.info("Route not changed, rtt difference %s < threshold %s", rtt_diff, self.config.get('rtt_threshold', 10))
                            continue

                    self.apply_route_config(host, host_data[0][0])

                checks = 0
                sums = {}

            await asyncio.sleep(self.config.get('scan_interval', 10))


def main():
    parser = argparse.ArgumentParser(
        prog='Lowest Latency Routes Optimizer',
        description='Sends ICMP requests to list of given hosts and set static routing for the fastest response')

    parser.add_argument('--config', type=str, help='Path to config file', required=True)

    args = parser.parse_args()

    try:
        with open(args.config, 'r') as stream:
            try:
                config = yaml.safe_load(stream)
            except yaml.YAMLError as ex:
                logging.exception(ex)
                exit(1)
    except Exception as e:
        logging.exception(e)
        exit(1)

    if not config:
        logging.error('Config could not be parsed')
        exit(1)

    if not config.get('monitor'):
        logging.error('Config does not contain monitor list')
        exit(1)

    if config.get('debug'):
        logging.getLogger('root').setLevel(logging.DEBUG)

    llro = LowestLatencyRoutesOptimizer(config)
    llro.run()


if __name__ == '__main__':
    main()
