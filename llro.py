#!/usr/bin/env python3
import yaml
import logging
from icmplib import async_multiping
import asyncio
import argparse
import subprocess

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
        hosts = set()
        for host in self.config['monitor']:
            hosts.add(host)
            if host in self.config.get('also_route', {}):
                hosts.update(self.config['also_route'][host])

        for host in hosts:
            cmd = f"{self.config.get('ip_bin', '/usr/sbin/ip')} route del {host}/32"
            logging.debug("cmd: %s", cmd)
            try:
                logging.debug(subprocess.check_output(cmd, stderr=subprocess.STDOUT, shell=True))
            except subprocess.CalledProcessError as e:
                if 'RTNETLINK answers: No such process' in e.output.decode('utf-8'):
                    continue
                logging.error("command '%s' return with error (code %s): %s", e.cmd, e.returncode, e.output)
            except Exception as e:
                logging.error(e)

        self.current_routes = {}

        # set fallback routes as no route set
        for host, gateway in self.config.get('fallback_routes', {}).items():
            self.apply_route_config(host, gateway)

    def clear_route(self, host):
        """
        Removes the route for the given host.

        Parameters:
            host (str): The host to remove the route for.

        Returns:
            None
        """

        logging.info("Remove %s", host)
        cmd = f"{self.config.get('ip_bin', '/usr/sbin/ip')} route del {host}/32"
        logging.debug("cmd: %s", cmd)
        try:
            logging.debug(subprocess.check_output(cmd, stderr=subprocess.STDOUT, shell=True))
        except subprocess.CalledProcessError as e:
            if 'RTNETLINK answers: No such process' in e.output.decode('utf-8'):
                return
            logging.error("command '%s' return with error (code %s): %s", e.cmd, e.returncode, e.output)
        except Exception as e:
            logging.error(e)

    def apply_route_config(self, host, gateway):
        """
        Applies the route configuration.

        Adds the route to the routing table for the given host and gateway.
        Additionally, if the also_route configuration is specified, it will also add the route for
        the given host to the routing table of the specified hosts.

        Parameters:
            host (str): The host to add the route for.
            gateway (str): The gateway to use for the route.

        Returns:
            None
        """
        hosts_to_add = [host] + self.config.get('also_route', {}).get(host, [])

        logging.info("Apply %s => %s", host, gateway)
        for _host in hosts_to_add:
            if _host not in self.current_routes:
                cmd = f"{self.config.get('ip_bin', '/usr/sbin/ip')} route add {_host}/32 via {gateway}"
                logging.debug("cmd: %s", cmd)
                try:
                    logging.debug(subprocess.check_output(cmd, stderr=subprocess.STDOUT, shell=True))
                    continue
                except subprocess.CalledProcessError as e:
                    if 'RTNETLINK answers: File exists' not in e.output.decode('utf-8'):
                        logging.error("command '%s' return with error (code %s): %s", e.cmd, e.returncode, e.output)
                        continue
                except Exception as e:
                    logging.exception(e)

            cmd = f"{self.config.get('ip_bin', '/usr/sbin/ip')} route replace {_host}/32 via {gateway}"
            logging.debug("cmd: %s", cmd)
            try:
                logging.debug(subprocess.check_output(cmd, stderr=subprocess.STDOUT, shell=True))
            except subprocess.CalledProcessError as e:
                logging.error("command '%s' return with error (code %s): %s", e.cmd, e.returncode, e.output)
            except Exception as e:
                logging.exception(e)

            self.current_routes[_host] = gateway

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

            # send ICMP requests
            tasks = []
            sources = []
            for etherif, src_ips in self.config['interfaces'].items():
                for src_ip in src_ips:
                    tasks.append(loop.create_task(async_multiping(self.config['monitor'], count=self.config.get(
                        'test_count', 3), source=src_ip, interval=float(self.config.get('test_interval', 0.5)))))
                    sources.append(src_ip)

            # wait and aggregate results
            result = await asyncio.gather(*tasks)

            # process results
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

            # sort by newest
            force_reset = False
            for host, results in host_data.items():
                if host in self.current_routes and self.current_routes[host] not in sources_up:
                    force_reset = True
                host_data[host] = sorted(results, key=lambda y: (y[2], y[1]))
                logging.debug("%s: %s", host, host_data[host])

            # apply routes
            checks += 1
            valid_source_found = []
            if checks >= self.config.get('test_count', 10) or force_reset or not self.current_routes:
                for host, results in sums.items():
                    host_data = []
                    for source, metrics in results.items():
                        metrics['rtt'] = metrics['rtt'] / checks
                        metrics['loss'] = metrics['loss'] / checks
                        host_data.append((source, metrics['rtt'], metrics['loss']))
                        logging.debug("%s: %s: %s %s", host, source, metrics['rtt'], metrics['loss'])

                    host_data = sorted(host_data, key=lambda y: (y[2], y[1]))[0]

                    # no routing set
                    if host not in self.current_routes or self.current_routes[host] not in results:
                        valid_source_found.append(host)
                        self.apply_route_config(host, host_data[0])
                        continue

                    # no change
                    if self.current_routes[host] == host_data[0]:
                        valid_source_found.append(host)
                        logging.debug("%s: Current route is already the fastest route", host)
                        continue

                    # paketloss
                    if results[self.current_routes[host]]['loss'] > self.config.get('paketloss_threshold', 5):
                        logging.warning("%s: Current route has paketloss, need to switch", host)
                    else:
                        # check rtt difference between current and fastest route
                        rtt_diff = results[self.current_routes[host]]['rtt'] - host_data[1]
                        logging.debug("%s: rtt_diff: %s, (%s) %s (%s) %s ", host, rtt_diff, self.current_routes[host], results[self.current_routes[host]]['rtt'], host_data[0], host_data[1])

                        if rtt_diff < self.config.get('rtt_threshold', 20):
                            valid_source_found.append(host)
                            logging.info("%s: Route not changed to %s, rtt difference %s < threshold %s",
                                         host, host_data[0], round(rtt_diff, 3), self.config.get('rtt_threshold', 10))
                            continue

                    valid_source_found.append(host)
                    self.apply_route_config(host, host_data[0])

                checks = 0
                sums = {}

                for sip in self.config['monitor']:
                    if sip in valid_source_found:
                        continue
                    logging.warning("No valid source found for %s", sip)

                    # fallback routes
                    if self.config.get('fallback_routes', {}).get(sip):
                        self.apply_route_config(sip, self.config.get('fallback_routes', {}).get(sip))
                    else:
                        logging.warning("No fallback routes configured for %s", sip)
                        self.clear_route(sip)

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
