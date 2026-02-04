import ipaddr from 'ipaddr.js';

export function isLocalHostname(host: string): boolean {
  return (
    host === 'localhost' ||
    host.endsWith('.localhost') ||
    host.endsWith('.local')
  );
}

export function isPrivateIp(ip: string): boolean {
  if (!ipaddr.isValid(ip)) {
    return true;
  }

  const addr = ipaddr.parse(ip);
  if (isIPv6Address(addr) && addr.isIPv4MappedAddress()) {
    return isPrivateIp(addr.toIPv4Address().toString());
  }

  return addr.range() !== 'unicast';
}

function isIPv6Address(addr: ipaddr.IPv4 | ipaddr.IPv6): addr is ipaddr.IPv6 {
  return addr.kind() === 'ipv6';
}