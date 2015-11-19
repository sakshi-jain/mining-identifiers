@load base/protocols/conn
#@load base/frameworks/packet-filter
@load base/utils/addrs
@load base/utils/files

global LOCAL_SUBNET: subnet &redef;
global ssl_ports = set( 22/tcp, 443/tcp, 993/tcp );

event connection_established(c: connection)
{
    if ( c$id$orig_h in LOCAL_SUBNET && 
         c$id$resp_p !in ssl_ports ) {
        c$extract_orig = T;
    }

    if ( c$id$resp_h in LOCAL_SUBNET &&
         c$id$resp_p !in ssl_ports ) {
        c$extract_resp = T;
    }
}
