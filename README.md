# file store service

### [Click to [File Store Client]](https://github.com/luyikk/file-store-client)


make config
```sh 
fss create
```

```toml
root="./store"


[service]
# the local IP address and port that the service listens on.
addr = "0.0.0.0:7556"

# used to verify whether the service_name in the client configuration is correct
service_name = "file-store-service"

# used to verify whether the verify_key in the client configuration is correct.
verify_key = ""

# the timeout period for the server to request the client
request_out_time = 5000

# the existence time of the client peer session. If it exceeds this time, the client peer will be cleared
session_save_time = 5000

## used to configure TLS communication encryption (optional).
## if not provided, TLS will not be used for communication encryption
[tls]
## ca file path (optional)
## if not provided, the client’s certificate will not be verified.
ca = "./tls/ca.crt"

## cert file path
cert = "./tls/server-crt.pem"

## key file path
key = "./tls/server-key.pem"
```

exec
```shell
fss exec
```

service install 
```shell
fss service install
```

service start
```shell
fss service start
```

service stop
```shell
fss service stop
```

service uninstall
```shell
fss service uninstall
```