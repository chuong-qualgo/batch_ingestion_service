# OpenBao demo config — file storage backend, no TLS
# NOT for production use

ui            = true
log_level     = "info"
disable_mlock = true       # required in Docker without IPC_LOCK in some envs

listener "tcp" {
  address     = "0.0.0.0:8200"
  tls_disable = "true"
}

storage "file" {
  path = "/openbao/data"
}

api_addr     = "http://0.0.0.0:8200"
cluster_addr = "http://0.0.0.0:8201"
