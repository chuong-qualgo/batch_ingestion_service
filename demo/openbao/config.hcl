# OpenBao production mode configuration
# Stores data locally in /openbao/data

storage "file" {
  path = "/openbao/data"
}

listener "tcp" {
  address       = "0.0.0.0:8200"
  tls_disable   = true
}

api_addr = "http://openbao:8200"
ui = true
