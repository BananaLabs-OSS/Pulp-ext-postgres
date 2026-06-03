module github.com/BananaLabs-OSS/Pulp-ext-postgres

go 1.25.6

require (
	github.com/BananaLabs-OSS/Pulp v0.0.0
	github.com/fergusstrange/embedded-postgres v1.34.0
	github.com/lib/pq v1.10.9
	github.com/tetratelabs/wazero v1.11.0
	github.com/vmihailenco/msgpack/v5 v5.4.1
)

require (
	github.com/vmihailenco/tagparser/v2 v2.0.0 // indirect
	github.com/xi2/xz v0.0.0-20171230120015-48954b6210f8 // indirect
	golang.org/x/sys v0.42.0 // indirect
)

replace github.com/BananaLabs-OSS/Pulp => ../Pulp
