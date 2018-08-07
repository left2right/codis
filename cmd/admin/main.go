// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package main

import (
	"fmt"

	"github.com/docopt/docopt-go"

	"github.com/CodisLabs/codis/pkg/utils"
	"github.com/CodisLabs/codis/pkg/utils/log"
)

func main() {
	const usage = `
	Usage:
	codis-admin [-v] --proxy=ADDR [--auth=AUTH] [config|model|stats|slots]
	codis-admin [-v] --proxy=ADDR [--auth=AUTH]  --start
	codis-admin [-v] --proxy=ADDR [--auth=AUTH]  --shutdown
	codis-admin [-v] --proxy=ADDR [--auth=AUTH]  --log-level=LEVEL
	codis-admin [-v] --proxy=ADDR [--auth=AUTH]  --fillslots=FILE [--locked]
	codis-admin [-v] --proxy=ADDR [--auth=AUTH]  --reset-stats
	codis-admin [-v] --proxy=ADDR [--auth=AUTH]  --forcegc
	codis-admin [-v] --codis-topom=ADDR	[--auth=AUTH]	[config|model]
	codis-admin [-v] --codis-topom=ADDR	[--auth=AUTH]	--shutdown
	codis-admin [-v] --codis-topom=ADDR	[--auth=AUTH]	--reload
	codis-admin [-v] --codis-topom=ADDR	[--auth=AUTH]	--log-level=LEVEL
	codis-admin [-v] --codis-topom=ADDR	[--auth=AUTH]   --list-product
	codis-admin [-v] --codis-topom=ADDR	[--auth=AUTH]	--codis-topom-list
	codis-admin [-v] --codis-topom=ADDR	[--auth=AUTH]	--product=NAME		[--product-auth=AUTH]     	[stats|slots|group|proxy]
	codis-admin [-v] --codis-topom=ADDR	[--auth=AUTH]	--product=NAME		[--product-auth=AUTH]		--create-product
	codis-admin [-v] --codis-topom=ADDR	[--auth=AUTH]	--product=NAME		[--product-auth=AUTH]		--update-product
	codis-admin [-v] --codis-topom=ADDR	[--auth=AUTH]	--product=NAME		[--product-auth=AUTH]		--remove-product	[--force]
	codis-admin [-v] --codis-topom=ADDR	[--auth=AUTH]	--product=NAME		[--product-auth=AUTH]		--slots-assign   --beg=ID --end=ID (--gid=ID|--offline) [--confirm]
	codis-admin [-v] --codis-topom=ADDR	[--auth=AUTH]	--product=NAME		[--product-auth=AUTH]		--slots-status
	codis-admin [-v] --codis-topom=ADDR	[--auth=AUTH]	--product=NAME		[--product-auth=AUTH]		--list-proxy
	codis-admin [-v] --codis-topom=ADDR	[--auth=AUTH]	--product=NAME		[--product-auth=AUTH]		--create-proxy   --addr=ADDR
	codis-admin [-v] --codis-topom=ADDR	[--auth=AUTH]	--product=NAME		[--product-auth=AUTH]		--online-proxy   --addr=ADDR
	codis-admin [-v] --codis-topom=ADDR	[--auth=AUTH]	--product=NAME		[--product-auth=AUTH]		--remove-proxy  (--addr=ADDR|--token=TOKEN|--pid=ID)       [--force]
	codis-admin [-v] --codis-topom=ADDR	[--auth=AUTH]	--product=NAME		[--product-auth=AUTH]		--reinit-proxy  (--addr=ADDR|--token=TOKEN|--pid=ID|--all) [--force]
	codis-admin [-v] --codis-topom=ADDR	[--auth=AUTH]	--product=NAME		[--product-auth=AUTH]		--proxy-status
	codis-admin [-v] --codis-topom=ADDR	[--auth=AUTH]	--product=NAME		[--product-auth=AUTH]		--list-group
	codis-admin [-v] --codis-topom=ADDR	[--auth=AUTH]	--product=NAME		[--product-auth=AUTH]		--create-group   --gid=ID
	codis-admin [-v] --codis-topom=ADDR	[--auth=AUTH]	--product=NAME		[--product-auth=AUTH]		--remove-group   --gid=ID
	codis-admin [-v] --codis-topom=ADDR	[--auth=AUTH]	--product=NAME		[--product-auth=AUTH]		--resync-group  [--gid=ID | --all]
	codis-admin [-v] --codis-topom=ADDR	[--auth=AUTH]	--product=NAME		[--product-auth=AUTH]		--group-add      --gid=ID --addr=ADDR [--datacenter=DATACENTER]
	codis-admin [-v] --codis-topom=ADDR	[--auth=AUTH]	--product=NAME		[--product-auth=AUTH]		--group-del      --gid=ID --addr=ADDR
	codis-admin [-v] --codis-topom=ADDR	[--auth=AUTH]	--product=NAME		[--product-auth=AUTH]		--group-status
	codis-admin [-v] --codis-topom=ADDR	[--auth=AUTH]	--product=NAME		[--product-auth=AUTH]		--replica-groups --gid=ID --addr=ADDR (--enable|--disable)
	codis-admin [-v] --codis-topom=ADDR	[--auth=AUTH]	--product=NAME		[--product-auth=AUTH]		--promote-server --gid=ID --addr=ADDR
	codis-admin [-v] --codis-topom=ADDR	[--auth=AUTH]	--product=NAME		[--product-auth=AUTH]		--sync-action    --create --addr=ADDR
	codis-admin [-v] --codis-topom=ADDR	[--auth=AUTH]	--product=NAME		[--product-auth=AUTH]		--sync-action    --remove --addr=ADDR
	codis-admin [-v] --codis-topom=ADDR	[--auth=AUTH]	--product=NAME		[--product-auth=AUTH]		--slot-action    --create --sid=ID --gid=ID
	codis-admin [-v] --codis-topom=ADDR	[--auth=AUTH]	--product=NAME		[--product-auth=AUTH]		--slot-action    --remove --sid=ID
	codis-admin [-v] --codis-topom=ADDR	[--auth=AUTH]	--product=NAME		[--product-auth=AUTH]		--slot-action    --create-some  --gid-from=ID --gid-to=ID --num-slots=N
	codis-admin [-v] --codis-topom=ADDR	[--auth=AUTH]	--product=NAME		[--product-auth=AUTH]		--slot-action    --create-range --beg=ID --end=ID --gid=ID
	codis-admin [-v] --codis-topom=ADDR	[--auth=AUTH]	--product=NAME		[--product-auth=AUTH]		--slot-action    --interval=VALUE
	codis-admin [-v] --codis-topom=ADDR	[--auth=AUTH]	--product=NAME		[--product-auth=AUTH]		--slot-action    --disabled=VALUE
	codis-admin [-v] --codis-topom=ADDR	[--auth=AUTH]	--product=NAME		[--product-auth=AUTH]		--rebalance     [--confirm]
	codis-admin [-v] --codis-topom=ADDR	[--auth=AUTH]	--product=NAME		[--product-auth=AUTH]		--sentinel-add   --addr=ADDR
	codis-admin [-v] --codis-topom=ADDR	[--auth=AUTH]	--product=NAME		[--product-auth=AUTH]		--sentinel-del   --addr=ADDR [--force]
	codis-admin [-v] --codis-topom=ADDR	[--auth=AUTH]	--product=NAME		[--product-auth=AUTH]		--sentinel-resync
	codis-admin [-v] --config-dump	--product=NAME (--zookeeper=ADDR [--zookeeper-auth=USR:PWD]|--etcd=ADDR [--etcd-auth=USR:PWD]|--filesystem=ROOT) [-1]
	codis-admin [-v] --config-restore=FILE	--product=NAME (--zookeeper=ADDR [--zookeeper-auth=USR:PWD]|--etcd=ADDR [--etcd-auth=USR:PWD]|--filesystem=ROOT) [--confirm]
	codis-admin [-v] --config-convert=FILE
	codis-admin --version

Options:
	-a AUTH, --auth=AUTH
	-x ADDR, --addr=ADDR
	-t TOKEN, --token=TOKEN
	-g ID, --gid=ID
`

	d, err := docopt.Parse(usage, nil, true, "", false)
	if err != nil {
		log.PanicError(err, "parse arguments failed")
	}
	log.SetLevel(log.LevelInfo)

	if d["-v"].(bool) {
		log.SetLevel(log.LevelDebug)
	}

	switch {
	case d["--proxy"] != nil:
		new(cmdProxy).Main(d)
	case d["--codis-topom"] != nil:
		new(cmdTopom).Main(d)
	case d["--version"].(bool):
		fmt.Println("version:", utils.Version)
		fmt.Println("compile:", utils.Compile)
		return

	default:
		new(cmdAdmin).Main(d)
	}
}
