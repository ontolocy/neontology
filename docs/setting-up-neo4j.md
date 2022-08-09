# Setting up Neo4j

In the first instance, consult the Neo4j [website](https://neo4j.com) for all the different ways to set up Neo4j.

## Using Aura

The easiest way to get started with Neo4j is to use their database-as-a-service offering: [aura](https://neo4j.com/cloud/platform/aura-graph-database/).

When you create an instance, you will get a connection URI, username and password - everything you need to start using neontology to populate it with data using Python.

## Using Neo4j Desktop and WSL

One setup is to use Neo4j Desktop to host your database and then to have your development environment running in Windows Subsystem for Linux (WSL).

If this is the case, you can find the IP address to use with `cat /etc/resolv.conf`.

You will need to edit the settings/config for your database to have it bind to 0.0.0.0 (all local IPs) or the specific local IP you want to connect on. You might also need to make sure this is allowed through the firewall. For example, you can try the following powershell command to enable communication from WSL to Windows and Neo4j Desktop.

```powershell
New-NetFirewallRule -DisplayName "WSL" -Direction Inbound  -InterfaceAlias "vEthernet (WSL)"  -Action Allow
```

More on [stackoverflow](https://superuser.com/questions/1535269/how-to-connect-wsl-to-a-windows-localhost)

## Using Docker

You can easily run the Community Edition of Neo4j free locally with [Docker](https://neo4j.com/developer/docker/).
