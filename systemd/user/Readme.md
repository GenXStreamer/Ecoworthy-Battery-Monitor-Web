# Systemd unit files

These files should be placed in:

```
~/.config/systemd/user
```

for the user you intend to run them as.

---

## Setup

If the directory does not exist:

```bash
mkdir -p ~/.config/systemd/user
```

1. Copy the unit files into `~/.config/systemd/user`
2. Edit them and update any paths as required

Reload systemd:

```bash
systemctl --user daemon-reload
```

Enable and start the services:

```bash
systemctl --user enable ew-battery-poller@"MAC address" --now
systemctl --user enable ew-battery-web --now
```
Note: ew-battery-poller is mult-instance
eg.

```bash
systemctl --user enable enable ew-battery-poller@a5:c2:37:6d:9f:de --now
```
