# Troubleshooting

## Give the podman machine enough memory to run the stack

If `podman machine inspect --format "{{.Resources.Memory}}"` is not at least 12 GB, then stop the machine, remove it, and re-initialise it with more memory using the following commands:

```shell
podman machine stop
podman machine rm
podman machine init --memory 12288 # 12 GB example
podman machine start
```
