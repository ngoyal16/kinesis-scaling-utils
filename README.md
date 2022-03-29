# kinesis-scaling-utils

Kinesis autoscaling utils will use shards metrics and findout cold and hot shards and merge and split based on the
results.

### Docker run

```shell
docker run \
  public.ecr.aws/pixelvide/aws/kinesis-scaling-utils/debug:9080d4e4483f62ea3d57d0cef473ed52835b0851 \
  -v config.json:/usr/src/node-app/config.json
```

#### Ref

1. https://github.com/awslabs/amazon-kinesis-scaling-utils
