# Kubernetes Notes

These manifests are a baseline, local-cluster deployment path. They expect locally built images named:

- `social-feed/user-service:local`
- `social-feed/post-service:local`
- `social-feed/feed-service:local`
- `social-feed/notification-service:local`
- `social-feed/healing-agent:local`

Example:

```bash
docker build -t social-feed/user-service:local services/user-service
docker build -t social-feed/post-service:local services/post-service
docker build -t social-feed/feed-service:local services/feed-service
docker build -t social-feed/notification-service:local services/notification-service
docker build -t social-feed/healing-agent:local services/healing-agent

kubectl apply -f infra/k8s/namespace.yaml
kubectl apply -f infra/k8s/config.yaml
kubectl apply -f infra/k8s/data.yaml
kubectl apply -f infra/k8s/apps.yaml
```

The Docker Compose path is the primary MVP runtime. The Kubernetes path is ready for extension with persistent volumes, Prometheus Operator, HPA, and a Kubernetes-native healing executor.

