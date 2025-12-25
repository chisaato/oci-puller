# OCI Puller

一个让你 Docker Pull 更快的加速器。

## 简介

OCI Puller 是一个高性能的 Docker 镜像拉取代理。它通过将 Docker 客户端的单线程下载请求转换为后端的多线程并行下载，并结合磁盘缓存和“边下边播”技术，显著提升了镜像拉取速度，特别是在网络环境受限或跨地域拉取时。

## 核心能力

- **多线程拉取**: 突破单线程限制，充分利用带宽。
- **智能缓存**: 避免重复下载，支持自动清理。
- **流式响应**: 无需等待全部下载完成即可开始向客户端返回数据。
- **多源负载均衡**: 自由配置镜像源，支持高可用。
- **自定义 Registry**: 不限制可以代理的 Registry,用户自行添加
- **自定义域名**: 访问域名不做限制,用户自行选择反向代理

## 配置指南

> 这个工具强烈建议在局域网/本地环境中使用

配置文件部分参阅 [配置文件](config.md)

强烈推荐使用 Docker Compose 部署

```yaml
services:
  oci-puller:
    image: ghcr.io/chisaato/oci-puller:main
    restart: always
    environment:
      - LOG_LEVEL=debug
    volumes:
      - ./config.yaml:/config.yaml:ro
      - ./data:/data
    ports:
      - "9800:9800"
```

然后下面说说反向代理. 核心思路就是控制发送到 `oci-puller` 的 `Host` 头. 下面给出一些常见反向代理的配置片段.

而且 Docker Registry 要求默认 TLS 访问,所以你得自己解决一下证书问题,当然配合 ACME 这很简单,这里就不展开了.

最后就是解析,既然是本地环境那各位选取自己喜欢的方法就行.

> 你问我多个镜像站怎么办? 那不就是设置多个域名绑定的事

### Nginx

```nginx
server {
    listen 443 ssl;
    server_name docker.example.com;

    ssl_certificate /path/to/cert.pem;
    ssl_certificate_key /path/to/key.pem;

    location / {
        proxy_pass http://127.0.0.1:8080;
        # 或者是在容器内使用域名访问
        # proxy_pass http://oci-puller:8080;
        proxy_set_header Host $host;
    }
}
```

### Caddy

```caddy
docker.example.com {
    reverse_proxy 127.0.0.1:8080 {
        header_up Host {host}
    }
    # 或者是在容器内使用域名访问
    # reverse_proxy oci-puller:8080 {
    #     header_up Host {host}
    # }
}
```

### Traefik

```yaml
http:
  routers:
    oci-puller:
      rule: Host(`docker.example.com`)
      service: oci-puller
      tls: {}

  services:
    oci-puller:
      loadBalancer:
        servers:
          # 既然你都用 Traefik 我假定你大概率是在容器里跑的
          - url: "http://oci-puller:8080"
```

## 使用方法

### Docker

Docker 可以配置多个镜像站,可以将 `oci-puller` 提供的地址作为第一位,万一失效了还能继续尝试其他站点

```json
{
	"registry-mirrors": ["https://docker.example.com", "https://xxxmirror.com"]
}
```

## 开源协议

本项目采用 [GPLv3](LICENSE) 协议开源。
