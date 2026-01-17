# AGENTS.md - OCI Puller 开发指南

本文档为在 OCI Puller 项目上工作的代理编码助手提供全面指南。OCI Puller 是一个高性能的 Docker 镜像拉取加速器，使用多线程下载、智能缓存和流式传输来加速容器镜像检索。

## 项目概述

OCI Puller 使用 Go 语言编写，作为代理服务器拦截 Docker 客户端请求，缓存镜像层，并使用并行下载来加速镜像拉取。该项目遵循标准 Go 约定，并包含中文文档。

## 构建、检查和测试命令

### 构建
```bash
# 构建主二进制文件
go build .

# 使用 Docker 构建（生产环境）
docker build -t oci-puller .

# 跨平台构建（如需要）
CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -ldflags="-s -w -extldflags '-static'" .
```

### 测试
```bash
# 运行所有测试
go test ./...

# 运行测试并显示详细信息
go test -v ./...

# 运行特定测试
go test -run TestName ./pkg/downloader

# 运行测试并检测竞态条件
go test -race ./...

# 运行测试并生成覆盖率报告
go test -cover ./...
go test -coverprofile=coverage.out ./...
go tool cover -html=coverage.out
```

### 检查和代码质量
```bash
# 格式化代码（仅检查）
gofmt -l .

# 格式化代码（应用更改）
gofmt -w .

# 检查潜在问题
go vet ./...

# 运行 golangci-lint（如果已安装）
golangci-lint run

# 整理模块依赖
go mod tidy
```

### 运行应用程序
```bash
# 使用默认配置启动服务器
./oci-puller server

# 使用自定义配置启动服务器
./oci-puller server -c /path/to/config.yaml

# 显示帮助信息
./oci-puller --help
```

## 代码风格指南

### 通用 Go 约定
- 遵循标准 Go 格式化（`gofmt`）
- 使用 `goimports` 组织导入
- 最大行长度：120 个字符（软限制）
- 使用有意义的变量和函数名称
- 为导出的函数、类型和方法添加注释文档

### 导入组织
```go
import (
    // 标准库导入
    "context"
    "fmt"
    "net/http"
    "os"

    // 第三方导入
    "github.com/spf13/cobra"
    "github.com/spf13/viper"

    // 项目导入（使用完整模块路径）
    "oci-puller/pkg/config"
    "oci-puller/pkg/logger"
)
```

### 命名约定
- **包名**：小写，单个单词（例如：`config`、`downloader`、`coordinator`）
- **类型**：PascalCase（导出的）或 camelCase（未导出的）
- **函数/方法**：PascalCase（导出的）或 camelCase（未导出的）
- **变量**：camelCase
- **常量**：PascalCase 或 ALL_CAPS 并带下划线
- **文件**：snake_case.go（例如：`retry_test.go`、`downloader.go`）

### 错误处理
```go
// 良好：显式错误检查
if err := someOperation(); err != nil {
    return fmt.Errorf("failed to perform operation: %w", err)
}

// 良好：处理特定错误类型
if errors.Is(err, io.EOF) {
    // 专门处理 EOF
    return nil
}

// 避免：无理由忽略错误
// 错误：_ = someOperation()
```

### 日志记录
- 使用结构化日志 `logger.S`（基于 zap）
- 在日志消息中包含相关上下文
- 使用适当的日志级别：Debug、Info、Warn、Error、Fatal

```go
logger.S.Debugw("processing request", "method", r.Method, "path", r.URL.Path)
logger.S.Infow("download completed", "digest", digest, "size", size)
logger.S.Errorw("operation failed", "error", err, "component", "downloader")
```

### 配置
- 使用 Viper 进行配置管理
- 使用适当的 `mapstructure` 标签定义配置结构体
- 提供合理的默认值
- 加载时验证配置

### HTTP 处理
- 使用标准的 `net/http` 模式
- 设置适当的头部和状态码
- 正确处理请求上下文
- 为外部请求使用超时

### 并发
- 适当使用 goroutine 和通道
- 优先使用 `sync.WaitGroup`、`errgroup` 或 `sync.Cond` 而非原始通道
- 使用 `context.Context` 进行取消
- 使用适当的互斥锁保护共享状态

### 测试
- 适当编写表驱动测试
- 使用描述性测试名称：`TestFunctionName_Scenario_Result`
- 模拟外部依赖（HTTP 服务器、文件系统）
- 清理测试产物
- 在测试设置中初始化所需依赖（配置、日志记录器）

```go
func TestDownloader_Retry(t *testing.T) {
    // 设置测试依赖
    logger.Init("debug")

    // 测试实现
    // ...

    // 清理
    defer os.Remove(tmpFile.Name())
}
```

## 项目结构

```
oci-puller/
├── cmd/                    # CLI 命令
│   ├── root.go            # 根命令
│   ├── server.go          # 服务器命令
│   └── manual-pull/       # 手动拉取工具
├── pkg/                   # 核心包
│   ├── config/           # 配置管理
│   ├── coordinator/      # 下载协调
│   ├── downloader/       # 多线程下载器
│   ├── interval/         # 区间管理
│   ├── logger/           # 日志工具
│   ├── manager/          # 下载管理
│   └── storage/          # 存储抽象
├── main.go               # 应用程序入口
├── go.mod                # Go 模块定义
├── Dockerfile            # 容器构建
├── docker-compose.yml    # 开发环境设置
├── config.yaml           # 默认配置
└── README.md             # 项目文档
```

## 依赖和库

### 核心依赖
- `github.com/spf13/cobra` - CLI 框架
- `github.com/spf13/viper` - 配置管理
- `go.uber.org/zap` - 结构化日志
- `go.etcd.io/bbolt` - 嵌入式键值数据库
- `golang.org/x/sync` - 同步工具

### 开发依赖
- 标准 Go 测试工具
- `golangci-lint`（推荐用于全面检查）

## 安全考虑

- 验证所有输入数据（URL、摘要、头部）
- 对外部通信使用 HTTPS
- 避免记录敏感信息
- 实现适当的超时和限制
- 验证文件路径并防止目录遍历

## 性能指南

- 对大文件使用流式 I/O
- 实现适当的连接池
- 缓存频繁访问的数据
- 使用高效的数据结构（位集、区间树）
- 对性能关键代码进行分析

## Git 工作流程

- 使用描述性的提交消息
- 尽可能遵循约定式提交格式
- 为新工作创建特性分支
- 为新功能编写测试
- 合并前确保 CI 通过

## 常见模式

### 带超时的 HTTP 客户端
```go
client := &http.Client{
    Timeout: 30 * time.Second,
}
```

### 上下文使用
```go
ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
defer cancel()

// 在操作中使用 ctx
```

### 文件操作
```go
file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
if err != nil {
    return err
}
defer file.Close()
```

### 错误包装
```go
return fmt.Errorf("failed to download chunk %d: %w", chunkIndex, err)
```

## 故障排除

### 常见问题
1. **由于配置导致的测试失败**：确保在测试中初始化 `config.GlobalConfig`
2. **网络超时**：检查代理设置和防火墙规则
3. **权限错误**：验证文件和目录权限
4. **内存问题**：监控 goroutine 泄漏和文件描述符使用情况

### 调试模式
在配置或环境中将日志级别设置为 "debug"：
```bash
export LOG_LEVEL=debug
```

本文档应随着项目发展而更新。如有疑问或澄清，请参考现有代码库和文档。

## 要求

所有代理编码助手的回答必须使用中文。代码注释和文档应使用中文。所有用户交互、错误消息和日志应使用中文。</content>
<parameter name="filePath">/data/SourceCode/oci-puller/AGENTS.md