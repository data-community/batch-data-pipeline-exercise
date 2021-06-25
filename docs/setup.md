# 批处理管道课前准备

在本次练习中，需要使用Python、Airflow、PostgreSQL、docker等完成练习。因此需要学员提前配置好开发环境。

主要包括：
* Docker 和 docker-compose。
* IDE 或者 代码编辑器。比如 Visual Studio Code。
* PostgreSQL 的 GUI 客户端。比如 Visual Studio Code 就有相应插件。
* Python。可选的，因为所有的代码都在docker里边运行。但如果想尝试某种 Python 语法，建议本地安装。
* 拉取练习所需的docker镜像。

## Docker 和 docker-compose

可以从[官网](https://docs.docker.com/docker-for-mac/install/)下载安装，或者使用homebrew安装

```
brew install docker
```

然后从 Launchpad 里边点击 Docker 图标打开。在 Docker 里的 Preferences 里调整内存大小。建议 6GB 以上。

安装之后终端里即有`docker`和`docker-compose`命令。

## IDE 或者代码编辑器

比如可使用 Visual Studio Code。

```
brew install visual-studio-code
```

或者使用你的常用工具。

## PostgreSQL GUI 客户端

有一些开源的客户端，可自行搜索。但如果使用 Visual Studio Code，那么也可以安装插件来使用，比如 SQL Tool。或者使用你的常用工具。

## Python （可选）

由于需要安装软件包或者使用不同版本的Python，因此不建议直接使用系统自带的 Python。可使用一些运行时管理工具来管理不同版本，比如 [asdf](https://asdf-vm.com/)。

ASDF 举例：
* 按照[说明](https://asdf-vm.com/#/core-manage-asdf)安装 asdf
* 安装Python插件 `asdf plugin add python`
* 安装特定版本的 Python `asdf  install python 3.8.10`
* 启用该版本 `asdf global python 3.8.10`

## 拉取镜像

克隆本代码库，并执行 `docker-compose pull` 拉取镜像

```
git clone https://github.com/data-community/batch-data-pipeline-exercise
cd batch-data-pipeline-exercise
docker-compose pull
```
