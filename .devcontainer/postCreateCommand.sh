#!/bin/bash
set -x

sed -i 's/ZSH_THEME="robbyrussell"/ZSH_THEME="xiong-chiamiov-plus"/' /root/.zshrc \
&& sed -i '/HIST_STAMPS="mm\/dd\/yyyy"/s/^#*//;s/mm\/dd\/yyyy/yyyy-mm-dd/' /root/.zshrc

cp /root/.gitconfig_mounted /root/.gitconfig \
&& cp -r /root/.ssh_mounted/* /root/.ssh/ \
&& ls /root/.ssh/* | grep -v pub | xargs chmod 400

chmod 755 /root/.ssh/config
chmod 755 /root/.ssh/known_hosts

git config --global --add safe.directory /workspaces/*

zsh


# set -x

# # 修改 Zsh 設定
# if [ -f "/root/.zshrc" ]; then
#     sed -i 's/ZSH_THEME="robbyrussell"/ZSH_THEME="xiong-chiamiov-plus"/' /root/.zshrc
#     sed -i '/HIST_STAMPS="mm\/dd\/yyyy"/s/^#*//;s/mm\/dd\/yyyy/yyyy-mm-dd/' /root/.zshrc
# fi

# # 檢查並複製 git 設定
# if [ -f "/root/.gitconfig_mounted" ]; then
#     cp /root/.gitconfig_mounted /root/.gitconfig
# fi

# # 檢查並複製 SSH 設定
# if [ -d "/root/.ssh_mounted" ]; then
#     cp -r /root/.ssh_mounted/* /root/.ssh/
# fi

# # 設定 SSH 權限
# if [ -d "/root/.ssh" ]; then
#     ls /root/.ssh/* 2>/dev/null | grep -v pub | xargs chmod 400
# fi

# if [ -f "/root/.ssh/config" ]; then
#     chmod 755 /root/.ssh/config
# fi

# if [ -f "/root/.ssh/known_hosts" ]; then
#     chmod 755 /root/.ssh/known_hosts
# fi

# # 設定 Git
# git config --global --add safe.directory /workspaces/*

# # 確保 zsh 存在
# if ! command -v zsh &> /dev/null; then
#     echo "zsh is not installed, installing..."
#     apt-get update && apt-get install -y zsh
# fi

# exec zsh
