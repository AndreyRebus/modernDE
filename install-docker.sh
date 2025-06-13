#!/bin/bash

set -e

echo "🧹 Удаление старого Docker..."
sudo apt remove -y docker docker-ce docker-ce-cli docker-compose-plugin containerd.io || true
sudo rm -rf /var/lib/docker /var/lib/containerd /etc/docker

echo "📦 Установка зависимостей..."
sudo apt update
sudo apt install -y ca-certificates curl gnupg lsb-release apt-transport-https

echo "⬇️ Скачивание стабильного Docker .deb-пакета..."
curl -fsSL https://download.docker.com/linux/ubuntu/dists/focal/pool/stable/amd64/docker-ce_24.0.9-1~ubuntu.20.04~focal_amd64.deb -o docker-ce.deb

echo "📥 Установка Docker .deb..."
sudo apt install -y ./docker-ce.deb
rm docker-ce.deb

echo "🔌 Установка Docker Compose как CLI-плагина..."
sudo mkdir -p /usr/libexec/docker/cli-plugins
curl -SL https://github.com/docker/compose/releases/latest/download/docker-compose-linux-x86_64 \
  -o /usr/libexec/docker/cli-plugins/docker-compose
sudo chmod +x /usr/libexec/docker/cli-plugins/docker-compose

echo "👤 Добавление текущего пользователя в группу docker..."
sudo usermod -aG docker "$USER"

echo "✅ Проверка установки:"
docker version
docker compose version

echo ""
echo "🚀 Установка завершена. Перезапусти терминал или выполни 'newgrp docker'."
