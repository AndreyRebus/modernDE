#!/bin/bash

set -e

echo "🔧 Установка зависимостей..."
sudo apt update
sudo apt install -y \
    ca-certificates \
    curl \
    gnupg \
    lsb-release

echo "🔑 Добавление GPG-ключа Docker..."
sudo install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | \
    sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
sudo chmod a+r /etc/apt/keyrings/docker.gpg

echo "📦 Добавление Docker репозитория..."
echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] \
  https://download.docker.com/linux/ubuntu \
  $(lsb_release -cs) stable" | \
  sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

echo "📥 Установка Docker и компонентов..."
sudo apt update
sudo apt install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin

echo "🧩 Установка Docker Compose вручную (ARM64)..."
mkdir -p ~/.docker/cli-plugins
curl -SL https://github.com/docker/compose/releases/latest/download/docker-compose-linux-aarch64 \
  -o ~/.docker/cli-plugins/docker-compose
chmod +x ~/.docker/cli-plugins/docker-compose

echo "👤 Добавление текущего пользователя в группу docker..."
sudo usermod -aG docker $USER

echo "✅ Проверка установки:"
docker --version
docker compose version

echo ""
echo "🚀 Готово! Перезайди в терминал или выполни 'newgrp docker' для применения прав."
