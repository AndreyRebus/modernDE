#!/bin/bash

set -e

echo "🧹 Удаление старого Docker (если был)..."
sudo apt remove -y docker docker-engine docker.io containerd runc docker-ce docker-ce-cli docker-compose-plugin || true
sudo rm -rf /var/lib/docker /var/lib/containerd /etc/docker || true

echo "📦 Установка зависимостей..."
sudo apt update
sudo apt install -y ca-certificates curl gnupg lsb-release apt-transport-https

echo "🔐 Добавление GPG ключа Docker..."
sudo install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg \
  | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
sudo chmod a+r /etc/apt/keyrings/docker.gpg

echo "📝 Добавление Docker репозитория..."
echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] \
  https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable" \
  | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

echo "📥 Установка Docker Engine и Compose..."
sudo apt update
sudo apt install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin

echo "👤 Добавление пользователя '$USER' в группу docker..."
sudo usermod -aG docker "$USER"

echo "✅ Проверка установки:"
docker --version
docker compose version

echo ""
echo "🚀 Установка завершена. Перезапусти терминал или выполни 'newgrp docker' для применения группы."
