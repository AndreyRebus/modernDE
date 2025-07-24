#!/usr/bin/env bash
set -euo pipefail
cd ./etc
# ───── Устанавливаем только нужные утилиты ─────
echo "==> Installing keytool & htpasswd (openjdk + apache2-utils)…"
sudo apt-get update -qq
sudo apt-get install -y -qq openjdk-17-jre-headless apache2-utils

# ───── Сбор данных от пользователя ─────
read -rp "Enter Trino admin username: " USERNAME
while [[ -z "$USERNAME" ]]; do read -rp "Username cannot be empty, try again: " USERNAME; done

read -srp "Enter password for ${USERNAME}: " PASSWORD; echo
read -srp "Repeat password: " PASSWORD2; echo
[[ "$PASSWORD" == "$PASSWORD2" ]] || { echo "Passwords do not match!"; exit 1; }

# ───── Пути и константы ─────
KEYSTORE="keystore.p12"
KEYPASS="changeit"                     # пароль хранилища (можно изменить)
AUTH_PROPS="password-authenticator.properties"
PASS_DB="password.db"

# ───── Генерируем TLS-хранилище (10 лет) ─────
echo "==> Generating ${KEYSTORE} self-signed TLS cert…"
keytool -genkeypair \
        -alias trino \
        -storetype PKCS12 \
        -keystore "${KEYSTORE}" \
        -dname "CN=trino" \
        -keyalg RSA -keysize 2048 \
        -validity 3650 \
        -storepass "${KEYPASS}" \
        -keypass "${KEYPASS}" \
        >/dev/null 2>&1

# ───── Создаём password-authenticator.properties ─────
cat > "${AUTH_PROPS}" <<EOF
password-authenticator.name=file
file.password-file=/etc/trino/password.db
EOF
echo "==> Created ${AUTH_PROPS}"

# ───── Формируем bcrypt-файл паролей ─────
htpasswd -B -C 10 -b -c "${PASS_DB}" "${USERNAME}" "${PASSWORD}"
echo "==> Created ${PASS_DB} (bcrypt cost 10)"

echo
echo "===== DONE ====="
echo "Файлы готовы в $(pwd):"
ls -1 "${KEYSTORE}" "${AUTH_PROPS}" "${PASS_DB}"
echo
echo "• Монтируйте этот каталог в контейнер по /etc/trino."
echo "• В config.properties укажите:"
echo "    http-server.https.keystore.path=/etc/trino/${KEYSTORE}"
echo "    http-server.https.keystore.key=${KEYPASS}"
echo "    http-server.authentication.type=PASSWORD"

CONFIG_FILE="config.properties"

cat > "${CONFIG_FILE}" <<EOF
###############################################################################
#  КОРОТКИЙ MINIMAL CONFIG ДЛЯ ОДНОГО КОНТЕЙНЕРА-КООРДИНАТОРА
###############################################################################
coordinator=true
node-scheduler.include-coordinator=true

# Discovery-URI лучше указывать по HTTPS:
discovery.uri=https://localhost:8443

####################  HTTPS / TLS  ###########################################
http-server.https.enabled=true
http-server.https.port=8443
http-server.https.keystore.path=/etc/trino/keystore.p12
http-server.https.keystore.key=${KEYPASS}  

##################  АУТЕНТИФИКАЦИЯ  ###########################################
http-server.authentication.type=PASSWORD
http-server.authentication.allow-insecure-over-http=false  

####################  CLUSTER SECRET  #########################################
internal-communication.shared-secret=$(openssl rand -hex 16)
EOF

echo "==> Created ${CONFIG_FILE}"