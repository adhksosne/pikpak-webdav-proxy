#!/bin/bash
# 多平台交叉编译（CGO_ENABLED=0 纯静态，无运行时依赖）
set -e

NAME=pikpak-proxy

platforms=(
  "windows/amd64"
  "linux/amd64"
  "linux/arm64"
  "linux/arm"
  "darwin/amd64"
  "darwin/arm64"
)

for p in "${platforms[@]}"; do
  GOOS=${p%/*}
  GOARCH=${p#*/}
  ext=""
  [ "$GOOS" = "windows" ] && ext=".exe"
  CGO_ENABLED=0 GOOS=$GOOS GOARCH=$GOARCH go build -trimpath -ldflags "-s -w" -o "${NAME}-${GOOS}-${GOARCH}${ext}" .
  echo "built ${NAME}-${GOOS}-${GOARCH}${ext}"
done
