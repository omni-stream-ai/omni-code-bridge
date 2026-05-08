#!/usr/bin/env bash
set -euo pipefail

pkgver="${1:?version required}"
repo="${2:-omni-stream-ai/omni-code-bridge}"
license_sha="${3:?license sha required}"
linux_x86_sha="${4:?linux x86 sha required}"
linux_arm_sha="${5:?linux arm sha required}"
output_dir="${6:-aur-bin}"

mkdir -p "$output_dir"

cat > "$output_dir/PKGBUILD" <<EOF
# Maintainer: Junjie <junjie@omni-stream.ai>
pkgname=omni-code-bridge-bin
pkgver=${pkgver}
pkgrel=1
pkgdesc="Rust bridge for Omni Code — connects mobile clients to local coding agents"
arch=('x86_64' 'aarch64')
url="https://github.com/${repo}"
license=('MIT')
depends=('gcc-libs')
provides=('omni-code-bridge')
conflicts=('omni-code-bridge')
source=("omni-code-bridge.service"
        "LICENSE-\$pkgver::https://raw.githubusercontent.com/${repo}/v\${pkgver}/LICENSE")
source_x86_64=("omni-code-bridge-\$pkgver-linux-x64.tar.gz::https://github.com/${repo}/releases/download/v\${pkgver}/omni-code-bridge-linux-x64.tar.gz")
source_aarch64=("omni-code-bridge-\$pkgver-linux-arm64.tar.gz::https://github.com/${repo}/releases/download/v\${pkgver}/omni-code-bridge-linux-arm64.tar.gz")
sha256sums=('SKIP'
            '${license_sha}')
sha256sums_x86_64=('${linux_x86_sha}')
sha256sums_aarch64=('${linux_arm_sha}')

package() {
    local asset_dir
    case "\$CARCH" in
        x86_64) asset_dir="omni-code-bridge-linux-x64" ;;
        aarch64) asset_dir="omni-code-bridge-linux-arm64" ;;
        *) echo "Unsupported architecture: \$CARCH" >&2; return 1 ;;
    esac

    install -Dm755 "\$asset_dir/omni-code-bridge" "\$pkgdir/usr/bin/omni-code-bridge"
    install -Dm644 "\$srcdir/LICENSE-\$pkgver" "\$pkgdir/usr/share/licenses/\$pkgname/LICENSE"
    install -Dm644 "\$srcdir/omni-code-bridge.service" "\$pkgdir/usr/lib/systemd/user/omni-code-bridge.service"
}
EOF

cat > "$output_dir/omni-code-bridge.service" <<'EOF'
[Unit]
Description=Omni Code Bridge
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
ExecStart=/usr/bin/omni-code-bridge serve
Restart=on-failure
RestartSec=3
Environment=RUST_LOG=info

[Install]
WantedBy=default.target
EOF

cat > "$output_dir/.SRCINFO" <<EOF
pkgbase = omni-code-bridge-bin
	pkgdesc = Rust bridge for Omni Code — connects mobile clients to local coding agents
	pkgver = ${pkgver}
	pkgrel = 1
	url = https://github.com/${repo}
	arch = x86_64
	arch = aarch64
	license = MIT
	depends = gcc-libs
	provides = omni-code-bridge
	conflicts = omni-code-bridge
	source = omni-code-bridge.service
	source = LICENSE-${pkgver}::https://raw.githubusercontent.com/${repo}/v${pkgver}/LICENSE
	sha256sums = SKIP
	sha256sums = ${license_sha}
	source_x86_64 = omni-code-bridge-${pkgver}-linux-x64.tar.gz::https://github.com/${repo}/releases/download/v${pkgver}/omni-code-bridge-linux-x64.tar.gz
	sha256sums_x86_64 = ${linux_x86_sha}
	source_aarch64 = omni-code-bridge-${pkgver}-linux-arm64.tar.gz::https://github.com/${repo}/releases/download/v${pkgver}/omni-code-bridge-linux-arm64.tar.gz
	sha256sums_aarch64 = ${linux_arm_sha}

pkgname = omni-code-bridge-bin
EOF
